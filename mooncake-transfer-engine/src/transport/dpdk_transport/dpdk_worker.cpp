// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "dpdk_worker.h"

#include <glog/logging.h>
#include <netinet/in.h>
#include <pthread.h>
#include <rte_byteorder.h>
#include <rte_cycles.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_lcore.h>
#include <rte_udp.h>
#include <sched.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <mutex>

#include "dpdk_mem.h"

namespace mooncake {
namespace dpdk {

namespace {

std::atomic<unsigned> g_ring_serial{0};

#ifdef MOONCAKE_DPDK_TRANSPORT_TEST_HOOKS
std::atomic<uint32_t> g_drop_data_percent{0};
std::atomic<uint32_t> g_drop_ack_percent{0};
std::atomic<uint64_t> g_injected_drops{0};
std::atomic<uint64_t> g_retransmit_baseline{0};
std::mutex g_workers_mutex;
std::vector<Worker *> g_workers;

uint64_t sumRetransmits() {
    std::lock_guard<std::mutex> lock(g_workers_mutex);
    uint64_t total = 0;
    for (Worker *w : g_workers)
        total += w->stats().retransmits.load(std::memory_order_relaxed);
    return total;
}
#endif

constexpr uint16_t kL3Offset = sizeof(struct rte_ether_hdr);
constexpr uint16_t kL4Offset = kL3Offset + sizeof(struct rte_ipv4_hdr);
constexpr uint16_t kMktpOffset = kL4Offset + sizeof(struct rte_udp_hdr);
static_assert(kMktpOffset == mktp::kL2L3L4Bytes, "header layout");

}  // namespace

Worker::Worker(unsigned id, const Port &port, const Options &options,
               mktp::Callbacks callbacks)
    : id_(id),
      port_(port),
      options_(options),
      callbacks_(std::move(callbacks)) {
    endpoint_ = std::make_unique<mktp::Endpoint>(options_.mktp, this,
                                                 callbacks_, &mktp_stats_);
    const unsigned serial = g_ring_serial.fetch_add(1);
    const int socket = port_.socket_id < 0 ? SOCKET_ID_ANY : port_.socket_id;
    std::string name = "mc_dpdk_sub_" + std::to_string(serial);
    submit_ring_ = rte_ring_create(name.c_str(), 8192, socket, RING_F_SC_DEQ);
    name = "mc_dpdk_fwd_" + std::to_string(serial);
    fwd_ring_ = rte_ring_create(name.c_str(), 4096, socket, RING_F_SC_DEQ);
    cycles_per_us_ = static_cast<double>(rte_get_tsc_hz()) / 1e6;
    rng_state_ ^= (uint64_t(id) + 1) * 0xD1B54A32D192ED03ull;
    std::memset(tx_buf_, 0, sizeof(tx_buf_));
}

Worker::~Worker() {
    stop();
    if (submit_ring_) rte_ring_free(submit_ring_);
    if (fwd_ring_) rte_ring_free(fwd_ring_);
}

int Worker::start() {
    if (!submit_ring_ || !fwd_ring_) {
        LOG(ERROR) << "DpdkTransport: cannot create worker rings: "
                   << rte_strerror(rte_errno);
        return -1;
    }
    running_ = true;
    thread_ = std::thread(&Worker::run, this);
#ifdef MOONCAKE_DPDK_TRANSPORT_TEST_HOOKS
    std::lock_guard<std::mutex> lock(g_workers_mutex);
    g_workers.push_back(this);
#endif
    return 0;
}

void Worker::stop() {
    if (running_.exchange(false) && thread_.joinable()) {
        thread_.join();
        LOG(INFO) << "DpdkTransport: worker " << id_ << " stopped: rx "
                  << stats_.rx_packets << ", tx " << stats_.tx_packets
                  << ", tx stalls " << stats_.tx_stalls << ", bad rx "
                  << stats_.rx_bad << ", forwarded " << stats_.forwarded
                  << ", transfers " << mktp_stats_.completed << " ok / "
                  << mktp_stats_.failed << " failed, retransmits "
                  << mktp_stats_.retransmits << ", rto events "
                  << mktp_stats_.rto_events << ", rejected "
                  << mktp_stats_.rejected;
    }
    if (endpoint_) endpoint_->shutdown();
    for (unsigned i = 0; i < tx_count_; ++i) rte_pktmbuf_free(tx_buf_[i]);
    tx_count_ = 0;
    void *item;
    while (submit_ring_ && rte_ring_dequeue(submit_ring_, &item) == 0)
        static_cast<Transport::Slice *>(item)->markFailed();
    while (fwd_ring_ && rte_ring_dequeue(fwd_ring_, &item) == 0)
        rte_pktmbuf_free(static_cast<struct rte_mbuf *>(item));
#ifdef MOONCAKE_DPDK_TRANSPORT_TEST_HOOKS
    std::lock_guard<std::mutex> lock(g_workers_mutex);
    g_workers.erase(std::remove(g_workers.begin(), g_workers.end(), this),
                    g_workers.end());
#endif
}

bool Worker::submit(Transport::Slice *slice) {
    return rte_ring_enqueue(submit_ring_, slice) == 0;
}

uint64_t Worker::nowUs() const {
    return static_cast<uint64_t>(rte_rdtsc() / cycles_per_us_);
}

void Worker::publishStats() {
    stats_.retransmits.store(mktp_stats_.retransmits,
                             std::memory_order_relaxed);
    stats_.nacks.store(mktp_stats_.nacks_sent, std::memory_order_relaxed);
}

void Worker::run() {
    if (rte_thread_register() != 0) {
        LOG(WARNING) << "DpdkTransport: rte_thread_register failed: "
                     << rte_strerror(rte_errno);
    }
    if (options_.cpu >= 0) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(options_.cpu, &set);
        if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set)) {
            PLOG(WARNING) << "DpdkTransport: cannot pin worker " << id_
                          << " to cpu " << options_.cpu;
        }
    }
    char name[16];
    std::snprintf(name, sizeof(name), "mc-dpdk-%u", id_);
    pthread_setname_np(pthread_self(), name);
    LOG(INFO) << "DpdkTransport: worker " << id_ << " polling " << port_.name
              << " queue " << options_.queue_id << " on udp port "
              << options_.udp_port << (options_.cpu >= 0 ? " (pinned)" : "");

    struct rte_mbuf *bufs[kBurst];
    void *items[32];
    uint64_t iterations = 0;
    uint64_t next_link_check_us = 0;
    int link_up = -1;
    while (running_.load(std::memory_order_relaxed)) {
        const uint64_t now = nowUs();
        if (now >= next_link_check_us) {
            next_link_check_us = now + 1000 * 1000;
            struct rte_eth_link link;
            if (rte_eth_link_get_nowait(port_.port_id, &link) == 0 &&
                link.link_status != link_up) {
                link_up = link.link_status;
                LOG(INFO) << "DpdkTransport: port " << port_.name << " link "
                          << (link_up ? "up" : "down");
            }
        }
        const uint16_t nrx =
            rte_eth_rx_burst(port_.port_id, options_.queue_id, bufs, kBurst);
        for (uint16_t i = 0; i < nrx; ++i) handleRx(bufs[i], now);
        const unsigned nfwd = rte_ring_dequeue_burst(
            fwd_ring_, reinterpret_cast<void **>(bufs), kBurst, nullptr);
        for (unsigned i = 0; i < nfwd; ++i) handleRx(bufs[i], now);
        const unsigned nsub =
            rte_ring_dequeue_burst(submit_ring_, items, 32, nullptr);
        for (unsigned i = 0; i < nsub; ++i)
            endpoint_->submit(static_cast<Transport::Slice *>(items[i]), now);
        const uint32_t sent = endpoint_->pump(now);
        flushTx();
        if ((++iterations & 63) == 0) publishStats();
        if (nrx + nfwd + nsub + sent == 0) rte_pause();
    }
    flushTx();
    publishStats();
    rte_thread_unregister();
}

// ---------------------------------------------------------------- receive

void Worker::handleRx(struct rte_mbuf *m, uint64_t now_us) {
    using namespace mktp;
    const uint32_t pkt_len = m->pkt_len;
    uint8_t hdrbuf[kWireOverhead];
    const uint8_t *p = nullptr;
    if (pkt_len >= kWireOverhead) {
        p = m->data_len >= kWireOverhead
                ? rte_pktmbuf_mtod(m, const uint8_t *)
                : static_cast<const uint8_t *>(
                      rte_pktmbuf_read(m, 0, kWireOverhead, hdrbuf));
    }
    if (!p) {
        stats_.rx_bad++;
        rte_pktmbuf_free(m);
        return;
    }
    const auto *eth = reinterpret_cast<const struct rte_ether_hdr *>(p);
    const auto *ip =
        reinterpret_cast<const struct rte_ipv4_hdr *>(p + kL3Offset);
    const auto *udp =
        reinterpret_cast<const struct rte_udp_hdr *>(p + kL4Offset);
    // Traffic that is not ours is normal on kernel-shared ports (AF_XDP,
    // bifurcated mlx5): drop it quietly.
    if (eth->ether_type != rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4) ||
        ip->version_ihl != 0x45 || ip->next_proto_id != IPPROTO_UDP ||
        (rte_be_to_cpu_16(ip->fragment_offset) & 0x3fff) != 0 ||
        rte_be_to_cpu_32(ip->dst_addr) != options_.ip) {
        rte_pktmbuf_free(m);
        return;
    }
    const uint16_t dport = rte_be_to_cpu_16(udp->dst_port);
    if (dport != options_.udp_port) {
        const int idx = int(dport) - int(options_.udp_port_base);
        if (idx >= 0 && static_cast<size_t>(idx) < siblings_.size() &&
            siblings_[idx] && siblings_[idx] != this &&
            rte_ring_enqueue(siblings_[idx]->fwdRing(), m) == 0) {
            stats_.forwarded++;
            return;
        }
        rte_pktmbuf_free(m);
        return;
    }
    const uint64_t l3 = m->ol_flags & RTE_MBUF_F_RX_IP_CKSUM_MASK;
    const uint64_t l4 = m->ol_flags & RTE_MBUF_F_RX_L4_CKSUM_MASK;
    bool bad =
        l3 == RTE_MBUF_F_RX_IP_CKSUM_BAD || l4 == RTE_MBUF_F_RX_L4_CKSUM_BAD;
    if (!bad && l3 != RTE_MBUF_F_RX_IP_CKSUM_GOOD &&
        rte_raw_cksum(ip, sizeof(*ip)) != 0xffff)
        bad = true;
    if (!bad && l4 != RTE_MBUF_F_RX_L4_CKSUM_GOOD && udp->dgram_cksum != 0 &&
        rte_ipv4_udptcp_cksum_mbuf_verify(m, ip, kL4Offset) != 0)
        bad = true;
    const uint32_t dgram_len = rte_be_to_cpu_16(udp->dgram_len);
    const auto *wire = reinterpret_cast<const Header *>(p + kMktpOffset);
    if (bad || dgram_len < sizeof(struct rte_udp_hdr) + kHeaderBytes ||
        kL4Offset + dgram_len > pkt_len ||
        wire->magic != rte_cpu_to_le_16(kMagic) || wire->version != kVersion) {
        stats_.rx_bad++;
        rte_pktmbuf_free(m);
        return;
    }
    Header hdr;
    hdr.magic = kMagic;
    hdr.version = kVersion;
    hdr.type = wire->type;
    hdr.flags = rte_le_to_cpu_16(wire->flags);
    hdr.session = rte_le_to_cpu_16(wire->session);
    hdr.batch = rte_le_to_cpu_16(wire->batch);
    hdr.reserved = 0;
    hdr.task = rte_le_to_cpu_32(wire->task);
    hdr.seq = rte_le_to_cpu_32(wire->seq);
    hdr.length = rte_le_to_cpu_32(wire->length);
    hdr.offset = rte_le_to_cpu_64(wire->offset);

    const uint32_t payload_bytes =
        dgram_len - sizeof(struct rte_udp_hdr) - kHeaderBytes;
    Payload payload;
    uint32_t skip = kWireOverhead, remaining = payload_bytes;
    for (struct rte_mbuf *seg = m; seg && remaining; seg = seg->next) {
        uint32_t len = seg->data_len;
        const uint8_t *data = rte_pktmbuf_mtod(seg, const uint8_t *);
        if (skip >= len) {
            skip -= len;
            continue;
        }
        data += skip;
        len -= skip;
        skip = 0;
        if (payload.nsegs == Payload::kMaxSegs) {
            // Unusually fragmented chain: linearize the rest.
            linear_.resize(payload_bytes);
            const void *lin = rte_pktmbuf_read(m, kWireOverhead, payload_bytes,
                                               linear_.data());
            payload.nsegs = 1;
            payload.segs[0] = {static_cast<const uint8_t *>(lin),
                               payload_bytes};
            payload.total = payload_bytes;
            remaining = 0;
            break;
        }
        const uint32_t take = std::min(len, remaining);
        payload.segs[payload.nsegs++] = {data, take};
        payload.total += take;
        remaining -= take;
    }
    if (payload.total != payload_bytes) {
        stats_.rx_bad++;
        rte_pktmbuf_free(m);
        return;
    }
    PeerAddr from;
    std::memcpy(from.mac, eth->src_addr.addr_bytes, sizeof(from.mac));
    from.ip = rte_be_to_cpu_32(ip->src_addr);
    from.udp_port = rte_be_to_cpu_16(udp->src_port);
    endpoint_->onPacket(from, hdr, payload, now_us);
    stats_.rx_packets++;
    rte_pktmbuf_free(m);
}

// ---------------------------------------------------------------- transmit

struct rte_mbuf *Worker::buildHeaders(const mktp::PeerAddr &to,
                                      const mktp::Header &hdr,
                                      uint32_t payload_len,
                                      struct rte_mempool *pool) {
    using namespace mktp;
    struct rte_mbuf *m = rte_pktmbuf_alloc(pool);
    if (!m) return nullptr;
    auto *p = reinterpret_cast<uint8_t *>(rte_pktmbuf_append(m, kWireOverhead));
    auto *eth = reinterpret_cast<struct rte_ether_hdr *>(p);
    std::memcpy(
        eth->dst_addr.addr_bytes,
        options_.use_gateway_mac ? options_.gateway_mac.addr_bytes : to.mac,
        RTE_ETHER_ADDR_LEN);
    rte_ether_addr_copy(&port_.mac, &eth->src_addr);
    eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

    auto *ip = reinterpret_cast<struct rte_ipv4_hdr *>(p + kL3Offset);
    ip->version_ihl = 0x45;
    ip->type_of_service = 0;
    ip->total_length = rte_cpu_to_be_16(static_cast<uint16_t>(
        sizeof(*ip) + sizeof(struct rte_udp_hdr) + kHeaderBytes + payload_len));
    ip->packet_id = 0;
    ip->fragment_offset = rte_cpu_to_be_16(RTE_IPV4_HDR_DF_FLAG);
    ip->time_to_live = 64;
    ip->next_proto_id = IPPROTO_UDP;
    ip->hdr_checksum = 0;
    ip->src_addr = rte_cpu_to_be_32(options_.ip);
    ip->dst_addr = rte_cpu_to_be_32(to.ip);

    auto *udp = reinterpret_cast<struct rte_udp_hdr *>(p + kL4Offset);
    udp->src_port = rte_cpu_to_be_16(options_.udp_port);
    udp->dst_port = rte_cpu_to_be_16(to.udp_port);
    udp->dgram_len = rte_cpu_to_be_16(
        static_cast<uint16_t>(sizeof(*udp) + kHeaderBytes + payload_len));
    udp->dgram_cksum = 0;

    auto *wire = reinterpret_cast<Header *>(p + kMktpOffset);
    wire->magic = rte_cpu_to_le_16(kMagic);
    wire->version = kVersion;
    wire->type = hdr.type;
    wire->flags = rte_cpu_to_le_16(hdr.flags);
    wire->session = rte_cpu_to_le_16(hdr.session);
    wire->batch = rte_cpu_to_le_16(hdr.batch);
    wire->reserved = 0;
    wire->task = rte_cpu_to_le_32(hdr.task);
    wire->seq = rte_cpu_to_le_32(hdr.seq);
    wire->length = rte_cpu_to_le_32(hdr.length);
    wire->offset = rte_cpu_to_le_64(hdr.offset);

    m->l2_len = sizeof(struct rte_ether_hdr);
    m->l3_len = sizeof(struct rte_ipv4_hdr);
    m->l4_len = sizeof(struct rte_udp_hdr);
    return m;
}

void Worker::finalizeChecksums(struct rte_mbuf *m) {
    auto *ip = rte_pktmbuf_mtod_offset(m, struct rte_ipv4_hdr *, kL3Offset);
    auto *udp = rte_pktmbuf_mtod_offset(m, struct rte_udp_hdr *, kL4Offset);
    if (port_.tx_ip_cksum) {
        m->ol_flags |= RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_IP_CKSUM;
    } else {
        ip->hdr_checksum = rte_ipv4_cksum(ip);
    }
    if (port_.tx_udp_cksum) {
        m->ol_flags |= RTE_MBUF_F_TX_IPV4 | RTE_MBUF_F_TX_UDP_CKSUM;
        udp->dgram_cksum = rte_ipv4_phdr_cksum(ip, m->ol_flags);
    } else {
        udp->dgram_cksum = m->nb_segs == 1
                               ? rte_ipv4_udptcp_cksum(ip, udp)
                               : rte_ipv4_udptcp_cksum_mbuf(m, ip, kL4Offset);
    }
}

bool Worker::sendCtrl(const mktp::PeerAddr &to, const mktp::Header &hdr,
                      const void *payload, uint32_t len) {
    struct rte_mbuf *m = buildHeaders(to, hdr, len, port_.ctrl_pool);
    if (!m) return false;
    if (len) {
        char *dst = rte_pktmbuf_append(m, static_cast<uint16_t>(len));
        if (!dst) {
            rte_pktmbuf_free(m);
            return false;
        }
        std::memcpy(dst, payload, len);
    }
    finalizeChecksums(m);
    return enqueueTx(m, hdr.type);
}

bool Worker::sendData(const mktp::PeerAddr &to, const mktp::Header &hdr,
                      const uint8_t *src, uint32_t len, const Region *region) {
    const uint64_t addr = reinterpret_cast<uint64_t>(src);
    const bool zero_copy = options_.tx_zerocopy && region &&
                           region->dma_mapped && addr >= region->map_addr &&
                           addr + len <= region->map_addr + region->map_len;
    struct rte_mbuf *m;
    if (zero_copy) {
        m = buildHeaders(to, hdr, len, port_.ctrl_pool);
        if (!m) return false;
        struct rte_mbuf *ext = rte_pktmbuf_alloc(port_.ctrl_pool);
        if (!ext) {
            rte_pktmbuf_free(m);
            return false;
        }
        rte_mbuf_ext_refcnt_update(&region->shinfo, 1);
        rte_pktmbuf_attach_extbuf(ext, const_cast<uint8_t *>(src),
                                  static_cast<rte_iova_t>(addr),
                                  static_cast<uint16_t>(len), &region->shinfo);
        rte_pktmbuf_append(ext, static_cast<uint16_t>(len));
        m->next = ext;
        m->nb_segs = 2;
        m->pkt_len += len;
    } else {
        m = buildHeaders(to, hdr, len, port_.mbuf_pool);
        if (!m) return false;
        char *dst = rte_pktmbuf_append(m, static_cast<uint16_t>(len));
        if (!dst) {
            rte_pktmbuf_free(m);
            return false;
        }
        rte_memcpy(dst, src, len);
    }
    finalizeChecksums(m);
    return enqueueTx(m, mktp::DATA);
}

bool Worker::enqueueTx(struct rte_mbuf *m, uint8_t type) {
#ifdef MOONCAKE_DPDK_TRANSPORT_TEST_HOOKS
    uint32_t percent = 0;
    if (type == mktp::DATA)
        percent = g_drop_data_percent.load(std::memory_order_relaxed);
    else if (type == mktp::ACK || type == mktp::NACK || type == mktp::GRANT)
        percent = g_drop_ack_percent.load(std::memory_order_relaxed);
    if (percent) {
        rng_state_ ^= rng_state_ << 13;
        rng_state_ ^= rng_state_ >> 7;
        rng_state_ ^= rng_state_ << 17;
        if (rng_state_ % 100 < percent) {
            rte_pktmbuf_free(m);
            stats_.injected_drops++;
            g_injected_drops.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
    }
#else
    (void)type;
#endif
    if (tx_count_ == kBurst) flushTx();
    if (tx_count_ == kBurst) {
        // The port's TX ring is full: refuse rather than drop, so the
        // protocol pauses instead of paying a retransmission.
        rte_pktmbuf_free(m);
        return false;
    }
    tx_buf_[tx_count_++] = m;
    if (tx_count_ == kBurst) flushTx();
    return true;
}

void Worker::flushTx() {
    if (!tx_count_) return;
    const uint16_t sent =
        rte_eth_tx_burst(port_.port_id, options_.queue_id, tx_buf_, tx_count_);
    stats_.tx_packets += sent;
    if (sent < tx_count_) {
        std::memmove(tx_buf_, tx_buf_ + sent,
                     (tx_count_ - sent) * sizeof(tx_buf_[0]));
        stats_.tx_stalls++;
    }
    tx_count_ -= sent;
}

}  // namespace dpdk

#ifdef MOONCAKE_DPDK_TRANSPORT_TEST_HOOKS
void dpdkTransportSetLossInjectionForTest(uint32_t data_drop_percent,
                                          uint32_t ack_drop_percent) noexcept {
    dpdk::g_drop_data_percent.store(std::min<uint32_t>(data_drop_percent, 100));
    dpdk::g_drop_ack_percent.store(std::min<uint32_t>(ack_drop_percent, 100));
}

uint64_t dpdkTransportInjectedDropCountForTest() noexcept {
    return dpdk::g_injected_drops.load();
}

uint64_t dpdkTransportRetransmitCountForTest() noexcept {
    return dpdk::sumRetransmits() - dpdk::g_retransmit_baseline.load();
}

void dpdkTransportResetStatsForTest() noexcept {
    dpdk::g_injected_drops.store(0);
    dpdk::g_retransmit_baseline.store(dpdk::sumRetransmits());
}
#endif

}  // namespace mooncake

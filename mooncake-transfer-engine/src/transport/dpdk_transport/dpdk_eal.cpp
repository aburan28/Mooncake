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

#include "dpdk_eal.h"

#include <arpa/inet.h>
#include <glog/logging.h>
#include <pthread.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_flow.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_ring.h>
#include <sched.h>
#include <unistd.h>
#ifdef MOONCAKE_DPDK_HAVE_NET_RING
#include <rte_eth_ring.h>
#endif

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <map>
#include <sstream>

namespace mooncake {
namespace dpdk {

namespace {

std::vector<std::string> split(const char *text, char sep) {
    std::vector<std::string> out;
    if (!text) return out;
    std::stringstream ss(text);
    std::string item;
    while (std::getline(ss, item, sep)) {
        size_t b = item.find_first_not_of(" \t");
        size_t e = item.find_last_not_of(" \t");
        if (b == std::string::npos) continue;
        out.push_back(item.substr(b, e - b + 1));
    }
    return out;
}

std::vector<std::string> splitWhitespace(const std::string &text) {
    std::vector<std::string> out;
    std::stringstream ss(text);
    std::string item;
    while (ss >> item) out.push_back(item);
    return out;
}

uint64_t parseUnsigned(const char *name, uint64_t fallback, uint64_t minimum,
                       uint64_t maximum) {
    const char *value = std::getenv(name);
    if (!value || !*value) return fallback;
    char *end = nullptr;
    unsigned long long parsed = std::strtoull(value, &end, 10);
    if (end && *end == '\0' && parsed >= minimum && parsed <= maximum)
        return parsed;
    LOG(WARNING) << "Invalid " << name << " value: " << value
                 << ", using default " << fallback;
    return fallback;
}

struct RingPair {
    struct rte_ring *a_to_b = nullptr;
    struct rte_ring *b_to_a = nullptr;
    int port_id[2] = {-1, -1};
    struct rte_mempool *mbuf_pool[2] = {nullptr, nullptr};
    struct rte_mempool *ctrl_pool[2] = {nullptr, nullptr};
    bool in_use[2] = {false, false};
};

std::map<int, RingPair> &ringPairs() {
    static std::map<int, RingPair> pairs;
    return pairs;
}

}  // namespace

Config Config::fromEnv() {
    Config config;
    config.ports = split(std::getenv("MC_DPDK_PORTS"), ',');
    config.ips = split(std::getenv("MC_DPDK_IP"), ',');
    for (const auto &core : split(std::getenv("MC_DPDK_LCORES"), ',')) {
        char *end = nullptr;
        long value = std::strtol(core.c_str(), &end, 10);
        if (end && *end == '\0' && value >= 0 && value < CPU_SETSIZE) {
            config.lcores.push_back(static_cast<int>(value));
        } else {
            LOG(WARNING) << "Invalid MC_DPDK_LCORES entry: " << core;
        }
    }
    if (const char *args = std::getenv("MC_DPDK_EAL_ARGS"))
        config.eal_args = args;
    if (const char *mac = std::getenv("MC_DPDK_GATEWAY_MAC"))
        config.gateway_mac = mac;
    config.udp_port = static_cast<uint16_t>(
        parseUnsigned("MC_DPDK_UDP_PORT", 5555, 1, 65535));
    config.mtu =
        static_cast<uint16_t>(parseUnsigned("MC_DPDK_MTU", 1500, 256, 9600));
    config.credit_bytes =
        parseUnsigned("MC_DPDK_CREDIT_BYTES", 4 << 20, 64 << 10, 1ull << 40);
    config.min_rto_us = static_cast<uint32_t>(
        parseUnsigned("MC_DPDK_RTO_US", 200, 20, 10ull * 1000 * 1000));
    config.timeout_ms = static_cast<uint32_t>(
        parseUnsigned("MC_DPDK_TIMEOUT_MS", 10000, 100, 3600ull * 1000));
    config.slice_bytes =
        parseUnsigned("MC_DPDK_SLICE_SIZE", 16 << 20, 64 << 10, 1ull << 30);
    config.tx_zerocopy =
        static_cast<int>(parseUnsigned("MC_DPDK_TX_ZEROCOPY", 2, 0, 1));
    if (config.tx_zerocopy == 2) config.tx_zerocopy = -1;
    return config;
}

std::string formatMac(const struct rte_ether_addr &mac) {
    char buf[RTE_ETHER_ADDR_FMT_SIZE];
    rte_ether_format_addr(buf, sizeof(buf), &mac);
    return buf;
}

bool parseMac(const std::string &text, struct rte_ether_addr &mac) {
    return rte_ether_unformat_addr(text.c_str(), &mac) == 0;
}

bool parseIpv4(const std::string &text, uint32_t &ip_host_order) {
    struct in_addr addr;
    if (inet_pton(AF_INET, text.c_str(), &addr) != 1) return false;
    ip_host_order = ntohl(addr.s_addr);
    return true;
}

Eal &Eal::instance() {
    static Eal eal;
    return eal;
}

int Eal::acquire(const Config &config) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_ && initEal(config)) return -1;
    refcount_++;
    return 0;
}

void Eal::release() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (refcount_ > 0) refcount_--;
    // rte_eal_cleanup() cannot be followed by another rte_eal_init(), so the
    // EAL stays initialized for the rest of the process.
}

int Eal::initEal(const Config &config) {
    std::vector<std::string> extra = splitWhitespace(config.eal_args);
    auto has = [&](const std::string &opt) {
        for (const auto &e : extra)
            if (e == opt || e.rfind(opt + "=", 0) == 0) return true;
        return false;
    };
    int core = config.lcores.empty() ? sched_getcpu() : config.lcores[0];
    if (core < 0) core = 0;

    std::vector<std::string> args = {"mooncake-dpdk", "-l",
                                     std::to_string(core)};
    if (!has("--no-telemetry") && !has("--telemetry"))
        args.push_back("--no-telemetry");
    // --no-huge implies --legacy-mem, which DPDK rejects with --in-memory.
    const bool no_huge = has("--no-huge") || has("--legacy-mem");
    const bool in_memory =
        has("--in-memory") || (!no_huge && !has("--no-shconf"));
    if (in_memory && !has("--in-memory")) args.push_back("--in-memory");
    if (!in_memory && !has("--file-prefix"))
        args.push_back("--file-prefix=mooncake-" + std::to_string(getpid()));
    if (no_huge && !has("--iova-mode")) args.push_back("--iova-mode=va");
    args.insert(args.end(), extra.begin(), extra.end());

    std::vector<std::vector<char>> storage;
    std::vector<char *> argv;
    std::string joined;
    for (const auto &a : args) {
        storage.emplace_back(a.begin(), a.end());
        storage.back().push_back('\0');
        joined += a + " ";
    }
    for (auto &s : storage) argv.push_back(s.data());

    // rte_eal_init pins the calling thread to the main lcore; the caller is
    // an application thread, so restore its affinity afterwards.
    cpu_set_t saved;
    CPU_ZERO(&saved);
    const bool have_affinity =
        pthread_getaffinity_np(pthread_self(), sizeof(saved), &saved) == 0;
    int ret = rte_eal_init(static_cast<int>(argv.size()), argv.data());
    if (have_affinity)
        pthread_setaffinity_np(pthread_self(), sizeof(saved), &saved);
    if (ret < 0) {
        LOG(ERROR) << "DpdkTransport: rte_eal_init failed: "
                   << rte_strerror(rte_errno) << " (args: " << joined
                   << "). Without hugepages set MC_DPDK_EAL_ARGS to include "
                      "'--no-huge -m <MB>'; without a DPDK-bound NIC use a "
                      "vdev port such as net_af_xdp,iface=<if> and add "
                      "--no-pci";
        return -1;
    }
    initialized_ = true;
    LOG(INFO) << "DpdkTransport: EAL initialized (iova "
              << (rte_eal_iova_mode() == RTE_IOVA_VA ? "va" : "pa")
              << ", args: " << joined << ")";
    return 0;
}

uint16_t Eal::reserveUdpPorts(uint16_t base, uint16_t nb) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (uint32_t start = std::max<uint32_t>(base, 1); start + nb <= 65536;
         ++start) {
        bool free = true;
        for (uint32_t p = start; p < start + nb; ++p) {
            if (udp_ports_[p]) {
                free = false;
                break;
            }
        }
        if (!free) continue;
        for (uint32_t p = start; p < start + nb; ++p) udp_ports_[p] = true;
        return static_cast<uint16_t>(start);
    }
    return 0;
}

void Eal::releaseUdpPorts(uint16_t base, uint16_t nb) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (uint32_t p = base; p < uint32_t(base) + nb && p < 65536; ++p)
        udp_ports_[p] = false;
}

int Eal::findPortByDevargs(const std::string &devargs, uint16_t &port_id) {
    const std::string name = devargs.substr(0, devargs.find(','));
    if (rte_eth_dev_get_port_by_name(name.c_str(), &port_id) == 0) return 0;
    uint16_t pid;
    RTE_ETH_FOREACH_DEV(pid) {
        struct rte_eth_dev_info info;
        if (rte_eth_dev_info_get(pid, &info) != 0 || !info.device) continue;
        if (name == rte_dev_name(info.device)) {
            port_id = pid;
            return 0;
        }
    }
    return -1;
}

int Eal::createPools(Port &port) {
    const int socket = port.socket_id < 0 ? SOCKET_ID_ANY : port.socket_id;
    const unsigned serial = pool_serial_++;
    const unsigned nb_mbufs = 16384u * port.nb_queues - 1;
    const uint16_t data_room = static_cast<uint16_t>(std::max<uint32_t>(
        RTE_MBUF_DEFAULT_BUF_SIZE, RTE_PKTMBUF_HEADROOM + port.mtu + 64));
    std::string name = "mc_dpdk_mb_" + std::to_string(serial);
    port.mbuf_pool = rte_pktmbuf_pool_create(name.c_str(), nb_mbufs, 256, 0,
                                             data_room, socket);
    if (!port.mbuf_pool) {
        LOG(ERROR) << "DpdkTransport: cannot create mbuf pool for " << port.spec
                   << ": " << rte_strerror(rte_errno);
        return -1;
    }
    name = "mc_dpdk_ctl_" + std::to_string(serial);
    port.ctrl_pool =
        rte_pktmbuf_pool_create(name.c_str(), 8192u * port.nb_queues - 1, 256,
                                0, RTE_PKTMBUF_HEADROOM + 512, socket);
    if (!port.ctrl_pool) {
        LOG(ERROR) << "DpdkTransport: cannot create control pool for "
                   << port.spec << ": " << rte_strerror(rte_errno);
        rte_mempool_free(port.mbuf_pool);
        port.mbuf_pool = nullptr;
        return -1;
    }
    return 0;
}

int Eal::configurePort(Port &port, uint16_t nb_queues, uint16_t mtu) {
    struct rte_eth_dev_info info;
    int ret = rte_eth_dev_info_get(port.port_id, &info);
    if (ret) {
        LOG(ERROR) << "DpdkTransport: rte_eth_dev_info_get(" << port.port_id
                   << ") failed: " << rte_strerror(-ret);
        return -1;
    }
    port.device = info.device;
    char name[RTE_ETH_NAME_MAX_LEN] = {0};
    rte_eth_dev_get_name_by_port(port.port_id, name);
    port.name = name;
    port.socket_id = rte_eth_dev_socket_id(port.port_id);
    if (nb_queues > info.max_rx_queues || nb_queues > info.max_tx_queues) {
        uint16_t max = std::min(info.max_rx_queues, info.max_tx_queues);
        LOG(WARNING) << "DpdkTransport: port " << port.name << " supports "
                     << max << " queue pairs, requested " << nb_queues;
        nb_queues = std::max<uint16_t>(max, 1);
    }
    port.nb_queues = nb_queues;
    port.mtu = static_cast<uint16_t>(
        std::min<uint32_t>(mtu, std::max<uint32_t>(info.max_mtu, 256)));
    if (port.mtu != mtu) {
        LOG(WARNING) << "DpdkTransport: port " << port.name << " clamps MTU to "
                     << port.mtu;
    }

    struct rte_eth_conf conf;
    std::memset(&conf, 0, sizeof(conf));
    conf.rxmode.mq_mode = RTE_ETH_MQ_RX_NONE;
    conf.rxmode.mtu = port.mtu;
    conf.txmode.mq_mode = RTE_ETH_MQ_TX_NONE;
    if (info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_IPV4_CKSUM) {
        conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_IPV4_CKSUM;
        port.tx_ip_cksum = true;
    }
    if (info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_UDP_CKSUM) {
        conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_UDP_CKSUM;
        port.tx_udp_cksum = true;
    }
    if (info.tx_offload_capa & RTE_ETH_TX_OFFLOAD_MULTI_SEGS) {
        conf.txmode.offloads |= RTE_ETH_TX_OFFLOAD_MULTI_SEGS;
        port.tx_multi_segs = true;
    }
    const uint64_t rx_cksum =
        RTE_ETH_RX_OFFLOAD_IPV4_CKSUM | RTE_ETH_RX_OFFLOAD_UDP_CKSUM;
    if ((info.rx_offload_capa & rx_cksum) == rx_cksum) {
        conf.rxmode.offloads |= rx_cksum;
        port.rx_cksum = true;
    }
    ret = rte_eth_dev_configure(port.port_id, nb_queues, nb_queues, &conf);
    if (ret) {
        LOG(ERROR) << "DpdkTransport: rte_eth_dev_configure(" << port.name
                   << ") failed: " << rte_strerror(-ret);
        return -1;
    }
    if (!port.mbuf_pool && createPools(port)) return -1;

    uint16_t nb_rxd = 1024, nb_txd = 1024;
    rte_eth_dev_adjust_nb_rx_tx_desc(port.port_id, &nb_rxd, &nb_txd);
    const int socket = port.socket_id < 0 ? SOCKET_ID_ANY : port.socket_id;
    for (uint16_t q = 0; q < nb_queues; ++q) {
        ret = rte_eth_rx_queue_setup(port.port_id, q, nb_rxd, socket, nullptr,
                                     port.mbuf_pool);
        if (ret) {
            LOG(ERROR) << "DpdkTransport: rx queue " << q << " setup on "
                       << port.name << " failed: " << rte_strerror(-ret);
            return -1;
        }
        ret = rte_eth_tx_queue_setup(port.port_id, q, nb_txd, socket, nullptr);
        if (ret) {
            LOG(ERROR) << "DpdkTransport: tx queue " << q << " setup on "
                       << port.name << " failed: " << rte_strerror(-ret);
            return -1;
        }
    }
    ret = rte_eth_dev_set_mtu(port.port_id, port.mtu);
    if (ret && ret != -ENOTSUP) {
        LOG(WARNING) << "DpdkTransport: cannot set MTU " << port.mtu << " on "
                     << port.name << ": " << rte_strerror(-ret);
    }
    ret = rte_eth_dev_start(port.port_id);
    if (ret) {
        LOG(ERROR) << "DpdkTransport: rte_eth_dev_start(" << port.name
                   << ") failed: " << rte_strerror(-ret);
        return -1;
    }
    port.started = true;
    rte_eth_macaddr_get(port.port_id, &port.mac);
    LOG(INFO) << "DpdkTransport: port " << port.name << " (" << port.spec
              << ") started: mac " << formatMac(port.mac) << ", mtu "
              << port.mtu << ", queues " << nb_queues << ", tx cksum offload "
              << (port.tx_ip_cksum && port.tx_udp_cksum ? "yes" : "no")
              << ", rx cksum offload " << (port.rx_cksum ? "yes" : "no")
              << ", multi-seg tx " << (port.tx_multi_segs ? "yes" : "no");
    return 0;
}

int Eal::openRingPair(const std::string &spec, Port &port) {
#ifndef MOONCAKE_DPDK_HAVE_NET_RING
    LOG(ERROR) << "DpdkTransport: " << spec
               << " needs the DPDK ring PMD (librte_net_ring), which was not "
                  "found at build time";
    return -1;
#else
    std::vector<std::string> fields = split(spec.c_str(), ':');
    if (fields.size() < 3 || fields.size() > 4 ||
        (fields[2] != "a" && fields[2] != "b")) {
        LOG(ERROR) << "DpdkTransport: invalid pseudo-port " << spec
                   << ", expected ringpair:<id>:<a|b>[:<ring_size>]";
        return -1;
    }
    const int id = std::atoi(fields[1].c_str());
    const int side = fields[2] == "a" ? 0 : 1;
    // Sized like a NIC TX ring so the software path sees the same
    // backpressure and queueing delay as hardware.
    unsigned ring_size =
        fields.size() == 4 ? std::atoi(fields[3].c_str()) : 4096;
    if (ring_size < 64 || (ring_size & (ring_size - 1))) {
        LOG(ERROR) << "DpdkTransport: ring size must be a power of two >= 64";
        return -1;
    }
    RingPair &pair = ringPairs()[id];
    if (!pair.a_to_b) {
        const std::string a_to_b = "mc_rp" + std::to_string(id) + "_atob";
        const std::string b_to_a = "mc_rp" + std::to_string(id) + "_btoa";
        pair.a_to_b =
            rte_ring_create(a_to_b.c_str(), ring_size, SOCKET_ID_ANY, 0);
        pair.b_to_a =
            rte_ring_create(b_to_a.c_str(), ring_size, SOCKET_ID_ANY, 0);
        if (!pair.a_to_b || !pair.b_to_a) {
            LOG(ERROR) << "DpdkTransport: cannot create rings for " << spec
                       << ": " << rte_strerror(rte_errno);
            return -1;
        }
        struct rte_ring *rx_a[1] = {pair.b_to_a}, *tx_a[1] = {pair.a_to_b};
        struct rte_ring *rx_b[1] = {pair.a_to_b}, *tx_b[1] = {pair.b_to_a};
        const std::string name_a = "mcrp" + std::to_string(id) + "a";
        const std::string name_b = "mcrp" + std::to_string(id) + "b";
        pair.port_id[0] =
            rte_eth_from_rings(name_a.c_str(), rx_a, 1, tx_a, 1, SOCKET_ID_ANY);
        pair.port_id[1] =
            rte_eth_from_rings(name_b.c_str(), rx_b, 1, tx_b, 1, SOCKET_ID_ANY);
        if (pair.port_id[0] < 0 || pair.port_id[1] < 0) {
            LOG(ERROR) << "DpdkTransport: rte_eth_from_rings failed for "
                       << spec << ": " << rte_strerror(rte_errno);
            return -1;
        }
        LOG(INFO) << "DpdkTransport: created ring pair " << id << " (ports "
                  << pair.port_id[0] << "/" << pair.port_id[1] << ", ring size "
                  << ring_size << ")";
    }
    if (pair.in_use[side]) {
        LOG(ERROR) << "DpdkTransport: " << spec << " is already in use";
        return -1;
    }
    port.port_id = static_cast<uint16_t>(pair.port_id[side]);
    port.is_ringpair = true;
    port.mbuf_pool = pair.mbuf_pool[side];
    port.ctrl_pool = pair.ctrl_pool[side];
    pair.in_use[side] = true;
    return 0;
#endif
}

int Eal::openPort(const std::string &spec, uint16_t nb_queues, uint16_t mtu,
                  Port &port) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!initialized_) return -1;
    port = Port();
    port.spec = spec;
    port.nb_queues = nb_queues;
    if (spec.rfind("ringpair:", 0) == 0) {
        if (openRingPair(spec, port)) return -1;
        nb_queues = 1;
    } else {
        uint16_t pid;
        if (findPortByDevargs(spec, pid)) {
            int ret = rte_dev_probe(spec.c_str());
            if (ret < 0 && ret != -EEXIST) {
                LOG(ERROR) << "DpdkTransport: cannot probe device " << spec
                           << ": " << rte_strerror(-ret)
                           << " (is the NIC bound to vfio-pci, or the PMD "
                              "installed?)";
                return -1;
            }
            if (findPortByDevargs(spec, pid)) {
                LOG(ERROR) << "DpdkTransport: no ethdev found for " << spec;
                return -1;
            }
        }
        port.port_id = pid;
    }
    if (configurePort(port, nb_queues, mtu)) {
        closePort(port);
        return -1;
    }
    if (port.is_ringpair) {
        // Keep the pools with the pair so a reopened side reuses them, and
        // drop packets left in the ring by a previous owner.
        for (auto &kv : ringPairs()) {
            for (int side = 0; side < 2; ++side) {
                if (kv.second.port_id[side] == port.port_id) {
                    kv.second.mbuf_pool[side] = port.mbuf_pool;
                    kv.second.ctrl_pool[side] = port.ctrl_pool;
                }
            }
        }
        struct rte_mbuf *stale[32];
        uint16_t n;
        while ((n = rte_eth_rx_burst(port.port_id, 0, stale, 32)) > 0)
            for (uint16_t i = 0; i < n; ++i) rte_pktmbuf_free(stale[i]);
    }
    return 0;
}

void Eal::closePort(Port &port) {
    if (port.started) rte_eth_dev_stop(port.port_id);
    port.started = false;
    if (port.is_ringpair) {
        for (auto &kv : ringPairs())
            for (int side = 0; side < 2; ++side)
                if (kv.second.port_id[side] == port.port_id)
                    kv.second.in_use[side] = false;
        return;
    }
    if (!port.name.empty()) rte_eth_dev_close(port.port_id);
    if (port.mbuf_pool) rte_mempool_free(port.mbuf_pool);
    if (port.ctrl_pool) rte_mempool_free(port.ctrl_pool);
    port.mbuf_pool = port.ctrl_pool = nullptr;
}

int installFlowSteering(const Port &port, uint16_t udp_base, uint16_t nb) {
    if (nb <= 1) return 0;
    struct rte_flow_error error;
    for (uint16_t q = 0; q < nb; ++q) {
        struct rte_flow_attr attr;
        std::memset(&attr, 0, sizeof(attr));
        attr.ingress = 1;
        struct rte_flow_item_udp udp_spec, udp_mask;
        std::memset(&udp_spec, 0, sizeof(udp_spec));
        std::memset(&udp_mask, 0, sizeof(udp_mask));
        udp_spec.hdr.dst_port = rte_cpu_to_be_16(udp_base + q);
        udp_mask.hdr.dst_port = 0xffff;
        struct rte_flow_item pattern[4];
        std::memset(pattern, 0, sizeof(pattern));
        pattern[0].type = RTE_FLOW_ITEM_TYPE_ETH;
        pattern[1].type = RTE_FLOW_ITEM_TYPE_IPV4;
        pattern[2].type = RTE_FLOW_ITEM_TYPE_UDP;
        pattern[2].spec = &udp_spec;
        pattern[2].mask = &udp_mask;
        pattern[3].type = RTE_FLOW_ITEM_TYPE_END;
        struct rte_flow_action_queue queue;
        queue.index = q;
        struct rte_flow_action actions[2];
        std::memset(actions, 0, sizeof(actions));
        actions[0].type = RTE_FLOW_ACTION_TYPE_QUEUE;
        actions[0].conf = &queue;
        actions[1].type = RTE_FLOW_ACTION_TYPE_END;
        std::memset(&error, 0, sizeof(error));
        if (rte_flow_validate(port.port_id, &attr, pattern, actions, &error) ||
            !rte_flow_create(port.port_id, &attr, pattern, actions, &error)) {
            LOG(WARNING) << "DpdkTransport: port " << port.name
                         << " cannot steer UDP port " << udp_base + q
                         << " to queue " << q << ": "
                         << (error.message ? error.message : "rte_flow error")
                         << "; packets are forwarded between workers in "
                            "software";
            return -1;
        }
    }
    LOG(INFO) << "DpdkTransport: port " << port.name << " steers UDP ports "
              << udp_base << "-" << udp_base + nb - 1 << " to queues 0-"
              << nb - 1;
    return 0;
}

}  // namespace dpdk
}  // namespace mooncake

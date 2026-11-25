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

#ifndef MOONCAKE_DPDK_WORKER_H_
#define MOONCAKE_DPDK_WORKER_H_

#include <rte_mbuf.h>
#include <rte_ring.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "dpdk_eal.h"
#include "mktp.h"

namespace mooncake {
namespace dpdk {

struct WorkerStats {
    std::atomic<uint64_t> rx_packets{0};
    std::atomic<uint64_t> tx_packets{0};
    std::atomic<uint64_t> tx_stalls{0};
    std::atomic<uint64_t> rx_bad{0};
    std::atomic<uint64_t> forwarded{0};
    std::atomic<uint64_t> injected_drops{0};
    std::atomic<uint64_t> retransmits{0};
    std::atomic<uint64_t> nacks{0};
};

// One polling thread per RX/TX queue pair. Each worker owns a UDP port
// (udp_port) so that a peer can address the worker directly; packets for a
// sibling worker that land on this queue (no flow steering) are forwarded
// through the sibling's fwd ring. Slices arrive through the submission ring
// and are driven by an MKTP endpoint; the worker turns endpoint output into
// Ethernet/IPv4/UDP frames with checksum offload when the port supports it
// and software checksums otherwise. RX placement copies from mbufs into the
// destination on this thread (TODO: rte_dma offload for Intel DSA/IOAT).
class Worker : public mktp::Sink {
   public:
    struct Options {
        uint16_t queue_id = 0;
        uint16_t udp_port = 0;
        uint16_t udp_port_base = 0;   // first port of this ethdev's workers
        uint16_t udp_port_count = 1;  // workers on this ethdev
        uint32_t ip = 0;              // host order
        int cpu = -1;                 // pin target, -1 = unpinned
        bool tx_zerocopy = false;
        struct rte_ether_addr gateway_mac{};
        bool use_gateway_mac = false;
        mktp::Params mktp;
    };

    Worker(unsigned id, const Port &port, const Options &options,
           mktp::Callbacks callbacks);
    ~Worker() override;

    int start();
    void stop();
    // Called by siblings that received a packet destined to this worker.
    void attachSiblings(const std::vector<Worker *> &siblings) {
        siblings_ = siblings;
    }

    // Submits a prepared slice; returns false when the ring is full.
    bool submit(Transport::Slice *slice);
    struct rte_ring *fwdRing() const { return fwd_ring_; }
    uint16_t udpPort() const { return options_.udp_port; }
    const WorkerStats &stats() const { return stats_; }
    const mktp::Stats &mktpStats() const { return mktp_stats_; }

    bool sendCtrl(const mktp::PeerAddr &to, const mktp::Header &hdr,
                  const void *payload, uint32_t len) override;
    bool sendData(const mktp::PeerAddr &to, const mktp::Header &hdr,
                  const uint8_t *src, uint32_t len,
                  const Region *region) override;

   private:
    void run();
    void handleRx(struct rte_mbuf *m, uint64_t now_us);
    bool enqueueTx(struct rte_mbuf *m, uint8_t type);
    void flushTx();
    uint64_t nowUs() const;
    void publishStats();
    struct rte_mbuf *buildHeaders(const mktp::PeerAddr &to,
                                  const mktp::Header &hdr, uint32_t payload_len,
                                  struct rte_mempool *pool);
    void finalizeChecksums(struct rte_mbuf *m);

    unsigned id_;
    Port port_;
    Options options_;
    mktp::Callbacks callbacks_;
    std::unique_ptr<mktp::Endpoint> endpoint_;
    mktp::Stats mktp_stats_;
    WorkerStats stats_;
    std::vector<Worker *> siblings_;
    struct rte_ring *submit_ring_ = nullptr;
    struct rte_ring *fwd_ring_ = nullptr;
    std::thread thread_;
    std::atomic<bool> running_{false};
    double cycles_per_us_ = 1.0;

    static constexpr unsigned kBurst = 64;
    struct rte_mbuf *tx_buf_[kBurst];
    unsigned tx_count_ = 0;
    uint64_t rng_state_ = 0x9E3779B97F4A7C15ull;
    std::vector<uint8_t> linear_;
};

}  // namespace dpdk
}  // namespace mooncake

#ifdef MOONCAKE_DPDK_TRANSPORT_TEST_HOOKS
namespace mooncake {
// Drops the given percentage of DATA and of ACK/NACK/GRANT packets before
// they reach the port, to exercise loss recovery on software ports.
void dpdkTransportSetLossInjectionForTest(uint32_t data_drop_percent,
                                          uint32_t ack_drop_percent) noexcept;
uint64_t dpdkTransportInjectedDropCountForTest() noexcept;
uint64_t dpdkTransportRetransmitCountForTest() noexcept;
void dpdkTransportResetStatsForTest() noexcept;
}  // namespace mooncake
#endif

#endif  // MOONCAKE_DPDK_WORKER_H_

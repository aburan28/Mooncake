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

#ifndef MOONCAKE_DPDK_EAL_H_
#define MOONCAKE_DPDK_EAL_H_

#include <rte_dev.h>
#include <rte_ether.h>
#include <rte_mempool.h>

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace mooncake {
namespace dpdk {

// Transport configuration read from MC_DPDK_* environment variables.
struct Config {
    std::vector<std::string> ports;   // MC_DPDK_PORTS entries
    std::vector<std::string> ips;     // MC_DPDK_IP, one per port
    std::vector<int> lcores;          // MC_DPDK_LCORES cpu ids (may be empty)
    std::string eal_args;             // MC_DPDK_EAL_ARGS
    std::string gateway_mac;          // MC_DPDK_GATEWAY_MAC (optional)
    uint16_t udp_port = 5555;         // MC_DPDK_UDP_PORT
    uint16_t mtu = 1500;              // MC_DPDK_MTU
    uint64_t credit_bytes = 4 << 20;  // MC_DPDK_CREDIT_BYTES
    uint32_t min_rto_us = 200;        // MC_DPDK_RTO_US
    uint32_t timeout_ms = 10000;      // MC_DPDK_TIMEOUT_MS
    uint64_t slice_bytes = 16 << 20;  // MC_DPDK_SLICE_SIZE
    int tx_zerocopy = -1;             // MC_DPDK_TX_ZEROCOPY (-1 auto)

    static Config fromEnv();
};

// One DPDK ethdev owned by a transport instance.
struct Port {
    uint16_t port_id = 0;
    std::string spec;
    std::string name;
    struct rte_ether_addr mac {};
    uint16_t mtu = 1500;
    uint16_t nb_queues = 1;
    int socket_id = 0;
    struct rte_mempool *mbuf_pool = nullptr;  // RX and copied TX payloads
    struct rte_mempool *ctrl_pool = nullptr;  // headers and control packets
    struct rte_device *device = nullptr;
    bool tx_ip_cksum = false;
    bool tx_udp_cksum = false;
    bool rx_cksum = false;
    bool tx_multi_segs = false;
    bool is_ringpair = false;
    bool started = false;
};

// Process-wide EAL lifecycle. rte_eal_init runs once per process and DPDK
// cannot re-initialize, so the EAL stays up for the process lifetime; the
// refcount tracks transport instances sharing it.
class Eal {
   public:
    static Eal &instance();

    int acquire(const Config &config);
    void release();
    bool initialized() const { return initialized_; }

    // Opens (probes, configures and starts) one MC_DPDK_PORTS entry with
    // nb_queues RX/TX queue pairs. Entries: a PCI address, a vdev string
    // such as net_af_xdp,iface=eth0, or the test pseudo-port
    // ringpair:<id>:<a|b>[:<ring_size>] backed by rte_eth_from_rings().
    int openPort(const std::string &spec, uint16_t nb_queues, uint16_t mtu,
                 Port &port);
    void closePort(Port &port);

    // Reserves nb consecutive UDP ports starting at or after base.
    uint16_t reserveUdpPorts(uint16_t base, uint16_t nb);
    void releaseUdpPorts(uint16_t base, uint16_t nb);

   private:
    Eal() = default;
    int initEal(const Config &config);
    int openRingPair(const std::string &spec, Port &port);
    int findPortByDevargs(const std::string &devargs, uint16_t &port_id);
    int configurePort(Port &port, uint16_t nb_queues, uint16_t mtu);
    int createPools(Port &port);

    std::mutex mutex_;
    int refcount_ = 0;
    bool initialized_ = false;
    std::vector<bool> udp_ports_ = std::vector<bool>(65536, false);
    unsigned pool_serial_ = 0;
};

// Steers UDP destination ports [udp_base, udp_base + nb) to RX queues
// 0..nb-1 with rte_flow, so multi-queue ports deliver each worker's traffic
// to its own queue. Returns -1 (with a warning) when the PMD cannot do it;
// workers then forward misrouted packets to each other in software.
int installFlowSteering(const Port &port, uint16_t udp_base, uint16_t nb);

std::string formatMac(const struct rte_ether_addr &mac);
bool parseMac(const std::string &text, struct rte_ether_addr &mac);
bool parseIpv4(const std::string &text, uint32_t &ip_host_order);

}  // namespace dpdk
}  // namespace mooncake

#endif  // MOONCAKE_DPDK_EAL_H_

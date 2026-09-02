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

#include "tcp_zero_copy.h"

#include <glog/logging.h>
#include <liburing.h>
#include <net/if.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>

#include "tcp_uring_uapi_shim.h"
#include "transport/tcp_transport/tcp_wire.h"

namespace mooncake {
namespace tcp_uring {

namespace {

bool envFlag(const char *name) {
    const char *env = std::getenv(name);
    return env && env[0] == '1' && env[1] == '\0';
}

std::vector<uint32_t> parseQueueList(const char *text) {
    std::vector<uint32_t> out;
    if (!text) return out;
    const char *p = text;
    while (*p) {
        while (*p == ',' || *p == ' ') ++p;
        if (!*p) break;
        char *end = nullptr;
        const unsigned long v = std::strtoul(p, &end, 10);
        if (end == p) break;
        out.push_back(static_cast<uint32_t>(v));
        p = end;
    }
    return out;
}

}  // namespace

ZeroCopyConfig ZeroCopyConfig::fromEnv() {
    ZeroCopyConfig config;
    config.enabled = envFlag("MC_TCP_ZC");
    const char *iface = std::getenv("MC_TCP_ZC_IFACE");
    if (iface) config.iface = iface;
    config.rxqs = parseQueueList(std::getenv("MC_TCP_ZC_RXQS"));
    const char *area = std::getenv("MC_TCP_ZCRX_AREA_MB");
    if (area) {
        const long v = std::atol(area);
        if (v > 0) config.area_mb = static_cast<size_t>(v);
    }
    return config;
}

// --- fragment planner ------------------------------------------------------

void FragmentPlanner::reset(uint64_t expected_bytes) {
    expected_ = expected_bytes;
    consumed_ = 0;
}

bool FragmentPlanner::add(const ZcFragment &frag, std::vector<ScatterOp> *out) {
    if (frag.len == 0) return true;
    if (consumed_ + frag.len > expected_) return false;
    if (out) {
        // Coalesce with the previous operation when both the area span and
        // the destination span continue it. Receiving a large payload out of
        // one refill run is the common case, so this usually collapses to a
        // single copy.
        if (!out->empty()) {
            ScatterOp &last = out->back();
            if (last.area_offset + last.len == frag.area_offset &&
                last.dest_offset + last.len == consumed_) {
                last.len += frag.len;
                consumed_ += frag.len;
                return true;
            }
        }
        out->push_back(ScatterOp{frag.area_offset, consumed_, frag.len});
    }
    consumed_ += frag.len;
    return true;
}

// --- refill accounting -----------------------------------------------------

bool RefillAccount::acquire(uint32_t count) {
    if (count > available()) return false;
    outstanding_ += count;
    return true;
}

uint32_t RefillAccount::release(uint32_t count) {
    const uint32_t returned = count < outstanding_ ? count : outstanding_;
    outstanding_ -= returned;
    refills_ += returned;
    return returned;
}

void applyScatterHost(const void *area_base, void *dest_base,
                      const std::vector<ScatterOp> &ops) {
    const char *src = static_cast<const char *>(area_base);
    char *dst = static_cast<char *>(dest_base);
    for (const auto &op : ops)
        memcpy(dst + op.dest_offset, src + op.area_offset,
               static_cast<size_t>(op.len));
}

// --- probe -----------------------------------------------------------------

TcpZeroCopy::TcpZeroCopy(ZeroCopyConfig config) : config_(std::move(config)) {}

TcpZeroCopy::~TcpZeroCopy() = default;

namespace {

// Attempts IORING_REGISTER_ZCRX_IFQ against a scratch ring. A kernel without
// zcrx answers -EINVAL for the opcode; a NIC without header/data split or
// without the requested queue answers -EOPNOTSUPP or -EINVAL from the driver.
// Either way the answer is "unsupported" and the caller advertises nothing.
int probeInterfaceQueue(const std::string &iface, uint32_t rxq,
                        std::string *reason) {
    const unsigned int if_idx = if_nametoindex(iface.c_str());
    if (if_idx == 0) {
        *reason = "interface " + iface + " not found";
        return -ENODEV;
    }

    struct io_uring ring;
    int ret = io_uring_queue_init(
        64, &ring, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
    if (ret < 0) ret = io_uring_queue_init(64, &ring, 0);
    if (ret < 0) {
        *reason = "io_uring unavailable: " + std::string(strerror(-ret));
        return ret;
    }

    // A registration without a backing area is rejected by every kernel that
    // knows the opcode, so the probe distinguishes "opcode unknown" (-EINVAL
    // on an old kernel reports the same code) from "driver refuses" only by
    // its message. Both outcomes disable the capability; the reason string is
    // what an operator reads in the log.
    struct io_uring_zcrx_ifq_reg reg;
    memset(&reg, 0, sizeof(reg));
    reg.if_idx = if_idx;
    reg.if_rxq = rxq;
    reg.rq_entries = 1024;
    ret = io_uring_register(ring.ring_fd, IORING_REGISTER_ZCRX_IFQ, &reg, 1);
    if (ret < 0) {
        *reason = "IORING_REGISTER_ZCRX_IFQ on " + iface + " queue " +
                  std::to_string(rxq) + " failed: " + strerror(-ret);
    }
    io_uring_queue_exit(&ring);
    return ret;
}

// Transmit needs a dmabuf bound to the netdev (NETDEV_CMD_BIND_TX) and a
// socket that accepts SO_ZEROCOPY. Only the cheap half is probed here: with
// no dmabuf-exportable device memory on this host, binding cannot be
// attempted, so the capability stays off unless a device region is present.
bool probeDevmemSend(const std::string &iface, std::string *reason) {
    if (if_nametoindex(iface.c_str()) == 0) {
        *reason = "interface " + iface + " not found";
        return false;
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        *reason = "socket() failed: " + std::string(strerror(errno));
        return false;
    }
    int one = 1;
    const bool ok =
        setsockopt(fd, SOL_SOCKET, SO_ZEROCOPY, &one, sizeof(one)) == 0;
    if (!ok) *reason = "SO_ZEROCOPY rejected: " + std::string(strerror(errno));
    close(fd);
    if (!ok) return false;
    // Without a dmabuf-capable device allocation there is nothing to bind, so
    // the transmit capability is not advertised even though the socket option
    // exists.
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
    return true;
#else
    *reason = "no device-memory support in this build";
    return false;
#endif
}

}  // namespace

const ZeroCopyProbe &TcpZeroCopy::probe() {
    if (probed_) return probe_result_;
    probed_ = true;

    if (!config_.enabled) {
        probe_result_.reason = "MC_TCP_ZC is not set";
        return probe_result_;
    }
    if (config_.iface.empty()) {
        probe_result_.reason = "MC_TCP_ZC_IFACE is not set";
        LOG(WARNING) << "TcpZeroCopy: " << probe_result_.reason
                     << "; zero copy disabled";
        return probe_result_;
    }
    if (config_.rxqs.empty()) {
        probe_result_.reason = "MC_TCP_ZC_RXQS is not set";
        LOG(WARNING) << "TcpZeroCopy: " << probe_result_.reason
                     << "; zero copy disabled";
        return probe_result_;
    }

    std::string reason;
    const int rc =
        probeInterfaceQueue(config_.iface, config_.rxqs.front(), &reason);
    if (rc < 0) {
        probe_result_.reason = reason;
        LOG(WARNING) << "TcpZeroCopy: " << reason
                     << "; falling back to pinned staging";
    } else {
        probe_result_.zcrx_recv = true;
    }

    if (config_.devmem_send) {
        std::string tx_reason;
        if (probeDevmemSend(config_.iface, &tx_reason)) {
            probe_result_.devmem_send = true;
        } else if (probe_result_.reason.empty()) {
            probe_result_.reason = tx_reason;
        }
    }

    if (probe_result_.zcrx_recv) {
        LOG(INFO) << "TcpZeroCopy: zcrx receive available on " << config_.iface
                  << " over " << config_.rxqs.size() << " queue(s), area "
                  << config_.area_mb << " MiB";
    }
    return probe_result_;
}

uint32_t TcpZeroCopy::caps() const {
    uint32_t caps = 0;
    if (probe_result_.zcrx_recv) caps |= tcp_wire::TCP_CAP_ZCRX_RECV;
    if (probe_result_.devmem_send) caps |= tcp_wire::TCP_CAP_DEVMEM_SEND;
    return caps;
}

bool TcpZeroCopy::shouldUseDataLane(uint32_t peer_caps,
                                    const std::vector<uint16_t> &peer_ports,
                                    uint64_t length, uint64_t min_length) {
    if ((peer_caps & tcp_wire::TCP_CAP_ZCRX_RECV) == 0) return false;
    if (peer_ports.empty()) return false;
    return length >= min_length;
}

}  // namespace tcp_uring
}  // namespace mooncake

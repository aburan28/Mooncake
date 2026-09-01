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

// dmabuf zero-copy data plane for the io_uring TCP backend: io_uring
// zero-copy receive (zcrx) into a device-memory area and devmem transmit
// from a dmabuf-bound region. See docs/source/design/transfer-engine/
// tcp_zero_copy.md.
//
// The capability is negotiated, never assumed: a segment advertises
// TCP_CAP_ZCRX_RECV only after the interface-queue registration probe
// succeeds locally, and an initiator uses the zero-copy data lane only when
// the peer advertises it. Everything below the probe degrades to the Phase 1
// pinned-staging path, which degrades to asio.
//
// The stream bookkeeping (fragment planner, refill accounting) is pure logic
// so it can be exercised on a host without an HDS-capable NIC.

#ifndef TCP_ZERO_COPY_H_
#define TCP_ZERO_COPY_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace mooncake {
namespace tcp_uring {

struct ZeroCopyConfig {
    bool enabled = false;         // MC_TCP_ZC
    std::string iface;            // MC_TCP_ZC_IFACE
    std::vector<uint32_t> rxqs;   // MC_TCP_ZC_RXQS, e.g. "8,9"
    size_t area_mb = 512;         // MC_TCP_ZCRX_AREA_MB
    bool devmem_send = true;      // transmit side follows the same switch

    static ZeroCopyConfig fromEnv();
};

// One zero-copy receive completion: a span of the registered area plus the
// token that returns it to the refill ring. Fragments of one socket arrive
// in stream order and may be smaller than the requested length.
struct ZcFragment {
    uint64_t area_offset = 0;
    uint32_t len = 0;
    uint32_t token = 0;
};

// A copy from the receive area into the request's destination. On a GPU
// destination these become one gather kernel (or cudaMemcpyBatchAsync); on a
// host destination they are plain memcpy, which is what makes the planner
// testable without hardware.
struct ScatterOp {
    uint64_t area_offset = 0;
    uint64_t dest_offset = 0;
    uint64_t len = 0;
};

// Turns the stream-ordered fragment list of one request into scatter
// operations. Adjacent fragments that are contiguous in both the area and
// the destination are coalesced so a 2 MiB payload delivered in 32 KiB
// fragments does not become 64 separate copies.
class FragmentPlanner {
   public:
    void reset(uint64_t expected_bytes);

    // Appends the scatter operations for `frag`. Returns false when the
    // fragment would overrun the request, which is a protocol error: the
    // data lane carries payload only, so the byte count must match the
    // control lane's SessionHeader exactly.
    bool add(const ZcFragment &frag, std::vector<ScatterOp> *out);

    uint64_t consumed() const { return consumed_; }
    uint64_t expected() const { return expected_; }
    bool complete() const { return consumed_ == expected_; }

   private:
    uint64_t expected_ = 0;
    uint64_t consumed_ = 0;
};

// Refill-ring bookkeeping. The NIC may not reuse a buffer until its token is
// returned, so an area of `entries` buffers bounds how many fragments may be
// outstanding; the worker must refill before posting the next receive.
class RefillAccount {
   public:
    RefillAccount() = default;
    explicit RefillAccount(uint32_t entries) : entries_(entries) {}

    // Returns false when the area is exhausted, which means the receive path
    // is running ahead of the scatter step and must wait.
    bool acquire(uint32_t count = 1);
    // Returns tokens to the refill ring; returns the number actually
    // returned (never more than are outstanding).
    uint32_t release(uint32_t count = 1);

    uint32_t entries() const { return entries_; }
    uint32_t outstanding() const { return outstanding_; }
    uint32_t available() const { return entries_ - outstanding_; }
    uint64_t refills() const { return refills_; }

   private:
    uint32_t entries_ = 0;
    uint32_t outstanding_ = 0;
    uint64_t refills_ = 0;
};

// Copies planned fragments out of a host-visible area. Device destinations
// go through the CUDA path in the .cpp (compiled only under the accelerator
// macros); this entry point exists so the planner can be verified end to end
// on plain host memory.
void applyScatterHost(const void *area_base, void *dest_base,
                      const std::vector<ScatterOp> &ops);

struct ZeroCopyProbe {
    bool zcrx_recv = false;
    bool devmem_send = false;
    // Human-readable reason the capability is unavailable; empty on success.
    std::string reason;
};

// Owns the local zero-copy resources and the capability decision. Construct
// it unconditionally: with MC_TCP_ZC unset it reports no capabilities and
// allocates nothing.
class TcpZeroCopy {
   public:
    explicit TcpZeroCopy(ZeroCopyConfig config);
    ~TcpZeroCopy();

    // Runs the interface-queue and dmabuf-bind probes. Never throws and
    // never fails the transport: an unsupported host simply advertises no
    // capability bits.
    const ZeroCopyProbe &probe();

    // Capability bits for SegmentDesc::tcp_caps (tcp_wire.h TcpCaps).
    uint32_t caps() const;
    // Data ports to advertise in SegmentDesc::tcp_zc_ports, one per
    // zero-copy RX queue. Empty unless zcrx_recv was probed successfully.
    const std::vector<uint16_t> &dataPorts() const { return data_ports_; }

    const ZeroCopyConfig &config() const { return config_; }

    // Decides whether an initiator should open a zero-copy data lane to a
    // peer: the peer must advertise TCP_CAP_ZCRX_RECV with at least one data
    // port and the payload must be large enough to pay for the extra
    // connection. Local devmem transmit is deliberately not required — a
    // host-memory sender still removes every copy on the receiving side.
    static bool shouldUseDataLane(uint32_t peer_caps,
                                  const std::vector<uint16_t> &peer_ports,
                                  uint64_t length, uint64_t min_length);

   private:
    ZeroCopyConfig config_;
    ZeroCopyProbe probe_result_;
    bool probed_ = false;
    std::vector<uint16_t> data_ports_;
};

}  // namespace tcp_uring
}  // namespace mooncake

#endif  // TCP_ZERO_COPY_H_

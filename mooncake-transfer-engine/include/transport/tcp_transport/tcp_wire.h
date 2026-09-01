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

// Wire-format definitions shared by every TCP data-plane backend (asio,
// io_uring). Both backends must stay byte-compatible: a segment advertises
// protocol "tcp" regardless of the backend serving it, and peers may mix
// backends freely during a rolling upgrade.

#ifndef TCP_WIRE_H_
#define TCP_WIRE_H_

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <string>

namespace mooncake {
namespace tcp_wire {

// Sent raw, including its natural padding, so sizeof() is the wire size.
struct SessionHeader {
    uint64_t size;
    uint64_t addr;
    uint8_t opcode;
};
constexpr size_t kSessionHeaderWireSize = sizeof(SessionHeader);

// --- Acknowledged framing (protocol v2, #2086) ------------------------------
// v2 requests set the high bit of the opcode; the server then prefixes every
// READ response with an 8-byte status frame and sends an 8-byte status frame
// for WRITE only after the final chunk has been applied to destination memory.
constexpr uint8_t kOpcodeV2Flag = 0x80;
constexpr uint64_t kStatusMagic = 0x4D435456ull << 32;  // "MCTV"
constexpr uint64_t kStatusOk = kStatusMagic | 0;
constexpr uint64_t kStatusAddrRejected = kStatusMagic | 1;
constexpr size_t kStatusFrameSize = sizeof(uint64_t);

inline bool statusFrameValid(uint64_t frame) {
    return (frame & 0xFFFFFFFF00000000ull) == kStatusMagic;
}

// Capability bits carried in SegmentDesc::tcp_caps (optional JSON field,
// absent = 0). Only the zero-copy data plane consults them today.
enum TcpCaps : uint32_t {
    TCP_CAP_ZCRX_RECV = 1u << 0,    // dedicated zero-copy RX queues advertised
    TCP_CAP_DEVMEM_SEND = 1u << 1,  // can transmit from dmabuf-bound memory
};

// Segmentation granularity of the data plane, MC_TCP_SLICE_SIZE (64 KiB).
inline size_t getChunkSize() {
    static const size_t val = [] {
        const char* env = std::getenv("MC_TCP_SLICE_SIZE");
        if (env) {
            try {
                size_t v = std::stoull(env);
                if (v > 0) return v;
                LOG(WARNING)
                    << "Ignore non-positive MC_TCP_SLICE_SIZE value: " << env
                    << ", using default 65536";
            } catch (const std::exception& e) {
                LOG(WARNING)
                    << "Invalid MC_TCP_SLICE_SIZE value: " << env
                    << ". Error: " << e.what() << ", using default 65536";
            }
        }
        return size_t(65536);
    }();
    return val;
}

// Operational escape hatch: MC_TCP_PROTO=1 forces initiators to speak the
// legacy unacknowledged framing even to v2-capable servers. Read per call so
// tests can cover both protocol modes in one process.
inline bool forceLegacyTcpProto() {
    const char* env = std::getenv("MC_TCP_PROTO");
    return env && env[0] == '1' && env[1] == '\0';
}

inline int parseTimeoutSecEnv(const char* name, int default_sec) {
    const char* env = std::getenv(name);
    if (env) {
        int v = std::atoi(env);
        if (v > 0) return v;
    }
    return default_sec;
}

// Deadline for a v2 status frame (MC_TCP_STATUS_TIMEOUT_SEC, 30s).
inline int statusFrameTimeoutSec() {
    return parseTimeoutSecEnv("MC_TCP_STATUS_TIMEOUT_SEC", 30);
}

// Sliding liveness deadline for payload progress
// (MC_TCP_PROGRESS_TIMEOUT_SEC, 30s).
inline int progressTimeoutSec() {
    return parseTimeoutSecEnv("MC_TCP_PROGRESS_TIMEOUT_SEC", 30);
}

}  // namespace tcp_wire
}  // namespace mooncake

#endif  // TCP_WIRE_H_

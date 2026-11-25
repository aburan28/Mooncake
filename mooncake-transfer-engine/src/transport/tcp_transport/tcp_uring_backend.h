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

// io_uring data-plane backend of the classic TcpTransport. Byte-compatible
// with the asio backend (tcp_wire.h): a segment keeps advertising protocol
// "tcp" and peers may mix backends. See docs/source/design/transfer-engine/
// tcp_io_uring.md for the operator view.

#ifndef TCP_URING_BACKEND_H_
#define TCP_URING_BACKEND_H_

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "tcp_uring_lanes.h"
#include "transport/transport.h"

namespace mooncake {
namespace tcp_uring {

class TcpZeroCopy;

struct UringConfig {
    size_t workers = 1;
    bool sqpoll = false;
    size_t zc_threshold = 16384;
    size_t staging_chunk = 2u << 20;
    size_t staging_depth = 4;
    // Requests of one task group kept in flight on a lane before their
    // status frames are consumed.
    size_t pipeline = 16;
    // Largest payload span posted as one SQE; keeps progress deadlines
    // meaningful for multi-gigabyte requests.
    size_t io_chunk = 4u << 20;
    int status_timeout_sec = 30;
    int progress_timeout_sec = 30;
    LaneConfig lanes;

    // Reads MC_TCP_URING_WORKERS, MC_TCP_URING_SQPOLL,
    // MC_TCP_URING_ZC_THRESHOLD, MC_TCP_STAGING_CHUNK, MC_TCP_STAGING_DEPTH
    // and the shared timeout knobs from tcp_wire.h.
    static UringConfig fromEnv();
};

struct UringStats {
    std::atomic<uint64_t> submits{0};
    std::atomic<uint64_t> sqes{0};
    std::atomic<uint64_t> cqes{0};
    std::atomic<uint64_t> zc_sends{0};
    std::atomic<uint64_t> zc_bytes{0};
    std::atomic<uint64_t> fixed_sends{0};
    std::atomic<uint64_t> staged_bytes{0};
    std::atomic<uint64_t> short_completions{0};
    std::atomic<uint64_t> accepted{0};
    std::atomic<uint64_t> connects{0};
    std::atomic<uint64_t> requests_served{0};
    std::atomic<uint64_t> slices_completed{0};
    std::atomic<uint64_t> failures{0};
    std::atomic<uint64_t> zc_data_lanes{0};
    std::atomic<uint64_t> zcrx_fragments{0};
    std::atomic<uint64_t> retries{0};

    std::string summary() const;
};

class TcpUringBackend {
   public:
    using ValidateAddrFn = std::function<bool(uint64_t, uint64_t)>;

    // Probes ring creation; returns 0 or -errno (EPERM/ENOSYS under seccomp
    // or on old kernels). The caller falls back to asio on failure.
    static int probe();

    TcpUringBackend(UringConfig config, ValidateAddrFn validate_addr);
    ~TcpUringBackend();

    // Binds one SO_REUSEPORT listener per worker on `data_port` and starts
    // the ring workers. `zero_copy` may be null; when present its data
    // listeners are bound too and its receive queues attached per worker.
    int start(uint16_t data_port, std::shared_ptr<TcpZeroCopy> zero_copy);
    void stop();

    // Enqueues the slices of one task group toward `host:port`. Slices are
    // marked terminal (SUCCESS/FAILED) by the workers; rejected work is
    // failed synchronously.
    void submit(const std::string &host, uint16_t port, LaneWork work,
                uint32_t peer_caps = 0,
                const std::vector<uint16_t> &peer_zc_ports = {});

    // Fixed-buffer registration for host regions (no-op for device memory
    // and for regions that fail registration).
    void registerRegion(void *addr, size_t length);
    void unregisterRegion(void *addr);

    // Listening port actually bound (equals the requested data port).
    uint16_t dataPort() const;

    const UringConfig &config() const { return config_; }
    UringStats &stats() { return stats_; }
    size_t workerCount() const;
    bool running() const;

   private:
    struct Impl;
    UringConfig config_;
    UringStats stats_;
    std::unique_ptr<Impl> impl_;
};

}  // namespace tcp_uring
}  // namespace mooncake

#endif  // TCP_URING_BACKEND_H_

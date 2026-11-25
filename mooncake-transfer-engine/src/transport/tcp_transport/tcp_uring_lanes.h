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

// Per-peer bounded work queues and connection lanes for the io_uring TCP
// backend. The admission policy mirrors the asio lane pool (direct queue,
// pending admissions with a deadline, hard rejection) and reads the same
// MC_TCP_* knobs; the connection state machine is simpler because every
// lane socket is driven by exactly one ring worker.

#ifndef TCP_URING_LANES_H_
#define TCP_URING_LANES_H_

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "transport/transport.h"

namespace mooncake {
namespace tcp_uring {

// The slices of one task group, pipelined on a single lane.
struct LaneWork {
    std::vector<Transport::Slice *> slices;
    bool use_v2 = false;
    // Number of leading slices already reported terminal (SUCCESS or
    // FAILED); the rest are still owned by the lane.
    size_t completed = 0;
    // Connection-level failures re-queue the un-acknowledged tail once, which
    // is what makes reusing a pooled socket the peer closed while idle
    // recoverable. A rejection is not retried: it is already terminal.
    uint32_t attempts = 0;
    // A peer that rejects one request answers it and closes, and the reset
    // discards the status frames of the requests it already accepted, so a
    // pipelined retry cannot tell which of them landed. A retry therefore
    // sends one request per connection: every outcome is then observed
    // before the next request goes out, so each attempt makes progress.
    bool serialize = false;
    std::chrono::steady_clock::time_point admission_deadline{};

    size_t remaining() const { return slices.size() - completed; }
};

// Attempts allowed per work item before the tail is failed.
constexpr uint32_t kMaxLaneAttempts = 3;

enum class LaneFailure {
    QUEUE_FULL,
    QUEUE_TIMEOUT,
    CONNECT_FAILED,
    SESSION_FAILED,
    SHUTDOWN,
};

struct LaneCounters {
    std::atomic<uint64_t> queue_full{0};
    std::atomic<uint64_t> queue_timeout{0};
    std::atomic<uint64_t> connect_failed{0};
    std::atomic<uint64_t> session_failed{0};
    std::atomic<uint64_t> shutdown{0};
};

struct LaneConfig {
    size_t lanes_per_peer = 4;
    size_t max_queued_per_peer = 1024;
    size_t max_pending_per_peer = 1024;
    std::chrono::milliseconds admission_timeout{1000};
    std::chrono::milliseconds reconnect_cooldown{1000};
};

enum class LaneState { DISCONNECTED, CONNECTING, IDLE, BUSY, CLOSED };

struct PeerKey {
    std::string host;
    uint16_t port = 0;
    bool operator==(const PeerKey &o) const {
        return host == o.host && port == o.port;
    }
};

struct PeerKeyHash {
    size_t operator()(const PeerKey &k) const {
        return std::hash<std::string>()(k.host) ^ (size_t(k.port) << 1);
    }
};

class PeerGroup;

// Lane bookkeeping. Only the scheduling state lives here; the socket and its
// in-flight request plan are worker-private, and a work item taken off the
// queue is owned by the worker until it hands the lane back.
struct Lane {
    size_t index = 0;
    size_t worker = 0;  // ring worker that owns this lane's socket
    // Guarded by PeerGroup::mutex.
    LaneState state = LaneState::DISCONNECTED;
    bool pump_posted = false;
};

class PeerGroup {
   public:
    PeerGroup(PeerKey key, const LaneConfig &config, size_t worker_count);

    std::mutex mutex;
    const PeerKey key;
    const size_t home_worker;
    std::vector<Lane> lanes;
    std::deque<LaneWork> queue;
    std::deque<LaneWork> pending;
    std::chrono::steady_clock::time_point cooldown_until{};
    bool timer_requested = false;
    bool closed = false;

    // Peer address resolved once by the first worker that connects; kept as
    // raw bytes so this header stays free of socket includes.
    bool addr_resolved = false;
    bool addr_failed = false;
    uint32_t addr_len = 0;
    unsigned char addr[128] = {};
};

// Fails every remaining slice of `work` and records the reason.
void failLaneWork(LaneWork &work, LaneFailure reason, LaneCounters &counters);

class LanePool {
   public:
    // Asks the owning worker of `lane` to look at the group queue.
    using PumpFn = std::function<void(size_t worker,
                                      const std::shared_ptr<PeerGroup> &group,
                                      size_t lane_index)>;
    // Asks the group's home worker to call tick() at `deadline`.
    using TimerFn =
        std::function<void(const std::shared_ptr<PeerGroup> &group,
                           std::chrono::steady_clock::time_point deadline)>;

    LanePool(LaneConfig config, size_t worker_count, PumpFn pump,
             TimerFn timer);

    const LaneConfig &config() const { return config_; }
    LaneCounters &counters() { return counters_; }

    // Producer side: admits `work` or fails it synchronously. Returns false
    // when the work was rejected.
    bool submit(const PeerKey &key, LaneWork work);
    std::shared_ptr<PeerGroup> groupFor(const PeerKey &key);

    // Re-queues the un-acknowledged tail of `work` after a connection-level
    // failure, or fails it when the attempt budget is spent.
    void resubmit(const PeerKey &key, LaneWork work);

    // Worker side. Every call takes the group mutex internally.
    // Pops the next work item for an IDLE lane, marks it BUSY, and hands
    // ownership to the caller.
    std::optional<LaneWork> takeWork(const std::shared_ptr<PeerGroup> &group,
                                     size_t lane_index);
    // Marks a DISCONNECTED lane CONNECTING when the cooldown allows it.
    bool beginConnect(const std::shared_ptr<PeerGroup> &group,
                      size_t lane_index);
    void connectSucceeded(const std::shared_ptr<PeerGroup> &group,
                          size_t lane_index);
    // Clears a lane's posted-pump flag without taking work, so a wake that
    // arrives while the worker is still busy does not suppress the next one.
    void clearPump(const std::shared_ptr<PeerGroup> &group, size_t lane_index);
    void connectFailed(const std::shared_ptr<PeerGroup> &group,
                       size_t lane_index);
    // Releases a BUSY lane; `clean` keeps the socket for reuse.
    void finishWork(const std::shared_ptr<PeerGroup> &group, size_t lane_index,
                    bool clean);
    // Expires pending admissions, promotes, and pumps idle lanes.
    void tick(const std::shared_ptr<PeerGroup> &group);
    bool queueHasWork(const std::shared_ptr<PeerGroup> &group);

    // Fails all queued work of every group and closes them.
    void shutdown();
    std::vector<std::shared_ptr<PeerGroup>> groups();

   private:
    void expireAndPromoteLocked(PeerGroup &group,
                                std::chrono::steady_clock::time_point now,
                                std::deque<LaneWork> &expired);
    void wakeLanesLocked(const std::shared_ptr<PeerGroup> &group,
                         std::vector<std::pair<size_t, size_t>> &wakes);
    void requestTimerLocked(
        const std::shared_ptr<PeerGroup> &group,
        std::chrono::steady_clock::time_point deadline,
        std::optional<std::chrono::steady_clock::time_point> &timer_request);

    LaneConfig config_;
    size_t worker_count_;
    PumpFn pump_;
    TimerFn timer_;
    LaneCounters counters_;
    std::mutex mutex_;
    bool shutting_down_ = false;
    std::unordered_map<PeerKey, std::shared_ptr<PeerGroup>, PeerKeyHash>
        groups_;
};

}  // namespace tcp_uring
}  // namespace mooncake

#endif  // TCP_URING_LANES_H_

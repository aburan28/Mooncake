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

#include "tcp_uring_lanes.h"

#include <glog/logging.h>

#include <algorithm>
#include <utility>

namespace mooncake {
namespace tcp_uring {

namespace {
bool shouldLogOccurrence(uint64_t occurrence) {
    return occurrence != 0 && (occurrence & (occurrence - 1)) == 0;
}

const char *failureName(LaneFailure reason) {
    switch (reason) {
        case LaneFailure::QUEUE_FULL:
            return "queue full";
        case LaneFailure::QUEUE_TIMEOUT:
            return "admission timeout";
        case LaneFailure::CONNECT_FAILED:
            return "connect failed";
        case LaneFailure::SESSION_FAILED:
            return "session failed";
        case LaneFailure::SHUTDOWN:
            return "shutdown";
    }
    return "unknown";
}

std::atomic<uint64_t> &counterFor(LaneCounters &counters, LaneFailure reason) {
    switch (reason) {
        case LaneFailure::QUEUE_FULL:
            return counters.queue_full;
        case LaneFailure::QUEUE_TIMEOUT:
            return counters.queue_timeout;
        case LaneFailure::CONNECT_FAILED:
            return counters.connect_failed;
        case LaneFailure::SESSION_FAILED:
            return counters.session_failed;
        case LaneFailure::SHUTDOWN:
            return counters.shutdown;
    }
    return counters.session_failed;
}
}  // namespace

void failLaneWork(LaneWork &work, LaneFailure reason, LaneCounters &counters) {
    const uint64_t occurrence =
        counterFor(counters, reason).fetch_add(1, std::memory_order_relaxed) +
        1;
    if (shouldLogOccurrence(occurrence)) {
        LOG(WARNING) << "TcpUring: failing " << work.remaining()
                     << " slice(s): " << failureName(reason) << " (occurrence "
                     << occurrence << ")";
    }
    for (size_t i = work.completed; i < work.slices.size(); ++i)
        work.slices[i]->markFailed();
    work.completed = work.slices.size();
}

PeerGroup::PeerGroup(PeerKey key_arg, const LaneConfig &config,
                     size_t worker_count)
    : key(std::move(key_arg)), home_worker(0) {
    lanes.resize(config.lanes_per_peer);
    for (size_t i = 0; i < lanes.size(); ++i) {
        lanes[i].index = i;
        lanes[i].worker = worker_count ? i % worker_count : 0;
    }
}

LanePool::LanePool(LaneConfig config, size_t worker_count, PumpFn pump,
                   TimerFn timer)
    : config_(config),
      worker_count_(worker_count),
      pump_(std::move(pump)),
      timer_(std::move(timer)) {}

void LanePool::expireAndPromoteLocked(PeerGroup &group,
                                      std::chrono::steady_clock::time_point now,
                                      std::deque<LaneWork> &expired) {
    while (!group.pending.empty() &&
           group.pending.front().admission_deadline <= now) {
        expired.emplace_back(std::move(group.pending.front()));
        group.pending.pop_front();
    }
    while (!group.pending.empty() &&
           group.queue.size() < config_.max_queued_per_peer) {
        group.queue.emplace_back(std::move(group.pending.front()));
        group.pending.pop_front();
    }
}

void LanePool::requestTimerLocked(
    const std::shared_ptr<PeerGroup> &group,
    std::chrono::steady_clock::time_point deadline,
    std::optional<std::chrono::steady_clock::time_point> &timer_request) {
    if (group->timer_requested || group->closed) return;
    group->timer_requested = true;
    timer_request = deadline;
}

void LanePool::wakeLanesLocked(const std::shared_ptr<PeerGroup> &group,
                               std::vector<std::pair<size_t, size_t>> &wakes) {
    size_t budget = group->queue.size();
    if (budget == 0 || group->closed) return;
    // Connected lanes first, then lanes that need a connection.
    for (int pass = 0; pass < 2 && budget > 0; ++pass) {
        const LaneState wanted =
            pass == 0 ? LaneState::IDLE : LaneState::DISCONNECTED;
        for (auto &lane : group->lanes) {
            if (budget == 0) break;
            if (lane.state != wanted || lane.pump_posted) continue;
            lane.pump_posted = true;
            wakes.emplace_back(lane.worker, lane.index);
            --budget;
        }
    }
}

bool LanePool::submit(const PeerKey &key, LaneWork work) {
    std::shared_ptr<PeerGroup> group;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (shutting_down_) {
            failLaneWork(work, LaneFailure::SHUTDOWN, counters_);
            return false;
        }
        auto it = groups_.find(key);
        if (it == groups_.end()) {
            group = std::make_shared<PeerGroup>(key, config_, worker_count_);
            groups_.emplace(key, group);
        } else {
            group = it->second;
        }
    }

    std::deque<LaneWork> expired;
    std::optional<LaneWork> rejected;
    LaneFailure rejection_reason = LaneFailure::QUEUE_FULL;
    std::vector<std::pair<size_t, size_t>> wakes;
    std::optional<std::chrono::steady_clock::time_point> timer_request;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        if (group->closed) {
            rejected.emplace(std::move(work));
            rejection_reason = LaneFailure::SHUTDOWN;
        } else {
            const auto now = std::chrono::steady_clock::now();
            expireAndPromoteLocked(*group, now, expired);
            if (group->pending.empty() &&
                group->queue.size() < config_.max_queued_per_peer) {
                group->queue.emplace_back(std::move(work));
            } else if (group->pending.size() < config_.max_pending_per_peer) {
                work.admission_deadline = now + config_.admission_timeout;
                group->pending.emplace_back(std::move(work));
            } else {
                rejected.emplace(std::move(work));
            }
            if (!group->pending.empty())
                requestTimerLocked(group,
                                   group->pending.front().admission_deadline,
                                   timer_request);
            wakeLanesLocked(group, wakes);
        }
    }

    for (auto &item : expired)
        failLaneWork(item, LaneFailure::QUEUE_TIMEOUT, counters_);
    if (rejected) failLaneWork(*rejected, rejection_reason, counters_);
    for (const auto &[worker, lane] : wakes) pump_(worker, group, lane);
    if (timer_request) timer_(group, *timer_request);
    return !rejected;
}

std::optional<LaneWork> LanePool::takeWork(
    const std::shared_ptr<PeerGroup> &group, size_t lane_index) {
    std::deque<LaneWork> expired;
    std::optional<std::chrono::steady_clock::time_point> timer_request;
    std::optional<LaneWork> taken;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        Lane &lane = group->lanes[lane_index];
        lane.pump_posted = false;
        if (lane.state == LaneState::IDLE && !group->queue.empty() &&
            !group->closed) {
            taken.emplace(std::move(group->queue.front()));
            group->queue.pop_front();
            lane.state = LaneState::BUSY;
            expireAndPromoteLocked(*group, std::chrono::steady_clock::now(),
                                   expired);
            if (!group->pending.empty())
                requestTimerLocked(group,
                                   group->pending.front().admission_deadline,
                                   timer_request);
        }
    }
    for (auto &item : expired)
        failLaneWork(item, LaneFailure::QUEUE_TIMEOUT, counters_);
    if (timer_request) timer_(group, *timer_request);
    return taken;
}

bool LanePool::beginConnect(const std::shared_ptr<PeerGroup> &group,
                            size_t lane_index) {
    std::optional<std::chrono::steady_clock::time_point> timer_request;
    bool begin = false;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        Lane &lane = group->lanes[lane_index];
        lane.pump_posted = false;
        if (lane.state == LaneState::DISCONNECTED && !group->queue.empty() &&
            !group->closed) {
            const auto now = std::chrono::steady_clock::now();
            if (now < group->cooldown_until) {
                requestTimerLocked(group, group->cooldown_until, timer_request);
            } else {
                lane.state = LaneState::CONNECTING;
                begin = true;
            }
        }
    }
    if (timer_request) timer_(group, *timer_request);
    return begin;
}

void LanePool::connectSucceeded(const std::shared_ptr<PeerGroup> &group,
                                size_t lane_index) {
    std::lock_guard<std::mutex> lock(group->mutex);
    Lane &lane = group->lanes[lane_index];
    if (lane.state == LaneState::CONNECTING) lane.state = LaneState::IDLE;
}

void LanePool::clearPump(const std::shared_ptr<PeerGroup> &group,
                         size_t lane_index) {
    std::lock_guard<std::mutex> lock(group->mutex);
    group->lanes[lane_index].pump_posted = false;
}

void LanePool::connectFailed(const std::shared_ptr<PeerGroup> &group,
                             size_t lane_index) {
    std::deque<LaneWork> failed;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        Lane &lane = group->lanes[lane_index];
        if (lane.state == LaneState::CONNECTING)
            lane.state = LaneState::DISCONNECTED;
        group->cooldown_until =
            std::chrono::steady_clock::now() + config_.reconnect_cooldown;
        const bool any_usable = std::any_of(
            group->lanes.begin(), group->lanes.end(), [](const Lane &l) {
                return l.state == LaneState::IDLE ||
                       l.state == LaneState::BUSY ||
                       l.state == LaneState::CONNECTING;
            });
        if (!any_usable) {
            failed.swap(group->queue);
            while (!group->pending.empty()) {
                failed.emplace_back(std::move(group->pending.front()));
                group->pending.pop_front();
            }
        }
    }
    for (auto &item : failed)
        failLaneWork(item, LaneFailure::CONNECT_FAILED, counters_);
}

void LanePool::finishWork(const std::shared_ptr<PeerGroup> &group,
                          size_t lane_index, bool clean) {
    std::vector<std::pair<size_t, size_t>> wakes;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        Lane &lane = group->lanes[lane_index];
        if (group->closed) {
            lane.state = LaneState::CLOSED;
        } else {
            lane.state = clean ? LaneState::IDLE : LaneState::DISCONNECTED;
            wakeLanesLocked(group, wakes);
        }
    }
    for (const auto &[worker, lane] : wakes) pump_(worker, group, lane);
}

void LanePool::resubmit(const PeerKey &key, LaneWork work) {
    if (work.remaining() == 0) return;
    if (++work.attempts >= kMaxLaneAttempts) {
        failLaneWork(work, LaneFailure::SESSION_FAILED, counters_);
        return;
    }
    submit(key, std::move(work));
}

std::shared_ptr<PeerGroup> LanePool::groupFor(const PeerKey &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = groups_.find(key);
    return it == groups_.end() ? nullptr : it->second;
}

void LanePool::tick(const std::shared_ptr<PeerGroup> &group) {
    std::deque<LaneWork> expired;
    std::vector<std::pair<size_t, size_t>> wakes;
    std::optional<std::chrono::steady_clock::time_point> timer_request;
    {
        std::lock_guard<std::mutex> lock(group->mutex);
        group->timer_requested = false;
        if (!group->closed) {
            expireAndPromoteLocked(*group, std::chrono::steady_clock::now(),
                                   expired);
            if (!group->pending.empty())
                requestTimerLocked(group,
                                   group->pending.front().admission_deadline,
                                   timer_request);
            wakeLanesLocked(group, wakes);
        }
    }
    for (auto &item : expired)
        failLaneWork(item, LaneFailure::QUEUE_TIMEOUT, counters_);
    for (const auto &[worker, lane] : wakes) pump_(worker, group, lane);
    if (timer_request) timer_(group, *timer_request);
}

bool LanePool::queueHasWork(const std::shared_ptr<PeerGroup> &group) {
    std::lock_guard<std::mutex> lock(group->mutex);
    return !group->queue.empty();
}

std::vector<std::shared_ptr<PeerGroup>> LanePool::groups() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::shared_ptr<PeerGroup>> out;
    out.reserve(groups_.size());
    for (auto &entry : groups_) out.push_back(entry.second);
    return out;
}

void LanePool::shutdown() {
    std::vector<std::shared_ptr<PeerGroup>> all;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutting_down_ = true;
        for (auto &entry : groups_) all.push_back(entry.second);
    }
    for (auto &group : all) {
        std::deque<LaneWork> failed;
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            group->closed = true;
            failed.swap(group->queue);
            while (!group->pending.empty()) {
                failed.emplace_back(std::move(group->pending.front()));
                group->pending.pop_front();
            }
            for (auto &lane : group->lanes) {
                if (lane.state != LaneState::BUSY &&
                    lane.state != LaneState::CONNECTING)
                    lane.state = LaneState::CLOSED;
            }
        }
        for (auto &item : failed)
            failLaneWork(item, LaneFailure::SHUTDOWN, counters_);
    }
}

}  // namespace tcp_uring
}  // namespace mooncake

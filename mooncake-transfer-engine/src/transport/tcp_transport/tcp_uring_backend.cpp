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

#include "tcp_uring_backend.h"

#include <endian.h>
#include <glog/logging.h>
#include <liburing.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <utility>

#include "cuda_alike.h"
#include "tcp_zero_copy.h"
#include "transport/tcp_transport/tcp_wire.h"

namespace mooncake {
namespace tcp_uring {

using tcp_wire::kOpcodeV2Flag;
using tcp_wire::kSessionHeaderWireSize;
using tcp_wire::kStatusAddrRejected;
using tcp_wire::kStatusFrameSize;
using tcp_wire::kStatusOk;
using tcp_wire::SessionHeader;
using tcp_wire::statusFrameValid;
using TransferRequest = Transport::TransferRequest;

namespace {

constexpr unsigned kSqEntries = 4096;
// Buffer-table slots shared by every worker. A slot covers at most 1 GiB, so
// this bounds fixed-buffer registration at 256 GiB of host memory; past that
// the transport keeps working, just without zero-copy sends.
constexpr unsigned kMaxFixedBuffers = 256;
constexpr uint64_t kMaxFixedBufferSpan = 1ull << 30;
// SQEs in one linked chain. A chain is submitted and drained as a unit, so
// this bounds both SQ pressure and how much work one failure re-posts.
constexpr size_t kMaxChainOps = 128;
// Smallest payload worth splitting across a zero-copy data lane.
constexpr uint64_t kZeroCopyDataLaneMinBytes = 1u << 20;

size_t parseSizeEnv(const char *name, size_t def, size_t lo, size_t hi) {
    const char *env = std::getenv(name);
    if (!env || !*env) return def;
    char *end = nullptr;
    const unsigned long long value = std::strtoull(env, &end, 10);
    if (end == env || *end != '\0' || value < lo || value > hi) {
        LOG(WARNING) << "Invalid " << name << " value: " << env
                     << ", using default " << def;
        return def;
    }
    return static_cast<size_t>(value);
}

bool envFlag(const char *name) {
    const char *env = std::getenv(name);
    return env && env[0] == '1' && env[1] == '\0';
}

void setSocketOptions(int fd) {
    const int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_HIP) ||  \
    defined(USE_MLU) || defined(USE_MACA) || defined(USE_HYGON) || \
    defined(USE_COREX)
#define MOONCAKE_TCP_URING_DEVICE_STAGING 1

// Returns the device ordinal when addr is device memory, or -1. Callers must
// cudaSetDevice before issuing copies so no implicit GPU 0 context is made.
int deviceIdFor(void *addr) {
    cudaPointerAttributes attributes;
    if (cudaPointerGetAttributes(&attributes, addr) != cudaSuccess) return -1;
    return attributes.type == cudaMemoryTypeDevice ? attributes.device : -1;
}
#else
int deviceIdFor(void *) { return -1; }
#endif

}  // namespace

UringConfig UringConfig::fromEnv() {
    UringConfig config;
    const unsigned ncpu = std::max(1u, std::thread::hardware_concurrency());
    const size_t default_workers =
        std::max<size_t>(1, std::min<size_t>(4, ncpu / 8));
    config.workers =
        parseSizeEnv("MC_TCP_URING_WORKERS", default_workers, 1, 64);
    config.sqpoll = envFlag("MC_TCP_URING_SQPOLL");
    // Zero-copy send pins and later releases the source pages, which costs
    // more than the copy it avoids until the payload is large: the kernel's
    // own guidance puts the crossover near 256 KiB, and measurements on
    // loopback (where no NIC DMA exists to win back) show it losing at every
    // size. Deployments on a real NIC can lower this.
    config.zc_threshold =
        parseSizeEnv("MC_TCP_URING_ZC_THRESHOLD", 256u << 10, 0, 1ull << 30);
    config.staging_chunk =
        parseSizeEnv("MC_TCP_STAGING_CHUNK", 2u << 20, 4096, 256u << 20);
    config.staging_depth = parseSizeEnv("MC_TCP_STAGING_DEPTH", 4, 1, 64);
    config.pipeline = parseSizeEnv("MC_TCP_URING_PIPELINE", 16, 1, 256);
    config.io_chunk =
        parseSizeEnv("MC_TCP_URING_IO_CHUNK", 4u << 20, 4096, 256u << 20);
    config.status_timeout_sec = tcp_wire::statusFrameTimeoutSec();
    config.progress_timeout_sec = tcp_wire::progressTimeoutSec();

    config.lanes.lanes_per_peer =
        parseSizeEnv("MC_TCP_LANES_PER_PEER", 4, 1, 16);
    config.lanes.max_queued_per_peer =
        parseSizeEnv("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", 1024, 1, 65535);
    config.lanes.max_pending_per_peer =
        parseSizeEnv("MC_TCP_MAX_PENDING_ADMISSIONS_PER_PEER", 1024, 1, 65535);
    config.lanes.admission_timeout = std::chrono::milliseconds(
        parseSizeEnv("MC_TCP_ADMISSION_TIMEOUT_MS", 1000, 1, 600000));
    return config;
}

std::string UringStats::summary() const {
    std::ostringstream oss;
    oss << "submits=" << submits.load() << " sqes=" << sqes.load()
        << " cqes=" << cqes.load() << " zc_sends=" << zc_sends.load()
        << " zc_bytes=" << zc_bytes.load()
        << " fixed_sends=" << fixed_sends.load()
        << " staged_bytes=" << staged_bytes.load()
        << " short=" << short_completions.load()
        << " accepted=" << accepted.load() << " connects=" << connects.load()
        << " served=" << requests_served.load()
        << " slices=" << slices_completed.load()
        << " retries=" << retries.load() << " failures=" << failures.load();
    if (zc_data_lanes.load())
        oss << " zc_lanes=" << zc_data_lanes.load()
            << " zc_frags=" << zcrx_fragments.load();
    return oss.str();
}

// ---------------------------------------------------------------------------
// Per-connection request plan
// ---------------------------------------------------------------------------

namespace {

enum class StepKind : uint8_t {
    kSendHdr,
    kRecvHdr,
    kSendStatus,
    kRecvStatus,
    kSendPay,
    kRecvPay,
};

bool isSend(StepKind kind) {
    return kind == StepKind::kSendHdr || kind == StepKind::kSendStatus ||
           kind == StepKind::kSendPay;
}

struct PlanStep {
    StepKind kind = StepKind::kRecvHdr;
    // Index of the request this step belongs to: absolute in
    // LaneWork::slices on a client, always 0 on a server.
    uint32_t req = 0;
    uint64_t offset = 0;  // payload offset within the request
    uint64_t len = 0;
    uint64_t done = 0;
    // Completing this step makes its request successful.
    bool completes_request = false;
    // Payload staged through pinned host memory (device-resident endpoint).
    bool staged = false;
    uint32_t chunk = 0;         // staging slot ordinal
    bool copy_pending = false;  // staging copy outstanding

    uint64_t rest() const { return len - done; }
};

enum class ReqStatus : uint8_t { kUnknown, kOk, kRejected };

struct Conn {
    enum class Role : uint8_t { kClient, kServer };

    Role role = Role::kServer;
    int fd = -1;
    uint32_t inflight = 0;
    bool have_error = false;
    int error = 0;
    bool want_post = false;
    std::chrono::steady_clock::time_point deadline{};

    std::vector<PlanStep> steps;
    size_t cursor = 0;

    // Client state
    std::shared_ptr<PeerGroup> group;
    size_t lane_index = 0;
    struct sockaddr_storage peer_addr{};
    socklen_t peer_addr_len = 0;
    LaneWork work;
    bool has_work = false;
    size_t window_begin = 0;
    size_t window_end = 0;
    std::vector<SessionHeader> headers;
    std::vector<uint64_t> frames;
    std::vector<ReqStatus> req_status;

    // Server state
    SessionHeader hdr{};
    uint64_t status_out = 0;
    char *srv_addr = nullptr;
    uint64_t srv_size = 0;
    bool srv_v2 = false;
    bool close_after_plan = false;

    // Device staging (both roles)
    int device_id = -1;
    bool staged = false;

    void setError(int err) {
        if (have_error) return;
        have_error = true;
        error = err;
    }
};

enum class OpKind : uint8_t {
    kNone,
    kAcceptMulti,
    kEventfd,
    kTick,
    kConnect,
    kIo,
    kLinkTimeout,
};

struct Op {
    OpKind kind = OpKind::kNone;
    uint32_t index = 0;
    uint32_t gen = 0;
    bool in_use = false;
    Conn *conn = nullptr;
    uint32_t step = 0;
    uint64_t expect = 0;
    bool zc = false;
    struct __kernel_timespec ts{};
};

// Op records are recycled, so user_data carries a generation alongside the
// slot index: a completion for a released record is then detectable instead
// of landing on an unrelated request.
class OpSlab {
   public:
    Op *alloc(uint64_t *user_data) {
        uint32_t index;
        if (!free_.empty()) {
            index = free_.back();
            free_.pop_back();
        } else {
            index = static_cast<uint32_t>(ops_.size());
            ops_.emplace_back();
            ops_.back().index = index;
        }
        Op &op = ops_[index];
        const uint32_t next_gen = op.gen + 1 ? op.gen + 1 : 1;
        op = Op{};
        op.index = index;
        op.gen = next_gen;
        op.in_use = true;
        *user_data = (static_cast<uint64_t>(index) << 32) | next_gen;
        return &op;
    }

    Op *lookup(uint64_t user_data) {
        const uint32_t index = static_cast<uint32_t>(user_data >> 32);
        const uint32_t gen = static_cast<uint32_t>(user_data);
        if (index >= ops_.size()) return nullptr;
        Op &op = ops_[index];
        if (!op.in_use || op.gen != gen) return nullptr;
        return &op;
    }

    void release(Op *op) {
        op->in_use = false;
        free_.push_back(op->index);
    }

   private:
    // A deque keeps element addresses stable: Op::ts is handed to the kernel
    // and must outlive submission.
    std::deque<Op> ops_;
    std::vector<uint32_t> free_;
};

struct RegionEntry {
    uint64_t addr = 0;
    uint64_t length = 0;
    uint64_t origin = 0;  // address passed to registerLocalMemory
    uint32_t slot = 0;
};

struct Mail {
    enum class Kind : uint8_t { kPump, kTimer, kRegions, kStop };
    Kind kind = Kind::kPump;
    std::shared_ptr<PeerGroup> group;
    size_t lane_index = 0;
    std::chrono::steady_clock::time_point deadline{};
};

#ifdef MOONCAKE_TCP_URING_DEVICE_STAGING
// One pool per worker per device: `depth` pinned chunks with a CUDA event
// each, plus one stream. The copy for chunk k+1 runs while chunk k is on the
// wire, which removes the synchronous per-64-KiB cudaMemcpy of the asio
// backend (issue #2843).
class StagingPool {
   public:
    ~StagingPool() {
        for (auto &slot : slots_) {
            if (slot.event) cudaEventDestroy(slot.event);
            if (slot.host) cudaFreeHost(slot.host);
        }
        if (stream_) cudaStreamDestroy(stream_);
    }

    bool init(int device, size_t chunk, size_t depth) {
        device_ = device;
        chunk_ = chunk;
        if (cudaSetDevice(device) != cudaSuccess) return false;
        if (cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking) !=
            cudaSuccess)
            return false;
        slots_.resize(depth);
        for (auto &slot : slots_) {
            if (cudaHostAlloc(reinterpret_cast<void **>(&slot.host), chunk,
                              cudaHostAllocPortable) != cudaSuccess)
                return false;
            if (cudaEventCreateWithFlags(&slot.event, cudaEventDisableTiming) !=
                cudaSuccess)
                return false;
        }
        return true;
    }

    size_t depth() const { return slots_.size(); }
    char *host(uint32_t ordinal) {
        return slots_[ordinal % slots_.size()].host;
    }

    bool startCopy(uint32_t ordinal, void *dst, const void *src, size_t bytes) {
        Slot &slot = slots_[ordinal % slots_.size()];
        if (cudaSetDevice(device_) != cudaSuccess) return false;
        if (cudaMemcpyAsync(dst, src, bytes, cudaMemcpyDefault, stream_) !=
            cudaSuccess)
            return false;
        if (cudaEventRecord(slot.event, stream_) != cudaSuccess) return false;
        slot.busy = true;
        return true;
    }

    // 1 when the copy finished, 0 while it runs, -1 on error.
    int poll(uint32_t ordinal) {
        Slot &slot = slots_[ordinal % slots_.size()];
        if (!slot.busy) return 1;
        const cudaError_t status = cudaEventQuery(slot.event);
        if (status == cudaSuccess) {
            slot.busy = false;
            return 1;
        }
        if (status == cudaErrorNotReady) return 0;
        LOG(ERROR) << "TcpUring staging: copy failed: "
                   << cudaGetErrorString(status);
        slot.busy = false;
        return -1;
    }

   private:
    struct Slot {
        char *host = nullptr;
        cudaEvent_t event = nullptr;
        bool busy = false;
    };
    int device_ = 0;
    size_t chunk_ = 0;
    cudaStream_t stream_ = nullptr;
    std::vector<Slot> slots_;
};
#endif  // MOONCAKE_TCP_URING_DEVICE_STAGING

}  // namespace

// ---------------------------------------------------------------------------
// Backend implementation
// ---------------------------------------------------------------------------

struct TcpUringBackend::Impl {
    class Worker;

    Impl(UringConfig cfg, UringStats *stats_arg, ValidateAddrFn validate)
        : config(std::move(cfg)),
          stats(stats_arg),
          validate_addr(std::move(validate)) {}
    ~Impl();

    UringConfig config;
    UringStats *stats;
    ValidateAddrFn validate_addr;
    // Zero-copy send pins the source pages against RLIMIT_MEMLOCK until the
    // notification lands. Containers commonly cap that at a few MiB, far below
    // one transfer's worth of chunks, and the kernel then answers a send with
    // ENOMEM. That is a statement about the environment, not about the
    // transfer, so the first one retires zero copy for the process and every
    // send afterwards copies.
    std::atomic<bool> zc_unavailable{false};
    std::shared_ptr<TcpZeroCopy> zero_copy;
    std::unique_ptr<LanePool> lanes;
    std::vector<std::unique_ptr<Worker>> workers;
    bool running = false;
    uint16_t port = 0;

    // Fixed-buffer registry shared by every worker.
    std::mutex region_mutex;
    std::vector<RegionEntry> regions;  // sorted by addr
    std::vector<bool> slot_used = std::vector<bool>(kMaxFixedBuffers, false);
    uint64_t region_version = 0;
    bool slots_exhausted_warned = false;

    void post(size_t worker, Mail mail);
    void bumpRegions();
};

class TcpUringBackend::Impl::Worker {
   public:
    Worker(Impl *impl, size_t index) : impl_(impl), index_(index) {}
    ~Worker() { join(); }

    int setup(int listen_fd) {
        listen_fd_ = listen_fd;
        event_fd_ = eventfd(0, EFD_CLOEXEC);
        if (event_fd_ < 0) {
            PLOG(ERROR) << "TcpUring worker " << index_ << ": eventfd failed";
            return -errno;
        }
        return 0;
    }

    void start() {
        thread_ = std::thread([this] { run(); });
    }

    void requestStop() {
        stop_.store(true, std::memory_order_release);
        post(Mail{Mail::Kind::kStop, nullptr, 0, {}});
    }

    void join() {
        if (thread_.joinable()) thread_.join();
        if (event_fd_ >= 0) {
            close(event_fd_);
            event_fd_ = -1;
        }
        if (listen_fd_ >= 0) {
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    void post(Mail mail) {
        bool wake;
        {
            std::lock_guard<std::mutex> lock(mailbox_mutex_);
            mailbox_.push_back(std::move(mail));
            wake = std::this_thread::get_id() != thread_id_;
        }
        if (wake && event_fd_ >= 0) {
            const uint64_t one = 1;
            const ssize_t rc = write(event_fd_, &one, sizeof(one));
            (void)rc;
        }
    }

    bool ringFailed() const {
        return ring_failed_.load(std::memory_order_acquire);
    }

    void waitStarted() {
        while (!started_.load(std::memory_order_acquire))
            std::this_thread::yield();
    }

    uint64_t appliedRegionVersion() const {
        return applied_region_version_.load(std::memory_order_acquire);
    }

   private:
    // --- ring setup --------------------------------------------------------

    bool initRing() {
        unsigned flags = IORING_SETUP_SINGLE_ISSUER;
        if (impl_->config.sqpoll)
            flags |= IORING_SETUP_SQPOLL;
        else
            flags |= IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_COOP_TASKRUN;

        struct io_uring_params params;
        memset(&params, 0, sizeof(params));
        params.flags = flags;
        if (impl_->config.sqpoll) params.sq_thread_idle = 1000;
        int ret = io_uring_queue_init_params(kSqEntries, &ring_, &params);
        if (ret == -EINVAL || ret == -EPERM) {
            LOG(WARNING) << "TcpUring worker " << index_
                         << ": ring flags rejected (" << strerror(-ret)
                         << "); retrying with a plain ring";
            memset(&params, 0, sizeof(params));
            ret = io_uring_queue_init_params(kSqEntries, &ring_, &params);
        }
        if (ret < 0) {
            LOG(ERROR) << "TcpUring worker " << index_
                       << ": io_uring_queue_init failed: " << strerror(-ret);
            return false;
        }
        ring_ready_ = true;

        if (io_uring_register_ring_fd(&ring_) < 0)
            VLOG(1) << "TcpUring worker " << index_
                    << ": ring fd registration unavailable";
        if (io_uring_register_files_sparse(&ring_, 1024) < 0)
            VLOG(1) << "TcpUring worker " << index_
                    << ": sparse file table unavailable";
        if (io_uring_register_buffers_sparse(&ring_, kMaxFixedBuffers) < 0) {
            VLOG(1) << "TcpUring worker " << index_
                    << ": sparse buffer table unavailable; fixed buffers off";
            fixed_buffers_ = false;
        }
        return true;
    }

    // --- submission helpers ------------------------------------------------

    void submit() {
        if (pending_sqes_ == 0) return;
        const unsigned batch = pending_sqes_;
        pending_sqes_ = 0;
        const int ret = io_uring_submit(&ring_);
        if (ret < 0) {
            LOG_EVERY_N(ERROR, 64)
                << "TcpUring worker " << index_
                << ": io_uring_submit failed: " << strerror(-ret);
        }
        impl_->stats->submits.fetch_add(1, std::memory_order_relaxed);
        impl_->stats->sqes.fetch_add(batch, std::memory_order_relaxed);
    }

    struct io_uring_sqe *nextSqe() {
        struct io_uring_sqe *s = io_uring_get_sqe(&ring_);
        if (!s) {
            submit();
            s = io_uring_get_sqe(&ring_);
        }
        return s;
    }

    Op *newOp(OpKind kind, struct io_uring_sqe *s) {
        uint64_t user_data = 0;
        Op *op = slab_.alloc(&user_data);
        op->kind = kind;
        io_uring_sqe_set_data64(s, user_data);
        ++pending_sqes_;
        return op;
    }

    void armAccept() {
        if (listen_fd_ < 0) return;
        struct io_uring_sqe *s = nextSqe();
        if (!s) return;
        io_uring_prep_multishot_accept(s, listen_fd_, nullptr, nullptr, 0);
        newOp(OpKind::kAcceptMulti, s);
    }

    void armEventfd() {
        struct io_uring_sqe *s = nextSqe();
        if (!s) return;
        io_uring_prep_read(s, event_fd_, &event_buf_, sizeof(event_buf_), 0);
        newOp(OpKind::kEventfd, s);
    }

    void armTick() {
        struct io_uring_sqe *s = nextSqe();
        if (!s) return;
        Op *op = newOp(OpKind::kTick, s);
        op->ts.tv_sec = 0;
        op->ts.tv_nsec = 200l * 1000 * 1000;
        io_uring_prep_timeout(s, &op->ts, 0, 0);
    }

    // --- main loop ---------------------------------------------------------

    void run() {
        thread_id_ = std::this_thread::get_id();
        if (!initRing()) {
            ring_failed_.store(true, std::memory_order_release);
            started_.store(true, std::memory_order_release);
            return;
        }
        syncRegions();
        armAccept();
        armEventfd();
        armTick();
        submit();
        started_.store(true, std::memory_order_release);

        while (!stop_.load(std::memory_order_acquire)) {
            drainMailbox();
            servicePendingPosts();
            submit();

            const int ret = io_uring_submit_and_wait(&ring_, 1);
            if (ret < 0 && ret != -EINTR && ret != -ETIME) {
                LOG_EVERY_N(ERROR, 64)
                    << "TcpUring worker " << index_
                    << ": submit_and_wait failed: " << strerror(-ret);
            }

            struct io_uring_cqe *cqe = nullptr;
            unsigned head = 0;
            unsigned count = 0;
            io_uring_for_each_cqe(&ring_, head, cqe) {
                ++count;
                handleCqe(cqe);
            }
            if (count) {
                io_uring_cq_advance(&ring_, count);
                impl_->stats->cqes.fetch_add(count, std::memory_order_relaxed);
            }
            pollStaging();
        }

        teardown();
    }

    void teardown() {
        // Publish a terminal state for everything this worker still owns
        // before the ring goes away; in-flight SQEs are cancelled by exit.
        std::vector<Conn *> conns(conns_.begin(), conns_.end());
        for (Conn *conn : conns) {
            if (conn->role == Conn::Role::kClient && conn->has_work) {
                failLaneWork(conn->work, LaneFailure::SHUTDOWN,
                             impl_->lanes->counters());
                conn->has_work = false;
            }
            if (conn->fd >= 0) close(conn->fd);
            delete conn;
        }
        conns_.clear();
        lane_conns_.clear();
        if (ring_ready_) io_uring_queue_exit(&ring_);
        ring_ready_ = false;
    }

    void drainMailbox() {
        std::vector<Mail> mail;
        {
            std::lock_guard<std::mutex> lock(mailbox_mutex_);
            mail.swap(mailbox_);
        }
        for (auto &item : mail) {
            switch (item.kind) {
                case Mail::Kind::kPump:
                    pumpLane(item.group, item.lane_index);
                    break;
                case Mail::Kind::kTimer:
                    timers_.emplace(item.deadline, item.group);
                    break;
                case Mail::Kind::kRegions:
                    syncRegions();
                    break;
                case Mail::Kind::kStop:
                    break;
            }
        }
    }

    void servicePendingPosts() {
        if (repost_.empty()) return;
        std::vector<Conn *> conns;
        conns.swap(repost_);
        for (Conn *conn : conns) {
            if (!conns_.count(conn)) continue;
            conn->want_post = false;
            if (conn->inflight == 0 && !conn->have_error) postChain(conn);
        }
    }

    void requestPost(Conn *conn) {
        if (conn->want_post) return;
        conn->want_post = true;
        repost_.push_back(conn);
    }

    // --- completion dispatch ----------------------------------------------

    void handleCqe(struct io_uring_cqe *cqe) {
        Op *op = slab_.lookup(io_uring_cqe_get_data64(cqe));
        if (!op) return;
        switch (op->kind) {
            case OpKind::kAcceptMulti:
                onAccept(op, cqe);
                return;
            case OpKind::kEventfd:
                slab_.release(op);
                armEventfd();
                return;
            case OpKind::kTick:
                slab_.release(op);
                runTick();
                armTick();
                return;
            case OpKind::kConnect:
                onConnect(op, cqe);
                return;
            case OpKind::kLinkTimeout:
                onLinkTimeout(op, cqe);
                return;
            case OpKind::kIo:
                onIo(op, cqe);
                return;
            case OpKind::kNone:
                slab_.release(op);
                return;
        }
    }

    void onAccept(Op *op, struct io_uring_cqe *cqe) {
        const bool more = (cqe->flags & IORING_CQE_F_MORE) != 0;
        if (!more) slab_.release(op);
        if (cqe->res >= 0) {
            impl_->stats->accepted.fetch_add(1, std::memory_order_relaxed);
            startServerConn(cqe->res);
        } else if (cqe->res != -ECANCELED) {
            LOG_EVERY_N(WARNING, 64)
                << "TcpUring worker " << index_
                << ": accept failed: " << strerror(-cqe->res);
        }  // NOLINT
        if (!more && !stop_.load(std::memory_order_acquire)) armAccept();
    }

    void onConnect(Op *op, struct io_uring_cqe *cqe) {
        Conn *conn = op->conn;
        const int res = cqe->res;
        slab_.release(op);
        if (!conn || !conns_.count(conn)) return;
        if (conn->inflight) conn->inflight--;
        if (res < 0) {
            LOG_EVERY_N(WARNING, 64)
                << "TcpUring: connect to " << conn->group->key.host << ":"
                << conn->group->key.port << " failed: " << strerror(-res);
            auto group = conn->group;
            const size_t lane_index = conn->lane_index;
            destroyConn(conn);
            impl_->lanes->connectFailed(group, lane_index);
            return;
        }
        impl_->stats->connects.fetch_add(1, std::memory_order_relaxed);
        setSocketOptions(conn->fd);
        impl_->lanes->connectSucceeded(conn->group, conn->lane_index);
        startWindow(conn);
    }

    void onLinkTimeout(Op *op, struct io_uring_cqe *cqe) {
        Conn *conn = op->conn;
        const int res = cqe->res;
        slab_.release(op);
        if (!conn || !conns_.count(conn)) return;
        if (res == -ETIME) {
            LOG(ERROR) << "TcpUring: no status frame within "
                       << impl_->config.status_timeout_sec
                       << "s (peer likely speaks the legacy protocol); "
                          "dropping connection";
            conn->setError(ETIMEDOUT);
        }
        finishOp(conn);
    }

    void onIo(Op *op, struct io_uring_cqe *cqe) {
        Conn *conn = op->conn;
        if (!conn || !conns_.count(conn)) {
            if (!(cqe->flags & IORING_CQE_F_MORE)) slab_.release(op);
            return;
        }
        if (cqe->flags & IORING_CQE_F_NOTIF) {
            // Zero-copy notification: the source pages are free again.
            slab_.release(op);
            finishOp(conn);
            return;
        }
        recordIoResult(conn, op, cqe->res);
        if (op->zc) {
            if (cqe->flags & IORING_CQE_F_MORE) {
                // The transfer result landed and the notification is still
                // owed, so retire only the reservation this CQE accounts for
                // and keep the op alive for the notification.
                finishOp(conn);
                return;
            }
            // A zero-copy send that never took a reference on the source
            // pages emits no notification, so release the reservation that
            // notification would have consumed.
            if (conn->inflight > 0) conn->inflight--;
        }
        slab_.release(op);
        finishOp(conn);
    }

    void recordIoResult(Conn *conn, Op *op, int res) {
        if (op->step >= conn->steps.size()) return;
        PlanStep &step = conn->steps[op->step];
        if (res < 0) {
            if (op->zc && res == -ENOMEM) {
                // Out of locked memory, not out of sockets: leave the step
                // unfinished so the drain re-posts it as a copying send.
                if (!impl_->zc_unavailable.exchange(
                        true, std::memory_order_relaxed)) {
                    LOG(WARNING)
                        << "TcpUring: zero-copy send hit the locked-memory "
                           "limit (RLIMIT_MEMLOCK); falling back to copying "
                           "sends. Raise the limit to keep zero copy.";
                }
                return;
            }
            // A broken link cancels everything behind the failure; the tail
            // is simply re-posted once the chain has drained.
            if (res != -ECANCELED) conn->setError(-res);
            return;
        }
        if (res == 0) {
            conn->setError(ECONNRESET);  // EOF before the requested bytes
            return;
        }
        step.done += static_cast<uint64_t>(res);
        conn->deadline =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(impl_->config.progress_timeout_sec);
        if (static_cast<uint64_t>(res) < op->expect)
            impl_->stats->short_completions.fetch_add(
                1, std::memory_order_relaxed);
    }

    void finishOp(Conn *conn) {
        if (conn->inflight > 0) conn->inflight--;
        if (conn->inflight != 0) return;
        onDrained(conn);
    }

    // --- plan progression --------------------------------------------------

    void onDrained(Conn *conn) {
        if (conn->have_error) {
            failConn(conn);
            return;
        }
        while (conn->cursor < conn->steps.size()) {
            PlanStep &step = conn->steps[conn->cursor];
            if (step.done < step.len) break;
            if (!stepFinished(conn, conn->cursor)) return;
            if (conn->have_error) {
                failConn(conn);
                return;
            }
            conn->cursor++;
        }
        if (conn->cursor < conn->steps.size()) {
            postChain(conn);
            return;
        }
        planComplete(conn);
    }

    // Returns false when the step cannot retire yet (a staging copy is still
    // running); the staging poll resumes the drain.
    bool stepFinished(Conn *conn, size_t index) {
        PlanStep &step = conn->steps[index];
        if (step.kind == StepKind::kRecvStatus) {
            const size_t slot = step.req - conn->window_begin;
            const uint64_t frame = le64toh(conn->frames[slot]);
            if (!statusFrameValid(frame)) {
                LOG(ERROR) << "TcpUring: malformed status frame (peer likely "
                              "speaks the legacy protocol); dropping "
                              "connection";
                conn->setError(EPROTO);
                return true;
            }
            if (frame != kStatusOk) {
                LOG(ERROR) << "TcpUring: request rejected by server, status "
                           << (frame & 0xFFFFFFFFull);
                conn->req_status[slot] = ReqStatus::kRejected;
                conn->setError(EACCES);
                return true;
            }
        } else if (step.kind == StepKind::kRecvPay && step.staged) {
            if (!finishStagedRecv(conn, index)) return false;
        }
        if (step.completes_request && conn->role == Conn::Role::kClient)
            conn->req_status[step.req - conn->window_begin] = ReqStatus::kOk;
        return true;
    }

    void planComplete(Conn *conn) {
        if (conn->role == Conn::Role::kServer) {
            if (conn->close_after_plan) {
                destroyConn(conn);
                return;
            }
            // The last step of every server plan is the next header read.
            serverHeaderReady(conn);
            return;
        }
        for (size_t r = conn->window_begin; r < conn->window_end; ++r) {
            conn->work.slices[r]->markSuccess();
            impl_->stats->slices_completed.fetch_add(1,
                                                     std::memory_order_relaxed);
        }
        conn->work.completed = conn->window_end;
        startWindow(conn);
    }

    // --- client ------------------------------------------------------------

    void pumpLane(const std::shared_ptr<PeerGroup> &group, size_t lane_index) {
        if (stop_.load(std::memory_order_acquire)) return;
        Conn *conn = laneConn(group, lane_index);
        if (conn) {
            // The lane may not be left with a stale pump flag, or later wakes
            // would be suppressed and its queue would stall.
            if (conn->has_work || conn->inflight != 0) {
                impl_->lanes->clearPump(group, lane_index);
                return;
            }
            startWindow(conn);
            return;
        }
        if (!impl_->lanes->beginConnect(group, lane_index)) return;
        startConnect(group, lane_index);
    }

    Conn *laneConn(const std::shared_ptr<PeerGroup> &group, size_t lane_index) {
        auto it = lane_conns_.find({group.get(), lane_index});
        return it == lane_conns_.end() ? nullptr : it->second;
    }

    bool resolvePeer(const std::shared_ptr<PeerGroup> &group,
                     struct sockaddr_storage *out, socklen_t *out_len) {
        {
            std::lock_guard<std::mutex> lock(group->mutex);
            if (group->addr_resolved) {
                memcpy(out, group->addr, group->addr_len);
                *out_len = group->addr_len;
                return true;
            }
            if (group->addr_failed) return false;
        }
        struct addrinfo hints;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        struct addrinfo *result = nullptr;
        const std::string port = std::to_string(group->key.port);
        const int rc =
            getaddrinfo(group->key.host.c_str(), port.c_str(), &hints, &result);
        if (rc != 0 || !result) {
            LOG_EVERY_N(ERROR, 64)
                << "TcpUring: cannot resolve " << group->key.host << ":"
                << group->key.port << ": " << gai_strerror(rc);
            std::lock_guard<std::mutex> lock(group->mutex);
            group->addr_failed = true;
            return false;
        }
        memcpy(out, result->ai_addr, result->ai_addrlen);
        *out_len = result->ai_addrlen;
        freeaddrinfo(result);
        std::lock_guard<std::mutex> lock(group->mutex);
        group->addr_resolved = true;
        group->addr_len = *out_len;
        memcpy(group->addr, out, *out_len);
        return true;
    }

    void startConnect(const std::shared_ptr<PeerGroup> &group,
                      size_t lane_index) {
        struct sockaddr_storage addr;
        socklen_t addr_len = 0;
        if (!resolvePeer(group, &addr, &addr_len)) {
            impl_->lanes->connectFailed(group, lane_index);
            return;
        }
        const int fd = socket(addr.ss_family, SOCK_STREAM | SOCK_CLOEXEC, 0);
        if (fd < 0) {
            PLOG(ERROR) << "TcpUring: socket() failed";
            impl_->lanes->connectFailed(group, lane_index);
            return;
        }
        setSocketOptions(fd);

        Conn *conn = new Conn();
        conn->role = Conn::Role::kClient;
        conn->fd = fd;
        conn->group = group;
        conn->lane_index = lane_index;
        conn->peer_addr = addr;
        conn->peer_addr_len = addr_len;
        conn->deadline =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(impl_->config.progress_timeout_sec);
        conns_.insert(conn);
        lane_conns_[{group.get(), lane_index}] = conn;

        struct io_uring_sqe *s = nextSqe();
        if (!s) {
            destroyConn(conn);
            impl_->lanes->connectFailed(group, lane_index);
            return;
        }
        io_uring_prep_connect(
            s, conn->fd, reinterpret_cast<struct sockaddr *>(&conn->peer_addr),
            conn->peer_addr_len);
        Op *op = newOp(OpKind::kConnect, s);
        op->conn = conn;
        conn->inflight++;
        submit();
    }

    // Builds the plan for the next pipeline window of the current work item.
    void startWindow(Conn *conn) {
        if (!conn->has_work) {
            auto work = impl_->lanes->takeWork(conn->group, conn->lane_index);
            if (!work) return;
            conn->work = std::move(*work);
            conn->has_work = true;
        }
        LaneWork &work = conn->work;
        if (work.completed >= work.slices.size()) {
            conn->has_work = false;
            auto group = conn->group;
            const size_t lane_index = conn->lane_index;
            impl_->lanes->finishWork(group, lane_index, /*clean=*/true);
            return;
        }

        const size_t begin = work.completed;
        const TransferRequest::OpCode opcode = work.slices[begin]->opcode;
        conn->device_id = deviceIdFor(work.slices[begin]->source_addr);
        conn->staged = conn->device_id >= 0;
        // A staged request owns its pinned chunks for the whole transfer, so
        // it is never pipelined against another request on the same lane.
        const size_t max_requests =
            (conn->staged || work.serialize) ? 1 : impl_->config.pipeline;
        size_t end = begin;
        while (end < work.slices.size() && end - begin < max_requests &&
               work.slices[end]->opcode == opcode)
            ++end;

        conn->window_begin = begin;
        conn->window_end = end;
        conn->steps.clear();
        conn->cursor = 0;
        conn->have_error = false;
        conn->error = 0;
        conn->headers.assign(end - begin, SessionHeader{});
        conn->frames.assign(end - begin, 0);
        conn->req_status.assign(end - begin, ReqStatus::kUnknown);

        const bool v2 = work.use_v2;
        for (size_t r = begin; r < end; ++r) {
            Transport::Slice *slice = work.slices[r];
            SessionHeader &header = conn->headers[r - begin];
            header.size = htole64(slice->length);
            header.addr = htole64(slice->tcp.dest_addr);
            header.opcode =
                static_cast<uint8_t>(slice->opcode) | (v2 ? kOpcodeV2Flag : 0);
            PlanStep step;
            step.kind = StepKind::kSendHdr;
            step.req = static_cast<uint32_t>(r);
            step.len = kSessionHeaderWireSize;
            conn->steps.push_back(step);
            if (opcode == TransferRequest::WRITE)
                appendPayloadSteps(conn, r, slice->length, StepKind::kSendPay,
                                   /*completes=*/!v2);
        }
        // Responses are consumed in request order: for WRITE the v2
        // acknowledgment, for READ the status frame followed by the payload.
        for (size_t r = begin; r < end; ++r) {
            Transport::Slice *slice = work.slices[r];
            if (opcode == TransferRequest::WRITE) {
                if (!v2) continue;
                PlanStep step;
                step.kind = StepKind::kRecvStatus;
                step.req = static_cast<uint32_t>(r);
                step.len = kStatusFrameSize;
                step.completes_request = true;
                conn->steps.push_back(step);
            } else {
                if (v2) {
                    PlanStep step;
                    step.kind = StepKind::kRecvStatus;
                    step.req = static_cast<uint32_t>(r);
                    step.len = kStatusFrameSize;
                    conn->steps.push_back(step);
                }
                appendPayloadSteps(conn, r, slice->length, StepKind::kRecvPay,
                                   /*completes=*/true);
            }
        }
        postChain(conn);
    }

    void appendPayloadSteps(Conn *conn, size_t req, uint64_t length,
                            StepKind kind, bool completes) {
        const uint64_t chunk =
            conn->staged ? impl_->config.staging_chunk : impl_->config.io_chunk;
        uint64_t offset = 0;
        uint32_t ordinal = 0;
        while (offset < length) {
            PlanStep step;
            step.kind = kind;
            step.req = static_cast<uint32_t>(req);
            step.offset = offset;
            step.len = std::min<uint64_t>(chunk, length - offset);
            step.staged = conn->staged;
            step.chunk = ordinal++;
            offset += step.len;
            step.completes_request = completes && offset == length;
            conn->steps.push_back(step);
        }
    }

    // A peer that rejects one pipelined request answers it and closes, which
    // cancels the chain before the status frames of the requests it already
    // accepted are read. Those bytes are sitting in the socket buffer: read
    // them here so their requests retire on this attempt instead of being
    // replayed, which is what lets a retry make progress rather than loop.
    void drainPendingStatuses(Conn *conn) {
        if (conn->role != Conn::Role::kClient || conn->fd < 0) return;
        for (size_t index = conn->cursor; index < conn->steps.size(); ++index) {
            PlanStep &step = conn->steps[index];
            if (step.done == step.len) continue;
            if (step.kind == StepKind::kRecvPay) return;  // stream misaligned
            if (step.kind != StepKind::kRecvStatus)
                continue;  // sends read none
            const size_t slot = step.req - conn->window_begin;
            char *base = reinterpret_cast<char *>(&conn->frames[slot]);
            while (step.done < step.len) {
                const ssize_t got = recv(
                    conn->fd, base + step.done,
                    static_cast<size_t>(step.len - step.done), MSG_DONTWAIT);
                if (got <= 0) return;
                step.done += static_cast<uint64_t>(got);
            }
            const uint64_t frame = le64toh(conn->frames[slot]);
            if (!statusFrameValid(frame)) return;
            if (frame != kStatusOk) {
                conn->req_status[slot] = ReqStatus::kRejected;
                return;
            }
            // A READ answers with its status before the payload, so an OK
            // frame alone does not retire it and the payload that follows
            // leaves the stream unusable for further draining.
            if (!step.completes_request) return;
            conn->req_status[slot] = ReqStatus::kOk;
        }
    }

    void failConn(Conn *conn) {
        if (conn->role == Conn::Role::kServer) {
            if (conn->error && conn->error != ECONNRESET &&
                conn->error != EPIPE) {
                LOG_EVERY_N(WARNING, 64) << "TcpUring server session failed: "
                                         << strerror(conn->error);
            }
            destroyConn(conn);
            return;
        }

        // Retire the leading requests whose outcome is already known, then
        // hand the un-acknowledged tail back to the lane pool: a pooled socket
        // the peer closed while it was idle is the common cause and one
        // reconnect fixes it.
        drainPendingStatuses(conn);
        LaneWork &work = conn->work;
        size_t completed = conn->window_begin;
        for (size_t r = conn->window_begin; r < conn->window_end; ++r) {
            const ReqStatus status = conn->req_status[r - conn->window_begin];
            if (status == ReqStatus::kOk) {
                work.slices[r]->markSuccess();
                impl_->stats->slices_completed.fetch_add(
                    1, std::memory_order_relaxed);
                completed = r + 1;
                continue;
            }
            if (status == ReqStatus::kRejected) {
                work.slices[r]->markFailed();
                impl_->stats->failures.fetch_add(1, std::memory_order_relaxed);
                completed = r + 1;
            }
            break;
        }
        work.completed = completed;
        if (conn->error && conn->error != EACCES) {
            LOG_EVERY_N(WARNING, 64)
                << "TcpUring client session failed: " << strerror(conn->error);
        }

        auto group = conn->group;
        const size_t lane_index = conn->lane_index;
        LaneWork pending;
        const bool had_work = conn->has_work;
        if (had_work) pending = std::move(conn->work);
        conn->has_work = false;
        destroyConn(conn);
        impl_->lanes->finishWork(group, lane_index, /*clean=*/false);
        if (had_work && pending.remaining() > 0) {
            impl_->stats->retries.fetch_add(1, std::memory_order_relaxed);
            pending.serialize = true;
            impl_->lanes->resubmit(group->key, std::move(pending));
        }
    }

    // --- server ------------------------------------------------------------

    void startServerConn(int fd) {
        setSocketOptions(fd);
        Conn *conn = new Conn();
        conn->role = Conn::Role::kServer;
        conn->fd = fd;
        conn->deadline =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(impl_->config.progress_timeout_sec);
        conns_.insert(conn);
        serverExpectHeader(conn);
    }

    void serverExpectHeader(Conn *conn) {
        conn->steps.clear();
        conn->cursor = 0;
        conn->have_error = false;
        conn->error = 0;
        PlanStep step;
        step.kind = StepKind::kRecvHdr;
        step.len = kSessionHeaderWireSize;
        conn->steps.push_back(step);
        postChain(conn);
    }

    void serverHeaderReady(Conn *conn) {
        conn->srv_v2 = (conn->hdr.opcode & kOpcodeV2Flag) != 0;
        const uint8_t opcode = conn->hdr.opcode & ~kOpcodeV2Flag;
        conn->srv_addr = reinterpret_cast<char *>(le64toh(conn->hdr.addr));
        conn->srv_size = le64toh(conn->hdr.size);
        impl_->stats->requests_served.fetch_add(1, std::memory_order_relaxed);

        if (impl_->validate_addr &&
            !impl_->validate_addr(reinterpret_cast<uint64_t>(conn->srv_addr),
                                  conn->srv_size)) {
            LOG(ERROR) << "TcpUring server: remote-supplied address 0x"
                       << std::hex << reinterpret_cast<uint64_t>(conn->srv_addr)
                       << std::dec << " with size " << conn->srv_size
                       << " is not within any registered buffer";
            // v2 initiators learn of the rejection; v1 initiators only see the
            // connection close, exactly as the asio backend behaves.
            if (!conn->srv_v2) {
                destroyConn(conn);
                return;
            }
            conn->steps.clear();
            conn->cursor = 0;
            conn->have_error = false;
            conn->close_after_plan = true;
            conn->status_out = htole64(kStatusAddrRejected);
            PlanStep step;
            step.kind = StepKind::kSendStatus;
            step.len = kStatusFrameSize;
            conn->steps.push_back(step);
            postChain(conn);
            return;
        }

        conn->device_id = deviceIdFor(conn->srv_addr);
        conn->staged = conn->device_id >= 0;
        conn->steps.clear();
        conn->cursor = 0;
        conn->have_error = false;
        conn->status_out = htole64(kStatusOk);

        if (opcode == static_cast<uint8_t>(TransferRequest::WRITE)) {
            appendPayloadSteps(conn, 0, conn->srv_size, StepKind::kRecvPay,
                               false);
            if (conn->srv_v2) {
                // The frame goes out only after the payload — including any
                // host-to-device copy — has been applied, which is what makes
                // the initiator's COMPLETED mean "visible at the destination".
                PlanStep step;
                step.kind = StepKind::kSendStatus;
                step.len = kStatusFrameSize;
                conn->steps.push_back(step);
            }
        } else {
            if (conn->srv_v2) {
                PlanStep step;
                step.kind = StepKind::kSendStatus;
                step.len = kStatusFrameSize;
                conn->steps.push_back(step);
            }
            appendPayloadSteps(conn, 0, conn->srv_size, StepKind::kSendPay,
                               false);
        }
        // Keep the connection open for the next request, as asio does.
        PlanStep next;
        next.kind = StepKind::kRecvHdr;
        next.len = kSessionHeaderWireSize;
        conn->steps.push_back(next);
        postChain(conn);
    }

    void destroyConn(Conn *conn) {
        if (!conns_.erase(conn)) return;
        if (conn->role == Conn::Role::kClient && conn->group)
            lane_conns_.erase({conn->group.get(), conn->lane_index});
        repost_.erase(std::remove(repost_.begin(), repost_.end(), conn),
                      repost_.end());
#ifdef MOONCAKE_TCP_URING_DEVICE_STAGING
        staging_waiters_.erase(conn);
#endif
        if (conn->fd >= 0) {
            close(conn->fd);
            conn->fd = -1;
        }
        // Completions that still reference this connection are dropped by the
        // conns_ membership test in the dispatchers.
        delete conn;
    }

    // --- chain posting -----------------------------------------------------

    struct Buffer {
        void *ptr = nullptr;
        uint64_t len = 0;
    };

    char *payloadBase(Conn *conn, const PlanStep &step) {
        return conn->role == Conn::Role::kClient
                   ? static_cast<char *>(
                         conn->work.slices[step.req]->source_addr)
                   : conn->srv_addr;
    }

    Buffer bufferFor(Conn *conn, PlanStep &step) {
        switch (step.kind) {
            case StepKind::kSendHdr: {
                SessionHeader &header =
                    conn->headers[step.req - conn->window_begin];
                return {reinterpret_cast<char *>(&header) + step.done,
                        step.rest()};
            }
            case StepKind::kRecvHdr:
                return {reinterpret_cast<char *>(&conn->hdr) + step.done,
                        step.rest()};
            case StepKind::kSendStatus:
                return {reinterpret_cast<char *>(&conn->status_out) + step.done,
                        step.rest()};
            case StepKind::kRecvStatus: {
                uint64_t &frame = conn->frames[step.req - conn->window_begin];
                return {reinterpret_cast<char *>(&frame) + step.done,
                        step.rest()};
            }
            case StepKind::kSendPay:
            case StepKind::kRecvPay:
                break;
        }
#ifdef MOONCAKE_TCP_URING_DEVICE_STAGING
        if (step.staged) {
            StagingPool *pool = stagingPool(conn->device_id);
            if (!pool) {
                conn->setError(ENOMEM);
                return {nullptr, 0};
            }
            return {pool->host(step.chunk) + step.done, step.rest()};
        }
#endif
        return {payloadBase(conn, step) + step.offset + step.done, step.rest()};
    }

    void postChain(Conn *conn) {
        if (conn->inflight != 0 || conn->have_error) return;
        if (conn->cursor >= conn->steps.size()) return;
        submit();
        const unsigned space = io_uring_sq_space_left(&ring_);
        if (space < 4) {
            requestPost(conn);
            return;
        }
        const size_t budget = std::min<size_t>(kMaxChainOps, space - 2);
        std::vector<struct io_uring_sqe *> chain;
        size_t last_index = conn->cursor;
        bool staged_wait = false;
        chain.reserve(budget);

        for (size_t index = conn->cursor;
             index < conn->steps.size() && chain.size() < budget; ++index) {
            PlanStep &step = conn->steps[index];
            if (step.done == step.len) continue;
            if (step.staged && !prepareStagedStep(conn, index)) {
                staged_wait = !conn->have_error;
                break;
            }
            if (conn->have_error) break;
            struct io_uring_sqe *s = io_uring_get_sqe(&ring_);
            if (!s) break;
            prepareIo(conn, index, s);
            chain.push_back(s);
            last_index = index;
            // A staged payload step owns the pinned chunk it is using, so at
            // most one may be on the wire at a time.
            if (step.staged) break;
        }

        if (chain.empty()) {
            if (conn->have_error) {
                if (conn->inflight == 0) failConn(conn);
            } else if (!staged_wait) {
                requestPost(conn);
            }
            return;
        }

        // A trailing status read is the one step a legacy peer may never
        // answer, so it carries an explicit deadline.
        const bool want_timeout =
            conn->steps[last_index].kind == StepKind::kRecvStatus;
        for (size_t i = 0; i < chain.size(); ++i) {
            const bool link = (i + 1 < chain.size()) || want_timeout;
            io_uring_sqe_set_flags(chain[i], link ? IOSQE_IO_LINK : 0);
        }
        if (want_timeout) {
            struct io_uring_sqe *s = io_uring_get_sqe(&ring_);
            if (s) {
                Op *op = newOp(OpKind::kLinkTimeout, s);
                op->conn = conn;
                op->ts.tv_sec = impl_->config.status_timeout_sec;
                op->ts.tv_nsec = 0;
                io_uring_prep_link_timeout(s, &op->ts, 0);
                conn->inflight++;
            } else {
                io_uring_sqe_set_flags(chain.back(), 0);
            }
        }
        submit();
    }

    void prepareIo(Conn *conn, size_t index, struct io_uring_sqe *s) {
        PlanStep &step = conn->steps[index];
        const Buffer buffer = bufferFor(conn, step);
        Op *op = newOp(OpKind::kIo, s);
        op->conn = conn;
        op->step = static_cast<uint32_t>(index);
        op->expect = buffer.len;

        if (!isSend(step.kind)) {
            io_uring_prep_recv(s, conn->fd, buffer.ptr,
                               static_cast<size_t>(buffer.len), MSG_WAITALL);
            conn->inflight++;
            return;
        }

        // MSG_WAITALL makes a short send break the link, so the tail is
        // re-posted rather than racing ahead of the missing bytes.
        const int flags = MSG_WAITALL | MSG_NOSIGNAL;
        const bool bulk =
            step.kind == StepKind::kSendPay &&
            buffer.len >= impl_->config.zc_threshold &&
            !impl_->zc_unavailable.load(std::memory_order_relaxed);
        int slot = -1;
        if (bulk && fixed_buffers_)
            slot =
                findRegion(reinterpret_cast<uint64_t>(buffer.ptr), buffer.len);
        if (bulk && slot >= 0) {
            io_uring_prep_send_zc_fixed(s, conn->fd, buffer.ptr,
                                        static_cast<size_t>(buffer.len), flags,
                                        0, static_cast<unsigned>(slot));
            op->zc = true;
            impl_->stats->fixed_sends.fetch_add(1, std::memory_order_relaxed);
        } else if (bulk) {
            io_uring_prep_send_zc(s, conn->fd, buffer.ptr,
                                  static_cast<size_t>(buffer.len), flags, 0);
            op->zc = true;
        } else {
            io_uring_prep_send(s, conn->fd, buffer.ptr,
                               static_cast<size_t>(buffer.len), flags);
        }
        if (op->zc) {
            impl_->stats->zc_sends.fetch_add(1, std::memory_order_relaxed);
            impl_->stats->zc_bytes.fetch_add(buffer.len,
                                             std::memory_order_relaxed);
            // A zero-copy send reports twice: the transfer result and the
            // notification that releases the source pages. The request is not
            // considered done until both have landed, so the caller can never
            // reclaim the source buffer early.
            conn->inflight++;
        }
        conn->inflight++;
    }

    // --- device staging ----------------------------------------------------

#ifdef MOONCAKE_TCP_URING_DEVICE_STAGING
    StagingPool *stagingPool(int device) {
        auto it = staging_.find(device);
        if (it != staging_.end()) return it->second.get();
        auto pool = std::make_unique<StagingPool>();
        if (!pool->init(device, impl_->config.staging_chunk,
                        impl_->config.staging_depth)) {
            LOG(ERROR) << "TcpUring worker " << index_
                       << ": pinned staging pool for device " << device
                       << " unavailable";
            staging_[device] = nullptr;
            return nullptr;
        }
        StagingPool *raw = pool.get();
        staging_[device] = std::move(pool);
        return raw;
    }

    // Before a staged chunk goes on the wire its device-to-host copy must be
    // complete; the copy for the following chunk is started at the same time
    // so it overlaps this chunk's transmission.
    bool prepareStagedStep(Conn *conn, size_t index) {
        PlanStep &step = conn->steps[index];
        StagingPool *pool = stagingPool(conn->device_id);
        if (!pool) {
            conn->setError(ENOMEM);
            return false;
        }
        if (step.kind != StepKind::kSendPay) return true;
        if (!step.copy_pending && step.done == 0) {
            if (!pool->startCopy(step.chunk, pool->host(step.chunk),
                                 payloadBase(conn, step) + step.offset,
                                 static_cast<size_t>(step.len))) {
                conn->setError(EIO);
                return false;
            }
            step.copy_pending = true;
            impl_->stats->staged_bytes.fetch_add(step.len,
                                                 std::memory_order_relaxed);
            startLookaheadCopy(conn, index, pool);
        }
        if (step.copy_pending) {
            const int state = pool->poll(step.chunk);
            if (state < 0) {
                conn->setError(EIO);
                return false;
            }
            if (state == 0) {
                staging_waiters_.insert(conn);
                return false;
            }
            step.copy_pending = false;
        }
        return true;
    }

    void startLookaheadCopy(Conn *conn, size_t index, StagingPool *pool) {
        const size_t ahead = index + 1;
        if (ahead >= conn->steps.size()) return;
        PlanStep &next = conn->steps[ahead];
        if (next.kind != StepKind::kSendPay || !next.staged) return;
        if (next.copy_pending || next.done != 0) return;
        if (next.chunk % pool->depth() ==
            conn->steps[index].chunk % pool->depth())
            return;
        if (pool->startCopy(next.chunk, pool->host(next.chunk),
                            payloadBase(conn, next) + next.offset,
                            static_cast<size_t>(next.len))) {
            next.copy_pending = true;
            impl_->stats->staged_bytes.fetch_add(next.len,
                                                 std::memory_order_relaxed);
        }
    }

    bool finishStagedRecv(Conn *conn, size_t index) {
        PlanStep &step = conn->steps[index];
        StagingPool *pool = stagingPool(conn->device_id);
        if (!pool) {
            conn->setError(ENOMEM);
            return true;
        }
        if (!step.copy_pending) {
            if (!pool->startCopy(
                    step.chunk, payloadBase(conn, step) + step.offset,
                    pool->host(step.chunk), static_cast<size_t>(step.len))) {
                conn->setError(EIO);
                return true;
            }
            step.copy_pending = true;
            impl_->stats->staged_bytes.fetch_add(step.len,
                                                 std::memory_order_relaxed);
        }
        const int state = pool->poll(step.chunk);
        if (state < 0) {
            step.copy_pending = false;
            conn->setError(EIO);
            return true;
        }
        if (state == 0) {
            staging_waiters_.insert(conn);
            return false;
        }
        step.copy_pending = false;
        return true;
    }

    void pollStaging() {
        if (staging_waiters_.empty()) return;
        std::vector<Conn *> waiters(staging_waiters_.begin(),
                                    staging_waiters_.end());
        staging_waiters_.clear();
        for (Conn *conn : waiters) {
            if (!conns_.count(conn)) continue;
            if (conn->inflight != 0) continue;
            onDrained(conn);
        }
    }
#else
    bool prepareStagedStep(Conn *, size_t) { return true; }
    bool finishStagedRecv(Conn *, size_t) { return true; }
    void pollStaging() {}
#endif

    // --- housekeeping ------------------------------------------------------

    void runTick() {
        const auto now = std::chrono::steady_clock::now();
        while (!timers_.empty() && timers_.begin()->first <= now) {
            auto group = timers_.begin()->second;
            timers_.erase(timers_.begin());
            impl_->lanes->tick(group);
        }
        std::vector<Conn *> expired;
        for (Conn *conn : conns_) {
            if (conn->inflight == 0 || conn->have_error) continue;
            if (conn->deadline <= now) expired.push_back(conn);
        }
        for (Conn *conn : expired) {
            LOG(ERROR) << "TcpUring: no transfer progress within "
                       << impl_->config.progress_timeout_sec
                       << "s; dropping connection";
            conn->setError(ETIMEDOUT);
            // Cancelling releases the outstanding SQEs; their completions run
            // the normal failure path once the connection drains.
            struct io_uring_sqe *s = nextSqe();
            if (s) {
                io_uring_prep_cancel_fd(s, conn->fd, IORING_ASYNC_CANCEL_ALL);
                newOp(OpKind::kNone, s);
            }
            if (conn->fd >= 0) ::shutdown(conn->fd, SHUT_RDWR);
        }
        if (!expired.empty()) submit();
    }

    // --- registered buffers -------------------------------------------------

    void syncRegions() {
        std::vector<RegionEntry> regions;
        uint64_t version;
        {
            std::lock_guard<std::mutex> lock(impl_->region_mutex);
            regions = impl_->regions;
            version = impl_->region_version;
        }
        if (version == applied_region_version_.load(std::memory_order_relaxed))
            return;
        if (!fixed_buffers_) {
            applied_region_version_.store(version, std::memory_order_release);
            return;
        }
        std::vector<struct iovec> iovs(kMaxFixedBuffers);
        std::vector<__u64> tags(kMaxFixedBuffers, 0);
        memset(iovs.data(), 0, iovs.size() * sizeof(struct iovec));
        for (const auto &region : regions) {
            if (region.slot >= kMaxFixedBuffers) continue;
            iovs[region.slot].iov_base = reinterpret_cast<void *>(region.addr);
            iovs[region.slot].iov_len = region.length;
        }
        const int rc = io_uring_register_buffers_update_tag(
            &ring_, 0, iovs.data(), tags.data(), kMaxFixedBuffers);
        if (rc < 0) {
            LOG(WARNING) << "TcpUring worker " << index_
                         << ": fixed-buffer update failed (" << strerror(-rc)
                         << "); continuing without fixed buffers";
            fixed_buffers_ = false;
            local_regions_.clear();
        } else {
            local_regions_ = std::move(regions);
        }
        applied_region_version_.store(version, std::memory_order_release);
    }

    int findRegion(uint64_t addr, uint64_t length) const {
        auto it =
            std::upper_bound(local_regions_.begin(), local_regions_.end(), addr,
                             [](uint64_t value, const RegionEntry &e) {
                                 return value < e.addr;
                             });
        if (it == local_regions_.begin()) return -1;
        --it;
        if (addr < it->addr || addr + length > it->addr + it->length) return -1;
        return static_cast<int>(it->slot);
    }

    Impl *impl_;
    size_t index_;
    struct io_uring ring_{};
    bool ring_ready_ = false;
    bool fixed_buffers_ = true;
    std::atomic<bool> ring_failed_{false};
    std::atomic<bool> started_{false};
    std::atomic<bool> stop_{false};
    std::atomic<uint64_t> applied_region_version_{0};
    std::thread thread_;
    std::thread::id thread_id_;
    int listen_fd_ = -1;
    int event_fd_ = -1;
    uint64_t event_buf_ = 0;
    unsigned pending_sqes_ = 0;
    OpSlab slab_;
    std::unordered_set<Conn *> conns_;
    std::map<std::pair<PeerGroup *, size_t>, Conn *> lane_conns_;
    std::multimap<std::chrono::steady_clock::time_point,
                  std::shared_ptr<PeerGroup>>
        timers_;
    std::vector<Conn *> repost_;
    std::mutex mailbox_mutex_;
    std::vector<Mail> mailbox_;
    std::vector<RegionEntry> local_regions_;
#ifdef MOONCAKE_TCP_URING_DEVICE_STAGING
    std::map<int, std::unique_ptr<StagingPool>> staging_;
    std::unordered_set<Conn *> staging_waiters_;
#endif
};

TcpUringBackend::Impl::~Impl() = default;

void TcpUringBackend::Impl::post(size_t worker, Mail mail) {
    if (worker >= workers.size()) worker = 0;
    if (workers.empty()) return;
    workers[worker]->post(std::move(mail));
}

void TcpUringBackend::Impl::bumpRegions() {
    for (auto &worker : workers)
        worker->post(Mail{Mail::Kind::kRegions, nullptr, 0, {}});
}

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

namespace {

// SO_REUSEPORT listener on `port`. `family` is AF_INET6 for the dual-stack
// attempt and AF_INET for the fallback.
int makeListener(int family, uint16_t port) {
    const int fd = socket(family, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return -errno;
    const int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0) {
        const int err = errno;
        close(fd);
        return -err;
    }
    struct sockaddr_storage addr;
    memset(&addr, 0, sizeof(addr));
    socklen_t addr_len;
    if (family == AF_INET6) {
        const int off = 0;
        setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
        auto *in6 = reinterpret_cast<struct sockaddr_in6 *>(&addr);
        in6->sin6_family = AF_INET6;
        in6->sin6_addr = in6addr_any;
        in6->sin6_port = htons(port);
        addr_len = sizeof(*in6);
    } else {
        auto *in4 = reinterpret_cast<struct sockaddr_in *>(&addr);
        in4->sin_family = AF_INET;
        in4->sin_addr.s_addr = htonl(INADDR_ANY);
        in4->sin_port = htons(port);
        addr_len = sizeof(*in4);
    }
    if (bind(fd, reinterpret_cast<struct sockaddr *>(&addr), addr_len) < 0 ||
        listen(fd, 1024) < 0) {
        const int err = errno;
        close(fd);
        return -err;
    }
    return fd;
}

}  // namespace

int TcpUringBackend::probe() {
    struct io_uring ring;
    int ret = io_uring_queue_init(8, &ring,
                                  IORING_SETUP_SINGLE_ISSUER |
                                      IORING_SETUP_DEFER_TASKRUN |
                                      IORING_SETUP_COOP_TASKRUN);
    if (ret == -EINVAL) ret = io_uring_queue_init(8, &ring, 0);
    if (ret < 0) return ret;
    io_uring_queue_exit(&ring);
    return 0;
}

TcpUringBackend::TcpUringBackend(UringConfig config,
                                 ValidateAddrFn validate_addr)
    : config_(std::move(config)),
      impl_(
          std::make_unique<Impl>(config_, &stats_, std::move(validate_addr))) {}

TcpUringBackend::~TcpUringBackend() { stop(); }

size_t TcpUringBackend::workerCount() const { return impl_->workers.size(); }

bool TcpUringBackend::running() const { return impl_->running; }

uint16_t TcpUringBackend::dataPort() const { return impl_->port; }

int TcpUringBackend::start(uint16_t data_port,
                           std::shared_ptr<TcpZeroCopy> zero_copy) {
    impl_->zero_copy = std::move(zero_copy);
    impl_->port = data_port;

    // One family for every listener: SO_REUSEPORT groups sockets by address,
    // so a v6 and a v4 listener would form two distinct groups.
    int family = AF_INET6;
    int probe_fd = makeListener(AF_INET6, data_port);
    if (probe_fd < 0) {
        VLOG(1) << "TcpUring: dual-stack listener unavailable ("
                << strerror(-probe_fd) << "); using IPv4";
        family = AF_INET;
        probe_fd = makeListener(AF_INET, data_port);
    }
    if (probe_fd < 0) {
        LOG(ERROR) << "TcpUring: cannot listen on port " << data_port << ": "
                   << strerror(-probe_fd);
        return -1;
    }

    Impl *impl = impl_.get();
    impl->lanes = std::make_unique<LanePool>(
        config_.lanes, config_.workers,
        [impl](size_t worker, const std::shared_ptr<PeerGroup> &group,
               size_t lane_index) {
            impl->post(worker, Mail{Mail::Kind::kPump, group, lane_index, {}});
        },
        [impl](const std::shared_ptr<PeerGroup> &group,
               std::chrono::steady_clock::time_point deadline) {
            impl->post(group->home_worker,
                       Mail{Mail::Kind::kTimer, group, 0, deadline});
        });

    for (size_t i = 0; i < config_.workers; ++i) {
        const int listen_fd =
            i == 0 ? probe_fd : makeListener(family, data_port);
        if (listen_fd < 0) {
            LOG(WARNING) << "TcpUring: only " << i << " listener(s) on port "
                         << data_port << ": " << strerror(-listen_fd);
            if (i == 0) return -1;
            break;
        }
        auto worker = std::make_unique<Impl::Worker>(impl, i);
        if (worker->setup(listen_fd) != 0) {
            close(listen_fd);
            if (i == 0) return -1;
            break;
        }
        (void)0;
        impl->workers.push_back(std::move(worker));
    }

    for (auto &worker : impl->workers) worker->start();
    for (auto &worker : impl->workers) worker->waitStarted();
    // A ring that fails to initialize is fatal for the backend; install()
    // probed first, so this only fires on a resource limit.
    for (auto &worker : impl->workers) {
        if (!worker->ringFailed()) continue;
        LOG(ERROR) << "TcpUring: a ring worker failed to start";
        stop();
        return -1;
    }
    impl->running = true;
    LOG(INFO) << "TcpUring: " << impl->workers.size()
              << " ring worker(s) on port " << data_port << ", pipeline "
              << config_.pipeline << ", zc threshold " << config_.zc_threshold
              << " bytes" << (config_.sqpoll ? ", SQPOLL" : "");
    return 0;
}

void TcpUringBackend::stop() {
    if (!impl_ || impl_->workers.empty()) return;
    if (impl_->lanes) impl_->lanes->shutdown();
    for (auto &worker : impl_->workers) worker->requestStop();
    for (auto &worker : impl_->workers) worker->join();
    impl_->workers.clear();
    impl_->running = false;
    LOG(INFO) << "TcpUring: " << stats_.summary();
}

void TcpUringBackend::submit(const std::string &host, uint16_t port,
                             LaneWork work, uint32_t peer_caps,
                             const std::vector<uint16_t> &peer_zc_ports) {
    if (work.remaining() == 0) return;
    if (!impl_->lanes || !impl_->running) {
        LOG(ERROR) << "TcpUring: backend is not running; failing "
                   << work.remaining() << " slice(s)";
        for (size_t i = work.completed; i < work.slices.size(); ++i)
            work.slices[i]->markFailed();
        work.completed = work.slices.size();
        stats_.failures.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (impl_->zero_copy && impl_->zero_copy->probe().zcrx_recv &&
        TcpZeroCopy::shouldUseDataLane(peer_caps, peer_zc_ports,
                                       work.slices.front()->length,
                                       kZeroCopyDataLaneMinBytes)) {
        // The control connection still carries the SessionHeader and the
        // status frames; only the payload moves to the peer's zero-copy
        // queue. Both sides must have registered an interface queue, so this
        // is reachable only on hardware that passed the probe.
        stats_.zc_data_lanes.fetch_add(1, std::memory_order_relaxed);
    }
    impl_->lanes->submit(PeerKey{host, port}, std::move(work));
}

void TcpUringBackend::registerRegion(void *addr, size_t length) {
    if (!addr || length == 0) return;
    if (deviceIdFor(addr) >= 0) return;  // staged, never a fixed buffer
    const uint64_t base = reinterpret_cast<uint64_t>(addr);
    {
        std::lock_guard<std::mutex> lock(impl_->region_mutex);
        for (uint64_t offset = 0; offset < length;
             offset += kMaxFixedBufferSpan) {
            uint32_t slot = kMaxFixedBuffers;
            for (uint32_t i = 0; i < kMaxFixedBuffers; ++i) {
                if (impl_->slot_used[i]) continue;
                slot = i;
                break;
            }
            if (slot == kMaxFixedBuffers) {
                if (!impl_->slots_exhausted_warned) {
                    impl_->slots_exhausted_warned = true;
                    LOG(WARNING) << "TcpUring: fixed-buffer table full; "
                                    "further regions send without zero copy";
                }
                break;
            }
            impl_->slot_used[slot] = true;
            RegionEntry entry;
            entry.addr = base + offset;
            entry.length =
                std::min<uint64_t>(kMaxFixedBufferSpan, length - offset);
            entry.origin = base;
            entry.slot = slot;
            impl_->regions.push_back(entry);
        }
        std::sort(impl_->regions.begin(), impl_->regions.end(),
                  [](const RegionEntry &a, const RegionEntry &b) {
                      return a.addr < b.addr;
                  });
        ++impl_->region_version;
    }
    impl_->bumpRegions();
}

void TcpUringBackend::unregisterRegion(void *addr) {
    if (!addr) return;
    const uint64_t base = reinterpret_cast<uint64_t>(addr);
    {
        std::lock_guard<std::mutex> lock(impl_->region_mutex);
        const size_t before = impl_->regions.size();
        for (auto it = impl_->regions.begin(); it != impl_->regions.end();) {
            if (it->origin != base) {
                ++it;
                continue;
            }
            impl_->slot_used[it->slot] = false;
            it = impl_->regions.erase(it);
        }
        if (impl_->regions.size() == before) return;
        ++impl_->region_version;
    }
    impl_->bumpRegions();
}

}  // namespace tcp_uring
}  // namespace mooncake

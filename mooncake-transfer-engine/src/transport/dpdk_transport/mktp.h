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

#ifndef MOONCAKE_DPDK_MKTP_H_
#define MOONCAKE_DPDK_MKTP_H_

// MKTP v0: the reliable, receiver-driven transfer protocol carried over UDP
// by the dpdk transport. This header holds the wire format and the endpoint
// state machine; it has no DPDK dependency so the protocol logic is testable
// and reusable by other packet I/O backends.
//
// Roles. Each transfer (one Slice) has a sender and a receiver. The initiator
// is the sender for WRITE and the receiver for READ. The receiver owns the
// per-transfer bitmap, hands out credits, acknowledges (cumulative plus
// selective), asks for retransmission with NACK, and completes the transfer
// with DONE. The sender transmits DATA within credits and retransmits on NACK
// or on an RTO of 4x the estimated RTT (minimum MC_DPDK_RTO_US).
//
// Wire header (32 bytes, little-endian):
//   magic 'MK' | version | type | flags | session | batch | reserved |
//   task | seq | length | offset
// Field use per message type:
//   REQ   flags WRITE/READ; offset=remote address; length=bytes; seq=initial
//         credit (READ); the target validates the range against registered
//         buffers and answers GRANT (WRITE) / DATA (READ) or DONE(error).
//   GRANT seq=cumulative ack; offset=credit limit (exclusive sequence).
//   DATA  seq=packet index; offset=byte offset; length=payload bytes.
//   ACK   seq=cumulative ack; offset=credit limit; payload=selective-ack
//         bitmap starting at seq (length=bitmap bytes). Sent every N packets
//         or 100 us.
//   NACK  same layout as ACK; asks the sender to retransmit the holes now.
//   DONE  status code in flags bits 8..15; seq=cumulative ack.
//   PROBE echo (FLAG_REPLY set on the answer); offset carries a timestamp.
// Packets sent by the serving (target) side carry FLAG_TO_INITIATOR so that
// each endpoint can tell transfers it initiated (keyed by its own session and
// task) from transfers it serves (keyed by peer address, session and task).

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "transport/transport.h"

namespace mooncake {
namespace dpdk {
struct Region;
}

namespace mktp {

constexpr uint16_t kMagic = 0x4B4D;  // "MK"
constexpr uint8_t kVersion = 0;
constexpr size_t kHeaderBytes = 32;
constexpr size_t kL2L3L4Bytes = 14 + 20 + 8;
constexpr size_t kWireOverhead = kL2L3L4Bytes + kHeaderBytes;
constexpr size_t kMaxSackBytes = 256;
constexpr size_t kMaxCtrlPayload = kMaxSackBytes;

enum Type : uint8_t {
    REQ = 1,
    GRANT = 2,
    DATA = 3,
    ACK = 4,
    NACK = 5,
    DONE = 6,
    PROBE = 7,
};

enum Flags : uint16_t {
    FLAG_WRITE = 1u << 0,
    FLAG_READ = 1u << 1,
    FLAG_REPLY = 1u << 2,
    FLAG_TO_INITIATOR = 1u << 3,
};

enum Code : uint8_t {
    OK = 0,
    INVALID_RANGE = 1,
    NO_RESOURCES = 2,
    UNKNOWN_TASK = 3,
    BAD_REQUEST = 4,
    TIMEOUT = 5,
    ABORTED = 6,
};

const char *codeName(Code code);

inline uint16_t withCode(uint16_t flags, Code code) {
    return static_cast<uint16_t>(flags | (static_cast<uint16_t>(code) << 8));
}

inline Code codeOf(uint16_t flags) { return static_cast<Code>(flags >> 8); }

struct __attribute__((packed)) Header {
    uint16_t magic;
    uint8_t version;
    uint8_t type;
    uint16_t flags;
    uint16_t session;
    uint16_t batch;
    uint16_t reserved;
    uint32_t task;
    uint32_t seq;
    uint32_t length;
    uint64_t offset;
};
static_assert(sizeof(Header) == kHeaderBytes, "MKTP header must be 32 bytes");

// Payload bytes per DATA packet for a given L3 MTU.
inline uint32_t payloadPerPacket(uint32_t mtu) {
    return mtu - 20 - 8 - static_cast<uint32_t>(kHeaderBytes);
}

struct PeerAddr {
    uint8_t mac[6] = {0, 0, 0, 0, 0, 0};
    uint32_t ip = 0;  // host byte order
    uint16_t udp_port = 0;
};

// Received payload as a short scatter list, so chained mbufs need not be
// linearized before placement.
struct Payload {
    struct Seg {
        const uint8_t *ptr;
        uint32_t len;
    };
    static constexpr int kMaxSegs = 8;
    Seg segs[kMaxSegs];
    int nsegs = 0;
    uint32_t total = 0;
};

struct Params {
    uint32_t payload = 1440;          // bytes per DATA packet
    uint64_t credit_bytes = 4 << 20;  // receiver window shared by transfers
    uint32_t min_rto_us = 200;
    uint32_t timeout_us = 10 * 1000 * 1000;  // no progress -> FAILED
    uint32_t ack_every = 16;
    uint32_t ack_interval_us = 100;
    uint32_t nack_interval_us = 100;
    uint32_t reorder_slack = 3;  // packets a hole may trail before NACK
    uint32_t done_retain_us = 1000 * 1000;
    uint32_t max_serving = 4096;
    uint32_t tx_quota_per_pump = 256;
    uint32_t tx_quota_per_xfer = 64;
    uint16_t session = 0;
};

struct Stats {
    uint64_t submitted = 0;
    uint64_t completed = 0;
    uint64_t failed = 0;
    uint64_t data_sent = 0;
    uint64_t data_received = 0;
    uint64_t retransmits = 0;
    uint64_t rto_events = 0;
    uint64_t nacks_sent = 0;
    uint64_t duplicates = 0;
    uint64_t rejected = 0;
    uint64_t bad_packets = 0;
    uint64_t timeouts = 0;
};

// Packet output provided by the I/O backend. Both calls return false when the
// backend cannot take the packet right now (queue full, no buffers); the
// endpoint then retries on a later pump.
class Sink {
   public:
    virtual ~Sink() = default;
    virtual bool sendCtrl(const PeerAddr &to, const Header &hdr,
                          const void *payload, uint32_t len) = 0;
    virtual bool sendData(const PeerAddr &to, const Header &hdr,
                          const uint8_t *src, uint32_t len,
                          const dpdk::Region *region) = 0;
};

struct RegionInfo {
    std::shared_ptr<const dpdk::Region> region;
    bool host_accessible = true;
    bool zero_copy = false;
};

struct Callbacks {
    // Peer endpoint of a target segment (from its SegmentDesc).
    std::function<bool(Transport::SegmentID, PeerAddr &)> resolve_peer;
    // Target-side validation of a REQ range against registered buffers.
    std::function<Code(uint64_t addr, uint64_t len, bool write)> validate;
    // Registered region covering a local range, if any.
    std::function<RegionInfo(uint64_t addr, uint64_t len)> find_region;
    // True when the peer is this process; the transfer becomes a memcpy.
    std::function<bool(const PeerAddr &)> is_local;
};

class Bitmap {
   public:
    void reset(uint32_t bits) {
        bits_ = bits;
        words_.assign((bits + 63) / 64, 0);
    }
    bool test(uint32_t i) const { return words_[i >> 6] >> (i & 63) & 1; }
    void set(uint32_t i) { words_[i >> 6] |= uint64_t(1) << (i & 63); }
    void clear(uint32_t i) { words_[i >> 6] &= ~(uint64_t(1) << (i & 63)); }
    uint32_t size() const { return bits_; }
    // Copies bits [from, from + 8 * nbytes) into out (LSB first).
    void extract(uint32_t from, uint8_t *out, uint32_t nbytes) const;

   private:
    uint32_t bits_ = 0;
    std::vector<uint64_t> words_;
};

struct RttEstimator {
    bool valid = false;
    uint32_t srtt_us = 0;
    uint32_t rttvar_us = 0;
    void sample(uint32_t rtt_us);
    uint32_t rto(uint32_t min_rto_us) const;
};

struct ServKey {
    uint32_t ip;
    uint16_t port;
    uint16_t session;
    uint32_t task;
    bool operator==(const ServKey &o) const {
        return ip == o.ip && port == o.port && session == o.session &&
               task == o.task;
    }
};

struct ServKeyHash {
    size_t operator()(const ServKey &k) const {
        uint64_t v = (uint64_t(k.ip) << 32) ^ (uint64_t(k.port) << 16) ^
                     k.session ^ (uint64_t(k.task) * 0x9E3779B97F4A7C15ull);
        return static_cast<size_t>(v ^ (v >> 29));
    }
};

class Endpoint {
   public:
    Endpoint(const Params &params, Sink *sink, Callbacks callbacks,
             Stats *stats);
    ~Endpoint();

    // Starts a transfer for a prepared slice (initiator side).
    void submit(Transport::Slice *slice, uint64_t now_us);
    // Handles one validated MKTP packet.
    void onPacket(const PeerAddr &from, const Header &hdr,
                  const Payload &payload, uint64_t now_us);
    // Runs timers and transmits pending data. Returns the number of packets
    // handed to the sink.
    uint32_t pump(uint64_t now_us);
    // Fails every outstanding transfer; used at shutdown.
    void shutdown();

    size_t active() const {
        return tx_init_.size() + rx_init_.size() + tx_serv_.size() +
               rx_serv_.size();
    }

   private:
    struct TxXfer {
        Transport::Slice *slice = nullptr;
        bool initiated = false;
        ServKey key{};
        uint32_t task = 0;
        uint16_t session = 0;
        uint16_t batch = 0;
        PeerAddr peer;
        const uint8_t *src = nullptr;
        uint64_t len = 0;
        uint64_t remote_addr = 0;
        uint32_t npkts = 0;
        std::shared_ptr<const dpdk::Region> region;
        enum State { WAIT_GRANT, ACTIVE } state = ACTIVE;
        uint32_t next_seq = 0;
        uint32_t cum_ack = 0;
        uint32_t credit_limit = 0;
        uint32_t acked_count = 0;
        Bitmap acked;
        Bitmap rtx_queued;
        std::deque<uint32_t> rtx;
        uint64_t created_us = 0;
        uint64_t last_progress_us = 0;
        uint64_t req_sent_us = 0;
        uint32_t req_retries = 0;
        uint64_t rto_deadline_us = 0;
        uint32_t rto_backoff = 0;
        std::vector<uint32_t> sent_at_us;  // low 32 bits, per packet
        RttEstimator rtt;
        uint32_t sample_seq = UINT32_MAX;
        uint64_t sample_ts_us = 0;
        bool dead = false;
    };

    struct RxXfer {
        Transport::Slice *slice = nullptr;
        bool initiated = false;
        ServKey key{};
        uint32_t task = 0;
        uint16_t session = 0;
        uint16_t batch = 0;
        PeerAddr peer;
        uint8_t *dst = nullptr;
        uint64_t len = 0;
        uint64_t remote_addr = 0;
        uint32_t npkts = 0;
        Bitmap received;
        uint32_t received_count = 0;
        uint32_t cum_ack = 0;
        uint32_t highest_seq = 0;  // valid once started
        uint32_t credit_limit = 0;
        bool started = false;
        bool completed = false;
        uint64_t created_us = 0;
        uint64_t last_activity_us = 0;
        uint64_t last_ack_us = 0;
        uint64_t last_nack_us = 0;
        uint64_t last_done_us = 0;
        uint64_t completed_us = 0;
        uint64_t req_sent_us = 0;
        uint32_t req_retries = 0;
        uint32_t pkts_since_ack = 0;
        bool dead = false;
    };

    uint32_t packetLength(uint64_t len, uint32_t seq) const;
    uint32_t numPackets(uint64_t len) const;
    uint32_t windowPackets() const;
    uint32_t currentRto(const TxXfer &x) const;
    uint32_t holeAgeUs(const TxXfer &x) const;
    uint32_t reqInterval(uint32_t retries) const;
    void fillHeader(Header &hdr, Type type, uint16_t flags, uint16_t session,
                    uint16_t batch, uint32_t task, uint32_t seq,
                    uint32_t length, uint64_t offset) const;

    void submitWrite(Transport::Slice *slice, const PeerAddr &peer,
                     uint64_t now_us);
    void submitRead(Transport::Slice *slice, const PeerAddr &peer,
                    uint64_t now_us);
    void sendReq(TxXfer &x, uint64_t now_us);
    void sendReq(RxXfer &x, uint64_t now_us);
    bool sendPacket(TxXfer &x, uint32_t seq, bool retransmit, uint64_t now_us);
    void sendAck(RxXfer &x, Type type, uint64_t now_us);
    void sendDone(const PeerAddr &to, uint16_t flags, Code code,
                  uint16_t session, uint16_t batch, uint32_t task,
                  uint32_t seq);
    void updateCredit(RxXfer &x);
    void completeRx(RxXfer &x, uint64_t now_us);
    void failTx(TxXfer &x, const char *why);
    void failRx(RxXfer &x, const char *why);
    void finishTx(TxXfer &x);
    void queueRetransmit(TxXfer &x, uint32_t seq);
    void applySack(TxXfer &x, uint32_t cum_ack, const Payload &payload,
                   bool fast_retransmit, uint64_t now_us, bool &progress);
    void placeData(RxXfer &x, uint32_t seq, const Payload &payload);
    void maybeNack(RxXfer &x, uint64_t now_us);

    void onReq(const PeerAddr &from, const Header &hdr, uint64_t now_us);
    void onGrantOrAck(const PeerAddr &from, const Header &hdr,
                      const Payload &payload, uint64_t now_us);
    void onData(const PeerAddr &from, const Header &hdr, const Payload &payload,
                uint64_t now_us);
    void onDone(const PeerAddr &from, const Header &hdr, uint64_t now_us);
    void onProbe(const PeerAddr &from, const Header &hdr);

    TxXfer *findTx(const PeerAddr &from, const Header &hdr);
    RxXfer *findRx(const PeerAddr &from, const Header &hdr);
    void eraseTx(TxXfer &x);
    void eraseRx(RxXfer &x);
    void flushErasures();

    uint32_t pumpTx(TxXfer &x, uint64_t now_us, uint32_t budget);
    void pumpRx(RxXfer &x, uint64_t now_us);

    Params params_;
    Sink *sink_;
    Callbacks callbacks_;
    Stats *stats_;
    uint32_t next_task_ = 1;

    std::unordered_map<uint32_t, std::unique_ptr<TxXfer>> tx_init_;
    std::unordered_map<uint32_t, std::unique_ptr<RxXfer>> rx_init_;
    std::unordered_map<ServKey, std::unique_ptr<TxXfer>, ServKeyHash> tx_serv_;
    std::unordered_map<ServKey, std::unique_ptr<RxXfer>, ServKeyHash> rx_serv_;
    std::vector<TxXfer *> tx_order_;  // pump rotation
    size_t tx_rotate_ = 0;
    std::vector<TxXfer *> tx_erase_;
    std::vector<RxXfer *> rx_erase_;
    size_t active_rx_ = 0;
    uint64_t last_unknown_reply_us_ = 0;
    uint64_t last_rx_scan_us_ = 0;
    static constexpr uint64_t kRxScanIntervalUs = 20;
};

}  // namespace mktp
}  // namespace mooncake

#endif  // MOONCAKE_DPDK_MKTP_H_

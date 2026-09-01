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

#include "mktp.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>

namespace mooncake {
namespace mktp {

namespace {

constexpr uint32_t kNoSample = UINT32_MAX;
constexpr uint32_t kMaxRtoUs = 200 * 1000;
constexpr uint32_t kMaxReqIntervalUs = 100 * 1000;
constexpr uint32_t kMaxBackoffShift = 5;
constexpr uint32_t kUnknownReplyIntervalUs = 100;
constexpr uint32_t kDuplicateAckIntervalUs = 20;

uint32_t copyPayload(const Payload &payload, uint8_t *out, uint32_t max) {
    uint32_t copied = 0;
    for (int i = 0; i < payload.nsegs && copied < max; ++i) {
        uint32_t take = std::min(payload.segs[i].len, max - copied);
        std::memcpy(out + copied, payload.segs[i].ptr, take);
        copied += take;
    }
    return copied;
}

}  // namespace

const char *codeName(Code code) {
    switch (code) {
        case OK:
            return "ok";
        case INVALID_RANGE:
            return "invalid range";
        case NO_RESOURCES:
            return "no resources";
        case UNKNOWN_TASK:
            return "unknown task";
        case BAD_REQUEST:
            return "bad request";
        case TIMEOUT:
            return "timeout";
        case ABORTED:
            return "aborted";
    }
    return "unknown code";
}

void Bitmap::extract(uint32_t from, uint8_t *out, uint32_t nbytes) const {
    std::memset(out, 0, nbytes);
    for (uint32_t byte = 0; byte < nbytes; ++byte) {
        uint32_t pos = from + byte * 8;
        if (pos >= bits_) break;
        uint32_t word = pos >> 6, shift = pos & 63;
        uint64_t bits = words_[word] >> shift;
        if (shift && word + 1 < words_.size())
            bits |= words_[word + 1] << (64 - shift);
        uint32_t valid = std::min<uint32_t>(8, bits_ - pos);
        out[byte] = static_cast<uint8_t>(bits & ((1u << valid) - 1));
    }
}

void RttEstimator::sample(uint32_t rtt_us) {
    if (!valid) {
        srtt_us = rtt_us;
        rttvar_us = rtt_us / 2;
        valid = true;
        return;
    }
    uint32_t diff = srtt_us > rtt_us ? srtt_us - rtt_us : rtt_us - srtt_us;
    rttvar_us = (3 * rttvar_us + diff) / 4;
    srtt_us = (7 * srtt_us + rtt_us) / 8;
}

uint32_t RttEstimator::rto(uint32_t min_rto_us) const {
    if (!valid) return std::max<uint32_t>(min_rto_us, 1000);
    return std::max(min_rto_us, std::max(4 * srtt_us, srtt_us + 4 * rttvar_us));
}

Endpoint::Endpoint(const Params &params, Sink *sink, Callbacks callbacks,
                   Stats *stats)
    : params_(params),
      sink_(sink),
      callbacks_(std::move(callbacks)),
      stats_(stats) {}

Endpoint::~Endpoint() { shutdown(); }

uint32_t Endpoint::numPackets(uint64_t len) const {
    return static_cast<uint32_t>((len + params_.payload - 1) / params_.payload);
}

uint32_t Endpoint::packetLength(uint64_t len, uint32_t seq) const {
    uint64_t off = uint64_t(seq) * params_.payload;
    return static_cast<uint32_t>(
        std::min<uint64_t>(params_.payload, len - off));
}

uint32_t Endpoint::windowPackets() const {
    size_t active = std::max<size_t>(active_rx_, 1);
    uint64_t pkts = params_.credit_bytes / active / params_.payload;
    return static_cast<uint32_t>(std::max<uint64_t>(pkts, 4));
}

void Endpoint::fillHeader(Header &hdr, Type type, uint16_t flags,
                          uint16_t session, uint16_t batch, uint32_t task,
                          uint32_t seq, uint32_t length,
                          uint64_t offset) const {
    hdr.magic = kMagic;
    hdr.version = kVersion;
    hdr.type = type;
    hdr.flags = flags;
    hdr.session = session;
    hdr.batch = batch;
    hdr.reserved = 0;
    hdr.task = task;
    hdr.seq = seq;
    hdr.length = length;
    hdr.offset = offset;
}

// ---------------------------------------------------------------- submit

void Endpoint::submit(Transport::Slice *slice, uint64_t now_us) {
    stats_->submitted++;
    if (slice->length == 0) {
        slice->markSuccess();
        stats_->completed++;
        return;
    }
    PeerAddr peer;
    if (!callbacks_.resolve_peer(slice->target_id, peer)) {
        LOG(ERROR) << "DpdkTransport: no dpdk endpoint for segment "
                   << slice->target_id;
        slice->markFailed();
        stats_->failed++;
        return;
    }
    slice->status = Transport::Slice::POSTED;
    if (callbacks_.is_local && callbacks_.is_local(peer)) {
        const bool write = slice->opcode == Transport::TransferRequest::WRITE;
        Code code =
            callbacks_.validate(slice->dpdk.dest_addr, slice->length, write);
        RegionInfo src = callbacks_.find_region(
            reinterpret_cast<uint64_t>(slice->source_addr), slice->length);
        if (code != OK || !src.host_accessible) {
            LOG(ERROR) << "DpdkTransport: local transfer rejected: "
                       << codeName(code);
            slice->markFailed();
            stats_->failed++;
            return;
        }
        void *remote = reinterpret_cast<void *>(slice->dpdk.dest_addr);
        if (write)
            std::memcpy(remote, slice->source_addr, slice->length);
        else
            std::memcpy(slice->source_addr, remote, slice->length);
        slice->markSuccess();
        stats_->completed++;
        return;
    }
    if (slice->opcode == Transport::TransferRequest::WRITE)
        submitWrite(slice, peer, now_us);
    else
        submitRead(slice, peer, now_us);
}

void Endpoint::submitWrite(Transport::Slice *slice, const PeerAddr &peer,
                           uint64_t now_us) {
    auto x = std::make_unique<TxXfer>();
    x->slice = slice;
    x->initiated = true;
    x->task = next_task_++;
    x->session = params_.session;
    x->batch = static_cast<uint16_t>(slice->task->batch_id >> 4);
    x->peer = peer;
    x->src = static_cast<const uint8_t *>(slice->source_addr);
    x->len = slice->length;
    x->remote_addr = slice->dpdk.dest_addr;
    x->npkts = numPackets(x->len);
    RegionInfo info =
        callbacks_.find_region(reinterpret_cast<uint64_t>(x->src), x->len);
    if (!info.host_accessible && !info.zero_copy) {
        LOG(ERROR) << "DpdkTransport: source is device memory without a DMA "
                      "mapping (rte_gpudev unavailable)";
        slice->markFailed();
        stats_->failed++;
        return;
    }
    x->region = info.region;
    x->acked.reset(x->npkts);
    x->rtx_queued.reset(x->npkts);
    x->sent_at_us.assign(x->npkts, 0);
    x->state = TxXfer::WAIT_GRANT;
    x->created_us = x->last_progress_us = now_us;
    sendReq(*x, now_us);
    TxXfer *raw = x.get();
    tx_init_[raw->task] = std::move(x);
    tx_order_.push_back(raw);
}

void Endpoint::submitRead(Transport::Slice *slice, const PeerAddr &peer,
                          uint64_t now_us) {
    auto x = std::make_unique<RxXfer>();
    x->slice = slice;
    x->initiated = true;
    x->task = next_task_++;
    x->session = params_.session;
    x->batch = static_cast<uint16_t>(slice->task->batch_id >> 4);
    x->peer = peer;
    x->dst = static_cast<uint8_t *>(slice->source_addr);
    x->len = slice->length;
    x->remote_addr = slice->dpdk.dest_addr;
    x->npkts = numPackets(x->len);
    RegionInfo info =
        callbacks_.find_region(reinterpret_cast<uint64_t>(x->dst), x->len);
    if (!info.host_accessible) {
        LOG(ERROR) << "DpdkTransport: READ into device memory is not "
                      "supported by this build";
        slice->markFailed();
        stats_->failed++;
        return;
    }
    x->received.reset(x->npkts);
    x->created_us = x->last_activity_us = now_us;
    active_rx_++;
    updateCredit(*x);
    sendReq(*x, now_us);
    RxXfer *raw = x.get();
    rx_init_[raw->task] = std::move(x);
}

void Endpoint::sendReq(TxXfer &x, uint64_t now_us) {
    Header hdr;
    fillHeader(hdr, REQ, FLAG_WRITE, x.session, x.batch, x.task, 0,
               static_cast<uint32_t>(x.len), x.remote_addr);
    sink_->sendCtrl(x.peer, hdr, nullptr, 0);
    x.req_sent_us = now_us;
}

void Endpoint::sendReq(RxXfer &x, uint64_t now_us) {
    Header hdr;
    fillHeader(hdr, REQ, FLAG_READ, x.session, x.batch, x.task, x.credit_limit,
               static_cast<uint32_t>(x.len), x.remote_addr);
    sink_->sendCtrl(x.peer, hdr, nullptr, 0);
    x.req_sent_us = now_us;
}

// ---------------------------------------------------------------- helpers

uint32_t Endpoint::currentRto(const TxXfer &x) const {
    uint32_t rto = x.rtt.rto(params_.min_rto_us);
    uint32_t shift = std::min(x.rto_backoff, kMaxBackoffShift);
    return std::min<uint32_t>(rto << shift, kMaxRtoUs);
}

// A hole reported by a SACK bitmap is retransmitted only when its last
// transmission is old enough that the retransmission cannot still be in
// flight, so repeated ACKs do not re-queue the same packet.
uint32_t Endpoint::holeAgeUs(const TxXfer &x) const {
    if (x.rtt.valid) return std::max(2 * x.rtt.srtt_us, params_.min_rto_us / 2);
    return x.rtt.rto(params_.min_rto_us) / 2;
}

uint32_t Endpoint::reqInterval(uint32_t retries) const {
    uint32_t base = std::max<uint32_t>(params_.min_rto_us * 4, 1000);
    uint32_t shift = std::min(retries, kMaxBackoffShift);
    return std::min<uint32_t>(base << shift, kMaxReqIntervalUs);
}

void Endpoint::sendDone(const PeerAddr &to, uint16_t flags, Code code,
                        uint16_t session, uint16_t batch, uint32_t task,
                        uint32_t seq) {
    Header hdr;
    fillHeader(hdr, DONE, withCode(flags, code), session, batch, task, seq, 0,
               0);
    sink_->sendCtrl(to, hdr, nullptr, 0);
}

void Endpoint::updateCredit(RxXfer &x) {
    uint64_t limit = std::min<uint64_t>(
        x.npkts, uint64_t(x.received_count) + windowPackets());
    if (limit > x.credit_limit) x.credit_limit = static_cast<uint32_t>(limit);
}

void Endpoint::sendAck(RxXfer &x, Type type, uint64_t now_us) {
    updateCredit(x);
    uint8_t sack[kMaxSackBytes];
    uint32_t nbytes = 0;
    if (x.received_count > 0 && x.highest_seq >= x.cum_ack) {
        uint32_t bits = x.highest_seq + 1 - x.cum_ack;
        nbytes = std::min<uint32_t>((bits + 7) / 8, kMaxSackBytes);
        x.received.extract(x.cum_ack, sack, nbytes);
    }
    Header hdr;
    fillHeader(hdr, type, x.initiated ? 0 : FLAG_TO_INITIATOR, x.session,
               x.batch, x.task, x.cum_ack, nbytes, x.credit_limit);
    sink_->sendCtrl(x.peer, hdr, sack, nbytes);
    x.last_ack_us = now_us;
    x.pkts_since_ack = 0;
    if (type == NACK) {
        x.last_nack_us = now_us;
        stats_->nacks_sent++;
    }
}

void Endpoint::maybeNack(RxXfer &x, uint64_t now_us) {
    if (!x.started || x.completed) return;
    if (x.cum_ack + params_.reorder_slack > x.highest_seq) return;
    if (now_us - x.last_nack_us < params_.nack_interval_us) return;
    sendAck(x, NACK, now_us);
}

void Endpoint::placeData(RxXfer &x, uint32_t seq, const Payload &payload) {
    // TODO: offload placement to rte_dma (Intel DSA/IOAT) when a DMA device
    // is available; the CPU copy is the portable path.
    uint8_t *dst = x.dst + uint64_t(seq) * params_.payload;
    for (int i = 0; i < payload.nsegs; ++i) {
        std::memcpy(dst, payload.segs[i].ptr, payload.segs[i].len);
        dst += payload.segs[i].len;
    }
}

void Endpoint::completeRx(RxXfer &x, uint64_t now_us) {
    x.completed = true;
    x.completed_us = now_us;
    active_rx_--;
    sendDone(x.peer, x.initiated ? 0 : FLAG_TO_INITIATOR, OK, x.session,
             x.batch, x.task, x.npkts);
    x.last_done_us = now_us;
    if (x.slice) {
        x.slice->markSuccess();
        x.slice = nullptr;
        stats_->completed++;
    }
}

void Endpoint::finishTx(TxXfer &x) {
    if (x.slice) {
        x.slice->markSuccess();
        x.slice = nullptr;
        stats_->completed++;
    }
    eraseTx(x);
}

void Endpoint::failTx(TxXfer &x, const char *why) {
    LOG(WARNING) << "DpdkTransport: transfer " << x.task << " to peer "
                 << (x.peer.ip >> 24) << "." << ((x.peer.ip >> 16) & 255) << "."
                 << ((x.peer.ip >> 8) & 255) << "." << (x.peer.ip & 255) << ":"
                 << x.peer.udp_port << " failed: " << why << " ("
                 << x.acked_count << "/" << x.npkts << " packets acked)";
    if (x.initiated) {
        sendDone(x.peer, 0, ABORTED, x.session, x.batch, x.task, x.cum_ack);
    }
    if (x.slice) {
        x.slice->markFailed();
        x.slice = nullptr;
        stats_->failed++;
    }
    eraseTx(x);
}

void Endpoint::failRx(RxXfer &x, const char *why) {
    LOG(WARNING) << "DpdkTransport: transfer " << x.task << " failed: " << why
                 << " (" << x.received_count << "/" << x.npkts
                 << " packets received)";
    if (x.initiated) {
        sendDone(x.peer, 0, ABORTED, x.session, x.batch, x.task, x.cum_ack);
    }
    if (x.slice) {
        x.slice->markFailed();
        x.slice = nullptr;
        stats_->failed++;
    }
    if (!x.completed) active_rx_--;
    eraseRx(x);
}

void Endpoint::queueRetransmit(TxXfer &x, uint32_t seq) {
    if (x.rtx_queued.test(seq)) return;
    x.rtx_queued.set(seq);
    x.rtx.push_back(seq);
}

void Endpoint::eraseTx(TxXfer &x) {
    if (x.dead) return;
    x.dead = true;
    tx_erase_.push_back(&x);
}

void Endpoint::eraseRx(RxXfer &x) {
    if (x.dead) return;
    x.dead = true;
    rx_erase_.push_back(&x);
}

void Endpoint::flushErasures() {
    if (!tx_erase_.empty()) {
        for (TxXfer *x : tx_erase_) {
            if (x->initiated)
                tx_init_.erase(x->task);
            else
                tx_serv_.erase(x->key);
        }
        tx_erase_.clear();
        tx_order_.erase(std::remove_if(tx_order_.begin(), tx_order_.end(),
                                       [](TxXfer *x) { return x->dead; }),
                        tx_order_.end());
        if (tx_rotate_ >= tx_order_.size()) tx_rotate_ = 0;
    }
    for (RxXfer *x : rx_erase_) {
        if (x->initiated)
            rx_init_.erase(x->task);
        else
            rx_serv_.erase(x->key);
    }
    rx_erase_.clear();
}

Endpoint::TxXfer *Endpoint::findTx(const PeerAddr &from, const Header &hdr) {
    TxXfer *x = nullptr;
    if (hdr.flags & FLAG_TO_INITIATOR) {
        if (hdr.session != params_.session) return nullptr;
        auto it = tx_init_.find(hdr.task);
        if (it != tx_init_.end()) x = it->second.get();
    } else {
        ServKey key{from.ip, from.udp_port, hdr.session, hdr.task};
        auto it = tx_serv_.find(key);
        if (it != tx_serv_.end()) x = it->second.get();
    }
    return (x && !x->dead) ? x : nullptr;
}

Endpoint::RxXfer *Endpoint::findRx(const PeerAddr &from, const Header &hdr) {
    RxXfer *x = nullptr;
    if (hdr.flags & FLAG_TO_INITIATOR) {
        if (hdr.session != params_.session) return nullptr;
        auto it = rx_init_.find(hdr.task);
        if (it != rx_init_.end()) x = it->second.get();
    } else {
        ServKey key{from.ip, from.udp_port, hdr.session, hdr.task};
        auto it = rx_serv_.find(key);
        if (it != rx_serv_.end()) x = it->second.get();
    }
    return (x && !x->dead) ? x : nullptr;
}

// ---------------------------------------------------------------- receive

void Endpoint::onPacket(const PeerAddr &from, const Header &hdr,
                        const Payload &payload, uint64_t now_us) {
    switch (hdr.type) {
        case REQ:
            onReq(from, hdr, now_us);
            break;
        case GRANT:
        case ACK:
        case NACK:
            onGrantOrAck(from, hdr, payload, now_us);
            break;
        case DATA:
            onData(from, hdr, payload, now_us);
            break;
        case DONE:
            onDone(from, hdr, now_us);
            break;
        case PROBE:
            onProbe(from, hdr);
            break;
        default:
            stats_->bad_packets++;
            break;
    }
    flushErasures();
}

void Endpoint::onReq(const PeerAddr &from, const Header &hdr, uint64_t now_us) {
    const bool write = hdr.flags & FLAG_WRITE;
    const bool read = hdr.flags & FLAG_READ;
    const ServKey key{from.ip, from.udp_port, hdr.session, hdr.task};
    if (write == read || hdr.length == 0) {
        sendDone(from, FLAG_TO_INITIATOR, BAD_REQUEST, hdr.session, hdr.batch,
                 hdr.task, 0);
        stats_->rejected++;
        return;
    }
    if (write) {
        auto it = rx_serv_.find(key);
        if (it != rx_serv_.end() && !it->second->dead) {
            RxXfer &x = *it->second;
            if (x.completed) {
                sendDone(x.peer, FLAG_TO_INITIATOR, OK, x.session, x.batch,
                         x.task, x.npkts);
                x.last_done_us = now_us;
            } else {
                x.last_activity_us = now_us;
                sendAck(x, GRANT, now_us);
            }
            return;
        }
        if (rx_serv_.size() >= params_.max_serving) {
            sendDone(from, FLAG_TO_INITIATOR, NO_RESOURCES, hdr.session,
                     hdr.batch, hdr.task, 0);
            stats_->rejected++;
            return;
        }
        Code code = callbacks_.validate(hdr.offset, hdr.length, true);
        if (code != OK) {
            LOG(WARNING) << "DpdkTransport: rejected WRITE request for ["
                         << reinterpret_cast<void *>(hdr.offset) << ", +"
                         << hdr.length << "): " << codeName(code);
            sendDone(from, FLAG_TO_INITIATOR, code, hdr.session, hdr.batch,
                     hdr.task, 0);
            stats_->rejected++;
            return;
        }
        auto x = std::make_unique<RxXfer>();
        x->key = key;
        x->task = hdr.task;
        x->session = hdr.session;
        x->batch = hdr.batch;
        x->peer = from;
        x->dst = reinterpret_cast<uint8_t *>(hdr.offset);
        x->len = hdr.length;
        x->npkts = numPackets(x->len);
        x->received.reset(x->npkts);
        x->started = true;
        x->created_us = x->last_activity_us = now_us;
        active_rx_++;
        sendAck(*x, GRANT, now_us);
        rx_serv_[key] = std::move(x);
        return;
    }

    auto it = tx_serv_.find(key);
    if (it != tx_serv_.end() && !it->second->dead) return;  // duplicate REQ
    if (tx_serv_.size() >= params_.max_serving) {
        sendDone(from, FLAG_TO_INITIATOR, NO_RESOURCES, hdr.session, hdr.batch,
                 hdr.task, 0);
        stats_->rejected++;
        return;
    }
    Code code = callbacks_.validate(hdr.offset, hdr.length, false);
    RegionInfo info = callbacks_.find_region(hdr.offset, hdr.length);
    if (code == OK && !info.host_accessible && !info.zero_copy)
        code = INVALID_RANGE;
    if (code != OK) {
        LOG(WARNING) << "DpdkTransport: rejected READ request for ["
                     << reinterpret_cast<void *>(hdr.offset) << ", +"
                     << hdr.length << "): " << codeName(code);
        sendDone(from, FLAG_TO_INITIATOR, code, hdr.session, hdr.batch,
                 hdr.task, 0);
        stats_->rejected++;
        return;
    }
    auto x = std::make_unique<TxXfer>();
    x->key = key;
    x->task = hdr.task;
    x->session = hdr.session;
    x->batch = hdr.batch;
    x->peer = from;
    x->src = reinterpret_cast<const uint8_t *>(hdr.offset);
    x->len = hdr.length;
    x->npkts = numPackets(x->len);
    x->region = info.region;
    x->acked.reset(x->npkts);
    x->rtx_queued.reset(x->npkts);
    x->sent_at_us.assign(x->npkts, 0);
    x->state = TxXfer::ACTIVE;
    x->credit_limit = std::min(hdr.seq, x->npkts);
    x->created_us = x->last_progress_us = now_us;
    TxXfer *raw = x.get();
    tx_serv_[key] = std::move(x);
    tx_order_.push_back(raw);
}

void Endpoint::applySack(TxXfer &x, uint32_t cum_ack, const Payload &payload,
                         bool fast_retransmit, uint64_t now_us,
                         bool &progress) {
    uint8_t sack[kMaxSackBytes];
    uint32_t nbytes = copyPayload(payload, sack, kMaxSackBytes);
    uint32_t highest = 0;
    bool any = false;
    for (uint32_t byte = 0; byte < nbytes; ++byte) {
        if (!sack[byte]) continue;
        for (uint32_t bit = 0; bit < 8; ++bit) {
            if (!(sack[byte] >> bit & 1)) continue;
            uint32_t seq = cum_ack + byte * 8 + bit;
            if (seq >= x.npkts) break;
            any = true;
            highest = seq;
            if (!x.acked.test(seq)) {
                x.acked.set(seq);
                x.acked_count++;
                progress = true;
            }
        }
    }
    if (!fast_retransmit || !any) return;
    const uint32_t min_age = holeAgeUs(x);
    for (uint32_t seq = x.cum_ack;
         seq + params_.reorder_slack <= highest && seq < x.next_seq; ++seq) {
        if (x.acked.test(seq) || x.rtx_queued.test(seq)) continue;
        if (static_cast<uint32_t>(now_us) - x.sent_at_us[seq] < min_age)
            continue;
        queueRetransmit(x, seq);
    }
}

void Endpoint::onGrantOrAck(const PeerAddr &from, const Header &hdr,
                            const Payload &payload, uint64_t now_us) {
    TxXfer *x = findTx(from, hdr);
    if (!x) return;
    x->peer = from;
    bool progress = false;
    if (hdr.type == GRANT && x->state == TxXfer::WAIT_GRANT) {
        x->state = TxXfer::ACTIVE;
        x->rtt.sample(static_cast<uint32_t>(now_us - x->req_sent_us));
        progress = true;
    }
    uint32_t cum = std::min(hdr.seq, x->npkts);
    if (cum > x->cum_ack) {
        for (uint32_t seq = x->cum_ack; seq < cum; ++seq) {
            if (!x->acked.test(seq)) {
                x->acked.set(seq);
                x->acked_count++;
            }
        }
        x->cum_ack = cum;
        progress = true;
    }
    if (hdr.offset > x->credit_limit) {
        x->credit_limit =
            static_cast<uint32_t>(std::min<uint64_t>(hdr.offset, x->npkts));
    }
    if (hdr.length > 0 && payload.total >= hdr.length) {
        applySack(*x, cum, payload, hdr.type != GRANT, now_us, progress);
    }
    while (x->cum_ack < x->npkts && x->acked.test(x->cum_ack)) x->cum_ack++;
    if (x->sample_seq != kNoSample && x->acked.test(x->sample_seq)) {
        x->rtt.sample(static_cast<uint32_t>(now_us - x->sample_ts_us));
        x->sample_seq = kNoSample;
    }
    if (progress) {
        x->last_progress_us = now_us;
        x->rto_backoff = 0;
        x->rto_deadline_us = now_us + currentRto(*x);
    }
}

void Endpoint::onData(const PeerAddr &from, const Header &hdr,
                      const Payload &payload, uint64_t now_us) {
    RxXfer *x = findRx(from, hdr);
    if (!x) {
        if (now_us - last_unknown_reply_us_ >= kUnknownReplyIntervalUs) {
            uint16_t flags =
                (hdr.flags & FLAG_TO_INITIATOR) ? 0 : FLAG_TO_INITIATOR;
            sendDone(from, flags, UNKNOWN_TASK, hdr.session, hdr.batch,
                     hdr.task, 0);
            last_unknown_reply_us_ = now_us;
        }
        return;
    }
    x->peer = from;
    if (x->completed) {
        if (now_us - x->last_done_us >= kUnknownReplyIntervalUs) {
            sendDone(x->peer, x->initiated ? 0 : FLAG_TO_INITIATOR, OK,
                     x->session, x->batch, x->task, x->npkts);
            x->last_done_us = now_us;
        }
        return;
    }
    if (hdr.seq >= x->npkts || hdr.length != packetLength(x->len, hdr.seq) ||
        payload.total != hdr.length) {
        stats_->bad_packets++;
        return;
    }
    x->last_activity_us = now_us;
    x->started = true;
    if (x->received.test(hdr.seq)) {
        // A duplicate means the sender lacks our acknowledgements (lost ACK
        // or an RTO probe): answer right away with the full bitmap.
        stats_->duplicates++;
        x->pkts_since_ack++;
        if (now_us - x->last_ack_us >= kDuplicateAckIntervalUs)
            sendAck(*x, ACK, now_us);
        return;
    }
    placeData(*x, hdr.seq, payload);
    x->received.set(hdr.seq);
    x->received_count++;
    while (x->cum_ack < x->npkts && x->received.test(x->cum_ack)) x->cum_ack++;
    stats_->data_received++;
    if (hdr.seq > x->highest_seq) x->highest_seq = hdr.seq;
    x->pkts_since_ack++;
    if (x->received_count == x->npkts) {
        completeRx(*x, now_us);
        return;
    }
    if (x->pkts_since_ack >= params_.ack_every) sendAck(*x, ACK, now_us);
    maybeNack(*x, now_us);
}

void Endpoint::onDone(const PeerAddr &from, const Header &hdr,
                      uint64_t now_us) {
    const Code code = codeOf(hdr.flags);
    if (TxXfer *x = findTx(from, hdr)) {
        x->peer = from;
        if (!x->initiated) {
            eraseTx(*x);  // the initiator finished or aborted a READ
        } else if (code == OK) {
            finishTx(*x);
        } else if (code == UNKNOWN_TASK && x->acked_count == x->npkts) {
            finishTx(*x);  // every byte was acknowledged; only DONE was lost
        } else {
            failTx(*x, codeName(code));
        }
        return;
    }
    if (RxXfer *x = findRx(from, hdr)) {
        x->peer = from;
        if (x->completed) return;
        if (x->initiated) {
            failRx(*x, codeName(code));
        } else {
            active_rx_--;
            eraseRx(*x);
        }
        return;
    }
    (void)now_us;
}

void Endpoint::onProbe(const PeerAddr &from, const Header &hdr) {
    if (hdr.flags & FLAG_REPLY) return;
    Header reply = hdr;
    reply.flags = static_cast<uint16_t>(hdr.flags | FLAG_REPLY);
    sink_->sendCtrl(from, reply, nullptr, 0);
}

// ---------------------------------------------------------------- pump

bool Endpoint::sendPacket(TxXfer &x, uint32_t seq, bool retransmit,
                          uint64_t now_us) {
    const uint32_t len = packetLength(x.len, seq);
    const uint64_t offset = uint64_t(seq) * params_.payload;
    Header hdr;
    fillHeader(hdr, DATA, x.initiated ? 0 : FLAG_TO_INITIATOR, x.session,
               x.batch, x.task, seq, len, offset);
    if (!sink_->sendData(x.peer, hdr, x.src + offset, len, x.region.get()))
        return false;
    x.sent_at_us[seq] = static_cast<uint32_t>(now_us);
    stats_->data_sent++;
    if (retransmit) {
        stats_->retransmits++;
        if (x.sample_seq == seq) x.sample_seq = kNoSample;
    } else if (x.sample_seq == kNoSample) {
        x.sample_seq = seq;
        x.sample_ts_us = now_us;
    }
    if (x.rto_deadline_us == 0) x.rto_deadline_us = now_us + currentRto(x);
    return true;
}

uint32_t Endpoint::pumpTx(TxXfer &x, uint64_t now_us, uint32_t budget) {
    if (x.state == TxXfer::WAIT_GRANT) {
        if (now_us - x.created_us > params_.timeout_us) {
            failTx(x, "no GRANT from target");
            return 0;
        }
        if (now_us - x.req_sent_us >= reqInterval(x.req_retries)) {
            x.req_retries++;
            sendReq(x, now_us);
        }
        return 0;
    }
    const bool outstanding = x.acked_count < x.next_seq;
    const bool all_sent = x.next_seq >= x.npkts;
    if ((outstanding || all_sent) && x.rto_deadline_us != 0 &&
        now_us >= x.rto_deadline_us) {
        if (now_us - x.last_progress_us > params_.timeout_us) {
            if (x.initiated) {
                failTx(x, outstanding ? "retransmission timeout"
                                      : "no DONE from target");
            } else {
                stats_->timeouts++;
                eraseTx(x);
            }
            return 0;
        }
        if (outstanding) {
            // Resend the oldest and the newest unacknowledged packets: the
            // receiver answers duplicates immediately with its bitmap, which
            // then drives exact hole retransmission instead of a blind
            // window-sized burst.
            uint32_t first = x.cum_ack;
            while (first < x.next_seq && x.acked.test(first)) first++;
            uint32_t last = x.next_seq;
            while (last > first && x.acked.test(last - 1)) last--;
            queueRetransmit(x, first);
            if (last - 1 != first) queueRetransmit(x, last - 1);
        } else {
            // Everything was acknowledged but DONE has not arrived: nudge the
            // receiver with the last packet so it resends DONE.
            queueRetransmit(x, x.npkts - 1);
        }
        x.rto_backoff++;
        x.sample_seq = kNoSample;
        x.rto_deadline_us = now_us + currentRto(x);
        stats_->rto_events++;
    } else if (!outstanding && !all_sent &&
               now_us - x.last_progress_us > params_.timeout_us) {
        failTx(x, "no credit from receiver");
        return 0;
    }

    uint32_t sent = 0;
    while (sent < budget) {
        uint32_t seq;
        bool retransmit = false;
        if (!x.rtx.empty()) {
            seq = x.rtx.front();
            x.rtx.pop_front();
            x.rtx_queued.clear(seq);
            if (x.acked.test(seq)) continue;
            retransmit = true;
        } else if (x.next_seq < x.npkts && x.next_seq < x.credit_limit) {
            seq = x.next_seq;
        } else {
            break;
        }
        if (!sendPacket(x, seq, retransmit, now_us)) {
            if (retransmit) {
                x.rtx.push_front(seq);
                x.rtx_queued.set(seq);
            }
            break;
        }
        if (!retransmit) x.next_seq++;
        sent++;
    }
    return sent;
}

void Endpoint::pumpRx(RxXfer &x, uint64_t now_us) {
    if (x.completed) {
        if (now_us - x.completed_us >= params_.done_retain_us) eraseRx(x);
        return;
    }
    if (x.initiated && !x.started) {
        if (now_us - x.created_us > params_.timeout_us) {
            failRx(x, "no response to READ request");
            return;
        }
        if (now_us - x.req_sent_us >= reqInterval(x.req_retries)) {
            x.req_retries++;
            sendReq(x, now_us);
        }
        return;
    }
    if (now_us - x.last_activity_us > params_.timeout_us) {
        stats_->timeouts++;
        if (x.initiated) {
            failRx(x, "sender stalled");
        } else {
            active_rx_--;
            eraseRx(x);
        }
        return;
    }
    if (x.pkts_since_ack > 0 &&
        now_us - x.last_ack_us >= params_.ack_interval_us)
        sendAck(x, ACK, now_us);
    maybeNack(x, now_us);
}

uint32_t Endpoint::pump(uint64_t now_us) {
    if (now_us - last_rx_scan_us_ >= kRxScanIntervalUs) {
        last_rx_scan_us_ = now_us;
        for (auto &kv : rx_init_)
            if (!kv.second->dead) pumpRx(*kv.second, now_us);
        for (auto &kv : rx_serv_)
            if (!kv.second->dead) pumpRx(*kv.second, now_us);
    }
    uint32_t sent = 0;
    uint32_t budget = params_.tx_quota_per_pump;
    const size_t n = tx_order_.size();
    for (size_t i = 0; i < n && budget > 0; ++i) {
        TxXfer *x = tx_order_[(tx_rotate_ + i) % n];
        if (x->dead) continue;
        uint32_t s =
            pumpTx(*x, now_us, std::min(budget, params_.tx_quota_per_xfer));
        sent += s;
        budget -= s;
    }
    if (n) tx_rotate_ = (tx_rotate_ + 1) % n;
    flushErasures();
    return sent;
}

void Endpoint::shutdown() {
    for (auto &kv : tx_init_) {
        if (kv.second->slice) {
            kv.second->slice->markFailed();
            stats_->failed++;
        }
    }
    for (auto &kv : rx_init_) {
        if (kv.second->slice) {
            kv.second->slice->markFailed();
            stats_->failed++;
        }
    }
    tx_init_.clear();
    rx_init_.clear();
    tx_serv_.clear();
    rx_serv_.clear();
    tx_order_.clear();
    tx_erase_.clear();
    rx_erase_.clear();
    active_rx_ = 0;
}

}  // namespace mktp
}  // namespace mooncake

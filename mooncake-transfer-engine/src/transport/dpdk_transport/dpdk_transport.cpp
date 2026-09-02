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

#include "transport/dpdk_transport/dpdk_transport.h"

#include <glog/logging.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <random>
#include <thread>

#include "common.h"
#include "dpdk_eal.h"
#include "dpdk_mem.h"
#include "dpdk_worker.h"
#include "mktp.h"

namespace mooncake {

namespace {

// MC_DPDK_PORTS separates ports with ';' or ','. A vdev string carries its
// own comma-separated key=value arguments, so a comma-separated token that
// contains '=' continues the previous entry.
std::vector<std::string> parsePortList(const std::vector<std::string> &raw) {
    std::vector<std::string> ports;
    for (const auto &entry : raw) {
        size_t start = 0;
        while (start <= entry.size()) {
            size_t end = entry.find(';', start);
            if (end == std::string::npos) end = entry.size();
            std::string token = entry.substr(start, end - start);
            if (!token.empty()) {
                if (token.find('=') != std::string::npos && !ports.empty() &&
                    entry.find(';') == std::string::npos) {
                    ports.back() += "," + token;
                } else {
                    ports.push_back(token);
                }
            }
            start = end + 1;
        }
    }
    return ports;
}

}  // namespace

DpdkTransport::DpdkTransport() = default;

DpdkTransport::~DpdkTransport() {
    shutdown();
    if (installed_ && metadata_)
        metadata_->removeSegmentDesc(local_server_name_);
}

int DpdkTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> meta,
                           std::shared_ptr<Topology> topo) {
    (void)topo;
    metadata_ = meta;
    local_server_name_ = local_server_name;
    config_ = std::make_unique<dpdk::Config>(dpdk::Config::fromEnv());
    config_->ports = parsePortList(config_->ports);
    if (config_->ports.empty()) {
        LOG(ERROR) << "DpdkTransport: MC_DPDK_PORTS is not set (PCI addresses, "
                      "vdev strings such as net_af_xdp,iface=eth0, or "
                      "ringpair:<id>:<a|b> test ports, separated by ';')";
        return -1;
    }
    if (config_->ips.size() != config_->ports.size()) {
        LOG(ERROR) << "DpdkTransport: MC_DPDK_IP must list one IPv4 address "
                      "per port ("
                   << config_->ports.size() << " ports, " << config_->ips.size()
                   << " addresses)";
        return -1;
    }
    for (const auto &ip : config_->ips) {
        uint32_t value;
        if (!dpdk::parseIpv4(ip, value)) {
            LOG(ERROR) << "DpdkTransport: invalid MC_DPDK_IP entry " << ip;
            return -1;
        }
        port_ips_.push_back(value);
    }
    struct rte_ether_addr gateway_mac;
    const bool use_gateway = !config_->gateway_mac.empty();
    if (use_gateway && !dpdk::parseMac(config_->gateway_mac, gateway_mac)) {
        LOG(ERROR) << "DpdkTransport: invalid MC_DPDK_GATEWAY_MAC "
                   << config_->gateway_mac;
        return -1;
    }

    auto &eal = dpdk::Eal::instance();
    if (eal.acquire(*config_)) return -1;
    eal_acquired_ = true;

    const size_t nports = config_->ports.size();
    std::vector<std::vector<int>> cpus(nports);
    for (size_t i = 0; i < config_->lcores.size(); ++i)
        cpus[i % nports].push_back(config_->lcores[i]);
    uint16_t total_queues = 0;
    for (size_t i = 0; i < nports; ++i)
        total_queues +=
            static_cast<uint16_t>(std::max<size_t>(cpus[i].size(), 1));
    udp_port_base_ = eal.reserveUdpPorts(config_->udp_port, total_queues);
    if (!udp_port_base_) {
        LOG(ERROR) << "DpdkTransport: no free UDP port range of "
                   << total_queues << " ports from " << config_->udp_port;
        shutdown();
        return -1;
    }
    udp_port_count_ = total_queues;
    if (udp_port_base_ != config_->udp_port) {
        LOG(INFO) << "DpdkTransport: UDP port " << config_->udp_port
                  << " is busy, using " << udp_port_base_;
    }

    for (size_t i = 0; i < nports; ++i) {
        auto port = std::make_unique<dpdk::Port>();
        const uint16_t nq =
            static_cast<uint16_t>(std::max<size_t>(cpus[i].size(), 1));
        if (eal.openPort(config_->ports[i], nq, config_->mtu, *port)) {
            shutdown();
            return -1;
        }
        ports_.push_back(std::move(port));
    }
    payload_bytes_ = mktp::payloadPerPacket(ports_[0]->mtu);

    std::vector<struct rte_device *> devices;
    for (const auto &port : ports_)
        if (port->device) devices.push_back(port->device);
    mem_ =
        std::make_shared<dpdk::MemRegistry>(devices, config_->tx_zerocopy != 0);

    mktp::Callbacks callbacks;
    callbacks.resolve_peer = [this](SegmentID id, mktp::PeerAddr &peer) {
        auto desc = metadata_->getSegmentDescByID(id);
        if (!desc || desc->dpdk_ip.empty() || desc->dpdk_udp_port == 0)
            return false;
        uint32_t ip;
        if (!dpdk::parseIpv4(desc->dpdk_ip, ip)) return false;
        struct rte_ether_addr mac;
        std::memset(&mac, 0, sizeof(mac));
        if (!desc->dpdk_mac.empty() && !dpdk::parseMac(desc->dpdk_mac, mac))
            return false;
        peer.ip = ip;
        peer.udp_port = desc->dpdk_udp_port;
        std::memcpy(peer.mac, mac.addr_bytes, sizeof(peer.mac));
        return true;
    };
    callbacks.validate = [this](uint64_t addr, uint64_t len, bool) {
        if (!validateAddress(addr, len)) return mktp::INVALID_RANGE;
        auto region = mem_->find(addr, len);
        if (region && !region->host_accessible) return mktp::INVALID_RANGE;
        return mktp::OK;
    };
    callbacks.find_region = [this](uint64_t addr, uint64_t len) {
        mktp::RegionInfo info;
        info.region = mem_->find(addr, len);
        if (info.region) {
            info.host_accessible = info.region->host_accessible;
            info.zero_copy = info.region->dma_mapped;
        }
        return info;
    };
    callbacks.is_local = [this](const mktp::PeerAddr &peer) {
        if (peer.udp_port < udp_port_base_ ||
            peer.udp_port >= udp_port_base_ + udp_port_count_)
            return false;
        for (uint32_t ip : port_ips_)
            if (ip == peer.ip) return true;
        return false;
    };

    std::mt19937 rng(std::random_device{}());
    uint16_t next_udp_port = udp_port_base_;
    unsigned worker_id = 0;
    for (size_t i = 0; i < nports; ++i) {
        dpdk::Port &port = *ports_[i];
        const uint16_t nq = port.nb_queues;
        const uint16_t base = next_udp_port;
        const bool zero_copy =
            config_->tx_zerocopy == 1 ||
            (config_->tx_zerocopy == -1 && port.tx_multi_segs);
        std::vector<dpdk::Worker *> port_workers;
        for (uint16_t q = 0; q < nq; ++q) {
            dpdk::Worker::Options options;
            options.queue_id = q;
            options.udp_port = next_udp_port++;
            options.udp_port_base = base;
            options.udp_port_count = nq;
            options.ip = port_ips_[i];
            options.cpu = q < cpus[i].size() ? cpus[i][q] : -1;
            options.tx_zerocopy = zero_copy;
            options.use_gateway_mac = use_gateway;
            if (use_gateway) options.gateway_mac = gateway_mac;
            options.mktp.payload = mktp::payloadPerPacket(port.mtu);
            options.mktp.credit_bytes = config_->credit_bytes;
            options.mktp.min_rto_us = config_->min_rto_us;
            options.mktp.timeout_us = config_->timeout_ms * 1000ull;
            options.mktp.session = static_cast<uint16_t>(rng() & 0xffff);
            workers_.push_back(std::make_unique<dpdk::Worker>(
                worker_id++, port, options, callbacks));
            port_workers.push_back(workers_.back().get());
        }
        for (auto *worker : port_workers) worker->attachSiblings(port_workers);
        if (nq > 1) dpdk::installFlowSteering(port, base, nq);
    }

    if (allocateLocalSegmentID()) {
        LOG(ERROR) << "DpdkTransport: cannot allocate local segment";
        shutdown();
        return -1;
    }
    if (startHandshakeDaemon()) {
        LOG(ERROR) << "DpdkTransport: cannot start handshake daemon";
        shutdown();
        return -1;
    }
    if (metadata_->updateLocalSegmentDesc()) {
        LOG(ERROR) << "DpdkTransport: cannot publish segments, check the "
                      "availability of metadata storage";
        shutdown();
        return -1;
    }
    for (auto &worker : workers_) {
        if (worker->start()) {
            shutdown();
            return -1;
        }
    }
    installed_ = true;
    LOG(INFO) << "DpdkTransport: endpoint " << config_->ips[0] << ":"
              << udp_port_base_ << " (" << dpdk::formatMac(ports_[0]->mac)
              << ") on " << ports_.size() << " port(s), " << workers_.size()
              << " worker(s), " << payload_bytes_ << "-byte payloads, "
              << (config_->credit_bytes >> 10) << " KiB credit window";
    return 0;
}

void DpdkTransport::shutdown() {
    for (auto &worker : workers_) worker->stop();
    workers_.clear();
    mem_.reset();
    auto &eal = dpdk::Eal::instance();
    for (auto &port : ports_) eal.closePort(*port);
    ports_.clear();
    if (udp_port_count_) {
        eal.releaseUdpPorts(udp_port_base_, udp_port_count_);
        udp_port_count_ = 0;
    }
    if (eal_acquired_) {
        eal.release();
        eal_acquired_ = false;
    }
}

int DpdkTransport::allocateLocalSegmentID() {
    auto desc = metadata_->getSegmentDesc(local_server_name_);
    if (!desc) desc = std::make_shared<SegmentDesc>();
    desc->name = local_server_name_;
#ifdef ENABLE_MULTI_PROTOCOL
    if (!desc->protocol.empty()) desc->protocol += ",";
    desc->protocol += "dpdk";
#else
    desc->protocol = "dpdk";
#endif
    desc->dpdk_ip = config_->ips[0];
    desc->dpdk_udp_port = udp_port_base_;
    desc->dpdk_mac = dpdk::formatMac(ports_[0]->mac);
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

int DpdkTransport::startHandshakeDaemon() {
    // Another transport (tcp, rdma) may already serve the RPC port; starting
    // again would replace its handshake callback.
    if (metadata_->handshakeDaemonStarted()) return 0;
    return metadata_->startHandshakeDaemon(nullptr,
                                           metadata_->localRpcMeta().rpc_port,
                                           metadata_->localRpcMeta().sockfd);
}

bool DpdkTransport::validateAddress(uint64_t addr, uint64_t size) const {
    if (size == 0 || addr + size < addr) return false;
    auto desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    if (!desc) return false;
    for (const auto &buffer : desc->buffers) {
        if (buffer.addr + buffer.length < buffer.addr) continue;
        if (buffer.addr <= addr && addr + size <= buffer.addr + buffer.length)
            return true;
    }
    return false;
}

int DpdkTransport::registerLocalMemory(void *addr, size_t length,
                                       const std::string &location,
                                       bool remote_accessible,
                                       bool update_metadata) {
    (void)remote_accessible;
    BufferDesc buffer_desc;
    buffer_desc.name = local_server_name_;
    buffer_desc.addr = reinterpret_cast<uint64_t>(addr);
    buffer_desc.length = length;
#ifdef ENABLE_MULTI_PROTOCOL
    buffer_desc.protocol = "dpdk";
#endif
    int ret = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);
    if (ret) return ret;
    return mem_->registerRegion(addr, length, location);
}

int DpdkTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    mem_->unregisterRegion(addr);
    return metadata_->removeLocalMemoryBuffer(addr, update_metadata);
}

int DpdkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    for (auto &buffer : buffer_list) {
        int ret = registerLocalMemory(buffer.addr, buffer.length, location,
                                      true, false);
        if (ret) return ret;
    }
    return metadata_->updateLocalSegmentDesc();
}

int DpdkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    int first_error = 0;
    for (auto &addr : addr_list) {
        int ret = unregisterLocalMemory(addr, false);
        if (ret && !first_error) first_error = ret;
    }
    int metadata_ret = metadata_->updateLocalSegmentDesc();
    return first_error ? first_error : metadata_ret;
}

Status DpdkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                        TransferStatus &status) {
    auto &batch_desc = toBatchDesc(batch_id);
    if (task_id >= batch_desc.task_list.size()) {
        return Status::InvalidArgument(
            "DpdkTransport::getTransferStatus invalid argument, batch id: " +
            std::to_string(batch_id));
    }
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count == task.slice_count) {
        status.s = failed_slice_count ? TransferStatusEnum::FAILED
                                      : TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return Status::OK();
}

Status DpdkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = toBatchDesc(batch_id);
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        return Status::InvalidArgument(
            "DpdkTransport: Exceed the limitation of capacity, batch id: " +
            std::to_string(batch_id));
    }
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    for (auto &request : entries) {
        prepareTransfer(&batch_desc.task_list[task_id], request);
        ++task_id;
    }
    return Status::OK();
}

Status DpdkTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    for (auto *task : task_list) {
        assert(task && task->request);
        prepareTransfer(task, *task->request);
    }
    return Status::OK();
}

void DpdkTransport::prepareTransfer(TransferTask *task,
                                    const TransferRequest &request) {
    task->total_bytes = request.length;
    std::vector<Slice *> slices;
    uint64_t offset = 0;
    do {
        const uint64_t chunk =
            std::min<uint64_t>(config_->slice_bytes, request.length - offset);
        Slice *slice = getSliceCache().allocate();
        slice->source_addr = static_cast<char *>(request.source) + offset;
        slice->length = chunk;
        slice->opcode = request.opcode;
        slice->target_id = request.target_id;
        slice->dpdk.dest_addr = request.target_offset + offset;
        slice->dpdk.seq_base = static_cast<uint32_t>(offset / payload_bytes_);
        slice->dpdk.flags = 0;
        slice->task = task;
        slice->status = Slice::PENDING;
        slice->ts = 0;
        task->slice_list.push_back(slice);
        __sync_fetch_and_add(&task->slice_count, 1);
        slices.push_back(slice);
        offset += chunk;
    } while (offset < request.length);
    for (Slice *slice : slices) dispatch(slice);
}

void DpdkTransport::dispatch(Slice *slice) {
    const size_t index = next_worker_.fetch_add(1) % workers_.size();
    for (int attempt = 0; attempt < 2000; ++attempt) {
        if (workers_[index]->submit(slice)) return;
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    LOG(ERROR) << "DpdkTransport: worker " << index
               << " submission queue is "
                  "full, failing slice";
    slice->markFailed();
}

}  // namespace mooncake

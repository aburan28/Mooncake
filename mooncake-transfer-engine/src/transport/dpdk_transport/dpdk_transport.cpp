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

namespace mooncake {

DpdkTransport::DpdkTransport() = default;

DpdkTransport::~DpdkTransport() = default;

int DpdkTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> meta,
                           std::shared_ptr<Topology> topo) {
    (void)topo;
    metadata_ = meta;
    local_server_name_ = local_server_name;
    LOG(ERROR) << "DpdkTransport: data plane not implemented in this build";
    return -1;
}

Status DpdkTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    (void)batch_id;
    (void)entries;
    return Status::NotImplemented("DpdkTransport::submitTransfer");
}

Status DpdkTransport::submitTransferTask(
    const std::vector<TransferTask *> &task_list) {
    (void)task_list;
    return Status::NotImplemented("DpdkTransport::submitTransferTask");
}

Status DpdkTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                        TransferStatus &status) {
    (void)batch_id;
    (void)task_id;
    (void)status;
    return Status::NotImplemented("DpdkTransport::getTransferStatus");
}

int DpdkTransport::registerLocalMemory(void *addr, size_t length,
                                       const std::string &location,
                                       bool remote_accessible,
                                       bool update_metadata) {
    (void)addr;
    (void)length;
    (void)location;
    (void)remote_accessible;
    (void)update_metadata;
    return -1;
}

int DpdkTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    (void)addr;
    (void)update_metadata;
    return -1;
}

int DpdkTransport::registerLocalMemoryBatch(
    const std::vector<Transport::BufferEntry> &buffer_list,
    const std::string &location) {
    (void)buffer_list;
    (void)location;
    return -1;
}

int DpdkTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    (void)addr_list;
    return -1;
}

}  // namespace mooncake

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

#ifndef DPDK_TRANSPORT_H_
#define DPDK_TRANSPORT_H_

#include <memory>
#include <string>
#include <vector>

#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {

// Kernel-bypass transport (protocol "dpdk"): moves registered memory between
// peers over UDP carried by DPDK poll-mode drivers (including the AF_XDP
// PMD), with a receiver-driven reliable protocol (MKTP). Selected for a
// request when the target segment advertises protocol "dpdk"; installed
// when MC_DPDK_PORTS is set.
class DpdkTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;

    DpdkTransport();
    ~DpdkTransport() override;

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

    const char *getName() const override { return "dpdk"; }

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo) override;

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata) override;

    int unregisterLocalMemory(void *addr, bool update_metadata) override;

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location) override;

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

   private:
    bool installed_ = false;
};

}  // namespace mooncake

#endif  // DPDK_TRANSPORT_H_

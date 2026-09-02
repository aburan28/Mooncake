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

#ifndef MOONCAKE_DPDK_MEM_H_
#define MOONCAKE_DPDK_MEM_H_

#include <rte_dev.h>
#include <rte_mbuf.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace mooncake {
namespace dpdk {

// A registered local memory range. Host regions are registered with DPDK
// (rte_extmem_register) and DMA-mapped for every port's device so that DATA
// packets can attach the source bytes as external mbuf buffers (zero-copy
// TX). When registration or mapping fails the region falls back to copying
// into mbufs, which is always correct. Device memory goes through rte_gpudev
// when the build has CUDA and the header is available; otherwise it is
// recorded as not host-accessible and transfers touching it fail cleanly.
struct Region {
    uint64_t addr = 0;
    uint64_t length = 0;
    uint64_t map_addr = 0;
    uint64_t map_len = 0;
    bool extmem_registered = false;
    bool dma_mapped = false;      // zero-copy TX eligible
    bool host_accessible = true;  // CPU may memcpy to/from it
    int16_t gpu_dev = -1;
    std::vector<struct rte_device *> mapped_devices;
    // Shared by every external mbuf attached to this region. The registry
    // holds one reference so the free callback never fires; unregister waits
    // for in-flight packets to drop theirs.
    mutable struct rte_mbuf_ext_shared_info shinfo;

    bool contains(uint64_t a, uint64_t l) const {
        return a >= addr && a + l <= addr + length && a + l >= a;
    }
};

class MemRegistry {
   public:
    MemRegistry(std::vector<struct rte_device *> devices, bool enable_dma);
    ~MemRegistry();

    int registerRegion(void *addr, size_t length, const std::string &location);
    int unregisterRegion(void *addr);
    std::shared_ptr<const Region> find(uint64_t addr, uint64_t length) const;
    bool dmaEnabled() const { return enable_dma_; }

   private:
    void mapHostRegion(Region &region);
    void unmapRegion(Region &region);
    bool mapDeviceRegion(Region &region, const std::string &location);

    std::vector<struct rte_device *> devices_;
    bool enable_dma_;
    mutable std::shared_mutex mutex_;
    std::map<uint64_t, std::shared_ptr<Region>> regions_;
};

}  // namespace dpdk
}  // namespace mooncake

#endif  // MOONCAKE_DPDK_MEM_H_

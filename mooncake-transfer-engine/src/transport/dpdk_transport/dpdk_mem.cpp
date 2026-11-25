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

#include "dpdk_mem.h"

#include <glog/logging.h>
#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_malloc.h>
#include <rte_memory.h>
#include <unistd.h>

#include <chrono>
#include <thread>

// GPU memory reaches the NIC through rte_gpudev (mlx5 only). The path is
// compiled only with CUDA builds that ship the header and is review-only in
// this tree.
#if defined(USE_CUDA) && __has_include(<rte_gpudev.h>)
#include <rte_gpudev.h>
#define MOONCAKE_DPDK_GPUDEV 1
#endif

namespace mooncake {
namespace dpdk {

namespace {

void noopExtbufFree(void *, void *) {}

bool isDeviceLocation(const std::string &location) {
    return !(location.empty() || location.rfind("cpu", 0) == 0 ||
             location.rfind("host", 0) == 0);
}

constexpr uint64_t kSmallRegionBytes = 64ull << 20;
constexpr uint64_t kLargePageBytes = 2ull << 20;

}  // namespace

MemRegistry::MemRegistry(std::vector<struct rte_device *> devices,
                         bool enable_dma)
    : devices_(std::move(devices)), enable_dma_(enable_dma) {
    if (enable_dma_ && rte_eal_iova_mode() != RTE_IOVA_VA) {
        LOG(WARNING) << "DpdkTransport: IOVA mode is PA; user memory cannot be "
                        "DMA-mapped for zero-copy TX, packets are copied";
        enable_dma_ = false;
    }
}

MemRegistry::~MemRegistry() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    for (auto &kv : regions_) unmapRegion(*kv.second);
    regions_.clear();
}

int MemRegistry::registerRegion(void *addr, size_t length,
                                const std::string &location) {
    auto region = std::make_shared<Region>();
    region->addr = reinterpret_cast<uint64_t>(addr);
    region->length = length;
    region->shinfo.free_cb = noopExtbufFree;
    region->shinfo.fcb_opaque = nullptr;
    rte_mbuf_ext_refcnt_set(&region->shinfo, 1);
    if (isDeviceLocation(location)) {
        region->host_accessible = false;
        if (!mapDeviceRegion(*region, location)) {
            LOG(WARNING) << "DpdkTransport: device memory at " << addr
                         << " registered without a DMA mapping; the dpdk "
                            "transport cannot move it";
        }
    } else if (enable_dma_) {
        mapHostRegion(*region);
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    regions_[region->addr] = region;
    return 0;
}

int MemRegistry::unregisterRegion(void *addr) {
    std::shared_ptr<Region> region;
    {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = regions_.find(reinterpret_cast<uint64_t>(addr));
        if (it == regions_.end()) return 0;
        region = it->second;
        regions_.erase(it);
    }
    unmapRegion(*region);
    return 0;
}

std::shared_ptr<const Region> MemRegistry::find(uint64_t addr,
                                                uint64_t length) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = regions_.upper_bound(addr);
    if (it == regions_.begin()) return nullptr;
    --it;
    if (!it->second->contains(addr, length)) return nullptr;
    return it->second;
}

void MemRegistry::mapHostRegion(Region &region) {
    // rte_extmem_register wants a page-aligned range and describes it in
    // page_sz units. Small regions use 4 KiB pages over the enclosing range;
    // large ones use 2 MiB units over the interior aligned range so the
    // memseg metadata stays small. Bytes outside the mapped range (at most
    // 2 MiB at each end) take the copy path.
    const uint64_t page = static_cast<uint64_t>(sysconf(_SC_PAGESIZE));
    uint64_t unit = page;
    uint64_t start = region.addr & ~(page - 1);
    uint64_t end = (region.addr + region.length + page - 1) & ~(page - 1);
    if (region.length > kSmallRegionBytes) {
        unit = kLargePageBytes;
        start = (region.addr + unit - 1) & ~(unit - 1);
        end = (region.addr + region.length) & ~(unit - 1);
        if (end <= start) return;
    }
    region.map_addr = start;
    region.map_len = end - start;
    int ret = rte_extmem_register(reinterpret_cast<void *>(start),
                                  region.map_len, nullptr, 0, unit);
    if (ret) {
        LOG(WARNING) << "DpdkTransport: rte_extmem_register("
                     << reinterpret_cast<void *>(start) << ", "
                     << region.map_len
                     << ") failed: " << rte_strerror(rte_errno)
                     << "; TX from this region is copied";
        region.map_len = 0;
        return;
    }
    region.extmem_registered = true;
    for (struct rte_device *dev : devices_) {
        ret = rte_dev_dma_map(dev, reinterpret_cast<void *>(start), start,
                              region.map_len);
        if (ret) {
            LOG(WARNING) << "DpdkTransport: rte_dev_dma_map on "
                         << rte_dev_name(dev)
                         << " failed: " << rte_strerror(-ret) << "; TX from "
                         << reinterpret_cast<void *>(region.addr)
                         << " is copied";
            for (struct rte_device *mapped : region.mapped_devices)
                rte_dev_dma_unmap(mapped, reinterpret_cast<void *>(start),
                                  start, region.map_len);
            region.mapped_devices.clear();
            return;
        }
        region.mapped_devices.push_back(dev);
    }
    region.dma_mapped = true;
}

bool MemRegistry::mapDeviceRegion(Region &region, const std::string &location) {
#ifdef MOONCAKE_DPDK_GPUDEV
    if (rte_gpu_count_avail() == 0) return false;
    const int16_t gpu = 0;
    int ret = rte_gpu_mem_register(gpu, region.length,
                                   reinterpret_cast<void *>(region.addr));
    if (ret) {
        LOG(WARNING) << "DpdkTransport: rte_gpu_mem_register failed: "
                     << rte_strerror(-ret);
        return false;
    }
    region.gpu_dev = gpu;
    region.map_addr = region.addr;
    region.map_len = region.length;
    for (struct rte_device *dev : devices_) {
        ret = rte_dev_dma_map(dev, reinterpret_cast<void *>(region.addr),
                              region.addr, region.length);
        if (ret) {
            LOG(WARNING) << "DpdkTransport: GPU rte_dev_dma_map on "
                         << rte_dev_name(dev)
                         << " failed: " << rte_strerror(-ret);
            for (struct rte_device *mapped : region.mapped_devices)
                rte_dev_dma_unmap(mapped, reinterpret_cast<void *>(region.addr),
                                  region.addr, region.length);
            region.mapped_devices.clear();
            rte_gpu_mem_unregister(gpu, reinterpret_cast<void *>(region.addr));
            region.gpu_dev = -1;
            return false;
        }
        region.mapped_devices.push_back(dev);
    }
    region.dma_mapped = true;
    return true;
#else
    (void)region;
    (void)location;
    return false;
#endif
}

void MemRegistry::unmapRegion(Region &region) {
    // In-flight packets still reference the region through external mbufs;
    // wait briefly for the NIC to release them.
    for (int i = 0; i < 10000 && rte_mbuf_ext_refcnt_read(&region.shinfo) > 1;
         ++i)
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    if (rte_mbuf_ext_refcnt_read(&region.shinfo) > 1) {
        LOG(WARNING) << "DpdkTransport: unregistering "
                     << reinterpret_cast<void *>(region.addr)
                     << " with packets still in flight";
    }
    for (struct rte_device *dev : region.mapped_devices)
        rte_dev_dma_unmap(dev, reinterpret_cast<void *>(region.map_addr),
                          region.map_addr, region.map_len);
    region.mapped_devices.clear();
    region.dma_mapped = false;
#ifdef MOONCAKE_DPDK_GPUDEV
    if (region.gpu_dev >= 0) {
        rte_gpu_mem_unregister(region.gpu_dev,
                               reinterpret_cast<void *>(region.addr));
        region.gpu_dev = -1;
    }
#endif
    if (region.extmem_registered) {
        rte_extmem_unregister(reinterpret_cast<void *>(region.map_addr),
                              region.map_len);
        region.extmem_registered = false;
    }
}

}  // namespace dpdk
}  // namespace mooncake

# io_uring and Kernel-Bypass Data Paths for PD KV-Cache Transfer

**Status:** implemented for Mooncake, vLLM and SGLang; see *Implementation status* below for what is verified and what still needs hardware. **Scope:** Mooncake Transfer Engine (classic TE and TENT), Mooncake Store, the vLLM `MooncakeConnector`, and the SGLang Mooncake PD backend plus HiCache storage tiers.

This document plans two related pieces of work:

1. Use **io_uring** for the network and storage data paths that move KV cache between prefill (P) and decode (D) nodes.
2. Add a **kernel-bypass** network path (DPDK, with AF_XDP as the portable variant) and, where the NIC allows it, reach GPU memory by DMA instead of staging through host memory.

The plan is grounded in a survey of the three repositories at these revisions: Mooncake `b8f294d`, vLLM `9db222c`, SGLang `6ff2a20`.

## 0. Implementation status

| Phase | State | Verified by |
|---|---|---|
| 1 io_uring TCP backend | Implemented (`MC_TCP_IO_BACKEND=io_uring`) | `tcp_uring_backend_test` 23/23; `tcp_write_visibility_test` 44/44 under asio, 11 wire-level under io_uring with 33 asio-internal cases skipped |
| 2 dmabuf zero copy | Implemented; negotiation and fallback exercised, data path needs hardware | Capability probe reports unsupported and falls back on a host without header/data split |
| 3 vLLM / SGLang | Implemented (async submit/poll, `mooncake_env`, page-major buf infos, io_uring storage clients) | vLLM 49 connector tests, 46 offload tests; SGLang 19 async, 3 page-major, 16 uring-client tests |
| 4 DPDK / AF_XDP | Implemented (`MC_DPDK_PORTS`, MKTP over UDP) | `dpdk_transport_test` 9/9 over an in-process ring PMD pair, including recovery under 20-50% injected packet loss |
| 5 TENT parity | Not started | Deferred: no released wheel profile builds TENT (`USE_TENT` is absent from every profile), so it reaches no user until that release decision is made |

Two defects the tests found, both fixed:

- A zero-copy send reserves two completions (result and notification) but
  retired only one, so a connection stalled on the first payload at or above
  the threshold.
- A peer that rejects one pipelined request closes the connection, and the
  reset discards the status frames of requests it had already accepted, so a
  pipelined retry replayed the same window until it gave up. Retries now send
  one request per connection.

Measured on this development VM (loopback, 4 shared cores, no NIC and no GPU),
`transfer_engine_bench --protocol=tcp`, 2 threads, batch 16:

| Backend | 64 KiB blocks | 4 MiB blocks |
|---|---|---|
| asio | 1.30 GB/s | 1.26 GB/s |
| io_uring, zero copy at 16 KiB (old default) | 0.03 GB/s | 1.08 GB/s |
| io_uring, zero copy disabled | 0.74 GB/s | — |

Loopback has no NIC DMA to win back, so zero copy loses at every size there
and the io_uring backend does not beat asio on this host. That is a property
of the environment, not a result for real hardware: the numbers say the
default zero-copy threshold was wrong (raised from 16 KiB to 256 KiB, the
kernel's own crossover guidance) and that Phase 0 must be re-run on the target
NICs before the backend is made the default.

## 1. Summary and recommendation

### 1.1 What the survey found

- **RDMA is already optimal.** With RDMA NICs, Transfer Engine performs one-sided (GPUDirect) RDMA with zero copies. Neither io_uring nor DPDK improves on that. This plan does not touch the RDMA path; it targets deployments where P and D are connected by plain Ethernet (cloud VMs without EFA or RoCE, Intel or Broadcom NICs without RDMA configured, virtio, gve).
- **The non-RDMA path is the classic TE `tcp` transport.** vLLM selects it with `kv_connector_extra_config.mooncake_protocol: "tcp"`; SGLang selects it with `--disaggregation-transfer-backend mooncake_tcp` (which sets `MC_FORCE_TCP=1`) or `MOONCAKE_PROTOCOL=tcp`. That transport runs one asio io thread, moves data in 64 KiB chunks (`MC_TCP_SLICE_SIZE`), and stages every GPU-resident chunk through host memory with a synchronous `cudaMemcpy`. Each GPU byte crosses PCIe twice and the host memory bus twice on each side.
- **Mooncake already ships io_uring for files.** Mooncake Store has `UringFile` (thread-local rings, registered buffers, O_DIRECT reads, batch reads), an NVMe `uring_cmd` passthrough executor, and TENT has a file-segment `IOUringTransport`. There is no io_uring or kernel-bypass **network** path in any of the three repos, and no DPDK code beyond SPDK's static libraries for the NVMe-oF SSD pool.
- **Released Mooncake wheels do not build TENT** (`USE_TENT` is absent from every wheel profile). Whatever ships to vLLM and SGLang users must land in classic TE first.
- **Upstream RFC #3446** ("TCP Transport Optimization for Non-RDMA Fallback") is already optimizing the asio transport (multi-stream windows, io_context pool, async connect, socket tuning) and lists an io_uring backend and `SO_ZEROCOPY` as non-goals because the bottleneck it measured is concurrency and buffer sizing, not syscall cost. That reasoning holds for host-to-host traffic at 100 GbE and below. This plan does not replace that work. It targets what #3446 leaves untouched: the GPU staging copies (issue #2843 quantifies them), zero-copy GPU<->NIC DMA over plain Ethernet, CPU efficiency at 200 GbE and above, and the storage tiers.
- **vLLM and SGLang both drive Transfer Engine synchronously** (`batch_transfer_sync_write` on a thread pool). A transport with a submit/poll shape lets both delete their thread-pool hop.

### 1.2 Recommended order

| Phase | Work | Where | Effort | Gate |
|---|---|---|---|---|
| 0 | Measure the non-RDMA path on the target NICs and kernels | all | 1–2 weeks | none |
| 1 | io_uring backend for the classic TE `tcp` transport, wire-compatible with asio, plus pipelined pinned staging for GPU memory | Mooncake | 3–4 weeks | Phase 0 baseline |
| 2 | Zero-copy GPU<->NIC over kernel TCP with dmabuf: io_uring zero-copy receive (zcrx) into GPU memory, devmem TCP transmit from GPU memory | Mooncake | 6–8 weeks | kernel ≥ 6.16, HDS-capable NIC |
| 3 | Async submit/poll, configuration surface, storage-tier io_uring | vLLM, SGLang | 2–3 weeks each, parallel with 1–2 | Phase 1 API |
| 4 | DPDK / AF_XDP kernel-bypass transport (experimental) | Mooncake | 10–14 weeks | only if Phases 1–2 miss the target on the deployment NICs |
| 5 | TENT parity for the new transports | Mooncake | 3–4 weeks | wheels build TENT |

Phase 2 is the largest available win: it is the same mechanism Google productizes as GPUDirect-TCPX and removes every host copy from the GPU path. Phase 4 is the most expensive and least portable option, so it is gated on measurements rather than scheduled unconditionally.

## 2. Current state (survey results)

### 2.1 How a KV block moves today on the non-RDMA path

vLLM `MooncakeConnector`, WRITE direction (P pushes into D):

1. D sends its destination block addresses to P over ZMQ (`MooncakeXferMetadata`). P builds `(src, dst, len)` lists coalesced over contiguous block runs (`group_concurrent_contiguous`) in `_build_transfer_params`.
2. P calls `engine.batch_transfer_sync_write(session, src_ptrs, dst_ptrs, lengths)` from `_send_blocks`, executed on a `ThreadPoolExecutor(num_workers=10)`.
3. `MultiTransport::selectTransport` (`mooncake-transfer-engine/src/multi_transport.cpp`) picks the transport from the **remote** segment descriptor's `protocol` string. For `tcp`, `TcpTransport::startTransfer` allocates one `Slice` per request and runs a `ClientSession` on the single io thread, over one of `MC_TCP_LANES_PER_PEER` (default 4) pooled connections.
4. Per 64 KiB chunk: if the source is CUDA memory, a synchronous `cudaMemcpy` into a `std::vector<char>` staging buffer, then `asio::async_write` (one `sendmsg`). The receiver does `async_read` into its own staging buffer, then `cudaMemcpy` H2D, and under protocol v2 sends an 8-byte status frame once the payload is applied.
5. `Slice::markSuccess()` updates `TransferTask` counters; `getTransferStatus` compares counters.

SGLang uses the same data plane through `MooncakeKVManager._transfer_data` -> `engine.batch_transfer_sync`, fed by `SGLANG_DISAGGREGATION_QUEUE_SIZE` queues and `SGLANG_DISAGGREGATION_THREAD_POOL_SIZE` threads.

Per-byte cost on the GPU path today: two CUDA copies (D2H on the sender, H2D on the receiver), one `copy_from_user` and one `copy_to_user` in the kernel, at least one `sendmsg` and one `recvmsg` per 64 KiB, plus epoll wakeups, all on one thread per process. Slices of a grouped task are strictly serialized (`startTransferSequence`).

### 2.2 Mooncake

| Item | Fact |
|---|---|
| Transport interface | `include/transport/transport.h`: `Transport` with pure virtual `submitTransfer`, `getTransferStatus`, `registerLocalMemory`, `unregisterLocalMemory`, `registerLocalMemoryBatch`, `unregisterLocalMemoryBatch`, `getName`; `MultiTransport` calls `submitTransferTask(std::vector<TransferTask*>)`. `BatchID` is a pointer to `BatchDesc`; `TransferTask` owns `Slice`s allocated from a thread-local cache. `Slice` carries an untagged per-transport union (`rdma`, `tcp {uint64_t dest_addr;}`, `nvmeof`, ...). |
| Dispatch | `MultiTransport::installTransport` is an `#ifdef`-gated chain keyed by protocol string (`"rdma"`, `"tcp"`, `"efa"`, `"nvmeof"`, ...). `selectTransport` reads `SegmentDesc::protocol` from the peer; under `ENABLE_MULTI_PROTOCOL` a comma-joined list is resolved by a fixed priority ladder (`hip/maca/musa=4, cxl=3, rdma=2, tcp=1`). `TransferEngineImpl::init` installs `tcp` when no HCA is found or `MC_FORCE_TCP` is set, and leaves the comment `// TODO: install other transports automatically`. |
| TCP transport | `src/transport/tcp_transport/`: standalone asio, one io thread (`TcpTransport::worker`), `SessionHeader {uint64_t size; uint64_t addr; uint8_t opcode;}` sent raw, protocol v2 status frames (`kStatusMagic`), connection lanes with admission control (`ConnectionLaneState`), `TcpStagingBuffer` + `cudaMemcpy` for device memory, env knobs `MC_TCP_*`, test hooks under `MOONCAKE_TCP_TRANSPORT_TEST_HOOKS`. Registration is metadata-only (`addLocalMemoryBuffer`), no pinning. |
| Metadata | `SegmentDesc` publishes `protocol`, `tcp_data_host`, `tcp_data_port`, `tcp_proto_version`, `buffers[]`; `encodeSegmentDesc`/`decodeSegmentDesc` in `src/transfer_metadata.cpp` have per-protocol branches, and the multi-protocol allowlist is duplicated in both. `HandShakeDesc::payload` is an opaque per-transport blob exchanged through `SocketHandShakePlugin` on the RPC port. |
| Existing io_uring | Store: `UringFile` (`mooncake-store/src/uring_file.cpp`, `SharedUringRing` thread-local, `io_uring_register_buffers`, `read_fixed/write_fixed`, `batch_read`, O_DIRECT reads), `uring_submit.h` retry helper, `nvme_kv_executor_io_uring.cpp` (`IORING_SETUP_SQE128|CQE32`, `NVME_URING_CMD_IO`); `MOONCAKE_OFFLOAD_USE_URING`. TENT: `tent/src/transport/io_uring/io_uring_transport.cpp` (file segments, one ring per sub-batch, bounce buffers for CUDA sources). Build: `mooncake_provide_liburing()` soft-detects liburing; consumers test `TARGET Mooncake::liburing` and define `USE_URING`. |
| TENT | `tent/src/transport/tcp/tcp_transport.cpp` moves data through coro_rpc with a full copy into a `std::string` on each side (`ControlClient::sendData/recvData`), on a thread pool. `TransportType` has `IOURING` (file transport) and `TCP`; `transport_loader.cpp` instantiates transports by config key `transports/<name>/enable`. `MC_USE_TENT=1` switches the Python binding to TENT, but no wheel profile passes `-DUSE_TENT=ON`. |
| Python binding | `mooncake-integration/transfer_engine/transfer_engine_py.cpp`: `initialize(hostname, metadata_server, protocol, device_name)`, `register_memory`, `batch_register_memory`, `transfer_sync_write/read`, `batch_transfer_sync_write/read`, `batch_transfer_async_write/read`, `get_batch_transfer_status`, `transfer_submit_write`, `transfer_check_status`, `get_notifies`, `send_probe`. `transport_hint` is accepted everywhere but only honored under `USE_TENT`. Module attributes `SUPPORT_EFA`, `SUPPORT_CUDA`, ... expose build capabilities. |
| Tests and benches | `tests/tcp_transport_test.cpp`, `tcp_write_visibility_test.cpp` (v1/v2 matrix and lane state machine, via test hooks), `tcp_cuda_staging_test.cpp`, `tcp_address_validation_test.cpp`; `example/transfer_engine_bench.cpp` (`--protocol tcp|rdma --use_vram --threads --block_size --batch_size`); `benchmark/` (tebench); `mooncake-store/benchmarks/file_interface_bench.cpp`, `uring_batch_read_bench.cpp`. |
| Process | `CONTRIBUTING.md`: an RFC issue is expected above 500 LOC; `AGENTS.md`: check for overlapping issues/PRs, PR-scoped `pre-commit`, lean diffs, AI-assistance disclosure. Related RFCs: #3446 (TCP optimization), #2843 (VRAM staging cost), #3377 (`rdma_twosided`, the template for a multi-PR transport RFC). |

### 2.3 vLLM

| Item | Fact |
|---|---|
| Connectors | `vllm/distributed/kv_transfer/kv_connector/v1/`: `MooncakeConnector` (P-push over TE), `MooncakeStoreConnector`, `NixlConnector`/`NixlPushConnector` (async `make_prepped_xfer` + `transfer` + `check_xfer_state`), LMCache, MoRIIO, `OffloadingConnector`, `SimpleCPUOffloadConnector`, HF3FS, and others. No io_uring, liburing, or DPDK anywhere in the repo. |
| MooncakeConnector | Engine init in `MooncakeConnectorWorker.__init__`: `TransferEngine().initialize(hostname, "P2PHANDSHAKE", protocol, device_name)`, protocol from `kv_connector_extra_config.mooncake_protocol` (default `"rdma"`), `device_name`, `num_workers` (default 10). Registration: `register_kv_caches` dedups by `untyped_storage().data_ptr()` and calls `batch_register_memory` once per distinct allocation. Transfer: `_send_blocks` -> `batch_transfer_sync_write`, blocking, on the thread pool; completion via `finished_sending_reqs` -> `get_finished`. Control plane: FastAPI bootstrap (`/register`, `/query`) plus ZMQ ROUTER/DEALER. Env: `VLLM_MOONCAKE_BOOTSTRAP_PORT`, `VLLM_MOONCAKE_ABORT_REQUEST_TIMEOUT`. Stats already report avg/P90 transfer time, MB/s, and descriptors per transfer (`MooncakeKVConnectorStats`). |
| KV layout | `vllm/v1/worker/utils.py::allocate_kv_cache`: every layer of every group lives in **one** `torch.int8` allocation rounded up to 4 KiB; `KVCacheTensor{offset, layer_stride, block_stride}` gives per-block byte offsets; logical shape `[L, B, H, N, C]` with `KVCacheLayout` (`LBHNC` required by Mooncake and NIXL unless MLA). |
| Disk and host paths | `vllm/v1/simple_kv_offload/disk_backend.py` (O_DIRECT, `preadv`/`pwritev` on coordinator threads, 4 KiB-aligned host slots) and `vllm/v1/kv_offload/tiering/fs/io.py` (O_DIRECT `os.write`/`os.readv`). NIXL host-buffer mode copies per layer with no batching or pinning (`copy_kv_blocks`), while `kv_offload/cpu/gpu_worker.py` already has pooled streams, pinned descriptor buffers, and `cudaHostRegister` on mmap regions. |
| Tests and CI | `tests/v1/kv_connector/unit/test_mooncake_connector.py` (`FakeMooncakeWrapper` with `initialize`, `get_rpc_port`, `batch_transfer_sync_write`, `batch_register_memory`), `mooncake_integration/config_sweep_accuracy_test.sh`; Buildkite area `disaggregated_mooncake.yaml` runs on the B200 Kubernetes pool because it is the only pool with `IPC_LOCK` and host networking. Pins: `mooncake-transfer-engine >= 0.3.12`. |

### 2.4 SGLang

| Item | Fact |
|---|---|
| Backend selection | `server_args.py`: `DISAGG_TRANSFER_BACKEND_CHOICES = ["mooncake", "nixl", "ascend", "fake", "mori", "mooncake_tcp"]` (a mutable list, extendable by plugins). `arg_groups/pd_disaggregation_hook.py` rewrites `mooncake_tcp` to `mooncake` and sets `MC_FORCE_TCP=1`. |
| Engine | `distributed/device_communicators/mooncake_transfer_engine.py`: process-global `MooncakeTransferEngine` created in `ModelRunner.__init__`; `initialize` uses `envs.MOONCAKE_PROTOCOL` (default `"rdma"`) and hardcodes `"P2PHANDSHAKE"`; wrappers `register`, `batch_register`, `transfer_sync`, `batch_transfer_sync` (no async wrapper). |
| Data plane | `disaggregation/mooncake/conn.py`: `MooncakeKVManager._transfer_data` is the single call into `batch_transfer_sync`; `_send_kvcache_generic` computes `src_ptr + page_index * item_len` after `group_concurrent_contiguous`; `send_kvcache_slice` (heterogeneous TP) emits one descriptor per layer, page, and token slot unless `SGLANG_DISAGG_STAGING_BUFFER=1`; `transfer_worker` threads per `FastQueue`; ZMQ control plane with `KVArgsRegisterInfo` (18 frames) and `TransferInfo` (10 frames); aiohttp bootstrap server. `KVPoll` is `Failed/Bootstrapping/WaitingForInput/Transferring/Success`. |
| KV layout | `memory_pool.py`: `get_contiguous_buf_infos()` returns per-buffer `(ptr, len, item_len)` with `item_len` = bytes per page; MHA is `[K_0..K_L-1, V_0..V_L-1]`, MLA one buffer per layer. `PageMajorMHATokenToKVPool.get_contiguous_buf_infos` raises with a TODO for a page-aware transfer scheme; HND layout is rejected. P and D must agree on `page_size` and `kv_cache_dtype`. |
| HiCache storage | `hicache_storage.py::HiCacheFile`: one file per page, buffered POSIX I/O, serial `_batch_io_v2`. `storage/hf3fs/hf3fs_usrbio_client.py` already has a prepare/submit/wait ring shape behind the `Hf3fsClient` ABC (`batch_read`, `batch_write`) and `create_hf3fs_client` dispatch. NIXL storage exposes `use_uring` and `SGLANG_HICACHE_NIXL_USE_DIRECT_IO`; `umbp` exposes `ssd_io_backend: io_uring` (implemented in the external `mori` package). Host pools are pinned (`pin_memory`) and know `is_stride_page_aligned(4096)`. |
| Env conventions | `python/sglang/srt/environ.py` `Envs` class; vendor-prefixed `MOONCAKE_*` variables are registered there and read with `.get()`. `MC_FORCE_TCP` and `MC_TCP_ENABLE_CONNECTION_POOL` are read raw today. The `env-var-conventions` skill forbids new raw `os.getenv` reads and forbids CLI flags that only forward to an env var. |
| Tests | `test/registered/disaggregation/` (21 files) on `PDDisaggregationServerBase` (`SGLANG_TEST_PD_DISAGG_BACKEND`, `SGLANG_TEST_PD_DISAGG_DEVICES`); HiCache tests under `test/registered/hicache/` and `test/registered/unit/mem_cache/`. No PD benchmark directory; `sglang.bench_serving` against the load balancer is the benchmark. |

## 3. Where the time goes and what each technique fixes

The table is for a P->D WRITE of GPU-resident KV over plain Ethernet. "Host copy" means a CPU memcpy of the payload; "PCIe" counts payload crossings on one host.

| Cost item | Today (asio `tcp`) | Phase 1 (io_uring backend) | Phase 2 (dmabuf zero copy) | Phase 4 (DPDK) |
|---|---|---|---|---|
| Sender CUDA copy | synchronous `cudaMemcpy` per 64 KiB, blocks the io thread | `cudaMemcpyAsync` into pinned registered chunks, double-buffered, off the ring thread | none (NIC DMAs from GPU memory via devmem TX) | none on mlx5 (gpudev), else same as Phase 1 |
| Sender kernel copy | `copy_from_user` per `sendmsg` | none for payload ≥ 16 KiB (`SEND_ZC` from registered buffers) | none | none |
| Receiver kernel copy | `copy_to_user` into staging | into the final host destination (host-to-host) or pinned chunk (GPU) | none (NIC DMAs into GPU zcrx area) | mbuf -> destination copy on CPU or DMA engine |
| Receiver CUDA copy | synchronous `cudaMemcpy` per chunk | `cudaMemcpyAsync` from pinned chunks, pipelined | one D2D scatter kernel per transfer (GPU-local, HBM speed) | H2D from mbufs or none on mlx5 |
| PCIe crossings per host | 2 | 2 | 1 | 1–2 |
| Syscalls per 64 KiB per side | ≥ 2 plus epoll | ~0 (one `io_uring_enter` per submitted batch, none with SQPOLL) | ~0 | 0 (poll mode) |
| Threads | 1 io thread | N ring workers, connections pinned | N ring workers, one HW RX queue each | dedicated polling lcores |
| Task slices | serialized per task | one `sendmsg` iovec per task (up to 1024 slices) | same | one credit window per task |
| Portability | any kernel | kernel ≥ 6.1 (full feature set), any NIC | kernel ≥ 6.16, HDS-capable NIC (bnxt, gve, mlx5 ≥ 6.17, fbnic), dmabuf-capable GPU driver | PMD per NIC; GPU DMA only on mlx5; AF_XDP for the rest |

Expected effect, qualitatively: Phase 1 lowers CPU per GB and removes the per-chunk CUDA stall that #2843 measured; it does not change the number of PCIe crossings. Phase 2 removes every host-side copy and halves PCIe traffic, which is what gets a 200 GbE link to line rate with about one core. Phase 4 buys CPU efficiency and latency on NICs that have neither RDMA nor HDS, at the price of a user-space reliability protocol and dedicated cores.

## 4. Design

### 4.1 Phase 1: io_uring backend for the classic TE `tcp` transport

**Decision: wire-compatible backend, not a new protocol string.** `selectTransport` routes by the peer's `protocol` string, so a new string (`tcp_uring`) would partition the cluster during rollout. Keeping `protocol = "tcp"`, `SessionHeader`, and the v2 status frames means an io_uring sender can talk to an asio receiver and vice versa, and #3446's lane pool, admission control, address validation, timeouts, and test hooks are reused rather than forked. Selection is per process: `MC_TCP_IO_BACKEND=asio|io_uring` (default `asio` until soak results are in), CMake `USE_IOURING_TCP` (auto-on when `Mooncake::liburing` is found), compile definition `USE_IOURING_TCP`.

**Files.** New `src/transport/tcp_transport/tcp_uring_worker.{h,cpp}`, `tcp_uring_session.{h,cpp}`, `tcp_staging_pool.{h,cpp}`; `tcp_transport.cpp::install` picks the backend and `startTransfer` dispatches to it; `tcp_transport.h` grows the backend enum. The session/framing constants move to a small `tcp_wire.h` shared by both backends (and later by TENT).

**Ring workers.** `MC_TCP_URING_WORKERS` workers (default `min(4, ncpu/8)`), each owning one ring created with `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_COOP_TASKRUN` (fallback to flags 0 on `EINVAL`), `sq_entries` 4096, `io_uring_register_ring_fd`, a sparse registered file table (`io_uring_register_files_sparse`) and a sparse registered buffer table (`io_uring_register_buffers_sparse`). `MC_TCP_URING_SQPOLL=1` enables `IORING_SETUP_SQPOLL` for deployments that can spare a core. Cross-thread submissions go through an MPSC queue plus `IORING_OP_MSG_RING` to wake the target worker.

**Accept and connect.** Each worker owns a `SO_REUSEPORT` listener on `tcp_data_port` (so the advertised port stays single) with multishot accept into the direct file table. Client lanes keep `ConnectionLaneState` but connect with `IORING_OP_CONNECT` and are pinned to a worker by lane id, removing the blocking `getConnection` path (#3446 bottleneck 3).

**Memory registration.** `registerLocalMemory` for host memory registers the region into every worker's buffer table (regions above 1 GiB are split into 1 GiB entries; failure disables fixed buffers for that region, matching `UringFile`'s behaviour). CUDA regions are recorded for staging only.

**Client WRITE (v2).** One `IORING_OP_SENDMSG_ZC` per task with `iov = [SessionHeader, slice_0, ..., slice_n]` (up to `IOV_MAX`) using `IORING_RECVSEND_FIXED_BUF` when the payload lies in a registered region, then a linked `RECV` for the 8-byte status frame with `IORING_OP_LINK_TIMEOUT` carrying `MC_TCP_STATUS_TIMEOUT_SEC`. Payloads below `MC_TCP_URING_ZC_THRESHOLD` (default 16 KiB) use copying `SEND` (zero copy loses below that size). Slice success is recorded only after the v2 status frame, which arrives after the receiver applied the data, so the `SEND_ZC` notification CQE (`IORING_CQE_F_NOTIF`) never gates buffer reuse. This answers the `SO_ZEROCOPY` objection in #3446: the source is a long-lived registered region, not a transient buffer, and completion is defined by the receiver's ack. Short completions re-post the tail.

**Client READ.** `SEND` header, then `RECV` with `MSG_WAITALL` of the status frame and payload directly into the destination address (host) or into a pinned chunk (CUDA).

**Server.** Multishot `RECV` on a provided-buffer ring (4 KiB entries) for headers; on a header, `validateTcpAddress`, then `RECV` (`MSG_WAITALL`, exact size) straight into the validated address for WRITE, or `SEND_ZC` from the address for READ, then the status frame. The host path stays zero copy exactly as asio's `writeBody` is today; the difference is one SQE per message instead of one callback and syscall per 64 KiB.

**GPU memory (staging).** Per worker and per device: a pinned pool (`cudaHostAlloc(cudaHostAllocPortable)`) registered as fixed buffers, chunk size `MC_TCP_STAGING_CHUNK` (default 2 MiB, not 64 KiB), depth 4, one CUDA stream per worker. D2H copies are `cudaMemcpyAsync` with `cudaEventQuery` polled between `io_uring_submit_and_wait` timeouts (or a `cudaLaunchHostFunc` that writes a registered eventfd), so chunk k+1 copies while chunk k is on the wire. On receive, the status frame is sent after the H2D copy completes, preserving v2 semantics. `cudaSetDevice` is called per worker from `getCudaDeviceId(addr)`. This alone addresses #2843.

**Completion and timeouts.** `Slice::markSuccess/markFailed` and `TcpTransport::getTransferStatus` are unchanged. `MC_TCP_PROGRESS_TIMEOUT_SEC` maps to `IORING_OP_TIMEOUT` re-armed on progress.

**Fallback.** `install()` probes `io_uring_queue_init`; `EPERM`/`ENOSYS` (seccomp, old kernel) falls back to asio with one WARNING. Missing flags (`DEFER_TASKRUN` on < 6.1) degrade to a plain ring.

**Metrics.** Under `MC_TE_METRIC`: SQEs per submit, CQEs per wait, zero-copy bytes, staging bytes, staging wait time, short completions.

**Tests.** Parametrize `tcp_write_visibility_test.cpp` on the backend env so the v1/v2 and lane matrices run under io_uring; add `tcp_uring_backend_test.cpp` (loopback; skipped when the probe fails), a mixed-backend interop test (asio client to io_uring server and back), and extend `tcp_cuda_staging_test.cpp` for the pipelined pool. CI: a `ci.yml` job on `ubuntu-24.04` (kernel 6.8) with `-DUSE_IOURING_TCP=ON` and `MC_TCP_IO_BACKEND=io_uring`.

**Size.** About 2.5–3.5k LOC plus tests. Exceeds the 500 LOC RFC threshold: file an RFC modelled on #3377 that references #3446 and #2843 and states the complementary scope.

### 4.2 Phase 2: zero-copy GPU<->NIC over kernel TCP (dmabuf)

**What it is.** Linux can now DMA TCP payload directly into and out of device memory without RDMA: devmem TCP receive (6.12), io_uring zero-copy receive `IORING_OP_RECV_ZC` with `IORING_REGISTER_ZCRX_IFQ` (6.15), dmabuf-backed zcrx areas `IORING_ZCRX_AREA_DMABUF` (6.16), devmem TCP transmit with `sendmsg(MSG_ZEROCOPY)` and `SCM_DEVMEM_DMABUF` (6.16), receive buffers larger than 4 KiB (6.18), and shared zcrx queues across rings (6.19). The NIC must support header/data split, flow steering, and RSS; today that is bnxt_en, gve, mlx5 (netmem/zcrx series merged for 6.17), and fbnic.

**Wire and capability negotiation.** Add optional fields to `SegmentDesc` (ignored by older peers): `tcp_caps` (bit 0 `ZCRX_RECV`, bit 1 `DEVMEM_SEND`) and `tcp_zc_ports[]` (one data port per zero-copy RX queue). `tcp_proto_version` stays 2 for the control connection. A zero-copy lane is a pair of sockets: a **control** connection carrying `SessionHeader` and status frames (plain TCP), and a **data** connection carrying payload only, steered to a zero-copy HW queue. The split is required because every payload byte of a zcrx socket lands in device memory, where the CPU cannot parse framing.

**Receive.** Per worker: `io_uring_register_ifq` with `if_idx`, a dedicated `if_rxq`, a refill ring, and an area backed by a dmabuf exported from a `cudaMalloc` region of `MC_TCP_ZCRX_AREA_MB` (default 512) using `cuMemGetHandleForAddressRange(CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD)`. Mooncake already exports dmabufs for RDMA registration when `WITH_NVIDIA_PEERMEM=0`; reuse that helper. `rx_buf_len` 16–32 KiB on 6.18+, else 4 KiB. `RECV_ZC` completions return `(area offset, len)` fragments in stream order; the worker attributes them to the current task from the control stream, builds a fragment list in pinned memory, and launches one gather kernel (or `cudaMemcpyBatchAsync` on CUDA 12.8+) per task to scatter the fragments into the destination KV blocks, then returns the fragments to the refill ring and sends the status frame after the kernel's event completes. This is the same "unpack" step TCPX performs; it costs HBM bandwidth, not PCIe.

**Steering.** Per zero-copy queue: `ethtool -N <if> flow-type tcp4 dst-port <zc_port> action <rxq>`, RSS context excluding those queues, `ethtool -G <if> tcp-data-split on`, MTU 9000. Provide `scripts/setup_tcp_zerocopy.sh` for operators; the transport verifies the queue and steering at install and refuses to advertise `ZCRX_RECV` otherwise. Requires `CAP_NET_ADMIN`.

**Transmit.** Bind the KV region's dmabuf to the netdev over netlink (`NETDEV_CMD_BIND_TX`), obtain `tx_dmabuf_id`, then `sendmsg(MSG_ZEROCOPY)` with iov entries expressed as dmabuf offsets and a `SCM_DEVMEM_DMABUF` cmsg; completions arrive on `MSG_ERRQUEUE`, read with io_uring `RECVMSG`. Whether `IORING_OP_SENDMSG_ZC` passes the cmsg through on the target kernel must be verified during Phase 0; the fallback is a syscall `sendmsg` from the worker, still zero copy. A sender without devmem support can still use a zero-copy receiver: host-staged bytes are DMA'd by the NIC into the GPU area, which removes the receiver's copies.

**Fallback ladder.** dmabuf zero copy -> Phase 1 pinned staging -> asio. Decided per peer from `tcp_caps`, per buffer from whether it is dmabuf-bound, and at runtime on `register_ifq` or bind failure.

**Tests.** The fragment-to-gather planner and the capability negotiation are unit-testable on CPU. Data-path tests need hardware (zcrx does not work on loopback); add a nightly job on a bnxt or gve or mlx5 node and a self-test binary modelled on the kernel's `tools/testing/selftests/drivers/net/hw/iou-zcrx.c`.

**Size.** About 3–4k LOC. 6–8 weeks, hardware-gated.

### 4.3 Phase 3 (vLLM)

1. **Configuration surface.** `mooncake_protocol: "tcp"` already works. Add an optional `mooncake_env: {"MC_TCP_IO_BACKEND": "io_uring", ...}` map in `kv_connector_extra_config`, applied with `os.environ.setdefault` before `TransferEngine()` is constructed, so a single `--kv-transfer-config` configures the transport under Ray and multi-node launches. Document the `MC_TCP_*`, zero-copy, and `MC_DPDK_*` knobs in `docs/features/mooncake_connector_usage.md`.
2. **Async submit/poll.** Replace `_send_blocks` on the 10-thread pool with `batch_transfer_async_write` plus one poller coroutine in `_sender_listener` that drains `get_batch_transfer_status` with an adaptive sleep, keeping `num_workers` as the in-flight batch cap. Keep the synchronous path when the installed wheel lacks the async API (`hasattr`). Completion still flows through `finished_sending_reqs` -> `get_finished`, so the scheduler contract does not change. Extend `FakeMooncakeWrapper` with the async methods and unit-test the poller.
3. **dmabuf readiness.** The KV cache is one 4 KiB-rounded allocation, so a single dmabuf export covers it. Verify during Phase 0 that export works under PyTorch's `expandable_segments`; if not, allocate the KV buffer from a `torch.cuda.MemPool` with an export-friendly allocator, as SGLang already does for `SGLANG_MOONCAKE_CUSTOM_MEM_POOL`.
4. **Storage tier.** Give `simple_kv_offload/disk_backend.py` and `kv_offload/tiering/fs/io.py` an io_uring backend through the shared `mooncake.uring` module (section 4.7): registered pinned slots, O_DIRECT, batch depth 32–128, no coordinator threads.
5. **CI.** Add a `tcp` + `MC_TCP_IO_BACKEND=io_uring` sweep to `mooncake_integration/config_sweep_accuracy_test.sh`; confirm the B200 pool's seccomp profile allows `io_uring_setup` (section 7).

### 4.4 Phase 3 (SGLang)

1. **Selection surface.** Register the vendor-prefixed knobs in `Envs` (keeping the `MC_` prefix, per the env-var conventions): `MC_FORCE_TCP`, `MC_TCP_IO_BACKEND`, `MC_TCP_ZC`, `MC_DPDK_PORTS`, and migrate the two raw `os.environ` reads. Add a `mooncake_dpdk` alias next to `mooncake_tcp` in `DISAGG_TRANSFER_BACKEND_CHOICES` and `pd_disaggregation_hook.py` (sets `MOONCAKE_PROTOCOL=dpdk`). No new CLI flag that only forwards an env var.
2. **Async transfer.** In `MooncakeTransferEngine`, add `batch_transfer_async` and `get_batch_transfer_status` wrappers with a capability probe. In `MooncakeKVManager`, gate a `_transfer_data` variant behind `SGLANG_DISAGGREGATION_ASYNC_TRANSFER = EnvBool(False)` that submits every layer batch of a chunk and polls, so one `transfer_worker` keeps many batches in flight instead of fanning out over `ThreadPoolExecutor`. Extend `unified_memory_disagg_move_gate` to wait for outstanding async batches, since the in-flight window gets longer.
3. **Page-aware transfers.** Implement `PageMajorMHATokenToKVPool.get_contiguous_buf_infos` by exposing the single `_raw` buffer with `item_len` = bytes of one page across all layers. One page becomes one contiguous chunk, so `group_concurrent_contiguous` yields far fewer, larger descriptors for every transport. Enable `SGLANG_DISAGG_STAGING_BUFFER` by default when the backend is TCP-based, since `send_kvcache_slice` per-token descriptors defeat any bulk transport.
4. **HiCache local NVMe over io_uring.** Add `UringLocalClient(Hf3fsClient)` in `storage/hf3fs/` backed by `mooncake.uring` on a local file (page slots, `batch_read`/`batch_write` with the pinned host pool registered as the fixed buffer, O_DIRECT), selected by `create_hf3fs_client`, and expose it as `--hicache-storage-backend hf3fs` with `{"client": "uring"}` in the extra config. This reuses the existing `numjobs`/`entries` batching and metrics rather than adding threads to `HiCacheFile`. Env: `SGLANG_HICACHE_FILE_BACKEND_IO = EnvStr("posix")` for the plain file backend if a lighter path is wanted.
5. **Tests.** A `TestDisaggregationMooncakeTcpUring` class in `test/registered/disaggregation/test_disaggregation_basic.py` (2-GPU runner, `SGLANG_TEST_PD_DISAGG_BACKEND=mooncake_tcp`, `MC_TCP_IO_BACKEND=io_uring`), unit tests for the async poller and the page-major buffer infos, and a `uring` variant of `test_hicache_storage_3fs_backend.py`.

### 4.5 Phase 4: DPDK / AF_XDP kernel-bypass transport (experimental)

**When it pays.** NICs with neither RDMA nor header/data split (many cloud vNICs, Intel E810/E830 without irdma, older Broadcom), or CPU-constrained hosts where kernel TCP with io_uring still costs too many cores at 200–400 GbE. On mlx5 the operator should use RDMA instead. DPDK reaches GPU memory by DMA only on mlx5 (`rte_gpudev`); everywhere else it lands in hugepage host memory and needs a copy to the destination, so its advantage over Phase 1 is CPU efficiency and latency, not fewer copies.

**Go/no-go.** Start only if Phase 0/1/2 measurements on the deployment NIC show under 80% of line rate at more than two cores per 100 Gb.

**Transport.** `src/transport/dpdk_transport/` implementing the five pure virtuals plus `submitTransferTask` and `getTransferStatus`; protocol string `"dpdk"`; `option(USE_DPDK OFF)`, `mooncake_provide_dpdk()` via `pkg-config libdpdk` (DPDK 23.11 or 24.11 LTS), `dependencies.sh` entry. `install()` initializes EAL from `MC_DPDK_EAL_ARGS` (default `-l <MC_DPDK_LCORES> --in-memory --no-telemetry --file-prefix mooncake-<pid>`) behind a probe so a DPDK-enabled build degrades to `tcp` when hugepages or ports are absent. Ports from `MC_DPDK_PORTS` (PCI address, or `net_af_xdp,iface=eth0` for the AF_XDP PMD). One RX and one TX queue per worker lcore; our UDP port is steered to those queues with `rte_flow` (mlx5 bifurcation) or the AF_XDP redirect program.

**Memory.** Host regions: `rte_extmem_register` + `rte_dev_dma_map`, zero-copy TX via external-buffer mbufs (`rte_pktmbuf_attach_extbuf`). GPU regions: `rte_gpu_mem_register` + `rte_dev_dma_map` where the NIC supports it, else the Phase 1 pinned staging pool. RX placement: mbuf to destination with `rte_dma` (Intel DSA/IOAT) when present, else CPU memcpy or batched `cudaMemcpyAsync`.

**Protocol (MKTP v0, over UDP).** 32-byte header: magic, version, flags, type (`REQ`, `GRANT`, `DATA`, `ACK`, `NACK`, `DONE`, `PROBE`), session, batch, task, sequence, offset, length; L3/L4 checksums offloaded. WRITE: initiator sends `REQ(task, dst_addr, len)`; the target validates the range against registered buffers and replies `GRANT(credits)`; the initiator sends MTU-sized `DATA` at line rate within credits; the target keeps a per-task bitmap and sends cumulative plus selective `ACK` every N packets or 100 µs; retransmission on `NACK` or an RTO of 4x estimated RTT (minimum 200 µs); `DONE` completes the slice. READ is symmetric. Receiver-driven credits handle incast and pace to the receiver's copy rate; ECN reaction is a v1 item. A `Slice` union arm `dpdk {uint64_t dest_addr; uint32_t seq_base;}` and per-worker `rte_ring` submission queues carry tasks; workers call `markSuccess`.

**Metadata.** `SegmentDesc` gains `dpdk_ip`, `dpdk_udp_port`, `dpdk_mac`, with encode/decode branches and the multi-protocol allowlist updated in both places; per-peer session parameters (MTU, credit window, queue ids) travel in `HandShakeDesc::payload` through the existing handshake daemon. `selectTransport`'s ladder places `dpdk` above `tcp` and below `rdma`. `TransferEngineImpl::init` auto-installs `dpdk` when `MC_DPDK_PORTS` is set.

**Deployment modes.** (a) mlx5 flow bifurcation: DPDK and the kernel share the port, no VF; (b) SR-IOV VF passthrough: a VF bound to `vfio-pci` is passed into the container or VM (Kubernetes SR-IOV device plugin plus Multus), the PF stays with the kernel; (c) AF_XDP on the primary interface with `XDP_ZEROCOPY` where the driver supports it, no vfio or hugepage-bound NIC. Hugepages are needed in all modes; `--in-memory` avoids leaking hugepage files; the polling lcores must be isolated from the inference scheduler's cores.

**Alternative.** libtpa (ByteDance's DPDK user-space TCP with zero-copy `tpa_zwritev`/`tpa_zreadv`) would remove the custom protocol, but it supports Mellanox NICs only, where RDMA is the better answer. Keep it as an optional backend only if a Mellanox-without-RDMA fleet exists.

**Tests.** Two engines connected through DPDK software PMDs (`net_memif` or `net_ring`) in CI, with a fault-injection hook that drops a percentage of `DATA` packets to exercise recovery; `transfer_engine_bench --protocol dpdk` on hardware nightly.

**Size.** About 8–12k LOC. 10–14 weeks. RFC required.

### 4.6 Phase 5: TENT parity

TENT's `TcpTransport` copies every payload into a `std::string` on both sides through coro_rpc, which is worse than classic TE. Replace its data path with the shared session code from Phase 1 (`tcp_wire.h`, `tcp_uring_session`), add a `DPDK` `TransportType` and `parseTransportType("dpdk")`, set `Capabilities.gpu_to_gpu` only when dmabuf or gpudev is available, and document policy entries such as `"transports": ["rdma", "dpdk", "tcp"]`. This phase only reaches vLLM and SGLang users once wheel profiles pass `-DUSE_TENT=ON`, which is a separate release decision.

### 4.7 Shared Python module `mooncake.uring`

Expose `UringFile`, `SharedUringRing`, `register_global_buffer`, `batch_read`, and `batch_write` through pybind as `mooncake.uring`, with a `SUPPORT_URING` attribute, so vLLM's disk offload, SGLang's HiCache client, and Mooncake Store share one io_uring file implementation instead of three. Add `SUPPORT_IOURING_TCP` and `SUPPORT_DPDK` attributes to `mooncake.engine` for feature detection.

The existing `get_batch_transfer_status` binding blocks until every listed batch is terminal and frees the ids, so neither vLLM nor SGLang can drive a poll loop with it. Add a non-blocking pair to `mooncake.engine.TransferEngine`: `batch_transfer_poll(batch_ids) -> list[int]` (0 completed, 1 in flight, -1 failed or timed out; never frees) and `batch_transfer_free(batch_ids)`. The connectors use the pair when present and fall back to the blocking call on older wheels.

## 5. Phases, milestones, and exit criteria

| Phase | Deliverables | Exit criteria |
|---|---|---|
| 0 Measure | Benchmark matrix (section 6) on the target NICs; syscall and `cudaMemcpy` profiles; dmabuf export and `SENDMSG_ZC` cmsg checks on the target kernel; written baseline | Baseline table published; kernel/NIC matrix confirmed; go/no-go for Phase 2 hardware |
| 1 io_uring TCP | RFC issue; backend behind `MC_TCP_IO_BACKEND`; pinned pipelined staging; tests; CI job; docs page `tcp_io_uring.md`; `mooncake.uring` module | Interop with asio peers; host-to-host CPU per GB down by at least 30% at 100 GbE; GPU path free of per-chunk stalls; no regression in `tcp_write_visibility_test` |
| 2 dmabuf zero copy | Capability negotiation; zcrx receive; devmem transmit; steering script; gather kernel; nightly hardware job; docs page `tcp_zero_copy.md` | GPU-to-GPU at ≥ 90% of line rate on the reference NIC with ≤ 1 core per 100 Gb; correct fallback when steering or export is unavailable |
| 3 vLLM / SGLang | Async submit/poll; config surface; page-major buffer infos (SGLang); storage-tier io_uring; tests; docs | Accuracy sweeps pass on `tcp` + io_uring; TTFT at 8k and 32k prompt lengths improves in proportion to measured bandwidth; thread counts reduced |
| 4 DPDK | RFC; transport; MKTP; software-PMD CI; deployment doc | Line rate on the reference non-HDS NIC with ≤ 2 cores per 100 Gb; loss recovery tests pass; experimental flag only |
| 5 TENT | Shared session code; DPDK transport type; selector docs | TENT tests green; wheels decision recorded |

Coordination with upstream: file the Phase 1 RFC before coding and reference #3446 and #2843; land the backend at the session level, in new files, so it does not conflict with #3446's lane and io_context changes; keep each PR under review-sized limits as #3377 did (benchmark and plumbing first, data path second, staging third).

## 6. Validation and benchmarking

**Tools already in the repos.** Mooncake `transfer_engine_bench` (`--protocol tcp --use_vram --threads --block_size --batch_size`) and tebench; `file_interface_bench --use-uring`; vLLM `vllm bench serve` with the random 7500-in/200-out profile from `examples/disaggregated/mooncake_connector/run_mooncake_connector.sh` plus `MooncakeKVConnectorStats` (avg and P90 transfer time, MB/s, descriptors per transfer); SGLang `sglang.bench_serving` through the load balancer plus `KVTransferMetric` (`transfer_latency_s`, `transfer_total_bytes`).

**Metrics.** Bytes per second per link; CPU cores per 100 Gb (`perf stat`, `mpstat`); syscalls per GB (`perf stat -e syscalls:sys_enter_sendmsg,syscalls:sys_enter_recvmsg,syscalls:sys_enter_io_uring_enter`); `cudaMemcpy` share of transfer time (nsys or nvtx ranges around staging); NIC drops and HDS counters (`ethtool -S`); end-to-end TTFT for 8k and 32k prompts at request rates 1, 2, 4 per second; P99 transfer time per request.

**Hardware matrix.** 100 GbE Intel E810 without irdma (Phase 1 and 4 target); 200 GbE Broadcom bnxt (Phase 2 target); GCP A3 with gve (Phase 2 target); mlx5 ConnectX-7 with RoCE disabled (Phase 2 on 6.17+, sanity comparison against RDMA); virtio or ENA (Phase 4 AF_XDP target). Kernels 6.8 (Ubuntu 24.04 GA), 6.16, and 6.18.

**Correctness.** Mooncake unit and interop tests per phase; vLLM `mooncake_integration/config_sweep_accuracy_test.sh` and `test_mooncake_connector.py`; SGLang `test_disaggregation_basic.py` plus the different-TP and PP variants under the TCP backend; HiCache storage tests with the uring client.

## 7. Prerequisites and risks

| Area | Requirement or risk | Mitigation |
|---|---|---|
| io_uring in containers | Docker's default seccomp profile blocks `io_uring_setup/enter/register` since 25.0, and runtimes that copy it do too | Ship a seccomp profile allowing the three syscalls; detect `EPERM` at install and fall back to asio with a clear log; document `securityContext.seccompProfile` for Kubernetes |
| Kernel versions | Full Phase 1 feature set needs 6.1+ (`SINGLE_ISSUER`, `DEFER_TASKRUN`, `SENDMSG_ZC`); Phase 2 needs 6.16+ (zcrx dmabuf, devmem TX), 6.17+ for mlx5, 6.18+ for large RX buffers | Feature-probe at install; degrade to plain rings; keep asio as the floor. Ubuntu 22.04 needs the HWE kernel |
| Registered buffers | Pinned pages are charged to the process memory cgroup; per-entry size limits on older kernels | Split regions into ≤ 1 GiB entries; size container memory limits to include the KV region; disable fixed buffers per region on failure |
| Zero-copy hardware | HDS, flow steering, and queue API are driver-specific; no loopback testing; `CAP_NET_ADMIN` for ethtool and netlink; dedicated RX queues reduce queues available to other traffic | Nightly hardware CI; operator script with verification; capability negotiation so mixed fleets keep working |
| GPU dmabuf export | Needs a driver with dmabuf support (open kernel modules); PyTorch allocator settings can affect exportability | Phase 0 check; `torch.cuda.MemPool` fallback; reuse Mooncake's existing dmabuf path |
| DPDK operations | Hugepages, IOMMU and vfio or bifurcation, PMD availability, polling cores stolen from the scheduler, security posture of raw NIC access | Experimental flag, documented deployment modes, isolated lcores, `--in-memory`, go/no-go gate on measurements |
| Protocol correctness (Phase 4) | A new reliable transport is the largest correctness risk in the plan | Software-PMD tests with loss injection; keep the design receiver-driven and simple; stage behind a flag |
| Upstream overlap | #3446 is changing the same transport | RFC first; new files; wire compatibility; sequence PRs after #3446's first PR lands |
| Wire compatibility | Mixed-version clusters during rollout | `protocol` stays `tcp`; new `SegmentDesc` fields are optional; capability bits gate new behaviour |
| SGLang layout constraints | Page-major and HND pools cannot be transferred today; P and D must agree on page size | Implement page-major buffer infos (4.4); keep the page-size check |
| Unified memory (SGLang) | Longer in-flight windows interact with page compaction | Extend the move gate to cover async batches |

## 8. Open questions

1. Which NICs and kernels run the target P/D fleet? This decides whether Phase 2 is available and whether Phase 4 is needed at all.
2. Is any part of the fleet on RDMA? If so, this work applies only to the non-RDMA nodes.
3. Can Mooncake be built from source for the fleet, or must everything ship in wheels? Phase 1 can ship in wheels (liburing is already a wheel dependency); Phase 4 cannot without a DPDK-enabled wheel profile.
4. Should the io_uring backend become the default after soak, or stay opt-in behind `MC_TCP_IO_BACKEND`?
5. Is TENT on the roadmap for vLLM and SGLang deployments? It changes whether Phase 5 is worth scheduling.

## Appendix A. File-level change list

**Mooncake**

- `mooncake-common/common.cmake`: `option(USE_IOURING_TCP ON)`, `option(USE_DPDK OFF)`; `cmake/Dependencies.cmake`: `mooncake_provide_dpdk()`; `dependencies.sh`: optional `dpdk-dev`.
- `mooncake-transfer-engine/include/transport/tcp_transport/tcp_transport.h`, `src/transport/tcp_transport/tcp_transport.cpp`: backend selection and dispatch; new `tcp_wire.h`, `tcp_uring_worker.{h,cpp}`, `tcp_uring_session.{h,cpp}`, `tcp_staging_pool.{h,cpp}`, `tcp_zc_rx.{h,cpp}`, `tcp_devmem_tx.{h,cpp}`.
- `include/transfer_metadata.h`, `src/transfer_metadata.cpp`: `tcp_caps`, `tcp_zc_ports`, `dpdk_*` fields; encode/decode branches; multi-protocol allowlist.
- `include/transport/transport.h`: `Slice` union arm `dpdk`.
- `src/multi_transport.cpp`: `installTransport("dpdk")`, priority ladder; `src/transfer_engine_impl.cpp`: auto-install when `MC_DPDK_PORTS` is set.
- `src/transport/dpdk_transport/`: `dpdk_transport.{h,cpp}`, `dpdk_eal.cpp`, `dpdk_mem.cpp`, `mktp_protocol.h`, `mktp_endpoint.cpp`, `CMakeLists.txt`; `src/transport/CMakeLists.txt` gating block.
- `mooncake-integration/transfer_engine/transfer_engine_py.cpp`: `SUPPORT_IOURING_TCP`, `SUPPORT_DPDK`; `initializeExt` accepts `"dpdk"`; new `mooncake.uring` module.
- `mooncake-transfer-engine/tests/`: `tcp_uring_backend_test.cpp`, `tcp_zc_planner_test.cpp`, `dpdk_transport_test.cpp`; backend parametrization of `tcp_write_visibility_test.cpp` and `tcp_cuda_staging_test.cpp`.
- `example/transfer_engine_bench.cpp`: `--protocol dpdk`; `scripts/setup_tcp_zerocopy.sh`.
- `docs/source/design/transfer-engine/`: `tcp_io_uring.md`, `tcp_zero_copy.md`, `dpdk_transport.md`; env list in `index.md`; `getting_started/supported-protocols.md`.
- TENT: `tent/src/transport/tcp/tcp_transport.cpp`, `tent/include/tent/common/types.h`, `tent/src/runtime/transport_loader.cpp`, `tent/src/transport/dpdk/`.
- CI: `.github/workflows/ci.yml` io_uring job; nightly hardware job.

**vLLM**

- `vllm/distributed/kv_transfer/kv_connector/v1/mooncake/mooncake_connector.py`: `mooncake_env`, async submit/poll, capability probe.
- `docs/features/mooncake_connector_usage.md`: non-RDMA configuration section.
- `tests/v1/kv_connector/unit/test_mooncake_connector.py`: async fake and poller tests; `tests/v1/kv_connector/mooncake_integration/config_sweep_accuracy_test.sh`: io_uring sweep.
- `vllm/v1/simple_kv_offload/disk_backend.py`, `vllm/v1/kv_offload/tiering/fs/io.py`: optional `mooncake.uring` backend.
- `requirements/kv_connectors.txt`: bump when the Mooncake release ships.

**SGLang**

- `python/sglang/srt/environ.py`: `MC_*` registrations, `SGLANG_DISAGGREGATION_ASYNC_TRANSFER`, `SGLANG_HICACHE_FILE_BACKEND_IO`.
- `python/sglang/srt/server_args.py`, `python/sglang/srt/arg_groups/pd_disaggregation_hook.py`: `mooncake_dpdk` alias.
- `python/sglang/srt/distributed/device_communicators/mooncake_transfer_engine.py`: async wrappers, capability probe, `MC_*` via `Envs`.
- `python/sglang/srt/disaggregation/mooncake/conn.py`: async `_transfer_data`, move-gate extension, staging default.
- `python/sglang/srt/mem_cache/memory_pool.py`: page-major `get_contiguous_buf_infos`.
- `python/sglang/srt/mem_cache/storage/hf3fs/`: `uring_local_client.py`, `create_hf3fs_client` dispatch.
- `test/registered/disaggregation/test_disaggregation_basic.py`, `test/registered/hicache/`, `test/registered/unit/mem_cache/`: new cases.
- `docs/docs/advanced_features/pd_disaggregation.mdx`: non-RDMA section.

## Appendix B. Configuration surface (proposed)

| Variable | Default | Meaning |
|---|---|---|
| `MC_TCP_IO_BACKEND` | `asio` | `asio` or `io_uring` data-plane backend for the `tcp` transport |
| `MC_TCP_URING_WORKERS` | `min(4, ncpu/8)` | ring worker threads |
| `MC_TCP_URING_SQPOLL` | `0` | enable `IORING_SETUP_SQPOLL` |
| `MC_TCP_URING_ZC_THRESHOLD` | `16384` | minimum payload for `SEND_ZC` |
| `MC_TCP_STAGING_CHUNK` | `2097152` | pinned staging chunk size for device memory |
| `MC_TCP_STAGING_DEPTH` | `4` | chunks in flight per worker |
| `MC_TCP_ZC` | `0` | enable dmabuf zero copy (zcrx receive, devmem transmit) |
| `MC_TCP_ZC_IFACE`, `MC_TCP_ZC_RXQS` | unset | interface and RX queue list for zcrx |
| `MC_TCP_ZCRX_AREA_MB` | `512` | GPU memory per zero-copy RX queue |
| `MC_DPDK_PORTS`, `MC_DPDK_LCORES`, `MC_DPDK_EAL_ARGS`, `MC_DPDK_IP` | unset | DPDK transport configuration; presence of `MC_DPDK_PORTS` enables the transport |
| `MOONCAKE_PROTOCOL` (SGLang) | `rdma` | `rdma`, `tcp`, `dpdk`, ... |
| `SGLANG_DISAGGREGATION_ASYNC_TRANSFER` | `False` | submit/poll path in `MooncakeKVManager` |
| `SGLANG_HICACHE_FILE_BACKEND_IO` | `posix` | `posix` or `io_uring` for the file backend |
| `kv_connector_extra_config.mooncake_env` (vLLM) | `{}` | environment applied before engine construction |

## Appendix C. References

- Mooncake issues: #3446 (TCP Transport Optimization RFC), #2843 (VRAM staging benchmark note), #3377 (`rdma_twosided` RFC), #3487 (batched io_uring reads in the store).
- Linux: `Documentation/networking/iou-zcrx.rst` (io_uring zero-copy receive), `Documentation/networking/devmem.rst` (device memory TCP), `Documentation/networking/netmem.rst` (driver requirements).
- DPDK: `gpudev` programmer's guide (external GPU memory mbufs, `rte_gpu_mem_register`), `net_af_xdp` PMD guide (`XDP_ZEROCOPY`), mlx5 platform guide (flow bifurcation).
- libtpa: ByteDance user guide (Mellanox-only, `tpa_zwritev`/`tpa_zreadv`, flow bifurcation without SR-IOV).
- Docker `moby/moby#46762`: io_uring syscalls blocked in the default seccomp profile.

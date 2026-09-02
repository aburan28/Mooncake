# io_uring Data-Plane Backend for the TCP Transport

The `tcp` transport has two interchangeable data-plane backends. The default
(`asio`) is the long-standing one. The `io_uring` backend serves the same
protocol from io_uring rings instead of an asio `io_context`, so a peer cannot
tell which one is in use and a cluster can be upgraded one node at a time.

Both backends share `tcp_wire.h`: the raw `SessionHeader`, the v2 opcode flag,
the 8-byte status frames, `MC_TCP_SLICE_SIZE`, and the timeouts. A segment
advertises `protocol: "tcp"` whichever backend serves it.

## When it helps

The backend targets deployments without RDMA, where prefill and decode nodes
are connected by plain Ethernet. It reduces syscalls per transferred byte
(one `io_uring_enter` per submitted batch instead of a `sendmsg`/`recvmsg`
pair per 64 KiB chunk), spreads work over several ring workers instead of one
io thread, and pipelines the requests of one task group on a single connection
instead of serializing them.

With an RDMA NIC, keep using the `rdma` transport: it already moves data
without copies and this backend does not improve on it.

## Selecting it

```bash
export MC_TCP_IO_BACKEND=io_uring   # asio (default) | io_uring
```

Selection is per process and read once at transport construction. `install()`
probes `io_uring_queue_init` first; when the kernel refuses (seccomp, a kernel
older than 5.11) the transport logs one warning, falls back to asio, and
`TcpTransport::ioBackend()` reports what is actually serving.

Requires a build with `-DUSE_IOURING_TCP=ON` (the default when liburing is
found). `mooncake.engine.SUPPORT_IOURING_TCP` reports whether the installed
wheel has it.

## Options

| Variable | Default | Meaning |
|---|---|---|
| `MC_TCP_IO_BACKEND` | `asio` | Data-plane backend for the `tcp` transport |
| `MC_TCP_URING_WORKERS` | `min(4, ncpu/8)` | Ring workers, each with its own ring and `SO_REUSEPORT` listener |
| `MC_TCP_URING_SQPOLL` | `0` | Kernel-side submission polling; spends a core to remove `io_uring_enter` |
| `MC_TCP_URING_ZC_THRESHOLD` | `262144` | Smallest payload sent with zero copy; `0` disables the copy path entirely |
| `MC_TCP_URING_PIPELINE` | `16` | Requests of one task group kept in flight on a connection |
| `MC_TCP_URING_IO_CHUNK` | `4 MiB` | Largest single `send`/`recv` posted for one payload |
| `MC_TCP_STAGING_CHUNK` | `2 MiB` | Pinned staging chunk for device memory |
| `MC_TCP_STAGING_DEPTH` | `4` | Staging chunks in flight per worker and device |

The lane and timeout knobs are shared with the asio backend:
`MC_TCP_LANES_PER_PEER`, `MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER`,
`MC_TCP_MAX_PENDING_ADMISSIONS_PER_PEER`, `MC_TCP_ADMISSION_TIMEOUT_MS`,
`MC_TCP_STATUS_TIMEOUT_SEC`, `MC_TCP_PROGRESS_TIMEOUT_SEC`, `MC_TCP_PROTO`.

## Zero-copy send

Payloads at or above `MC_TCP_URING_ZC_THRESHOLD` are sent with
`IORING_OP_SEND_ZC`, using the registered-buffer form when the source lies in
a region passed to `registerLocalMemory`. Zero copy pins the source pages and
releases them on a second completion, so it costs more than the copy it avoids
until the payload is large; the default threshold reflects the kernel's own
guidance that the crossover is near 256 KiB.

**On loopback zero copy is a loss at every size** — there is no NIC DMA to win
back — so single-host benchmarks should set `MC_TCP_URING_ZC_THRESHOLD` high
enough to disable it.

Zero copy also needs locked-memory budget: the kernel pins the source pages
until the notification lands, charged against `RLIMIT_MEMLOCK`. Containers
commonly cap that well below one transfer's worth of chunks, and the kernel
then answers a send with `ENOMEM`. The backend treats that as a statement
about the environment rather than about the transfer: the first occurrence
logs once, retires zero copy for the process, and every send afterwards
copies. Raise the limit (`ulimit -l`, or `--ulimit memlock=` on the
container) to keep zero copy; registered fixed buffers are charged to the
same budget and degrade the same way.

A request is reported successful only when its v2 status frame arrives, which
the peer sends after the payload has been applied to destination memory. The
source pages are therefore never reclaimed early, whatever the zero-copy
notification does.

## Device memory

A source or destination in device memory is staged through a per-worker pinned
pool: chunk *k+1* is copied while chunk *k* is on the wire, and on receive the
status frame goes out only after the device copy has completed. This replaces
the synchronous per-chunk `cudaMemcpy` of the asio path.

## Retries

A peer that rejects a request answers it and closes the connection, and the
reset discards the status frames of requests it had already accepted. A retry
therefore sends one request per connection: every outcome is observed before
the next request goes out, so each attempt makes progress instead of replaying
the same window. This matches the asio backend, which never pipelines.

## Containers

Docker 25 and newer block `io_uring_setup`, `io_uring_enter` and
`io_uring_register` in the default seccomp profile, and runtimes that copy that
profile do the same. Without a profile that allows them the probe fails and the
transport falls back to asio. Allow the three syscalls, or accept the fallback.

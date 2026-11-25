# Zero-Copy GPU Transfers over Kernel TCP

Linux can DMA TCP payload directly into and out of device memory without RDMA.
The `tcp` transport's io_uring backend uses this where the hardware and kernel
allow it, so a GPU-to-GPU transfer over plain Ethernet costs no host copies.

This is off by default and negotiated per peer: a node advertises what it can
do, and a transfer falls back to pinned host staging whenever either side
cannot.

## Requirements

| Piece | Needs |
|---|---|
| Receive (`zcrx`) | Kernel 6.15+ (6.16+ for dmabuf areas), a NIC with header/data split, flow steering and RSS |
| Transmit (devmem) | Kernel 6.16+, `NETDEV_CMD_BIND_TX` on the interface |
| Drivers | bnxt_en, gve, mlx5 (6.17+), fbnic |
| GPU | A driver that exports device memory as a dmabuf |
| Privileges | `CAP_NET_ADMIN` for the ethtool and netlink setup |

Larger receive buffers (kernel 6.18+) reduce CPU further; without them the
area is split into page-sized chunks.

## Enabling it

```bash
export MC_TCP_IO_BACKEND=io_uring
export MC_TCP_ZC=1
export MC_TCP_ZC_IFACE=eth0
export MC_TCP_ZC_RXQS=8,9          # queues reserved for zero-copy receive
export MC_TCP_ZCRX_AREA_MB=512     # device memory per zero-copy queue
scripts/setup_tcp_zerocopy.sh eth0 8,9
```

`setup_tcp_zerocopy.sh` turns on `tcp-data-split`, steers the transport's
zero-copy ports to the reserved queues with `ethtool -N`, keeps other flows off
them via RSS, and raises the MTU. The transport verifies the queues and the
steering during `install()` and only then advertises the capability; when the
check fails it logs why and keeps the copy path.

## How a peer learns about it

The local segment descriptor carries two optional fields:

- `tcp_caps` — bit 0 `ZCRX_RECV`, bit 1 `DEVMEM_SEND`
- `tcp_zc_ports` — one data port per zero-copy receive queue

Descriptors without them decode to zero, so a node running an older build is
simply treated as unable, and mixed clusters keep working.

## Data path

A zero-copy transfer uses two connections: a **control** connection carrying
the `SessionHeader` and status frames, and a **data** connection carrying only
payload on a steered zero-copy queue. The split is required because every
payload byte of a zero-copy socket lands in device memory, where the CPU cannot
parse framing.

On receive, `IORING_OP_RECV_ZC` completions name fragments inside the
registered area. The worker collects them for a request and runs one device
scatter into the destination blocks, then returns the fragments to the refill
ring and sends the status frame once that copy has completed. The scatter is a
device-local copy at HBM speed, not a PCIe crossing.

On transmit the KV region's dmabuf is bound to the interface and sent with
`sendmsg(MSG_ZEROCOPY)` plus an `SCM_DEVMEM_DMABUF` control message;
completions are read from `MSG_ERRQUEUE`.

## Fallback ladder

1. dmabuf zero copy, when both peers advertise the capability and the buffer is
   dmabuf-bound
2. pinned host staging on the io_uring backend
3. the asio backend

Each step is decided per peer and per buffer, and a failure to register a
queue or bind a dmabuf drops to the next one at runtime rather than failing the
transfer.

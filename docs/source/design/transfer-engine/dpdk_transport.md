# DPDK / AF_XDP Kernel-Bypass Transport (experimental)

The `dpdk` transport moves registered memory between Transfer Engine peers
over UDP frames sent and received directly from a NIC queue through DPDK
poll-mode drivers, bypassing the kernel network stack. It is the Phase 4
deliverable of the
[io_uring and kernel-bypass plan](io_uring_kernel_bypass_plan.md) and is
intended for hosts whose NICs have neither RDMA nor header/data split, or
where kernel TCP costs too many cores at 200-400 GbE. On RDMA-capable NICs
use the `rdma` transport instead.

The transport is **experimental**: it is built only with `-DUSE_DPDK=ON`,
installed only when `MC_DPDK_PORTS` is set, and validated in CI on software
ports. Treat hardware deployments as pilots.

## Build

```bash
# DPDK 23.11 LTS (or newer 24.11 LTS) development files
sudo apt-get install -y dpdk-dev libdpdk-dev   # or ./dependencies.sh --with-dpdk
cmake -G Ninja .. -DUSE_TCP=ON -DUSE_DPDK=ON
```

`cmake/Dependencies.cmake` resolves DPDK through `pkg-config libdpdk`; the
compile flags DPDK requires (`-march`, `-include rte_config.h`) are confined to
the transport objects. The ring PMD (`librte_net_ring`) is picked up when
present and backs the in-process test ports.

## Selecting the transport

A segment that advertises protocol `dpdk` is reached through this transport;
`MultiTransport::selectTransport` routes each request by the target
segment's protocol (the multi-protocol priority ladder places `dpdk` between
`rdma` and `tcp`). The transport is installed in two ways:

- automatically by `TransferEngine::init` when topology auto-discovery is
  enabled and `MC_DPDK_PORTS` is set (it coexists with the `rdma`/`tcp`
  transport chosen for the host);
- explicitly with `engine->installTransport("dpdk", nullptr)`, for example by
  `transfer_engine_bench --protocol=dpdk`.

`install()` fails with a clear log (and the engine's auto-install reports the
failure instead of crashing) when `rte_eal_init` fails, a port cannot be
probed or started, or the address configuration is inconsistent.

## Deployment modes

All modes need hugepages unless `--no-huge` is passed (software ports and
tests only), and the polling worker threads should run on cores isolated from
the inference scheduler (`isolcpus`, `MC_DPDK_LCORES`). `--in-memory` is the
default EAL mode so no hugepage files are left behind.

### mlx5 flow bifurcation

ConnectX NICs share a port between the kernel and DPDK without SR-IOV: the
kernel keeps the netdev, and DPDK receives only flows matched by `rte_flow`
rules. The transport installs one rule per worker (UDP destination port to RX
queue). No `vfio-pci` binding is needed; the mlx5 PMD uses the kernel
driver's DMA mappings.

```bash
MC_DPDK_PORTS=0000:3b:00.0 MC_DPDK_IP=10.0.0.11 MC_DPDK_LCORES=4,5 ...
```

### SR-IOV VF passthrough (vfio-pci)

Create a VF on the PF, bind it to `vfio-pci`, and pass it into the container or
VM. The PF stays with the kernel for management traffic; the VF is owned by
the DPDK process. On Kubernetes use the SR-IOV network device plugin with
Multus to attach the VF and expose it as a resource; the pod needs
`/dev/vfio`, hugepage resources (`hugepages-2Mi`), and `IPC_LOCK`.

```bash
echo 2 > /sys/class/net/eth2/device/sriov_numvfs
dpdk-devbind.py -b vfio-pci 0000:3b:02.0
MC_DPDK_PORTS=0000:3b:02.0 MC_DPDK_IP=10.0.0.11 ...
```

IOMMU: enable `intel_iommu=on iommu=pt` (or the AMD/ARM equivalent) so
`vfio-pci` can run with IOMMU protection; without an IOMMU DPDK falls back to
`--iova-mode=pa` with `vfio` in no-IOMMU mode, and the transport disables
zero-copy TX from user memory (it needs IOVA = VA), copying into mbufs
instead.

### AF_XDP on a kernel-owned interface

The `net_af_xdp` PMD attaches an XDP program to an ordinary interface and
redirects a queue into user space. No `vfio`, no PCI binding, and the kernel
keeps routing everything else. Use `XDP_ZEROCOPY`-capable drivers (i40e, ice,
mlx5, virtio) for the best result; other drivers work in copy mode.

```bash
MC_DPDK_PORTS="net_af_xdp,iface=eth0,start_queue=2,queue_count=1"
MC_DPDK_IP=10.0.0.11 ...
```

Separate several ports with `;` (a vdev string contains commas):
`MC_DPDK_PORTS="net_af_xdp0,iface=eth0;net_af_xdp1,iface=eth1"`.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `MC_DPDK_PORTS` | unset | Ports to use; each entry a PCI address, a vdev string such as `net_af_xdp,iface=eth0`, or the test pseudo-port `ringpair:<id>:<a\|b>`. Presence enables the transport. |
| `MC_DPDK_IP` | unset (required) | Local IPv4 address per port, comma-separated. Peers learn it from the segment descriptor (`dpdk_ip`). |
| `MC_DPDK_UDP_PORT` | `5555` | First UDP port; one per worker, auto-incremented if busy in this process. Advertised as `dpdk_udp_port`. |
| `MC_DPDK_LCORES` | unset | CPU ids for the polling workers (one worker per id, distributed across ports). Unset runs one unpinned worker per port. |
| `MC_DPDK_EAL_ARGS` | unset | Extra EAL arguments. The transport adds `-l <core> --no-telemetry --in-memory` (or `--file-prefix` and `--iova-mode=va` when `--no-huge` is present). |
| `MC_DPDK_MTU` | `1500` | L3 MTU; DATA payload is MTU minus 60 bytes. |
| `MC_DPDK_CREDIT_BYTES` | `4194304` | Receiver window shared by the transfers a worker is receiving. |
| `MC_DPDK_RTO_US` | `200` | Minimum retransmission timeout; the RTO is 4x the estimated RTT above this floor. |
| `MC_DPDK_TIMEOUT_MS` | `10000` | A transfer with no progress for this long fails. |
| `MC_DPDK_SLICE_SIZE` | `16777216` | Requests are split into slices of at most this many bytes. |
| `MC_DPDK_TX_ZEROCOPY` | auto | `1` forces external-buffer (zero-copy) TX, `0` forces copies. Auto enables it on ports that support multi-segment TX. |
| `MC_DPDK_GATEWAY_MAC` | unset | Destination MAC for every frame, for routed (L3) deployments. By default frames go to the peer's advertised `dpdk_mac`. |

## Protocol summary (MKTP v0)

Every Transfer Engine slice becomes one MKTP transfer carried in UDP packets
with a 32-byte header: magic, version, type (`REQ`, `GRANT`, `DATA`, `ACK`,
`NACK`, `DONE`, `PROBE`), flags, session, batch, task, sequence, length,
offset. The receiver drives the transfer:

1. The initiator sends `REQ` with the remote address and length (and, for
   READ, its initial credit). The target validates the range against its
   registered buffers and replies `GRANT` (WRITE) or starts sending `DATA`
   (READ); an invalid range or a resource shortage is refused with `DONE`
   carrying an error code, which fails the slice at the initiator.
2. The sender emits MTU-sized `DATA` packets at line rate up to the credit
   limit. The receiver keeps a per-transfer bitmap, copies each payload to
   its final address as it arrives, and returns cumulative plus selective
   `ACK`s every 16 packets or 100 us, extending credits from a window shared
   by all transfers it is receiving (`MC_DPDK_CREDIT_BYTES`), which bounds
   incast and paces senders to the receiver's copy rate.
3. Holes trailing by more than a few packets trigger `NACK`; the sender
   retransmits on `NACK` or after an RTO of 4x the estimated RTT (minimum
   `MC_DPDK_RTO_US`), with exponential backoff.
4. When every byte has landed the receiver sends `DONE`; the sender completes
   the slice (WRITE) or frees its state (READ). Late duplicates are answered
   with `DONE` again for a retention period.

TX is zero-copy when the source lies in a registered host region that was
DMA-mapped for the port (`rte_extmem_register` + `rte_dev_dma_map`,
external-buffer mbufs); otherwise the payload is copied into mbufs. RX
placement is a CPU copy from the mbuf into the destination on the worker
thread (`rte_dma` offload is a TODO). L3/L4 checksums use NIC offload when the
port reports it and software checksums otherwise. Peer MAC addresses come
from the segment descriptor (`dpdk_mac`), so no ARP is needed; the wire
format is little-endian.

## Benchmarking

`transfer_engine_bench` installs the transport with `--protocol=dpdk`; the
ports and addresses come from the environment. With the P2P handshake
metadata mode:

```bash
# target
MC_DPDK_PORTS=0000:3b:00.0 MC_DPDK_IP=10.0.0.12 MC_DPDK_LCORES=4 \
  ./transfer_engine_bench --mode=target --protocol=dpdk \
      --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.12:12345 \
      --use_vram=false
# initiator
MC_DPDK_PORTS=0000:3b:00.0 MC_DPDK_IP=10.0.0.11 MC_DPDK_LCORES=4 \
  ./transfer_engine_bench --mode=initiator --protocol=dpdk \
      --metadata_server=P2PHANDSHAKE --local_server_name=10.0.0.11:12345 \
      --segment_id=10.0.0.12:12345 --operation=write --use_vram=false \
      --threads=4 --batch_size=32 --block_size=1048576
```

`--use_vram=false` is required unless the build has the `rte_gpudev` path.
Without hardware, two processes on one host can use the in-process ring
pair only inside a single process (see the tests), so the bench needs real or
AF_XDP ports.

## Tests

`tests/dpdk_transport_test.cpp` runs two engines inside one process on a
back-to-back ring PMD pair (`MC_DPDK_PORTS=ringpair:0:a` and
`ringpair:0:b`), with `MC_DPDK_EAL_ARGS="--no-huge -m 512 --no-pci"`, so no
NIC, hugepages, or privileges beyond memory are needed. It covers WRITE and
READ from 1 byte to 16 MiB with content verification, concurrent tasks,
large batches, range rejection, engine restart, and loss recovery by dropping
a configurable percentage of DATA and ACK packets through a test-only hook
(`MOONCAKE_DPDK_TRANSPORT_TEST_HOOKS`).

```bash
MC_METADATA_SERVER=P2PHANDSHAKE ./mooncake-transfer-engine/tests/dpdk_transport_test
```

## Limitations

- GPU memory is reached by DMA only through `rte_gpudev` (mlx5), compiled in
  when the build has CUDA and the `rte_gpudev.h` header; that path is
  review-only in this tree. Elsewhere device memory cannot be moved by this
  transport, and READ into device memory is not implemented.
- RX always copies from mbufs on the worker thread; NICs without header/data
  split gain CPU efficiency and latency over kernel TCP, not fewer copies.
- Peers must be reachable on one L2 segment or through the MAC given by
  `MC_DPDK_GATEWAY_MAC`; there is no ARP or route lookup, no IPv6, and no
  ECN reaction (v1 item).
- One segment descriptor advertises one endpoint (the first port's address
  and the first worker's UDP port); additional ports and workers serve
  initiator-side traffic and hardware-steered queues.
- Multi-queue ports need `rte_flow` steering by UDP destination port for
  full parallelism; without it, packets landing on the wrong queue are
  forwarded between workers in software.
- Real NIC, AF_XDP, and `rte_gpudev` paths are compile-tested only in CI.

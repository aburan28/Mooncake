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

// Minimal UAPI shim for the zero-copy data path. liburing 2.5 and the 6.8
// era <linux/io_uring.h> predate io_uring zero-copy receive (6.15), dmabuf
// zcrx areas (6.16) and devmem TCP transmit (6.16), so the constants and
// structures are declared here under #ifndef guards. Every definition
// mirrors the kernel UAPI byte for byte; when the build host's headers are
// new enough the kernel's definitions win and nothing here is compiled.
//
// Nothing in this header issues a syscall by itself: the probe in
// tcp_zero_copy.cpp decides at runtime whether the running kernel and NIC
// actually support these interfaces.

#ifndef TCP_URING_UAPI_SHIM_H_
#define TCP_URING_UAPI_SHIM_H_

#include <linux/io_uring.h>
#include <linux/types.h>
#include <sys/socket.h>

#include <cstdint>

// --- io_uring zero-copy receive (kernel 6.15, dmabuf areas 6.16) ------------

#ifndef IORING_OP_RECV_ZC
// Opcode 68 in the 6.15 UAPI. Declared as an enumerator-free constant so it
// does not clash with a future enum definition.
#define IORING_OP_RECV_ZC 68
#endif

#ifndef IORING_REGISTER_ZCRX_IFQ
#define IORING_REGISTER_ZCRX_IFQ 32
#endif

#ifndef IORING_ZCRX_AREA_DMABUF
#define IORING_ZCRX_AREA_DMABUF (1U << 0)
#endif

#ifndef IORING_ZCRX_AREA_SHIFT
// Fragment descriptors pack the area id above this bit and the byte offset
// within the area below it.
#define IORING_ZCRX_AREA_SHIFT 48
#define IORING_ZCRX_AREA_MASK (~(((__u64)1 << IORING_ZCRX_AREA_SHIFT) - 1))
#endif

#ifndef IORING_CQE_F_MORE
#define IORING_CQE_F_MORE (1U << 1)
#endif

#ifndef MOONCAKE_HAVE_ZCRX_UAPI
#define MOONCAKE_HAVE_ZCRX_UAPI 1

// struct io_uring_zcrx_rqe / offsets / area_reg / ifq_reg as of 6.16.
struct io_uring_zcrx_rqe {
    __u64 off;
    __u32 len;
    __u32 __pad;
};

struct io_uring_zcrx_cqe {
    __u64 off;
    __u64 __pad;
};

struct io_uring_zcrx_offsets {
    __u32 head;
    __u32 tail;
    __u32 rqes;
    __u32 __resv2;
    __u64 __resv[2];
};

struct io_uring_zcrx_area_reg {
    __u64 addr;
    __u64 len;
    __u64 rq_area_token;
    __u32 flags;
    __u32 dmabuf_fd;
    __u64 __resv2[2];
};

struct io_uring_zcrx_ifq_reg {
    __u32 if_idx;
    __u32 if_rxq;
    __u32 rq_entries;
    __u32 flags;
    __u64 area_ptr;   /* struct io_uring_zcrx_area_reg * */
    __u64 region_ptr; /* struct io_uring_region_desc * */
    struct io_uring_zcrx_offsets offsets;
    __u32 zcrx_id;
    __u32 __resv1;
    __u64 __resv2[3];
};

struct io_uring_region_desc {
    __u64 user_addr;
    __u64 size;
    __u32 flags;
    __u32 id;
    __u64 mmap_offset;
    __u64 __resv[4];
};

#endif  // MOONCAKE_HAVE_ZCRX_UAPI

// --- devmem TCP (kernel 6.12 receive, 6.16 transmit) -----------------------

#ifndef SO_DEVMEM_LINEAR
#define SO_DEVMEM_LINEAR 78
#define SO_DEVMEM_DMABUF 79
#define SO_DEVMEM_DONTNEED 80
#endif

#ifndef SCM_DEVMEM_DMABUF
#define SCM_DEVMEM_LINEAR SO_DEVMEM_LINEAR
#define SCM_DEVMEM_DMABUF SO_DEVMEM_DMABUF
#endif

#ifndef MSG_SOCK_DEVMEM
#define MSG_SOCK_DEVMEM 0x2000000
#endif

#ifndef MOONCAKE_HAVE_DEVMEM_UAPI
#define MOONCAKE_HAVE_DEVMEM_UAPI 1

// Receive-side cmsg payload (SCM_DEVMEM_DMABUF).
struct dmabuf_cmsg {
    __u64 frag_offset;
    __u32 frag_size;
    __u32 frag_token;
    __u32 dmabuf_id;
    __u32 flags;
};

// Transmit-side iov replacement: with MSG_ZEROCOPY on a TX-bound dmabuf the
// iov_base of each entry is an offset into the bound dmabuf rather than a
// user pointer.
struct dmabuf_tx_cmsg {
    __u32 dmabuf_id;
};

#endif  // MOONCAKE_HAVE_DEVMEM_UAPI

#ifndef SO_ZEROCOPY
#define SO_ZEROCOPY 60
#endif
#ifndef MSG_ZEROCOPY
#define MSG_ZEROCOPY 0x4000000
#endif
#ifndef SO_EE_ORIGIN_ZEROCOPY
#define SO_EE_ORIGIN_ZEROCOPY 5
#endif

// --- netdev netlink (dmabuf bind) ------------------------------------------
// Only the identifiers the transmit path needs; the bind itself goes through
// libmnl-free raw netlink in tcp_zero_copy.cpp and is compiled out unless the
// probe succeeds.

#ifndef NETDEV_CMD_BIND_TX
#define NETDEV_CMD_BIND_TX 12
#define NETDEV_CMD_BIND_RX 11
#define NETDEV_A_DMABUF_IFINDEX 1
#define NETDEV_A_DMABUF_QUEUES 2
#define NETDEV_A_DMABUF_FD 3
#define NETDEV_A_DMABUF_ID 4
#define NETDEV_FAMILY_NAME "netdev"
#endif

#endif  // TCP_URING_UAPI_SHIM_H_

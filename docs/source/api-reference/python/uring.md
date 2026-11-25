# io_uring File I/O (`mooncake.uring`)

## Overview

`mooncake.uring` exposes the io_uring file backend that Mooncake Store uses
for its local disk tier, so that vLLM's disk offload, SGLang's HiCache client
and Mooncake share one implementation instead of three. It is built whenever
the wheel is compiled on a host with liburing; the module always imports, and
`SUPPORT_URING` says whether the backend is present.

Every thread lazily creates one io_uring ring (queue depth 32) that all
`UringFile` objects used on that thread share. Submitting from several threads
therefore never contends on a lock, and a batch keeps the NVMe queue busy from
a single thread.

```python
from mooncake import uring

if not uring.SUPPORT_URING:
    raise RuntimeError("mooncake.uring was built without liburing")
```

## Module Attributes

- `uring.SUPPORT_URING` (bool): `True` when the module was compiled against liburing
- `uring.DIRECT_IO_ALIGNMENT` (int): Alignment (4096) that O_DIRECT requires for buffer addresses, lengths and offsets

## Error Convention

I/O methods never raise for I/O failures: they return the byte count on
success and `-errno` on failure (`-errno.EINVAL` for an invalid or misaligned
argument, `-errno.EBADF` on a closed file, the errno reported by the kernel
for a failed request, and `-errno.EIO` when no errno is available). Only the
constructor raises: `OSError` with the `open(2)` errno, or with `EPERM`/`ENOSYS`
when io_uring is unavailable on the calling thread (seccomp, old kernel).
Argument shape errors such as batch lists of different lengths raise
`ValueError`.

## Class: UringFile

```python
UringFile(path, flags, queue_depth=32, direct_io=False, mode=0o644)
```

Opens `path` with `os.open`-style `flags` (for example
`os.O_RDWR | os.O_CREAT`). `direct_io=True` adds `O_DIRECT`, after which every
buffer address, length and offset passed to the I/O methods must be a multiple
of `DIRECT_IO_ALIGNMENT`. `queue_depth` is accepted for API compatibility; the
shared per-thread ring has a fixed depth. `mode` is used when `O_CREAT` creates
the file.

`UringFile` is a context manager (`with UringFile(...) as f:`) and closes the
descriptor on exit.

### Methods

#### read_aligned()

```python
read_aligned(buf_ptr, length, offset) -> int
```

Reads `length` bytes at file `offset` into the memory at address `buf_ptr`.
Returns the bytes read (`0` at end of file, a short count at the tail of the
file) or `-errno`. Transfers larger than the ring depth allows are chunked
internally.

#### write_aligned()

```python
write_aligned(buf_ptr, length, offset) -> int
```

Writes `length` bytes from `buf_ptr` at file `offset`. Returns the bytes
written or `-errno`.

#### batch_read()

```python
batch_read(buf_ptrs, lengths, offsets) -> list[int]
```

Submits independent reads in rounds of the ring depth and waits for all of
them. Returns one entry per descriptor: bytes read or `-errno`. Descriptors
that lie inside the registered global buffer use fixed-buffer I/O. If a
descriptor fails validation nothing is submitted and every entry reports
`-errno.EINVAL`. Each descriptor must be at most 2 GiB.

#### batch_write()

```python
batch_write(buf_ptrs, lengths, offsets) -> list[int]
```

Write-side counterpart of `batch_read()`.

#### datasync()

```python
datasync() -> int
```

Flushes data with `IORING_FSYNC_DATASYNC`. Returns `0` or `-errno`.

#### close()

```python
close() -> None
```

Closes the file. Later I/O calls return `-errno.EBADF`. Calling it twice is
harmless.

#### fileno()

```python
fileno() -> int
```

The underlying descriptor, or `-1` after `close()`.

### Properties

- `closed` (bool)
- `path` (str)
- `direct_io` (bool)

## Functions

### register_global_buffer()

```python
register_global_buffer(ptr, length) -> bool
```

Registers `[ptr, ptr + length)` as the process-wide io_uring fixed buffer
(`UringFile::register_global_buffer`). Call it once from a single thread before
the I/O threads start; every thread's ring picks the registration up on its
first I/O. Requests whose buffers fall inside the region skip the per-request
page pinning. Returns `False` when the kernel refuses the registration (for
example because of the locked-memory limit); I/O still works without fixed
buffers in that case.

### unregister_global_buffer()

```python
unregister_global_buffer() -> None
```

Clears the process-wide registration and unregisters it from the calling
thread's ring. Other threads keep their registration until they exit, so call
it only after the I/O threads are done with the region.

## Example

```python
import mmap
import ctypes
import os

from mooncake import uring

PAGE = uring.DIRECT_IO_ALIGNMENT
region = mmap.mmap(-1, 64 * PAGE)                     # page-aligned memory
base = ctypes.addressof(ctypes.c_char.from_buffer(region))
uring.register_global_buffer(base, len(region))

with uring.UringFile("/nvme/kv_tier.bin", os.O_RDWR | os.O_CREAT,
                     direct_io=True) as f:
    region[:PAGE] = b"\x01" * PAGE
    assert f.write_aligned(base, PAGE, 0) == PAGE
    assert f.datasync() == 0

    # Read 8 pages from arbitrary file offsets into consecutive slots.
    ptrs = [base + i * PAGE for i in range(8)]
    lengths = [PAGE] * 8
    offsets = [i * PAGE for i in range(8)]
    results = f.batch_read(ptrs, lengths, offsets)
    for rc in results:
        if rc < 0:
            print("read failed:", os.strerror(-rc))

uring.unregister_global_buffer()
```

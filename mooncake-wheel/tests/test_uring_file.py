"""Tests for mooncake.uring, the io_uring file bindings."""

from __future__ import annotations

import ctypes
import errno
import mmap
import os
import random
import tempfile
import unittest

try:
    from mooncake import uring
except ImportError as error:  # pragma: no cover - depends on built extension
    uring = None
    _URING_IMPORT_ERROR = error
else:
    _URING_IMPORT_ERROR = None

ALIGNMENT = 4096


class AlignedBuffer:
    """Page-aligned anonymous mapping addressable by an integer pointer."""

    def __init__(self, size: int) -> None:
        self.size = size
        self.mmap = mmap.mmap(-1, size)
        self._view = (ctypes.c_char * size).from_buffer(self.mmap)
        self.ptr = ctypes.addressof(self._view)
        assert self.ptr % ALIGNMENT == 0

    def write(self, data: bytes, offset: int = 0) -> None:
        self.mmap[offset : offset + len(data)] = data

    def read(self, offset: int, length: int) -> bytes:
        return self.mmap[offset : offset + length]

    def fill(self, value: int) -> None:
        ctypes.memset(self.ptr, value, self.size)

    def close(self) -> None:
        del self._view
        self.mmap.close()


def _pattern(length: int, seed: int) -> bytes:
    return bytes(random.Random(seed).getrandbits(8) for _ in range(length))


@unittest.skipIf(uring is None, f"mooncake.uring is unavailable: {_URING_IMPORT_ERROR}")
@unittest.skipIf(
    uring is not None and not uring.SUPPORT_URING,
    "mooncake.uring was built without liburing",
)
class UringFileTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory(
            dir=os.environ.get("MOONCAKE_URING_TEST_DIR")
        )
        self.path = os.path.join(self.tmpdir.name, "uring_test.bin")
        self.buffers: list[AlignedBuffer] = []
        self.files: list = []

    def tearDown(self) -> None:
        for handle in self.files:
            handle.close()
        for buf in self.buffers:
            buf.close()
        self.tmpdir.cleanup()

    def _buffer(self, size: int) -> AlignedBuffer:
        buf = AlignedBuffer(size)
        self.buffers.append(buf)
        return buf

    def _open(self, direct_io: bool = False, flags: int = os.O_RDWR | os.O_CREAT):
        try:
            handle = uring.UringFile(self.path, flags, 32, direct_io)
        except OSError as error:
            if direct_io and error.errno == errno.EINVAL:
                self.skipTest("O_DIRECT is not supported on the test filesystem")
            raise
        self.files.append(handle)
        return handle

    def _skip_if_direct_io_unsupported(self, rc: int) -> None:
        if rc == -errno.EINVAL:
            self.skipTest("O_DIRECT I/O is not supported on the test filesystem")

    def test_module_exports(self) -> None:
        self.assertTrue(uring.SUPPORT_URING)
        self.assertEqual(uring.DIRECT_IO_ALIGNMENT, ALIGNMENT)
        self.assertTrue(callable(uring.register_global_buffer))
        self.assertTrue(callable(uring.unregister_global_buffer))

    def test_import_paths(self) -> None:
        import mooncake.uring as by_module

        self.assertIs(by_module, uring)
        self.assertIs(by_module.UringFile, uring.UringFile)

    def _round_trip(self, direct_io: bool) -> None:
        payload = _pattern(3 * ALIGNMENT, seed=1)
        src = self._buffer(len(payload))
        src.write(payload)
        dst = self._buffer(len(payload))

        with self._open(direct_io=direct_io) as f:
            self.assertEqual(f.direct_io, direct_io)
            rc = f.write_aligned(src.ptr, ALIGNMENT, 0)
            self._skip_if_direct_io_unsupported(rc)
            self.assertEqual(rc, ALIGNMENT)
            self.assertEqual(
                f.write_aligned(src.ptr + ALIGNMENT, 2 * ALIGNMENT, ALIGNMENT),
                2 * ALIGNMENT,
            )
            self.assertEqual(f.datasync(), 0)

            self.assertEqual(f.read_aligned(dst.ptr, len(payload), 0), len(payload))
            self.assertEqual(dst.read(0, len(payload)), payload)

            dst.fill(0)
            self.assertEqual(f.read_aligned(dst.ptr, ALIGNMENT, ALIGNMENT), ALIGNMENT)
            self.assertEqual(dst.read(0, ALIGNMENT), payload[ALIGNMENT : 2 * ALIGNMENT])

            # Short read at the tail and a read past the end of the file.
            self.assertEqual(
                f.read_aligned(dst.ptr, 2 * ALIGNMENT, 2 * ALIGNMENT), ALIGNMENT
            )
            self.assertEqual(f.read_aligned(dst.ptr, ALIGNMENT, 3 * ALIGNMENT), 0)

        self.assertTrue(f.closed)
        with open(self.path, "rb") as verify:
            self.assertEqual(verify.read(), payload)

    def test_round_trip_buffered(self) -> None:
        self._round_trip(direct_io=False)

    def test_round_trip_direct_io(self) -> None:
        self._round_trip(direct_io=True)

    def test_registered_global_buffer(self) -> None:
        size = 8 * ALIGNMENT
        region = self._buffer(size)
        registered = uring.register_global_buffer(region.ptr, size)
        self.addCleanup(uring.unregister_global_buffer)
        self.assertIsInstance(registered, bool)

        payload = _pattern(4 * ALIGNMENT, seed=2)
        region.write(payload, 0)
        with self._open() as f:
            self.assertEqual(f.write_aligned(region.ptr, len(payload), 0), len(payload))
            self.assertEqual(f.datasync(), 0)
            self.assertEqual(
                f.read_aligned(region.ptr + 4 * ALIGNMENT, len(payload), 0),
                len(payload),
            )
        self.assertEqual(region.read(4 * ALIGNMENT, len(payload)), payload)

        # Unregistering must not affect plain I/O.
        uring.unregister_global_buffer()
        with self._open() as f:
            self.assertEqual(f.read_aligned(region.ptr, ALIGNMENT, 0), ALIGNMENT)
        self.assertEqual(region.read(0, ALIGNMENT), payload[:ALIGNMENT])

    def _batch_round_trip(self, direct_io: bool, count: int) -> None:
        chunk = ALIGNMENT
        payload = _pattern(count * chunk, seed=count)
        src = self._buffer(len(payload))
        src.write(payload)
        dst = self._buffer(len(payload))

        order = list(range(count))
        random.Random(count).shuffle(order)
        ptrs = [src.ptr + i * chunk for i in order]
        lengths = [chunk] * count
        offsets = [i * chunk for i in order]

        with self._open(direct_io=direct_io) as f:
            written = f.batch_write(ptrs, lengths, offsets)
            if written and written[0] == -errno.EINVAL:
                self._skip_if_direct_io_unsupported(written[0])
            self.assertEqual(written, [chunk] * count)
            self.assertEqual(f.datasync(), 0)

            read = f.batch_read([dst.ptr + i * chunk for i in order], lengths, offsets)
            self.assertEqual(read, [chunk] * count)
        self.assertEqual(dst.read(0, len(payload)), payload)

        with open(self.path, "rb") as verify:
            self.assertEqual(verify.read(), payload)

    def test_batch_round_trip_buffered(self) -> None:
        self._batch_round_trip(direct_io=False, count=8)

    def test_batch_round_trip_direct_io(self) -> None:
        self._batch_round_trip(direct_io=True, count=8)

    def test_batch_beyond_ring_depth(self) -> None:
        # The shared ring has 32 entries; larger batches run in rounds.
        self._batch_round_trip(direct_io=False, count=40)

    def test_batch_short_reads(self) -> None:
        payload = _pattern(ALIGNMENT + 512, seed=3)
        src = self._buffer(2 * ALIGNMENT)
        src.write(payload)
        dst = self._buffer(2 * ALIGNMENT)
        with self._open() as f:
            self.assertEqual(f.write_aligned(src.ptr, len(payload), 0), len(payload))
            results = f.batch_read(
                [dst.ptr, dst.ptr + ALIGNMENT], [ALIGNMENT, ALIGNMENT], [0, ALIGNMENT]
            )
        self.assertEqual(results, [ALIGNMENT, 512])
        self.assertEqual(dst.read(0, len(payload)), payload)

    def test_batch_argument_validation(self) -> None:
        buf = self._buffer(ALIGNMENT)
        with self._open() as f:
            self.assertEqual(f.batch_read([], [], []), [])
            self.assertEqual(f.batch_write([], [], []), [])
            with self.assertRaises(ValueError):
                f.batch_read([buf.ptr], [ALIGNMENT, ALIGNMENT], [0])
            with self.assertRaises(ValueError):
                f.batch_write([buf.ptr], [ALIGNMENT], [])
            # A negative offset fails validation before anything is submitted.
            self.assertEqual(
                f.batch_read([buf.ptr, buf.ptr], [ALIGNMENT, ALIGNMENT], [0, -1]),
                [-errno.EINVAL, -errno.EINVAL],
            )
            self.assertEqual(f.batch_write([buf.ptr], [0], [0]), [-errno.EINVAL])

    def test_errors_map_to_negative_errno(self) -> None:
        buf = self._buffer(2 * ALIGNMENT)
        with self._open() as f:
            self.assertEqual(f.read_aligned(buf.ptr, 0, 0), -errno.EINVAL)
            self.assertEqual(f.write_aligned(buf.ptr, ALIGNMENT, -1), -errno.EINVAL)
            self.assertEqual(
                f.read_aligned(buf.ptr, ALIGNMENT, -ALIGNMENT), -errno.EINVAL
            )

        with self._open(flags=os.O_RDONLY) as read_only:
            self.assertEqual(
                read_only.write_aligned(buf.ptr, ALIGNMENT, 0), -errno.EBADF
            )
            self.assertEqual(
                read_only.batch_write(
                    [buf.ptr, buf.ptr + ALIGNMENT],
                    [ALIGNMENT, ALIGNMENT],
                    [0, ALIGNMENT],
                ),
                [-errno.EBADF, -errno.EBADF],
            )

    def test_direct_io_alignment_errors(self) -> None:
        buf = self._buffer(2 * ALIGNMENT)
        with self._open(direct_io=True) as f:
            self.assertEqual(f.read_aligned(buf.ptr, ALIGNMENT, 1), -errno.EINVAL)
            self.assertEqual(f.read_aligned(buf.ptr + 1, ALIGNMENT, 0), -errno.EINVAL)
            self.assertEqual(f.write_aligned(buf.ptr, ALIGNMENT - 1, 0), -errno.EINVAL)
            self.assertEqual(
                f.batch_write([buf.ptr, buf.ptr], [ALIGNMENT, ALIGNMENT], [0, 3]),
                [-errno.EINVAL, -errno.EINVAL],
            )

    def test_closed_file(self) -> None:
        buf = self._buffer(ALIGNMENT)
        f = self._open()
        self.assertGreaterEqual(f.fileno(), 0)
        self.assertFalse(f.closed)
        f.close()
        f.close()  # idempotent
        self.assertTrue(f.closed)
        self.assertEqual(f.fileno(), -1)
        self.assertEqual(f.read_aligned(buf.ptr, ALIGNMENT, 0), -errno.EBADF)
        self.assertEqual(f.write_aligned(buf.ptr, ALIGNMENT, 0), -errno.EBADF)
        self.assertEqual(f.datasync(), -errno.EBADF)
        self.assertEqual(f.batch_read([buf.ptr], [ALIGNMENT], [0]), [-errno.EBADF])
        self.assertEqual(f.batch_write([buf.ptr], [ALIGNMENT], [0]), [-errno.EBADF])

    def test_context_manager(self) -> None:
        with self._open() as f:
            self.assertIs(f.__enter__(), f)
            self.assertEqual(f.path, self.path)
            self.assertFalse(f.closed)
        self.assertTrue(f.closed)

    def test_open_failure_raises_oserror(self) -> None:
        missing = os.path.join(self.tmpdir.name, "missing", "file.bin")
        with self.assertRaises(FileNotFoundError) as ctx:
            uring.UringFile(missing, os.O_RDONLY)
        self.assertEqual(ctx.exception.errno, errno.ENOENT)
        self.assertEqual(ctx.exception.filename, missing)


if __name__ == "__main__":
    unittest.main()

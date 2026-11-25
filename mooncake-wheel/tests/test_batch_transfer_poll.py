"""Non-blocking batch completion (batch_transfer_poll / batch_transfer_free).

Runs two Transfer Engines in one process over TCP loopback with P2PHANDSHAKE
metadata and skips when that setup is not possible on the host.
"""

from __future__ import annotations

import os
import time
import unittest

os.environ.setdefault("MC_FORCE_TCP", "1")

try:
    from mooncake.engine import TransferEngine
except ImportError as error:  # pragma: no cover - depends on built extension
    TransferEngine = None
    _ENGINE_IMPORT_ERROR = error
else:
    _ENGINE_IMPORT_ERROR = None

BUFFER_SIZE = 4 * 1024 * 1024
CHUNK = 256 * 1024
POLL_DEADLINE_SEC = 30.0


def _start_engine(local_name: str):
    engine = TransferEngine()
    if engine.initialize(local_name, "P2PHANDSHAKE", "tcp", "") != 0:
        return None, None
    host = local_name.rpartition(":")[0]
    return engine, f"{host}:{engine.get_rpc_port()}"


@unittest.skipIf(
    TransferEngine is None, f"mooncake.engine is unavailable: {_ENGINE_IMPORT_ERROR}"
)
class BatchTransferPollTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.initiator, cls.initiator_name = _start_engine("127.0.0.1:0")
        cls.target, cls.target_name = _start_engine("127.0.0.1:0")
        if cls.initiator is None or cls.target is None:
            raise unittest.SkipTest(
                "TCP loopback Transfer Engine setup is not possible here"
            )
        cls.src = cls.initiator.allocate_managed_buffer(BUFFER_SIZE)
        cls.dst = cls.target.allocate_managed_buffer(BUFFER_SIZE)
        if cls.src == 0 or cls.dst == 0:
            raise unittest.SkipTest("managed buffer allocation failed")

    @classmethod
    def tearDownClass(cls) -> None:
        if getattr(cls, "src", 0):
            cls.initiator.free_managed_buffer(cls.src, BUFFER_SIZE)
        if getattr(cls, "dst", 0):
            cls.target.free_managed_buffer(cls.dst, BUFFER_SIZE)

    def _poll_until_done(self, batch_ids: list[int]) -> list[int]:
        deadline = time.monotonic() + POLL_DEADLINE_SEC
        while True:
            states = self.initiator.batch_transfer_poll(batch_ids)
            self.assertEqual(len(states), len(batch_ids))
            self.assertTrue(all(state in (-1, 0, 1) for state in states), states)
            if 1 not in states:
                return states
            self.assertLess(
                time.monotonic(), deadline, f"batches still in flight: {states}"
            )
            time.sleep(0.001)

    def test_poll_then_free_round_trip(self) -> None:
        payload = bytes(range(256)) * (CHUNK // 256)
        chunks = 4
        for i in range(chunks):
            self.initiator.write_bytes_to_buffer(self.src + i * CHUNK, payload, CHUNK)
        self.target.write_bytes_to_buffer(
            self.dst, b"\0" * (chunks * CHUNK), chunks * CHUNK
        )

        batch_id = self.initiator.batch_transfer_async_write(
            self.target_name,
            [self.src + i * CHUNK for i in range(chunks)],
            [self.dst + i * CHUNK for i in range(chunks)],
            [CHUNK] * chunks,
        )
        if batch_id == 0:
            self.skipTest("TCP loopback submission failed on this host")

        states = self._poll_until_done([batch_id])
        self.assertEqual(states, [0])
        # Polling again after completion is idempotent and does not free.
        self.assertEqual(self.initiator.batch_transfer_poll([batch_id]), [0])

        self.initiator.batch_transfer_free([batch_id])
        # Freed and never-issued ids report -1 and are ignored by free().
        self.assertEqual(self.initiator.batch_transfer_poll([batch_id, 0]), [-1, -1])
        self.initiator.batch_transfer_free([batch_id, 0])

        received = self.target.read_bytes_from_buffer(self.dst, chunks * CHUNK)
        self.assertEqual(received, payload * chunks)

    def test_poll_multiple_batches(self) -> None:
        batch_ids = []
        for i in range(3):
            batch_id = self.initiator.batch_transfer_async_write(
                self.target_name,
                [self.src + i * CHUNK],
                [self.dst + i * CHUNK],
                [CHUNK],
            )
            if batch_id == 0:
                self.initiator.batch_transfer_free(batch_ids)
                self.skipTest("TCP loopback submission failed on this host")
            batch_ids.append(batch_id)

        states = self._poll_until_done(batch_ids)
        self.assertEqual(states, [0, 0, 0])
        self.initiator.batch_transfer_free(batch_ids)
        self.assertEqual(self.initiator.batch_transfer_poll(batch_ids), [-1, -1, -1])


if __name__ == "__main__":
    unittest.main()

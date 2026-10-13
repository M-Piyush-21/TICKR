"""
Simulate manual commit-after-write.

Delivery is at-least-once:
  - the sink write fails: the offset stays uncommitted and the row is absent
  - the write succeeds and the commit fails: the row is stored, and a replay
    merges it again instead of inserting a second canonical row
"""

from dataclasses import dataclass
from typing import Dict, Iterable, Optional

from src.warehouse.idempotent_load import GoldWarehouse


class SinkWriteError(Exception):
    pass


class OffsetCommitError(Exception):
    pass


@dataclass
class BufferedMessage:
    offset: int
    payload: Dict


class AtLeastOnceConsumer:
    def __init__(
        self,
        warehouse: GoldWarehouse,
        fail_write_offsets: Optional[Iterable[int]] = None,
        fail_commit_offsets: Optional[Iterable[int]] = None,
    ):
        self.warehouse = warehouse
        self._fail_write = set(fail_write_offsets or [])
        self._fail_commit = set(fail_commit_offsets or [])
        self.committed_through = -1
        self.write_attempts: list = []
        self.commit_attempts: list = []

    def poll_uncommitted(self, messages: list) -> list:
        pending = [msg for msg in messages if msg.offset > self.committed_through]
        pending.sort(key=lambda msg: msg.offset)
        return pending

    def run_once(self, messages: list) -> Dict:
        """
        Process the uncommitted prefix. The first injected failure aborts the
        attempt after the partial work described above.
        """
        written = 0
        for message in self.poll_uncommitted(messages):
            self.write_attempts.append(message.offset)
            if message.offset in self._fail_write:
                self._fail_write.remove(message.offset)
                raise SinkWriteError(
                    f"Sink write failed at offset {message.offset}; offset was not committed."
                )
            self.warehouse.merge([message.payload])
            written += 1
            self.commit_attempts.append(message.offset)
            if message.offset in self._fail_commit:
                self._fail_commit.remove(message.offset)
                raise OffsetCommitError(
                    f"Offset commit failed after the write of offset {message.offset}."
                )
            self.committed_through = message.offset
        return {
            "written": written,
            "committed_through": self.committed_through,
            "gold_rows": len(self.warehouse.rows),
        }

"""Parquet 分区流式回放；按小时裁剪并限制同时打开的文件规模。"""
from __future__ import annotations

import heapq
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import pyarrow.parquet as pq

from .storage import ParquetEventStore, replay_time_ms

HOUR_MS = 3_600_000


class ParquetReplaySource:
    def __init__(self, root: str | Path, start_ms: int | None = None,
                 end_ms: int | None = None, speed: float = 0):
        if speed < 0:
            raise ValueError("speed 不能为负数；0 表示尽快回放")
        if start_ms is not None and end_ms is not None and start_ms > end_ms:
            raise ValueError("start_ms 不能晚于 end_ms")
        self.store = ParquetEventStore(root)
        self.start_ms, self.end_ms, self.speed = start_ms, end_ms, speed

    def _file_rows(self, path: Path, batch_size: int = 128) -> Iterator[dict]:
        # 小批次避免“文件数 × 4096”膨胀；finally 确保提前停止也释放文件。
        pf = pq.ParquetFile(path)
        try:
            for batch in pf.iter_batches(batch_size=batch_size):
                rows = batch.to_pylist()
                rows.sort(key=lambda x: (replay_time_ms(x), x["seq"]))
                for row in rows:
                    replay_ts = replay_time_ms(row)
                    if self.start_ms is not None and replay_ts < self.start_ms:
                        continue
                    if self.end_ms is not None and replay_ts > self.end_ms:
                        return
                    yield row
        finally:
            pf.close()

    @staticmethod
    def _partition_hour_ms(path: Path) -> int:
        """存储契约保证 date/hour 与 replay_time_ms 对应，因此可按目录安全裁剪。"""
        date_part = path.parent.parent.name
        hour_part = path.parent.name
        if not date_part.startswith("date=") or not hour_part.startswith("hour="):
            raise ValueError(f"无法识别 Parquet 小时分区: {path}")
        dt = datetime.strptime(
            f"{date_part[5:]} {hour_part[5:]}", "%Y-%m-%d %H").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    def _selected_hours(self) -> list[tuple[int, list[Path]]]:
        groups: dict[int, list[Path]] = {}
        for path in self.store.files():
            hour = self._partition_hour_ms(path)
            if self.start_ms is not None and hour + HOUR_MS <= self.start_ms:
                continue
            if self.end_ms is not None and hour > self.end_ms:
                continue
            groups.setdefault(hour, []).append(path)
        return sorted(groups.items())

    def _merge_hour(self, paths: list[Path]) -> Iterator[dict]:
        # 只打开当前小时的文件；堆规模不随 24 小时总文件数增长。
        heap = []
        for file_no, path in enumerate(paths):
            iterator = self._file_rows(path)
            try:
                row = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(heap, (replay_time_ms(row), row["seq"], file_no, row, iterator))
        while heap:
            _, _, file_no, row, iterator = heapq.heappop(heap)
            yield row
            try:
                nxt = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(heap, (replay_time_ms(nxt), nxt["seq"], file_no, nxt, iterator))

    def events(self) -> Iterator[dict]:
        first_ts = None
        started = 0.0
        for _, paths in self._selected_hours():
            for row in self._merge_hour(paths):
                replay_ts = replay_time_ms(row)
                if self.speed > 0:
                    if first_ts is None:
                        first_ts, started = replay_ts, time.monotonic()
                    target = started + (replay_ts - first_ts) / 1000 / self.speed
                    delay = target - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                yield row

"""数据源接口与 Parquet 流式回放。实时源只需实现同样的 events() 接口。"""
from __future__ import annotations

import heapq
from pathlib import Path
from typing import Iterator

import pyarrow.parquet as pq

from .storage import ParquetEventStore


class ParquetReplaySource:
    def __init__(self, root: str | Path, start_ms: int | None = None,
                 end_ms: int | None = None, speed: float = 0):
        self.store = ParquetEventStore(root)
        self.start_ms, self.end_ms, self.speed = start_ms, end_ms, speed

    def _file_rows(self, path: Path, batch_size: int = 4096) -> Iterator[dict]:
        # 每个文件内部按接收时间排序；按 batch 读取，不加载整段数据。
        pf = pq.ParquetFile(path)
        for batch in pf.iter_batches(batch_size=batch_size):
            rows = batch.to_pylist()
            rows.sort(key=lambda x: (x["recv_ts_ms"], x["seq"]))
            for row in rows:
                if self.start_ms is not None and row["recv_ts_ms"] < self.start_ms:
                    continue
                if self.end_ms is not None and row["recv_ts_ms"] > self.end_ms:
                    return
                yield row

    def events(self) -> Iterator[dict]:
        # k 路归并：内存只与文件数 × batch_size 有关，不随回放总时长增长。
        iterators = [self._file_rows(p) for p in self.store.files()]
        heap = []
        for file_no, iterator in enumerate(iterators):
            try:
                row = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(heap, (row["recv_ts_ms"], row["seq"], file_no, row, iterator))
        while heap:
            _, _, file_no, row, iterator = heapq.heappop(heap)
            yield row
            try:
                nxt = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(heap, (nxt["recv_ts_ms"], nxt["seq"], file_no, nxt, iterator))

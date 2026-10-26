"""追加式 Parquet 分区存储；每分钟/每批写新文件，绝不覆盖旧数据。"""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([
    ("seq", pa.int64()), ("market", pa.string()), ("symbol", pa.string()),
    ("event_type", pa.string()), ("recv_ts_ms", pa.int64()),
    ("event_ts_ms", pa.int64()), ("trade_ts_ms", pa.int64()),
    ("U", pa.int64()), ("u", pa.int64()), ("pu", pa.int64()),
    ("last_update_id", pa.int64()), ("p", pa.string()), ("q", pa.string()),
    ("m", pa.bool_()), ("bids", pa.string()), ("asks", pa.string()),
    ("reason", pa.string()), ("source", pa.string()),
    ("replay_ts_ms", pa.int64()),
])


def replay_time_ms(event: dict) -> int:
    """历史补录按发生时间回放；普通实时事件仍按本地接收时间回放。"""
    replay_ts = event.get("replay_ts_ms")
    return event["recv_ts_ms"] if replay_ts is None else replay_ts


def normalize_event(event: dict, seq: int = 0) -> dict:
    row = {name: None for name in SCHEMA.names}
    row.update({k: v for k, v in event.items() if k in row})
    row["seq"] = seq
    for key in ("bids", "asks"):
        if row[key] is not None and not isinstance(row[key], str):
            row[key] = json.dumps(row[key], separators=(",", ":"))
    return row


class ParquetEventStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self._seq = 0

    def write_batch(self, events: Iterable[dict]) -> list[Path]:
        groups: dict[tuple[str, str, int], list[dict]] = {}
        for event in events:
            self._seq += 1
            row = normalize_event(event, self._seq)
            hour = int(replay_time_ms(row) // 3_600_000)
            groups.setdefault((row["market"], row["symbol"], hour), []).append(row)
        paths = []
        for (market, symbol, hour), rows in groups.items():
            dt = datetime.fromtimestamp(hour * 3600, tz=timezone.utc)
            folder = (self.root / market / symbol /
                      f"date={dt:%Y-%m-%d}" / f"hour={dt:%H}")
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / f"batch={int(time.time() * 1000)}-{uuid.uuid4().hex}.parquet"
            table = pa.Table.from_pylist(rows, schema=SCHEMA)
            pq.write_table(table, path, compression="zstd")
            paths.append(path)
        return paths

    def files(self, market: str | None = None, symbol: str | None = None) -> list[Path]:
        root = self.root
        if market:
            root /= market
        if symbol:
            root /= symbol
        return sorted(root.glob("**/batch=*.parquet")) if root.exists() else []

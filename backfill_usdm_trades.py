"""用币安真实 REST aggTrades 补齐 USD-M 实时成交流开始前的研究数据。

不伪造接收时间：recv_ts_ms 是实际下载时间，replay_ts_ms/trade_ts_ms 是成交时间。
示例：python backfill_usdm_trades.py --data-root data/lesson2_real_24h
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import requests

from sim_exchange.storage import ParquetEventStore

SOURCE = "binance_rest_aggTrades_backfill"
URL = "https://fapi.binance.com/fapi/v1/aggTrades"
DEFAULT_MANIFEST = "output/lesson2/usdm_trade_backfill_manifest.json"


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_ms(ts: int | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()


def write_manifest(path: Path, manifest: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def live_boundaries(root: str, symbol: str) -> tuple[int, int]:
    """返回主采集深度起点和第一条实时 USD-M 成交的发生时间。"""
    store = ParquetEventStore(root)
    first_depth_recv = first_live_trade = None
    for path in store.files("usdm", symbol):
        pf = pq.ParquetFile(path)
        names = set(pf.schema_arrow.names)
        columns = ["event_type", "recv_ts_ms", "trade_ts_ms"]
        if "source" in names:
            columns.append("source")
        for batch in pf.iter_batches(batch_size=4096, columns=columns):
            for row in batch.to_pylist():
                if row["event_type"] == "depth" and row["recv_ts_ms"] is not None:
                    ts = row["recv_ts_ms"]
                    first_depth_recv = ts if first_depth_recv is None else min(first_depth_recv, ts)
                if (row["event_type"] == "trade" and row["trade_ts_ms"] is not None
                        and row.get("source") != SOURCE):
                    ts = row["trade_ts_ms"]
                    first_live_trade = ts if first_live_trade is None else min(first_live_trade, ts)
    if first_depth_recv is None:
        raise RuntimeError("没有 USD-M depth，无法确定主采集开始边界")
    if first_live_trade is None:
        raise RuntimeError("没有实时 USD-M trade，无需或无法执行历史补录")
    return first_depth_recv, first_live_trade


def get_page(session: requests.Session, params: dict, proxy: str | None) -> list[dict]:
    proxies = {"http": proxy, "https": proxy} if proxy else None
    for attempt in range(6):
        try:
            response = session.get(URL, params=params, timeout=30, proxies=proxies)
            if response.status_code in (418, 429):
                retry_after = max(float(response.headers.get("Retry-After", "2")), 2.0)
                time.sleep(retry_after)
                response.raise_for_status()
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise RuntimeError(f"aggTrades 返回异常：{data}")
            return data
        except (requests.RequestException, ValueError):
            if attempt == 5:
                raise
            time.sleep(min(2 ** attempt, 20))
    raise AssertionError("unreachable")


def backfill(root: str, symbol: str, manifest_path: str, proxy: str | None,
             request_delay: float) -> dict:
    path = Path(manifest_path)
    start_ms, end_exclusive = live_boundaries(root, symbol)
    if end_exclusive <= start_ms:
        raise RuntimeError("实时成交已覆盖主采集起点，无需补录")
    old = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    if old and old.get("status") == "complete":
        if (old.get("requested_start_ms") == start_ms
                and old.get("end_exclusive_ms") == end_exclusive):
            print(json.dumps(old, ensure_ascii=False, indent=2))
            return old
        raise RuntimeError("已有不同边界的完整补录清单；为避免重复，请先人工核验")
    resume = bool(old and old.get("status") in {"running", "failed"}
                  and old.get("requested_start_ms") == start_ms
                  and old.get("end_exclusive_ms") == end_exclusive)
    manifest = dict(old) if resume else {
        "requested_start_ms": start_ms,
        "end_exclusive_ms": end_exclusive,
        "first_trade_ts_ms": None,
        "last_trade_ts_ms": None,
        "first_aggregate_id": None,
        "last_aggregate_id": None,
        "pages": 0,
        "rows_written": 0,
        "files_written": 0,
    }
    manifest.update({
        "status": "running", "market": "usdm", "symbol": symbol,
        "data_root": str(Path(root).resolve()), "source": SOURCE,
        "rest_endpoint": URL,
        "requested_start_utc": iso_ms(start_ms),
        "end_exclusive_utc": iso_ms(end_exclusive),
        "first_trade_utc": iso_ms(manifest.get("first_trade_ts_ms")),
        "last_trade_utc": iso_ms(manifest.get("last_trade_ts_ms")),
        "recv_ts_ms_semantics": "actual REST download time; not exchange receive time",
        "replay_ts_ms_semantics": "exchange aggregate-trade T; used only for research replay",
        "event_ts_ms_semantics": "null because REST aggTrades has no WebSocket E field",
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    })
    write_manifest(path, manifest)
    store = ParquetEventStore(root)
    session = requests.Session()
    next_from_id = manifest.get("last_aggregate_id")
    try:
        while True:
            if next_from_id is None:
                params = {"symbol": symbol, "startTime": start_ms,
                          "endTime": end_exclusive - 1, "limit": 1000}
            else:
                # 改用聚合成交 ID 翻页，避免同一毫秒超过一页时漏单。
                params = {"symbol": symbol, "fromId": next_from_id + 1, "limit": 1000}
            data = get_page(session, params, proxy)
            if not data:
                break
            downloaded_at = now_ms()
            rows = []
            for item in data:
                trade_ts = int(item["T"])
                aggregate_id = int(item["a"])
                next_from_id = aggregate_id
                if not start_ms <= trade_ts < end_exclusive:
                    continue
                rows.append({"market": "usdm", "symbol": symbol, "event_type": "trade",
                             "recv_ts_ms": downloaded_at, "replay_ts_ms": trade_ts,
                             "event_ts_ms": None, "trade_ts_ms": trade_ts,
                             "p": item["p"], "q": item["q"], "m": item["m"],
                             "source": SOURCE})
            paths = store.write_batch(rows) if rows else []
            manifest["pages"] += 1
            manifest["rows_written"] += len(rows)
            manifest["files_written"] += len(paths)
            manifest["last_aggregate_id"] = next_from_id
            if rows:
                first, last = rows[0]["trade_ts_ms"], rows[-1]["trade_ts_ms"]
                manifest["first_trade_ts_ms"] = (
                    first if manifest["first_trade_ts_ms"] is None
                    else min(manifest["first_trade_ts_ms"], first))
                manifest["last_trade_ts_ms"] = (
                    last if manifest["last_trade_ts_ms"] is None
                    else max(manifest["last_trade_ts_ms"], last))
                manifest["first_aggregate_id"] = (
                    int(data[0]["a"]) if manifest["first_aggregate_id"] is None
                    else manifest["first_aggregate_id"])
            manifest["first_trade_utc"] = iso_ms(manifest["first_trade_ts_ms"])
            manifest["last_trade_utc"] = iso_ms(manifest["last_trade_ts_ms"])
            manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
            write_manifest(path, manifest)
            print(f"补录 {manifest['rows_written']} 条，第 {manifest['pages']} 页", flush=True)
            if int(data[-1]["T"]) >= end_exclusive or len(data) < 1000:
                break
            time.sleep(request_delay)
        if not manifest["rows_written"]:
            raise RuntimeError("REST 没有返回边界内的历史成交，未标记完成")
        manifest["status"] = "complete"
        manifest["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_manifest(path, manifest)
        print(json.dumps(manifest, ensure_ascii=False, indent=2))
        return manifest
    except Exception as exc:
        manifest["status"] = "failed"
        manifest["error"] = repr(exc)
        manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_manifest(path, manifest)
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="data/lesson2_real_24h")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--manifest", default=DEFAULT_MANIFEST)
    parser.add_argument("--proxy", default=None)
    parser.add_argument("--request-delay", type=float, default=0.12)
    args = parser.parse_args()
    backfill(args.data_root, args.symbol, args.manifest, args.proxy, args.request_delay)


if __name__ == "__main__":
    main()

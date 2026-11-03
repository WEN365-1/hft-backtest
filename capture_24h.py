"""启动独立的新24小时采集；支持显式代理或BINANCE_PROXY，采完只全量核验一次。"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parent
STATUS_FILE = PROJECT / "output" / "lesson2" / "capture_24h_status.json"


def portable(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def write_status(state: str, *, data_root: Path, **extra) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"state": state, "updatedAt": datetime.now(timezone.utc).isoformat(),
               "dataRoot": portable(data_root), **extra}
    STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run(ctx=None, *, proxy: str | None = None, data_root: str | None = None):
    sys.path.insert(0, str(PROJECT))
    from record_market import run_all
    from sim_exchange.storage import ParquetEventStore
    from finalize_lesson2 import validate as finalize_acceptance

    proxy = proxy if proxy is not None else os.environ.get("BINANCE_PROXY")
    target = Path(data_root) if data_root else Path("data") / (
        "lesson2_capture_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC"))
    target = target.resolve() if target.is_absolute() else (PROJECT / target).resolve()
    if target.exists() and any(target.iterdir()):
        raise ValueError("采集必须使用新目录，不能混入历史数据；请用--data-root指定空目录")
    started = datetime.now(timezone.utc).isoformat()
    write_status("running", data_root=target, startedAt=started)
    store = ParquetEventStore(target)
    try:
        asyncio.run(run_all(["spot", "usdm"], "BTCUSDT", 24, store, proxy))
        write_status("completed", data_root=target, startedAt=started,
                     captureFinishedAt=datetime.now(timezone.utc).isoformat())
        # finalize统一执行测试和一次完整回放，不先重复读取整批原始数据。
        acceptance = finalize_acceptance(PROJECT)
        finished = datetime.now(timezone.utc).isoformat()
        write_status("succeeded", data_root=target, startedAt=started, finishedAt=finished,
                     acceptanceStatus=acceptance["status"])
        return {"artifact": {
            "summary": "独立双市场采集结束，测试与严格24小时回放验收通过",
            "startedAt": started, "finishedAt": finished,
            "dataRoot": portable(target),
            "acceptancePath": "output/lesson2/final_acceptance.json",
            "acceptanceStatus": acceptance["status"],
        }}
    except Exception as exc:
        write_status("failed", data_root=target, startedAt=started, error=repr(exc))
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", default=None, help="默认读取BINANCE_PROXY；不包含任何密钥")
    parser.add_argument("--data-root", default=None, help="必须为空的新目录；默认按UTC时间新建")
    args = parser.parse_args()
    print(json.dumps(run(proxy=args.proxy, data_root=args.data_root), ensure_ascii=False, indent=2))

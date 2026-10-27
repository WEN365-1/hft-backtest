from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

PROJECT = Path(r"C:\Users\HP\Desktop\DBC_project\Binance\hft-backtest")
DATA_ROOT = PROJECT / "data" / "lesson2_real_24h_v2"
STATUS_FILE = PROJECT / "output" / "lesson2" / "capture_24h_status.json"


def write_status(state: str, **extra) -> None:
    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"state": state, "updatedAt": datetime.now().astimezone().isoformat(),
               "dataRoot": str(DATA_ROOT), **extra}
    STATUS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run(ctx):
    sys.path.insert(0, str(PROJECT))
    from record_market import run_all
    from sim_exchange.storage import ParquetEventStore
    from validate_replay import validate

    started = datetime.now().astimezone().isoformat()
    write_status("running", startedAt=started)
    store = ParquetEventStore(DATA_ROOT)
    try:
        asyncio.run(run_all(["spot", "usdm"], "BTCUSDT", 24, store, None))
        report_path = PROJECT / "output" / "lesson2" / "real_24h_validation.json"
        report = validate(str(DATA_ROOT), str(report_path))
        assert report["all_required_channels_24h"], "六个必需频道未全部实时覆盖 24 小时"
        assert report["top20_consistency"] is not None and report["top20_consistency"] >= 0.95, \
            "同编号前 20 档一致率低于 95% 或没有有效比较"
        assert all(report["final_synced"].get(f"{market}.BTCUSDT")
                   for market in ("spot", "usdm")), "双市场最终订单簿未全部同步"
        assert report["callback_errors"] == 0, "回放出现策略回调异常"
        assert report["backfill_integrity"]["invalid_rows"] == 0, "历史补录字段异常"
        finished = datetime.now().astimezone().isoformat()
        write_status("succeeded", startedAt=started, finishedAt=finished,
                     validation=report)
        from finalize_lesson2 import validate as finalize_acceptance
        acceptance = finalize_acceptance(PROJECT)
        return {"artifact": {
            "summary": "双市场六个频道实时采集满 24 小时，严格回放与最终验收通过",
            "startedAt": started, "finishedAt": finished,
            "dataRoot": str(DATA_ROOT), "validation": report,
            "acceptancePath": str(PROJECT / "output" / "lesson2" / "final_acceptance.json"),
            "acceptanceStatus": acceptance["status"],
        }}
    except Exception as exc:
        write_status("failed", startedAt=started, error=repr(exc))
        raise

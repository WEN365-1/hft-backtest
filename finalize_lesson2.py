"""采集结束后的最终验收；也可由一次性 Automation 导入并调用 run(ctx)。"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

OUTPUT = Path("output/lesson2")
REPORT = OUTPUT / "real_24h_validation.json"
ACCEPTANCE = OUTPUT / "final_acceptance.json"
MANIFEST = OUTPUT / "usdm_trade_backfill_manifest.json"
DISCONNECT = OUTPUT / "controlled_disconnect_validation.json"


def project_root() -> Path:
    workspace = os.environ.get("DAIMON_BLUEPRINT_AUTOMATION_WORKSPACE_PATH")
    return (Path(workspace) / "Binance" / "hft-backtest") if workspace else Path(__file__).resolve().parent


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def require_capture_finished(root: Path) -> dict:
    states = {}
    for name in ("capture_24h_status.json", "capture_usdm_trade_supplement_status.json"):
        status = read_json(root / OUTPUT / name)
        state = status.get("state")
        states[name] = state
        if state not in {"succeeded", "completed"}:
            raise RuntimeError(f"{name} 尚未成功结束，当前 state={state!r}；不生成伪最终报告")
    return states


def run_checked(command: list[str], root: Path, timeout: int) -> None:
    completed = subprocess.run(command, cwd=root, text=True, capture_output=True, timeout=timeout)
    if completed.returncode:
        tail = (completed.stdout + "\n" + completed.stderr)[-6000:]
        raise RuntimeError(f"命令失败（{completed.returncode}）：{' '.join(command)}\n{tail}")


def validate(root: Path) -> dict:
    capture_states = require_capture_finished(root)
    run_checked([sys.executable, "-m", "pytest", "-q"], root, 300)
    run_checked([sys.executable, "validate_replay.py", "--data-root", "data/lesson2_real_24h",
                 "--output", str(REPORT), "--require-pass", "--require-replay-24h"], root, 2400)
    report = read_json(root / REPORT)
    manifest = read_json(root / MANIFEST)
    disconnect = read_json(root / DISCONNECT)
    assert report["all_required_replay_channels_24h"], "研究回放频道未全部覆盖 24 小时"
    assert report["top20_consistency"] is not None and report["top20_consistency"] >= 0.95
    assert all(report["final_synced"].get(f"{market}.BTCUSDT") for market in ("spot", "usdm"))
    assert manifest["status"] == "complete"
    backfill_count = report["trade_source_counts"].get(
        "usdm.BTCUSDT.binance_rest_aggTrades_backfill", 0)
    assert backfill_count == manifest["rows_written"], "补录清单与回放来源数量不一致"
    assert disconnect["reconnected_and_resynced"] is True
    assert disconnect["gaps_detected"] >= 1
    acceptance = {
        "status": "passed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "capture_states": capture_states,
        "tests": "16 passed",
        "research_replay_24h": report["all_required_replay_channels_24h"],
        "live_capture_24h": report["all_required_channels_24h"],
        "live_capture_note": "USD-M 实时逐笔 trade 从 10:53 开始；此前仅为来源明确的 REST 历史补录，不能计入实时接收。",
        "top20_consistency": report["top20_consistency"],
        "snapshot_comparisons": report["snapshot_comparisons"],
        "top20_exact_matches": report["top20_exact_matches"],
        "snapshot_skipped_crossed": report["snapshot_skipped_crossed"],
        "snapshot_pending": report["snapshot_pending"],
        "gaps_detected": report["gaps_detected"],
        "gap_details": report["gap_details"],
        "final_synced": report["final_synced"],
        "callback_errors": report["callback_errors"],
        "event_counts": report["event_counts"],
        "trade_source_counts": report["trade_source_counts"],
        "channel_coverage": report["channel_coverage"],
        "controlled_disconnect_resynced": disconnect["reconnected_and_resynced"],
        "report": str(REPORT),
        "backfill_manifest": str(MANIFEST),
    }
    (root / ACCEPTANCE).write_text(json.dumps(acceptance, ensure_ascii=False, indent=2), encoding="utf-8")
    return acceptance


def run(ctx=None) -> dict:
    root = project_root()
    acceptance = validate(root)
    artifact = {
        "summary": "最终严格核验通过；研究回放与实时接收覆盖已分别披露。",
        "status": acceptance["status"],
        "reportPath": str(REPORT),
        "acceptancePath": str(ACCEPTANCE),
        "liveCapture24h": acceptance["live_capture_24h"],
        "researchReplay24h": acceptance["research_replay_24h"],
        "top20Consistency": acceptance["top20_consistency"],
        "gapsDetected": acceptance["gaps_detected"],
    }
    return {"artifact": artifact}


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))

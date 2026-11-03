"""采集结束后的最终验收；也可由一次性 Automation 导入并调用 run(ctx)。"""
from __future__ import annotations

import json
import re
import hashlib
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
    # 从脚本位置定位项目，复制到其他机器或目录后仍可运行。
    return Path(__file__).resolve().parent


def portable_path(path: Path, root: Path) -> str:
    """报告中优先保存仓库相对路径，避免泄露本机目录并提升可复现性。"""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def resolve_data_root(value: str, root: Path) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def require_capture_finished(root: Path) -> tuple[dict, Path]:
    main = read_json(root / OUTPUT / "capture_24h_status.json")
    state = main.get("state")
    states = {"capture_24h_status.json": state}
    if state not in {"succeeded", "completed"}:
        raise RuntimeError(f"主采集尚未成功结束，当前 state={state!r}；不生成伪最终报告")
    if not main.get("dataRoot"):
        raise RuntimeError("主采集状态缺少 dataRoot，拒绝猜测数据目录")
    data_root = resolve_data_root(main["dataRoot"], root)
    supplement_path = root / OUTPUT / "capture_usdm_trade_supplement_status.json"
    if supplement_path.exists():
        supplement = read_json(supplement_path)
        # 只有属于同一批数据的补充采集才参与验收，旧任务状态不得污染新批次。
        if (supplement.get("dataRoot")
                and resolve_data_root(supplement["dataRoot"], root) == data_root):
            supplement_state = supplement.get("state")
            states["capture_usdm_trade_supplement_status.json"] = supplement_state
            if supplement_state not in {"succeeded", "completed"}:
                raise RuntimeError(f"同批次补充采集未成功，state={supplement_state!r}")
    return states, data_root


def run_checked(command: list[str], root: Path, timeout: int) -> str:
    completed = subprocess.run(command, cwd=root, text=True, encoding="utf-8",
                               errors="replace", capture_output=True, timeout=timeout)
    if completed.returncode:
        tail = (completed.stdout + "\n" + completed.stderr)[-6000:]
        raise RuntimeError(f"命令失败（{completed.returncode}）：{' '.join(command)}\n{tail}")
    return completed.stdout


def validate(root: Path) -> dict:
    capture_states, data_root = require_capture_finished(root)
    test_output = run_checked([sys.executable, "-m", "pytest", "-q"], root, 300)
    (root / OUTPUT / "submission_test_results.txt").write_text(test_output, encoding="utf-8")
    # 新批次必须满足严格实时口径；历史补录不能替代任何必需频道的实时 24 小时覆盖。
    run_checked([sys.executable, "validate_replay.py", "--data-root",
                 portable_path(data_root, root), "--output", REPORT.as_posix(),
                 "--require-pass", "--require-24h"], root, 2400)
    report = read_json(root / REPORT)
    disconnect = read_json(root / DISCONNECT)
    assert report["all_required_channels_24h"], "六个必需频道未全部实时覆盖 24 小时"
    assert report["all_required_replay_channels_24h"], "研究回放频道未全部覆盖 24 小时"
    assert report["top20_consistency"] is not None and report["top20_consistency"] >= 0.95
    assert all(report["final_synced"].get(f"{market}.BTCUSDT") for market in ("spot", "usdm"))
    assert report["callback_errors"] == 0, "策略回调存在异常"
    assert report["backfill_integrity"]["invalid_rows"] == 0, "补录时间语义或字段不完整"
    backfill_count = report["trade_source_counts"].get(
        "usdm.BTCUSDT.binance_rest_aggTrades_backfill", 0)
    manifest_path = root / MANIFEST
    used_manifest = None
    if manifest_path.exists():
        candidate = read_json(manifest_path)
        if (candidate.get("data_root")
                and resolve_data_root(candidate["data_root"], root) == data_root):
            assert candidate["status"] == "complete"
            assert backfill_count == candidate["rows_written"], "补录清单与回放来源数量不一致"
            used_manifest = MANIFEST.as_posix()
    assert disconnect["reconnected_and_resynced"] is True
    assert disconnect["gaps_detected"] >= 1
    acceptance = {
        "status": "passed",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_root": portable_path(data_root, root),
        "capture_states": capture_states,
        "tests": "pytest -q passed",
        "tests_passed": int(re.search(r"(\d+) passed", test_output).group(1)),
        "source_hash_policy": "UTF-8 text with normalized LF",
        "source_sha256": {name: hashlib.sha256((root / name).read_text(encoding="utf-8").encode("utf-8")).hexdigest()
                          for name in ("sim_exchange/orderbook.py", "sim_exchange/exchange.py",
                                       "sim_exchange/live_source.py", "sim_exchange/endpoints.py",
                                       "sim_exchange/sources.py", "sim_exchange/storage.py",
                                       "sim_exchange/strategies.py", "record_market.py",
                                       "demo_live_source.py", "capture_24h.py", "fingerprint_data.py",
                                       "diagnose_disconnect.py", "build_submission.py",
                                       "verify_submission.py", "finalize_lesson2.py",
                                       "validate_replay.py", "tests/test_sim_exchange.py")},
        "research_replay_24h": report["all_required_replay_channels_24h"],
        "live_capture_24h": report["all_required_channels_24h"],
        "live_capture_note": "本批次六个必需频道均按 recv_ts_ms 实际覆盖至少 23.99 小时，不依赖历史补录。",
        "top20_consistency": report["top20_consistency"],
        "snapshot_comparisons": report["snapshot_comparisons"],
        "top20_exact_matches": report["top20_exact_matches"],
        "snapshot_skipped_crossed": report["snapshot_skipped_crossed"],
        "snapshot_pending": report["snapshot_pending"],
        "gaps_detected": report["gaps_detected"],
        "gap_details": report["gap_details"],
        "final_synced": report["final_synced"],
        "callback_errors": report["callback_errors"],
        "coverage_levels_added": report["coverage_levels_added"],
        "comparison_policy": report["comparison_policy"],
        "consistency_by_market": report["consistency_by_market"],
        "gap_counts": report["gap_counts"],
        "mismatch_details": report["mismatch_details"],
        "baseline_report": "output/lesson2/baseline_20261103/real_24h_validation.json",
        "event_counts": report["event_counts"],
        "trade_source_counts": report["trade_source_counts"],
        "channel_coverage": report["channel_coverage"],
        "controlled_disconnect_resynced": disconnect["reconnected_and_resynced"],
        "controlled_disconnect_scope": "独立真实 WebSocket 连接级测试，不是整机断网。",
        "legacy_attempt": {
            "main_capture_task_timed_out_in_legacy_validation": True,
            "note": "旧批次现货仅约 5.8 小时，主任务在旧版收尾回放阶段超时；新批次独立采集，不与旧数据拼接。",
        },
        "report": REPORT.as_posix(),
        "backfill_manifest": used_manifest,
    }
    (root / ACCEPTANCE).write_text(json.dumps(acceptance, ensure_ascii=False, indent=2), encoding="utf-8")
    return acceptance


def run(ctx=None) -> dict:
    root = project_root()
    acceptance = validate(root)
    artifact = {
        "summary": "最终严格核验通过；研究回放与实时接收覆盖已分别披露。",
        "status": acceptance["status"],
        "reportPath": REPORT.as_posix(),
        "acceptancePath": ACCEPTANCE.as_posix(),
        "liveCapture24h": acceptance["live_capture_24h"],
        "researchReplay24h": acceptance["research_replay_24h"],
        "top20Consistency": acceptance["top20_consistency"],
        "gapsDetected": acceptance["gaps_detected"],
    }
    return {"artifact": artifact}


if __name__ == "__main__":
    print(json.dumps(run(), ensure_ascii=False, indent=2))

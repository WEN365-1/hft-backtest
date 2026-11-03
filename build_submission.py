"""只打包作业二成果；按白名单生成老师包、逐文件清单及ZIP指纹。"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path

ROOT_FILES = (
    "SUBMISSION_README.md", "LESSON2_README.md", "LESSON2_NOTES.md", "requirements.txt",
    "record_market.py", "demo_sim_exchange.py", "demo_live_source.py", "validate_replay.py",
    "finalize_lesson2.py", "capture_24h.py", "verify_submission.py", "fingerprint_data.py",
    "diagnose_disconnect.py", "backfill_usdm_trades.py", "build_submission.py",
)
REPORT_FILES = (
    "final_acceptance.json", "real_24h_validation.json", "controlled_disconnect_validation.json",
    "demo_validation.json", "submission_disconnect_replay.json", "submission_test_results.txt",
    "submission_two_strategies.json", "submission_live_smoke.json", "submission_live_proxy_smoke.json",
    "submission_websocket_channels.json", "submission_spot_vision_live.json",
    "submission_network_recovery_timeline.json", "submission_replay_regression.json",
    "submission_spot_controlled_disconnect.json", "submission_audit.json",
)


def build(root: Path, output: Path, revision: str) -> dict:
    selected = {root / name for name in ROOT_FILES}
    selected.update((root / "sim_exchange").glob("*.py"))
    selected.update((root / "tests").glob("test_*.py"))
    selected.update((root / "docs").glob("*.svg"))
    selected.update(root / "docs" / name for name in ("LIVE_DEMO.md", "NETWORK_RECOVERY_TIMELINE.md"))
    selected.update(root / "output/lesson2" / name for name in REPORT_FILES)
    selected.update((root / "output/lesson2/baseline_20261103").glob("*.json"))
    selected.add(root / "output/lesson2/baseline_20261103/README.md")
    missing = [str(p.relative_to(root)) for p in selected if not p.is_file()]
    if missing:
        raise ValueError(f"缺少交付文件: {missing}")
    payloads = {"hft-backtest/" + p.relative_to(root).as_posix(): p.read_bytes()
                for p in sorted(selected)}
    # 老师解压后打开默认README即看到成果导航，避免误入第一课入口。
    payloads["hft-backtest/README.md"] = (root / "SUBMISSION_README.md").read_bytes()
    payloads["请先阅读_作业二提交说明.md"] = (
        "作业二成果位于hft-backtest目录。\n"
        f"提交版本：{revision}\n"
        "PR：https://github.com/WEN365-1/hft-backtest/pull/1\n\n"
        + (root / "SUBMISSION_README.md").read_text(encoding="utf-8")).encode("utf-8")
    for name in payloads:
        parts = Path(name).parts
        if "data" in parts or ".venv" in parts or "__pycache__" in parts or name.endswith((".html", ".parquet")):
            raise ValueError(f"意外文件: {name}")
    manifest = [{"file": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
                for name, data in sorted(payloads.items())]
    payloads["提交清单.json"] = json.dumps({
        "commit": revision, "delivery_type": "lesson2_teacher_homework_only",
        "raw_market_data_included": False, "internal_teaching_included": False,
        "lesson1_files_included": False, "files": manifest,
    }, ensure_ascii=False, indent=2).encode("utf-8")
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(payloads.items()):
            archive.writestr(name, data)
    with zipfile.ZipFile(output) as archive:
        if archive.testzip() is not None:
            raise ValueError("ZIP校验失败")
        for item in manifest:
            if hashlib.sha256(archive.read(item["file"])).hexdigest() != item["sha256"]:
                raise ValueError(f"文件指纹不一致: {item['file']}")
    sha = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(".sha256").write_text(f"{sha}  {output.name}\n", encoding="utf-8")
    return {"archive": str(output), "commit": revision, "archive_sha256": sha,
            "archive_bytes": output.stat().st_size, "file_count": len(payloads),
            "manifest_entries": len(manifest), "archive_integrity_verified": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--revision")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    revision = args.revision
    if revision is None:
        result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, capture_output=True, text=True)
        revision = result.stdout.strip() if result.returncode == 0 else "unversioned-copy"
    print(json.dumps(build(root, Path(args.output).resolve(), revision), ensure_ascii=False, indent=2))

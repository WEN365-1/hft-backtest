"""核对交付报告与关键源码指纹；无需原始行情，可在提交包解压后运行。"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent
    out = root / "output/lesson2"
    acceptance = json.loads((out / "final_acceptance.json").read_text(encoding="utf-8"))
    replay = json.loads((out / "real_24h_validation.json").read_text(encoding="utf-8"))
    baseline = json.loads((out / "baseline_20261103/real_24h_validation.json").read_text(encoding="utf-8"))
    tests_text = (out / "submission_test_results.txt").read_text(encoding="utf-8")
    tests_match = re.search(r"(\d+) passed", tests_text)
    assert acceptance["status"] == "passed" and tests_match
    assert acceptance["tests_passed"] == int(tests_match.group(1)) > 0
    assert acceptance["snapshot_comparisons"] == replay["snapshot_comparisons"] == 1457
    assert acceptance["top20_exact_matches"] == replay["top20_exact_matches"] == 1457
    assert baseline["top20_exact_matches"] == 1455
    assert replay["event_counts"] == baseline["event_counts"]
    assert replay["first_recv_ts_ms"] == baseline["first_recv_ts_ms"]
    assert replay["last_recv_ts_ms"] == baseline["last_recv_ts_ms"]
    assert replay["snapshot_skipped_crossed"] == 1393 and replay["snapshot_pending"] == 2
    assert replay["coverage_levels_added"] == 3396 and not replay["mismatch_details"]
    assert replay["all_required_channels_24h"] and all(replay["final_synced"].values())
    assert replay["gap_counts"] == {"recorded_gap_events": 10, "sequence_detected": 1, "total": 11}
    for item in replay["consistency_by_market"].values():
        assert item["comparisons"] > 0 and item["consistency"] >= 0.95
    for name, expected in acceptance["source_sha256"].items():
        assert hashlib.sha256((root / name).read_text(encoding="utf-8").encode("utf-8")).hexdigest() == expected, name
    demo = json.loads((out / "demo_validation.json").read_text(encoding="utf-8"))
    dual = json.loads((out / "submission_two_strategies.json").read_text(encoding="utf-8"))
    assert demo["two_strategies_identical"] and demo["samples_per_strategy"] == 302
    assert dual["two_strategies_callback_digest_equal"] and dual["two_strategies_all_samples_equal"]
    print("PASS: reports, baseline, source hashes, coverage, gap counts and dual-strategy evidence")
    print("This verifies delivered evidence; rerunning the real 24h replay requires the original local Parquet files.")


if __name__ == "__main__":
    main()

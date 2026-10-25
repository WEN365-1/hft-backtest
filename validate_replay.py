"""真实录制结果一键核验：python validate_replay.py --data-root data/lesson2_real

加 --require-pass 可在缺字段、最终未同步或前 20 档一致率低于 95% 时返回非零。
24 小时是否足够，由事件时间跨度和文件分区另行核对，不把短样本伪装成长录制。
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from sim_exchange import ParquetReplaySource, SimExchange


def validate(root: str, output: str) -> dict:
    counts: Counter = Counter()
    first_ts, last_ts = None, None
    for event in ParquetReplaySource(root).events():
        counts[(event["market"], event["symbol"], event["event_type"])] += 1
        ts = event["recv_ts_ms"]
        first_ts = ts if first_ts is None else min(first_ts, ts)
        last_ts = ts if last_ts is None else max(last_ts, ts)
    ex = SimExchange(ParquetReplaySource(root))
    stats = ex.run()
    pairs = sorted({(market, symbol) for market, symbol, _ in counts})
    books = {f"{m}.{s}": ex.book(m, s) for m, s in pairs}
    report = {
        "data_root": root,
        "event_counts": {f"{m}.{s}.{k}": v for (m, s, k), v in sorted(counts.items())},
        "first_recv_ts_ms": first_ts,
        "last_recv_ts_ms": last_ts,
        "covered_hours": (last_ts - first_ts) / 3_600_000 if first_ts is not None else 0,
        "snapshot_comparisons": stats.snapshot_comparisons,
        "top20_exact_matches": stats.exact_matches,
        "top20_consistency": (stats.exact_matches / stats.snapshot_comparisons
                              if stats.snapshot_comparisons else None),
        "gaps_detected": stats.gaps,
        "final_synced": {m: b["synced"] for m, b in books.items()},
        "callback_errors": stats.callback_errors,
        "note": "gap 允许在主动断网时出现，关键是通知、暂停并成功恢复；一致率需结合快照时间差复核。",
    }
    out = Path(output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="data/lesson2_real")
    parser.add_argument("--output", default="output/lesson2/real_validation.json")
    parser.add_argument("--require-pass", action="store_true")
    args = parser.parse_args()
    report = validate(args.data_root, args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.require_pass:
        required = ("spot", "usdm")
        for market in required:
            assert any(key.startswith(f"{market}.") and key.endswith(".trade") and value > 0
                       for key, value in report["event_counts"].items()), f"{market} 缺少成交流"
            assert any(key.startswith(f"{market}.") and synced
                       for key, synced in report["final_synced"].items()), f"{market} 最终订单簿未同步"
        assert report["top20_consistency"] is not None, "没有有效快照比较"
        assert report["top20_consistency"] >= 0.95, "前 20 档一致率低于 95%"


if __name__ == "__main__":
    main()

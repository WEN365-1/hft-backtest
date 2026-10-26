"""真实录制结果一键核验：python validate_replay.py --data-root data/lesson2_real

加 --require-pass 可在缺成交流、最终未同步或前 20 档一致率低于 95% 时返回非零。
再加 --require-24h 可要求双市场 depth/snapshot/trade 各自覆盖至少 23.99 小时。
覆盖时长只是首尾跨度，仍须结合 gap 与补录说明判断中间是否完整。
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from sim_exchange import ParquetReplaySource, SimExchange

MIN_24H_COVERAGE = 23.99


def validate(root: str, output: str) -> dict:
    counts: Counter = Counter()
    first_by_channel, last_by_channel = {}, {}
    gap_details = []
    first_ts, last_ts = None, None
    for event in ParquetReplaySource(root).events():
        key = (event["market"], event["symbol"], event["event_type"])
        counts[key] += 1
        ts = event["recv_ts_ms"]
        first_by_channel[key] = min(first_by_channel.get(key, ts), ts)
        last_by_channel[key] = max(last_by_channel.get(key, ts), ts)
        first_ts = ts if first_ts is None else min(first_ts, ts)
        last_ts = ts if last_ts is None else max(last_ts, ts)
        if event["event_type"] == "gap":
            gap_details.append({"market": event["market"], "symbol": event["symbol"],
                                "recv_ts_ms": ts, "reason": event.get("reason")})
    ex = SimExchange(ParquetReplaySource(root))
    stats = ex.run()
    pairs = sorted({(market, symbol) for market, symbol, _ in counts})
    books = {f"{m}.{s}": ex.book(m, s) for m, s in pairs}
    channel_coverage = {}
    for key in sorted(counts):
        name = f"{key[0]}.{key[1]}.{key[2]}"
        hours = (last_by_channel[key] - first_by_channel[key]) / 3_600_000
        channel_coverage[name] = {
            "event_count": counts[key],
            "first_recv_ts_ms": first_by_channel[key],
            "last_recv_ts_ms": last_by_channel[key],
            "covered_hours": hours,
            "meets_24h_span": hours >= MIN_24H_COVERAGE,
        }
    required_channels_24h = all(
        channel_coverage.get(f"{market}.BTCUSDT.{kind}", {}).get("meets_24h_span", False)
        for market in ("spot", "usdm") for kind in ("depth", "snapshot", "trade")
    )
    report = {
        "data_root": root,
        "event_counts": {f"{m}.{s}.{k}": v for (m, s, k), v in sorted(counts.items())},
        "first_recv_ts_ms": first_ts,
        "last_recv_ts_ms": last_ts,
        "covered_hours": (last_ts - first_ts) / 3_600_000 if first_ts is not None else 0,
        "channel_coverage": channel_coverage,
        "all_required_channels_24h": required_channels_24h,
        "snapshot_comparisons": stats.snapshot_comparisons,
        "top20_exact_matches": stats.exact_matches,
        "top20_consistency": (stats.exact_matches / stats.snapshot_comparisons
                              if stats.snapshot_comparisons else None),
        "gaps_detected": stats.gaps,
        "gap_details": gap_details,
        "final_synced": {m: b["synced"] for m, b in books.items()},
        "callback_errors": stats.callback_errors,
        "note": "首尾覆盖不等于中间无缺失；gap 允许在主动断网时出现，关键是通知、暂停并成功恢复。补录频道与时段须单列。",
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
    parser.add_argument("--require-24h", action="store_true")
    args = parser.parse_args()
    report = validate(args.data_root, args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    required = ("spot", "usdm")
    if args.require_pass:
        for market in required:
            assert any(key.startswith(f"{market}.") and key.endswith(".trade") and value > 0
                       for key, value in report["event_counts"].items()), f"{market} 缺少成交流"
            assert any(key.startswith(f"{market}.") and synced
                       for key, synced in report["final_synced"].items()), f"{market} 最终订单簿未同步"
        assert report["top20_consistency"] is not None, "没有有效快照比较"
        assert report["top20_consistency"] >= 0.95, "前 20 档一致率低于 95%"
    if args.require_24h:
        assert report["covered_hours"] >= MIN_24H_COVERAGE, "总体覆盖不足 24 小时"
        assert report["all_required_channels_24h"], "双市场 depth/snapshot/trade 未全部覆盖 24 小时"


if __name__ == "__main__":
    main()

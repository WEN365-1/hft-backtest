"""真实录制结果一键核验：python validate_replay.py --data-root data/lesson2_real

加 --require-pass 可在缺成交流、最终未同步或前 20 档一致率低于 95% 时返回非零。
--require-24h 只认可实时接收覆盖；--require-replay-24h 另允许有来源标记的历史补录。
覆盖时长只是首尾跨度，仍须结合 gap、来源明细与补录清单判断中间是否完整。
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

from sim_exchange import ParquetReplaySource, SimExchange
from sim_exchange.storage import replay_time_ms

MIN_24H_COVERAGE = 23.99
BACKFILL_SOURCE = "binance_rest_aggTrades_backfill"


def validate(root: str, output: str) -> dict:
    counts: Counter = Counter()
    trade_sources: Counter = Counter()
    first_recv, last_recv, first_replay, last_replay = {}, {}, {}, {}
    gap_details = []
    first_ts = last_ts = first_replay_ts = last_replay_ts = None
    for event in ParquetReplaySource(root).events():
        key = (event["market"], event["symbol"], event["event_type"])
        counts[key] += 1
        ts, replay_ts = event["recv_ts_ms"], replay_time_ms(event)
        first_recv[key] = min(first_recv.get(key, ts), ts)
        last_recv[key] = max(last_recv.get(key, ts), ts)
        first_replay[key] = min(first_replay.get(key, replay_ts), replay_ts)
        last_replay[key] = max(last_replay.get(key, replay_ts), replay_ts)
        first_ts = ts if first_ts is None else min(first_ts, ts)
        last_ts = ts if last_ts is None else max(last_ts, ts)
        first_replay_ts = (replay_ts if first_replay_ts is None
                           else min(first_replay_ts, replay_ts))
        last_replay_ts = (replay_ts if last_replay_ts is None
                          else max(last_replay_ts, replay_ts))
        if event["event_type"] == "trade":
            source = event.get("source") or "binance_websocket_live"
            trade_sources[f"{key[0]}.{key[1]}.{source}"] += 1
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
        hours = (last_recv[key] - first_recv[key]) / 3_600_000
        replay_hours = (last_replay[key] - first_replay[key]) / 3_600_000
        channel_coverage[name] = {
            "event_count": counts[key],
            "first_recv_ts_ms": first_recv[key],
            "last_recv_ts_ms": last_recv[key],
            "covered_hours": hours,
            "meets_24h_span": hours >= MIN_24H_COVERAGE,
            "first_replay_ts_ms": first_replay[key],
            "last_replay_ts_ms": last_replay[key],
            "replay_covered_hours": replay_hours,
            "meets_replay_24h_span": replay_hours >= MIN_24H_COVERAGE,
        }
    required_channels_24h = all(
        channel_coverage.get(f"{market}.BTCUSDT.{kind}", {}).get("meets_24h_span", False)
        for market in ("spot", "usdm") for kind in ("depth", "snapshot", "trade")
    )
    required_replay_channels_24h = all(
        channel_coverage.get(f"{market}.BTCUSDT.{kind}", {}).get(
            "meets_replay_24h_span", False)
        for market in ("spot", "usdm") for kind in ("depth", "snapshot", "trade")
    )
    report = {
        "data_root": root,
        "event_counts": {f"{m}.{s}.{k}": v for (m, s, k), v in sorted(counts.items())},
        "trade_source_counts": dict(sorted(trade_sources.items())),
        "first_recv_ts_ms": first_ts,
        "last_recv_ts_ms": last_ts,
        "covered_hours": (last_ts - first_ts) / 3_600_000 if first_ts is not None else 0,
        "first_replay_ts_ms": first_replay_ts,
        "last_replay_ts_ms": last_replay_ts,
        "replay_covered_hours": ((last_replay_ts - first_replay_ts) / 3_600_000
                                 if first_replay_ts is not None else 0),
        "channel_coverage": channel_coverage,
        "all_required_channels_24h": required_channels_24h,
        "all_required_replay_channels_24h": required_replay_channels_24h,
        "snapshot_comparisons": stats.snapshot_comparisons,
        "snapshot_skipped_stale": stats.snapshot_skipped_stale,
        "snapshot_skipped_crossed": stats.snapshot_skipped_crossed,
        "snapshot_pending": stats.snapshot_pending,
        "top20_exact_matches": stats.exact_matches,
        "top20_consistency": (stats.exact_matches / stats.snapshot_comparisons
                              if stats.snapshot_comparisons else None),
        "gaps_detected": stats.gaps,
        "gap_details": gap_details,
        "final_synced": {m: b["synced"] for m, b in books.items()},
        "callback_errors": stats.callback_errors,
        "note": "receive 覆盖只表示本地实时接收；replay 覆盖可含明确标记的 REST 历史补录。一致率只比较同一 lastUpdateId；首尾覆盖不等于中间无缺失。",
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
    parser.add_argument("--require-replay-24h", action="store_true")
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
        assert report["covered_hours"] >= MIN_24H_COVERAGE, "总体实时接收覆盖不足 24 小时"
        assert report["all_required_channels_24h"], "双市场实时 depth/snapshot/trade 未全部覆盖 24 小时"
    if args.require_replay_24h:
        assert report["replay_covered_hours"] >= MIN_24H_COVERAGE, "总体研究回放覆盖不足 24 小时"
        assert report["all_required_replay_channels_24h"], "双市场研究回放频道未全部覆盖 24 小时"


if __name__ == "__main__":
    main()

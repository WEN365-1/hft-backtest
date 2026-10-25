"""可重复离线演示：双市场、缺页恢复、快照体检、双策略与异常隔离。

python demo_sim_exchange.py --data-root data/lesson2_demo --minutes 3
合成数据只用于验证工程机制，不代表真实行情或策略收益。
"""
from __future__ import annotations

import argparse
import json
import math
import shutil
from pathlib import Path

from sim_exchange import ParquetEventStore, ParquetReplaySource, SimExchange
from sim_exchange.strategies import BrokenStrategy, OneSecondMetricsStrategy


def make_demo(root: Path, minutes: int = 3) -> None:
    if root.exists():
        shutil.rmtree(root)
    store = ParquetEventStore(root)
    start = 1_761_400_000_000  # 固定 UTC 小时边界附近，便于复现分区。
    start -= start % 3_600_000
    batch = []
    for market_no, market in enumerate(("spot", "usdm")):
        uid = 1000 + market_no * 10000
        bids = [[59999.5, 2.0], [59999.0, 3.0]]
        asks = [[60000.5, 1.5], [60001.0, 2.5]]
        batch.append({"market": market, "symbol": "BTCUSDT",
                      "event_type": "snapshot", "recv_ts_ms": start - 10 + market_no,
                      "last_update_id": uid, "bids": bids, "asks": asks})
        for tick in range(minutes * 600):
            ts = start + tick * 100 + market_no
            # 第 90 秒记录断线；到下一分钟快照前不再发送行情。
            if tick == 900:
                batch.append({"market": market, "symbol": "BTCUSDT",
                              "event_type": "gap", "recv_ts_ms": ts,
                              "reason": "synthetic disconnect"})
                continue
            if tick % 600 == 0 and tick:
                batch.append({"market": market, "symbol": "BTCUSDT",
                    "event_type": "snapshot", "recv_ts_ms": ts,
                    "last_update_id": uid, "bids": bids, "asks": asks})
            uid += 1
            wave = math.sin(tick / 30)
            bids = [[59999.5, round(2 + wave, 6)], [59999.0, 3.0]]
            asks = [[60000.5, round(1.5 - wave, 6)], [60001.0, 2.5]]
            event = {"market": market, "symbol": "BTCUSDT", "event_type": "depth",
                     "recv_ts_ms": ts, "event_ts_ms": ts - 5,
                     "U": uid, "u": uid, "pu": uid - 1,
                     "bids": bids, "asks": asks}
            batch.append(event)
            batch.append({"market": market, "symbol": "BTCUSDT", "event_type": "trade",
                "recv_ts_ms": ts, "event_ts_ms": ts - 4, "trade_ts_ms": ts - 6,
                "p": "60000.5", "q": str(0.01 + tick % 3 * 0.005), "m": tick % 2 == 0})
            if len(batch) >= 100:
                store.write_batch(batch)
                batch = []
    if batch:
        store.write_batch(batch)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default="data/lesson2_demo")
    parser.add_argument("--minutes", type=int, default=3)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    root = Path(args.data_root)
    make_demo(root, args.minutes)
    ex = SimExchange(ParquetReplaySource(root))
    a = OneSecondMetricsStrategy("策略A", args.verbose)
    b = OneSecondMetricsStrategy("策略B", args.verbose)
    topics = ["spot.BTCUSDT.book", "spot.BTCUSDT.trade", "spot.BTCUSDT.status",
              "usdm.BTCUSDT.book", "usdm.BTCUSDT.trade", "usdm.BTCUSDT.status"]
    for strategy in (a, b):
        ex.subscribe(strategy, topics)
    stats = ex.run()
    report = {
        "data_mode": "synthetic / engineering validation only",
        "markets": ["spot", "usdm"], "duration_seconds": args.minutes * 60,
        "snapshots_compared": stats.snapshot_comparisons,
        "top20_exact_matches": stats.exact_matches,
        "top20_consistency": stats.consistency,
        "gaps_detected_and_recovered": stats.gaps,
        "strategy_callback_errors_isolated": stats.callback_errors,
        "two_strategies_identical": a.samples == b.samples and a.statuses == b.statuses,
        "samples_per_strategy": len(a.samples),
        "callbacks": dict(stats.pushed),
    }
    out = Path("output/lesson2")
    out.mkdir(parents=True, exist_ok=True)
    (out / "demo_validation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    assert stats.consistency >= 0.95
    assert report["two_strategies_identical"]
    assert stats.gaps == 2


if __name__ == "__main__":
    main()

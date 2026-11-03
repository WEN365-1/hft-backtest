"""真实实时源示例：只替换 source，SimExchange 与策略代码保持不变。

python demo_live_source.py --market both --duration 60
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from sim_exchange import BinanceWebSocketSource, SimExchange
from sim_exchange.strategies import OneSecondMetricsStrategy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", choices=["spot", "usdm", "both"], default="both")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration", type=float, default=60, help="演示秒数；只读公开行情")
    parser.add_argument("--proxy", default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--report", help="保存本次真实实时演示摘要，不代表24小时验收")
    args = parser.parse_args()
    markets = ["spot", "usdm"] if args.market == "both" else [args.market]

    # 双市场依次短跑，避免把两个独立实时连接伪装成已经完成跨源时钟归并。
    # 严格跨源研究回放使用录制后的 ParquetReplaySource。
    results = []
    for market in markets:
        started = time.monotonic()
        source = BinanceWebSocketSource(
            market, args.symbol, proxy=args.proxy, duration_seconds=args.duration)
        strategy = OneSecondMetricsStrategy(f"{market}-实时", args.verbose)
        ex = SimExchange(source)
        topics = [f"{market}.{args.symbol}.book", f"{market}.{args.symbol}.trade",
                  f"{market}.{args.symbol}.status"]
        ex.subscribe(strategy, topics)
        stats = ex.run()
        book = ex.book(market, args.symbol)
        results.append({"market": market, "symbol": args.symbol,
                        "elapsed_seconds": time.monotonic() - started,
                        "requested_seconds": args.duration,
                        "final_synced": book["synced"], "gaps": stats.gaps,
                        "callback_errors": stats.callback_errors,
                        "callbacks": dict(stats.pushed), "samples": len(strategy.samples),
                        "nonempty_bids": bool(book["bids"]), "nonempty_asks": bool(book["asks"])})
        print({"market": market, "book": ex.book(market, args.symbol),
               "samples": len(strategy.samples), "gaps": stats.gaps,
               "callback_errors": stats.callback_errors})
    if args.report:
        out = Path(args.report)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"kind": "real_live_demo_not_24h",
                                  "checked_at_utc": datetime.now(timezone.utc).isoformat(),
                                  "results": results}, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

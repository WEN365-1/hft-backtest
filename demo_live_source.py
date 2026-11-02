"""真实实时源示例：只替换 source，SimExchange 与策略代码保持不变。

python demo_live_source.py --market both --duration 60
"""
from __future__ import annotations

import argparse

from sim_exchange import BinanceWebSocketSource, SimExchange
from sim_exchange.strategies import OneSecondMetricsStrategy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", choices=["spot", "usdm", "both"], default="both")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration", type=float, default=60, help="演示秒数；只读公开行情")
    parser.add_argument("--proxy", default=None)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    markets = ["spot", "usdm"] if args.market == "both" else [args.market]

    # 双市场依次短跑，避免把两个独立实时连接伪装成已经完成跨源时钟归并。
    # 严格跨源研究回放使用录制后的 ParquetReplaySource。
    for market in markets:
        source = BinanceWebSocketSource(
            market, args.symbol, proxy=args.proxy, duration_seconds=args.duration)
        strategy = OneSecondMetricsStrategy(f"{market}-实时", args.verbose)
        ex = SimExchange(source)
        topics = [f"{market}.{args.symbol}.book", f"{market}.{args.symbol}.trade",
                  f"{market}.{args.symbol}.status"]
        ex.subscribe(strategy, topics)
        stats = ex.run()
        print({"market": market, "book": ex.book(market, args.symbol),
               "samples": len(strategy.samples), "gaps": stats.gaps,
               "callback_errors": stats.callback_errors})


if __name__ == "__main__":
    main()

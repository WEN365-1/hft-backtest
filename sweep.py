"""
参数扫描：阈值 × 持仓时长，每组跑两遍（不计费 / 计费）。

目的：回答"0.30 和 5 秒是怎么来的"——
  1. 看不同参数下毛利怎么变（策略对参数敏不敏感）
  2. 看有没有哪组参数能覆盖手续费
  3. 说明"在同一段数据上挑最好的参数"就是过拟合风险

用法：python sweep.py [--run RUN_ID]
输出：output/runs/<run_id>/sweep.csv
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

import config
import runpaths
from backtester import load_data, run_backtest

THRESHOLDS = [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90]
HOLDS = [1, 3, 5, 10, 30, 60, 120, 300]


def main(run_id: str | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    run_id = runpaths.resolve(run_id)
    out_dir = runpaths.output_dir(run_id)
    depth_df, agg_df, snapshots = load_data(run_id)

    rows = []
    for th in THRESHOLDS:
        for hold in HOLDS:
            gross = run_backtest(depth_df, agg_df, snapshots,
                                 fee=0.0, latency_ms=0,
                                 threshold=th, hold_seconds=hold)["stats"]
            net = run_backtest(depth_df, agg_df, snapshots,
                               fee=config.TAKER_FEE, latency_ms=0,
                               threshold=th, hold_seconds=hold)["stats"]
            rows.append({
                "阈值": th,
                "持仓秒": hold,
                "交易次数": gross["trade_count"],
                "无费总盈亏": round(gross["total_pnl"], 2),
                "无费每笔": round(gross["avg_pnl"], 3),
                "有费总盈亏": round(net["total_pnl"], 2),
                "有费每笔": round(net["avg_pnl"], 3),
            })

    df = pd.DataFrame(rows)
    path = os.path.join(out_dir, "sweep.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")

    print("\n=== 无费总盈亏（阈值 × 持仓秒）===")
    print(df.pivot(index="阈值", columns="持仓秒", values="无费总盈亏")
            .to_string())
    print("\n=== 有费总盈亏（阈值 × 持仓秒）===")
    print(df.pivot(index="阈值", columns="持仓秒", values="有费总盈亏")
            .to_string())
    print("\n=== 交易次数（阈值 × 持仓秒）===")
    print(df.pivot(index="阈值", columns="持仓秒", values="交易次数")
            .to_string())

    best = df.loc[df["无费总盈亏"].idxmax()]
    print(f"\n无费最优组合：阈值={best['阈值']}, 持仓={best['持仓秒']}s, "
          f"总盈亏={best['无费总盈亏']}, 每笔={best['无费每笔']}")
    print(f"该组合计费后：{best['有费总盈亏']} USDT")
    print(f"\n完整结果: {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", default=None)
    args = parser.parse_args()
    main(args.run)

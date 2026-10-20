"""
对比实验（作业第 4 章第 4 条）：
同一段数据跑三组 —— 无费用无延迟 / 有费用 / 有费用 + 50ms 延迟，
输出一张对比表 + PnL 曲线图。
"""

from __future__ import annotations

import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")           # 不弹窗，直接存图
import matplotlib.pyplot as plt
import pandas as pd

# 让图表能显示中文（Windows 自带微软雅黑）
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False

import config
import runpaths
from backtester import load_data, run_backtest

SCENARIOS = [
    ("无费用无延迟", 0.0, 0),
    ("有费用", config.TAKER_FEE, 0),
    ("有费用+50ms延迟", config.TAKER_FEE, 50),
]


def main(run_id: str | None = None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    run_id = runpaths.resolve(run_id)
    out_dir = runpaths.output_dir(run_id)
    depth_df, agg_df, snapshots = load_data(run_id)

    rows, curves = [], {}
    for name, fee, latency in SCENARIOS:
        result = run_backtest(depth_df, agg_df, snapshots,
                              fee=fee, latency_ms=latency)
        s = result["stats"]
        rows.append({
            "场景": name,
            "交易次数": s["trade_count"],
            "胜率": f"{s['win_rate']:.2%}",
            "总盈亏(USDT)": round(s["total_pnl"], 4),
            "手续费合计": round(s["total_fees"], 4),
            "最大回撤": round(s["max_drawdown"], 4),
            "平均每笔": round(s["avg_pnl"], 6),
        })
        curves[name] = result["equity"]

        # 单独保存每个场景的明细（写进本次 run 的独立文件夹）
        result["trades"].to_csv(
            os.path.join(out_dir, f"trades_{name}.csv"),
            index=False, encoding="utf-8-sig")

    table = pd.DataFrame(rows)
    table.to_csv(os.path.join(out_dir, "comparison.csv"),
                 index=False, encoding="utf-8-sig")

    print("\n=== 对比实验 ===")
    print(table.to_string(index=False))

    # ---- PnL 曲线图 ----
    plt.figure(figsize=(10, 5.5))
    for name, eq in curves.items():
        if eq.empty:
            continue
        x = (eq["ts"] - eq["ts"].iloc[0]) / 60000.0
        plt.step(x, eq["cum_pnl"], where="post", label=name)
    plt.axhline(0, color="gray", linewidth=0.8)
    plt.xlabel("Time (minutes)")
    plt.ylabel("Cumulative PnL (USDT)")
    plt.title("Imbalance Strategy: Cost & Latency Comparison")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    chart = os.path.join(out_dir, "pnl_comparison.png")
    plt.savefig(chart, dpi=140)
    print(f"\n结果目录: {out_dir}")
    print(f"图表已保存: {chart}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", default=None, help="指定运行；不填用最近一次")
    args = parser.parse_args()
    main(args.run)

"""
样本内选参 / 样本外验证（防止"挑运气"）。

实验 A：前 20 分钟选参数，后 10 分钟验证
实验 B：滚动验证（每次用 10 分钟选参，随后 5 分钟验证）

关键思想：
  在训练段挑"无费收益最高"的参数，然后看它在没见过的测试段还灵不灵。
  如果训练段最优 ≠ 测试段最优，说明参数选择很容易过拟合。
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import pandas as pd

import config
import runpaths
from backtester import load_data, run_backtest
from orderbook import LocalOrderBook

THRESHOLDS = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
HOLDS = [1, 3, 5, 10, 30, 60, 120, 300]

MIN_TRAIN_TRADES = 8   # 训练段至少要有这么多笔，避免"只做 1 笔运气好"


def to_snapshot(book: LocalOrderBook, ts: int) -> dict:
    return {
        "recv_ts_ms": ts,
        "snapshot": {
            "lastUpdateId": book.last_update_id,
            "bids": [[f"{p:.8f}", f"{q:.8f}"]
                     for p, q in sorted(book.bids.items(), key=lambda x: -x[0])],
            "asks": [[f"{p:.8f}", f"{q:.8f}"]
                     for p, q in sorted(book.asks.items())],
        },
    }


def anchor_at(depth_df, snapshots, ts: int) -> dict:
    """回放到 ts 时刻，把当时的订单簿拍成一张快照，作为该段的起点。"""
    book = LocalOrderBook()
    book.apply_snapshot(snapshots[0]["snapshot"])
    pi = 1
    for row in depth_df.itertuples(index=False):
        if row.recv_ts_ms > ts:
            break
        while pi < len(snapshots) and snapshots[pi]["recv_ts_ms"] <= row.recv_ts_ms:
            book.apply_snapshot(snapshots[pi]["snapshot"])
            pi += 1
        ev = {"U": row.U, "u": row.u, "b": json.loads(row.b),
              "a": json.loads(row.a)}
        r = book.apply_diff(ev)
        if r == "gap" and pi < len(snapshots) and snapshots[pi]["recv_ts_ms"] <= ts:
            book.apply_snapshot(snapshots[pi]["snapshot"])
            pi += 1
    return to_snapshot(book, ts)


def slice_data(depth_df, agg_df, snapshots, start_ms, end_ms, anchor):
    d = depth_df[(depth_df["recv_ts_ms"] > start_ms)
                 & (depth_df["recv_ts_ms"] <= end_ms)].reset_index(drop=True)
    a = agg_df[(agg_df["recv_ts_ms"] > start_ms)
               & (agg_df["recv_ts_ms"] <= end_ms)].reset_index(drop=True)
    s = [anchor] + [x for x in snapshots if start_ms < x["recv_ts_ms"] <= end_ms]
    return d, a, s


def best_on(d, a, s, min_trades: int):
    """在给定数据段上遍历参数，返回无费收益最高且交易数达标的一组。"""
    rows = []
    for th in THRESHOLDS:
        for hold in HOLDS:
            st = run_backtest(d, a, s, fee=0.0, latency_ms=0,
                              threshold=th, hold_seconds=hold)["stats"]
            if st["trade_count"] >= min_trades:
                rows.append({"阈值": th, "持仓秒": hold,
                             "交易次数": st["trade_count"],
                             "无费盈亏": st["total_pnl"]})
    if not rows:
        return None
    return max(rows, key=lambda r: r["无费盈亏"])


def eval_combo(d, a, s, th, hold, fee):
    st = run_backtest(d, a, s, fee=fee, latency_ms=0,
                      threshold=th, hold_seconds=hold)["stats"]
    return st["trade_count"], st["total_pnl"]


def main(run_id: str | None = None) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    run_id = runpaths.resolve(run_id)
    out_dir = runpaths.output_dir(run_id)
    depth_df, agg_df, snapshots = load_data(run_id)
    snapshots.sort(key=lambda s: s["recv_ts_ms"])

    t0 = int(depth_df["recv_ts_ms"].min())
    t_end = int(depth_df["recv_ts_ms"].max())
    print(f"数据范围: {t0} ~ {t_end}，共 {(t_end - t0) / 60000:.1f} 分钟")

    # ================= 实验 A：前 20 分钟训练，后 10 分钟测试 =================
    split = t0 + 20 * 60 * 1000
    anchor_train = snapshots[0]
    train = slice_data(depth_df, agg_df, snapshots, t0 - 1, split, anchor_train)

    anchor_test = anchor_at(depth_df, snapshots, split)
    test = slice_data(depth_df, agg_df, snapshots, split, t_end, anchor_test)

    best_train = best_on(*train, min_trades=MIN_TRAIN_TRADES)
    print("\n=== 实验 A：前 20 分钟选参 → 后 10 分钟验证 ===")
    if best_train is None:
        print("训练段没有达到最少交易数的参数组合")
    else:
        print(f"训练段最优: 阈值={best_train['阈值']}, 持仓={best_train['持仓秒']}s, "
              f"交易={best_train['交易次数']}, 无费盈亏={best_train['无费盈亏']:.2f}")
        tr_n, tr_pnl = eval_combo(*train, best_train["阈值"], best_train["持仓秒"], 0.0)
        te_n0, te_gross = eval_combo(*test, best_train["阈值"], best_train["持仓秒"], 0.0)
        te_n1, te_net = eval_combo(*test, best_train["阈值"], best_train["持仓秒"],
                                   config.TAKER_FEE)
        print(f"  训练段复核: {tr_n} 笔, {tr_pnl:.2f} USDT")
        print(f"  测试段无费: {te_n0} 笔, {te_gross:.2f} USDT")
        print(f"  测试段计费: {te_n1} 笔, {te_net:.2f} USDT")

        best_test = best_on(*test, min_trades=3)
        if best_test:
            print(f"测试段事后最优（作弊参考）: 阈值={best_test['阈值']}, "
                  f"持仓={best_test['持仓秒']}s, 无费盈亏={best_test['无费盈亏']:.2f}")
        print("默认参数 0.3 / 5s 的测试段表现:")
        d_n0, d_gross = eval_combo(*test, 0.3, 5, 0.0)
        d_n1, d_net = eval_combo(*test, 0.3, 5, config.TAKER_FEE)
        print(f"  无费 {d_n0} 笔 {d_gross:.2f} USDT；计费 {d_n1} 笔 {d_net:.2f} USDT")

    # ================= 实验 B：滚动验证 =================
    print("\n=== 实验 B：滚动验证（10 分钟选参 → 5 分钟验证）===")
    folds = [(0, 10, 15), (5, 15, 20), (10, 20, 25), (15, 25, 30)]
    rows = []
    for start_min, split_min, end_min in folds:
        s_ms = t0 + start_min * 60 * 1000
        sp_ms = t0 + split_min * 60 * 1000
        e_ms = t0 + end_min * 60 * 1000
        anc_tr = anchor_at(depth_df, snapshots, s_ms)
        tr = slice_data(depth_df, agg_df, snapshots, s_ms, sp_ms, anc_tr)
        anc_te = anchor_at(depth_df, snapshots, sp_ms)
        te = slice_data(depth_df, agg_df, snapshots, sp_ms, e_ms, anc_te)
        best = best_on(*tr, min_trades=5)
        if best is None:
            continue
        n0, gross = eval_combo(*te, best["阈值"], best["持仓秒"], 0.0)
        n1, net = eval_combo(*te, best["阈值"], best["持仓秒"], config.TAKER_FEE)
        rows.append({
            "训练区间(min)": f"{start_min}-{split_min}",
            "验证区间(min)": f"{split_min}-{end_min}",
            "选中阈值": best["阈值"],
            "选中持仓秒": best["持仓秒"],
            "训练无费": round(best["无费盈亏"], 2),
            "验证笔数": n0,
            "验证无费": round(gross, 2),
            "验证计费": round(net, 2),
        })
        print(f"fold {start_min}-{split_min} → {split_min}-{end_min}: "
              f"选 {best['阈值']}/{best['持仓秒']}s，"
              f"训练 {best['无费盈亏']:.2f}，验证无费 {gross:.2f}，验证计费 {net:.2f}")

    pd.DataFrame(rows).to_csv(os.path.join(out_dir, "walkforward.csv"),
                              index=False, encoding="utf-8-sig")
    print(f"\n滚动验证结果: {os.path.join(out_dir, 'walkforward.csv')}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", default=None)
    args = parser.parse_args()
    main(args.run)

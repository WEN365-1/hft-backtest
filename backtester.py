"""
事件驱动回测器（第 1 课：看电影）。

流程：
  1. 把 depth 和 aggTrade 两路数据按本地接收时间戳归并成一条事件流
  2. 逐条回放：depth 事件 -> 更新订单簿 -> 算不平衡度
  3. 不平衡度 > 阈值且空仓 -> 市价买入开仓（吃卖档，付 taker 费）
  4. 持仓满固定时长 -> 市价卖出平仓（吃买档，再付一次 taker 费）
  5. 延迟：信号在 t 发出，成交在 t + latency 之后
"""

from __future__ import annotations

import argparse
import heapq
import json
import os

import pandas as pd

import config
import runpaths
from orderbook import LocalOrderBook


# ---------------------------------------------------------------- 数据加载
def load_data(run_id: str | None = None):
    """run_id 不填时读取最近一次运行的数据。"""
    run_id = runpaths.resolve(run_id)
    base = runpaths.data_dir(run_id, create=False)
    print(f"读取运行: {run_id}")
    depth_df = pd.read_parquet(os.path.join(base, "depth.parquet"))
    agg_df = pd.read_parquet(os.path.join(base, "aggtrade.parquet"))
    snapshots = []
    with open(os.path.join(base, "snapshots.jsonl"), "r", encoding="utf-8") as f:
        for line in f:
            snapshots.append(json.loads(line))
    return depth_df, agg_df, snapshots


def merge_events(depth_df: pd.DataFrame, agg_df: pd.DataFrame):
    """把两路数据按时间戳归并成一条有序事件流（像洗两叠牌）。"""
    def depth_iter():
        for row in depth_df.itertuples(index=False):
            yield (row.recv_ts_ms, 0, "depth",
                   {"U": row.U, "u": row.u,
                    "b": json.loads(row.b), "a": json.loads(row.a)})

    def agg_iter():
        for row in agg_df.itertuples(index=False):
            yield (row.recv_ts_ms, 1, "agg",
                   {"p": float(row.p), "q": float(row.q), "m": bool(row.m)})

    return heapq.merge(depth_iter(), agg_iter(), key=lambda x: (x[0], x[1]))


# ---------------------------------------------------------------- 回测主体
def run_backtest(depth_df, agg_df, snapshots,
                 fee: float = config.TAKER_FEE,
                 latency_ms: int = config.LATENCY_MS,
                 threshold: float = config.IMBALANCE_THRESHOLD,
                 hold_seconds: float = config.HOLD_SECONDS,
                 levels: int = config.IMBALANCE_LEVELS,
                 qty: float = config.ORDER_QTY) -> dict:
    book = LocalOrderBook()
    book_ok = False
    snap_i = 0

    position = None          # 当前持仓
    pending: list[tuple[float, str]] = []   # 待执行订单 [(执行时间, 'open'/'close')]
    trades: list[dict] = []
    equity: list[tuple[float, float]] = []

    realized = 0.0
    total_fees = 0.0
    gaps = stale = applied = reanchors = 0
    last_ts = None

    for ts, _, kind, payload in merge_events(depth_df, agg_df):
        last_ts = ts

        # ---- 1. 到点就重锚定：用最新快照重建本地簿（官方第 5 步）----
        while snap_i < len(snapshots) and snapshots[snap_i]["recv_ts_ms"] <= ts:
            book.apply_snapshot(snapshots[snap_i]["snapshot"])
            book_ok = True
            reanchors += 1
            snap_i += 1

        # ---- 2. 回放事件 ----
        if kind == "depth" and book_ok:
            r = book.apply_diff(payload)
            if r == "gap":
                gaps += 1
                book_ok = False          # 缺页 -> 停用，等下一张快照重同步
            elif r == "ok":
                applied += 1
            else:
                stale += 1

        # ---- 3. 执行到期的挂起订单（这就是"延迟"：t 的信号 t+latency 才成交）----
        while pending and pending[0][0] <= ts:
            _, action = pending.pop(0)
            if not book_ok:
                continue
            if action == "open" and position is None:
                price, filled = book.walk_buy(qty)
                if filled > 0:
                    fee_paid = price * filled * fee
                    total_fees += fee_paid
                    position = {"entry_ts": ts, "entry_price": price,
                                "qty": filled, "entry_fee": fee_paid}
            elif action == "close" and position is not None:
                price, filled = book.walk_sell(position["qty"])
                if filled > 0:
                    exit_fee = price * filled * fee
                    total_fees += exit_fee
                    gross = (price - position["entry_price"]) * filled
                    pnl = gross - position["entry_fee"] - exit_fee
                    realized += pnl
                    trades.append({
                        "entry_ts": position["entry_ts"],
                        "exit_ts": ts,
                        "hold_s": (ts - position["entry_ts"]) / 1000,
                        "entry_price": position["entry_price"],
                        "exit_price": price,
                        "qty": filled,
                        "gross": gross,
                        "fee": position["entry_fee"] + exit_fee,
                        "pnl": pnl,
                        "cum_pnl": realized,
                    })
                    equity.append((ts, realized))
                    position = None

        # ---- 4. 策略判断（只在订单簿健康时）----
        if book_ok and kind == "depth":
            if position is None and not any(a == "open" for _, a in pending):
                imb = book.imbalance(levels)
                if imb is not None and imb > threshold:
                    pending.append((ts + latency_ms, "open"))
            elif position is not None and not any(a == "close" for _, a in pending):
                if ts - position["entry_ts"] >= hold_seconds * 1000:
                    pending.append((ts + latency_ms, "close"))

    # ---- 5. 收盘强平（回测结束时不许留着仓位）----
    if position is not None and book_ok:
        price, filled = book.walk_sell(position["qty"])
        if filled > 0:
            exit_fee = price * filled * fee
            total_fees += exit_fee
            pnl = (price - position["entry_price"]) * filled - position["entry_fee"] - exit_fee
            realized += pnl
            trades.append({
                "entry_ts": position["entry_ts"], "exit_ts": last_ts,
                "hold_s": (last_ts - position["entry_ts"]) / 1000,
                "entry_price": position["entry_price"], "exit_price": price,
                "qty": filled, "gross": (price - position["entry_price"]) * filled,
                "fee": position["entry_fee"] + exit_fee, "pnl": pnl, "cum_pnl": realized,
            })
            equity.append((last_ts, realized))

    trades_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity, columns=["ts", "cum_pnl"])

    # ---- 6. 统计指标 ----
    wins = int((trades_df["pnl"] > 0).sum()) if len(trades_df) else 0
    stats = {
        "trade_count": len(trades_df),
        "win_rate": wins / len(trades_df) if len(trades_df) else 0.0,
        "total_pnl": realized,
        "total_fees": total_fees,
        "avg_pnl": trades_df["pnl"].mean() if len(trades_df) else 0.0,
        "max_drawdown": _max_drawdown(equity_df),
        "gaps": gaps,
        "stale": stale,
        "applied": applied,
        "reanchors": reanchors,
        "fee_rate": fee,
        "latency_ms": latency_ms,
        "threshold": threshold,
    }
    return {"trades": trades_df, "equity": equity_df, "stats": stats}


def _max_drawdown(equity_df: pd.DataFrame) -> float:
    """最大回撤：从历史最高点跌下来的最大幅度。"""
    if equity_df.empty:
        return 0.0
    peak = equity_df["cum_pnl"].cummax()
    dd = equity_df["cum_pnl"] - peak
    return float(dd.min())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", default=None, help="指定运行；不填用最近一次")
    args = parser.parse_args()

    depth_df, agg_df, snapshots = load_data(args.run)
    result = run_backtest(depth_df, agg_df, snapshots)
    s = result["stats"]
    print("=== 回测结果（默认参数） ===")
    for k, v in s.items():
        print(f"{k:14s}: {v:.6f}" if isinstance(v, float) else f"{k:14s}: {v}")

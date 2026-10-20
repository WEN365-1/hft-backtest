"""
生成"模拟行情"用于离线测试全流程（真实录制需要能连币安，见 recorder.py）。

它模拟的是：一个 BTCUSDT 订单簿每 100ms 的变化 + 随机成交，
并每隔 60 秒拍一张"快照"，格式和币安官方接口完全一致。
所有数据都是假的，只用来验证代码逻辑。
"""

from __future__ import annotations

import argparse
import json
import os
import random

import pandas as pd

import config
import runpaths

random.seed(42)

TICK = 0.1
LEVELS = 10
STEPS_PER_SEC = 10          # depth@100ms = 每秒 10 条
MID_START = 60000.0


def make_book(mid: float) -> tuple[dict[float, float], dict[float, float]]:
    """围绕 mid 生成买卖各 LEVELS 档。"""
    bids, asks = {}, {}
    for i in range(LEVELS):
        bids[round(mid - TICK * (i + 1), 1)] = round(random.uniform(0.3, 3.0), 3)
        asks[round(mid + TICK * (i + 1), 1)] = round(random.uniform(0.3, 3.0), 3)
    return bids, asks


def snapshot_of(seq: int, bids: dict, asks: dict) -> dict:
    return {
        "recv_ts_ms": None,  # 由主循环填
        "snapshot": {
            "lastUpdateId": seq,
            "bids": [[f"{p:.1f}", f"{q:.3f}"] for p, q in sorted(bids.items(), key=lambda x: -x[0])],
            "asks": [[f"{p:.1f}", f"{q:.3f}"] for p, q in sorted(asks.items())],
        },
    }


def generate(minutes: int = 30, run_id: str | None = None) -> None:
    run_id = run_id or runpaths.new_run_id("sample")
    out_dir = runpaths.data_dir(run_id)

    start_ts = 1_760_000_000_000  # 固定的假起始时间（毫秒）
    total_steps = minutes * 60 * STEPS_PER_SEC
    snap_every = config.SNAPSHOT_EVERY_SEC * STEPS_PER_SEC

    mid = MID_START
    bids, asks = make_book(mid)

    depth_rows: list[dict] = []
    agg_rows: list[dict] = []
    snapshots: list[dict] = []

    # 第 0 秒先拍一张初始快照（相当于录制开始时先要一次 REST 快照）
    snap = snapshot_of(0, bids, asks)
    snap["recv_ts_ms"] = start_ts
    snapshots.append(snap)

    seq = 0
    for step in range(1, total_steps + 1):
        ts = start_ts + step * 100
        # ---- 1. 中间价随机游走（每步最多动 1 个 tick）----
        mid = round(mid + random.choice([-TICK, 0.0, 0.0, TICK]), 1)

        # ---- 2. 重建盘口，并找出和上一刻不同的档位（只发"变化"）----
        new_bids, new_asks = make_book(mid)
        changed_b, changed_a = [], []

        for price, qty in new_bids.items():
            old = bids.get(price, 0.0)
            if abs(old - qty) > 1e-9:
                changed_b.append([f"{price:.1f}", f"{qty:.3f}"])
        for price in bids:
            if price not in new_bids:
                changed_b.append([f"{price:.1f}", "0"])   # qty=0 表示删档

        for price, qty in new_asks.items():
            old = asks.get(price, 0.0)
            if abs(old - qty) > 1e-9:
                changed_a.append([f"{price:.1f}", f"{qty:.3f}"])
        for price in asks:
            if price not in new_asks:
                changed_a.append([f"{price:.1f}", "0"])

        bids, asks = new_bids, new_asks
        seq += 1

        depth_rows.append({
            "recv_ts_ms": ts,
            "E": ts,
            "U": seq,
            "u": seq,
            "b": json.dumps(changed_b),
            "a": json.dumps(changed_a),
        })

        # ---- 3. 随机产生成交（aggTrade）----
        if random.random() < 0.35:
            best_bid = max(bids)
            best_ask = min(asks)
            side = random.choice(["buy", "sell"])
            agg_rows.append({
                "recv_ts_ms": ts,
                "T": ts,
                "p": f"{(best_ask if side == 'buy' else best_bid):.1f}",
                "q": f"{random.uniform(0.001, 0.8):.4f}",
                "m": side == "sell",   # 主动卖 => 买方是 maker
            })

        # ---- 4. 每 60 秒拍一张快照（体检 + 重锚定）----
        if step % snap_every == 0:
            snap = snapshot_of(seq, bids, asks)
            snap["recv_ts_ms"] = ts
            snapshots.append(snap)

    # ---- 写盘：本次 run 的独立文件夹 ----
    pd.DataFrame(depth_rows).to_parquet(
        os.path.join(out_dir, "depth.parquet"), index=False)
    pd.DataFrame(agg_rows).to_parquet(
        os.path.join(out_dir, "aggtrade.parquet"), index=False)
    with open(os.path.join(out_dir, "snapshots.jsonl"), "w", encoding="utf-8") as f:
        for s in snapshots:
            f.write(json.dumps(s) + "\n")
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "kind": "sample", "minutes": minutes},
                  f, ensure_ascii=False, indent=2)

    runpaths.set_latest(run_id)

    print(f"已生成模拟数据：{minutes} 分钟")
    print(f"  run id       : {run_id}")
    print(f"  depth 事件   : {len(depth_rows)} 条")
    print(f"  aggTrade 事件: {len(agg_rows)} 条")
    print(f"  快照         : {len(snapshots)} 张")
    print(f"  数据目录     : {out_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=30)
    parser.add_argument("--run", default=None,
                        help="本次运行的名字；不填自动用 sample_时间戳")
    args = parser.parse_args()
    generate(minutes=args.minutes, run_id=args.run)

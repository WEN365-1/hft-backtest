"""
订单簿重建 + 体检（作业第 4 章第 2 条）。

做两件事：
  1. 从第一张快照出发，连续应用所有 depth 增量（不回退、不重锚定）
  2. 每张录制下来的 REST 快照到达时，和本地簿比对前 20 档，统计不一致次数
  3. 发现缺页（U > 本地编号 + 1）时，用下一张快照自动重同步

运行：python verify_orderbook.py
输出：每个快照的比对结果 + 汇总统计
"""

from __future__ import annotations

import argparse
import json
import os

import pandas as pd

import runpaths
from orderbook import LocalOrderBook, compare_books


def main(run_id: str | None = None) -> None:
    run_id = runpaths.resolve(run_id)
    base = runpaths.data_dir(run_id, create=False)
    print(f"读取运行: {run_id}")
    depth_df = pd.read_parquet(os.path.join(base, "depth.parquet"))
    with open(os.path.join(base, "snapshots.jsonl"), "r", encoding="utf-8") as f:
        snapshots = [json.loads(line) for line in f]
    snapshots.sort(key=lambda s: s["recv_ts_ms"])

    book = LocalOrderBook()
    book.apply_snapshot(snapshots[0]["snapshot"])   # 起点：第一张快照
    pending_checks = snapshots[1:]
    pi = 0

    applied = stale = gaps = resyncs = checks = 0
    mismatch_counts: list[int] = []

    for row in depth_df.itertuples(index=False):
        ev = {"U": row.U, "u": row.u,
              "b": json.loads(row.b), "a": json.loads(row.a)}
        r = book.apply_diff(ev)

        if r == "gap":
            gaps += 1
            # 缺页 -> 用下一张快照重同步（真实系统里就是重新拍一张快照）
            if pi < len(pending_checks):
                book.apply_snapshot(pending_checks[pi]["snapshot"])
                pi += 1
                resyncs += 1
            else:
                print("缺页且没有可用快照，停止回放")
                break
            continue
        if r == "stale":
            stale += 1
            continue
        applied += 1

        # 本地编号追上某张快照的编号时，比对一次前 20 档
        while (pi < len(pending_checks)
               and book.last_update_id >= pending_checks[pi]["snapshot"]["lastUpdateId"]):
            mismatch = compare_books(book, pending_checks[pi]["snapshot"], n=20)
            mismatch_counts.append(mismatch)
            checks += 1
            pi += 1

    print("=== 订单簿重建体检 ===")
    print(f"应用增量事件      : {applied}")
    print(f"跳过旧事件(stale) : {stale}")
    print(f"缺页次数(gap)     : {gaps}")
    print(f"自动重同步次数     : {resyncs}")
    print(f"快照比对次数       : {checks}")
    if checks:
        print(f"不一致档位数       : 平均 {sum(mismatch_counts)/checks:.2f}，"
              f"最大 {max(mismatch_counts)}")
        perfect = sum(1 for m in mismatch_counts if m == 0)
        print(f"完全一致的比例     : {perfect}/{checks} = {perfect/checks:.1%}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", default=None, help="指定运行；不填用最近一次")
    args = parser.parse_args()
    main(args.run)

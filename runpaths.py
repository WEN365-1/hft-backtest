"""
运行目录管理：每次录制/回测一个带时间戳的文件夹，互不覆盖。

目录结构：
  data/runs/btcusdt_20261020_114313/
      depth.parquet
      aggtrade.parquet
      snapshots.jsonl
      meta.json
  output/runs/btcusdt_20261020_114313/
      comparison.csv
      trades_*.csv
      pnl_comparison.png

data/latest_run.txt 记录最近一次 run id，方便不写 --run 时默认用最新一份。
"""

from __future__ import annotations

import os
import time

import config


def new_run_id(prefix: str = "btcusdt") -> str:
    """生成带时间戳的 run id，例如 btcusdt_20261020_114313。"""
    return f"{prefix}_{time.strftime('%Y%m%d_%H%M%S')}"


def data_dir(run_id: str, create: bool = True) -> str:
    path = os.path.join(config.RUNS_DIR, run_id)
    if create:
        os.makedirs(path, exist_ok=True)
    return path


def output_dir(run_id: str, create: bool = True) -> str:
    path = os.path.join(config.OUTPUT_RUNS_DIR, run_id)
    if create:
        os.makedirs(path, exist_ok=True)
    return path


def set_latest(run_id: str) -> None:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    with open(config.LATEST_POINTER, "w", encoding="utf-8") as f:
        f.write(run_id)


def latest_run_id() -> str:
    if not os.path.exists(config.LATEST_POINTER):
        raise SystemExit("还没有任何运行记录：请先录制数据（recorder.py）"
                         "或生成模拟数据（make_sample_data.py）")
    with open(config.LATEST_POINTER, "r", encoding="utf-8") as f:
        return f.read().strip()


def resolve(run_id: str | None) -> str:
    """--run 不填时，自动使用最近一次运行。"""
    return run_id or latest_run_id()

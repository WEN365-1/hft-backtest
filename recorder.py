"""
录制器（作业第 4 章第 1 条）：订阅 depth@100ms + aggTrade，录 30 分钟。

要点：
  - 一条 WS 连接订阅两个频道（合并流），消息外层带 "stream" 标签
  - 每条消息落盘时加"本地接收时间戳"recv_ts_ms（防前视偏差的关键）
  - 每 60 秒拍一张 REST 快照（lastUpdateId + 多档挂单），用于回测重锚定和体检
  - 断线自动重连；重连后立刻补一张快照

用法：
  python recorder.py            # 录 30 分钟
  python recorder.py --minutes 1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

import pandas as pd
import requests

import config
import runpaths

try:  # websockets >= 14 推荐的新 API
    from websockets.asyncio.client import connect
except ImportError:  # 兼容旧版本
    from websockets import connect

WS_URL = ("wss://stream.binance.com:443/stream?streams="
          f"{config.DEPTH_STREAM}/{config.AGG_STREAM}")
REST_DEPTH = "https://api.binance.com/api/v3/depth"


def now_ms() -> int:
    return int(time.time() * 1000)


def fetch_snapshot(limit: int = 1000, proxy: str | None = None) -> dict:
    """REST 深度快照。权重：limit 501~1000 -> 50。"""
    proxies = {"http": proxy, "https": proxy} if proxy else None
    r = requests.get(REST_DEPTH,
                     params={"symbol": config.SYMBOL, "limit": limit},
                     timeout=15, proxies=proxies)
    r.raise_for_status()
    data = r.json()
    return {
        "recv_ts_ms": now_ms(),
        "snapshot": {
            "lastUpdateId": data["lastUpdateId"],
            "bids": data["bids"],
            "asks": data["asks"],
        },
    }


async def record(minutes: float, proxy: str | None = None,
                 run_id: str | None = None) -> None:
    run_id = run_id or runpaths.new_run_id()
    out_dir = runpaths.data_dir(run_id)
    end_ts = time.time() + minutes * 60
    print(f"[{time.strftime('%H:%M:%S')}] 本次运行: {run_id} -> {out_dir}")

    depth_rows: list[dict] = []
    agg_rows: list[dict] = []
    snapshots: list[dict] = []

    backoff = 1
    while time.time() < end_ts:
        try:
            conn_kwargs = {"ping_interval": 20, "ping_timeout": 60}
            if proxy:
                conn_kwargs["proxy"] = proxy
            async with connect(WS_URL, **conn_kwargs) as ws:
                print(f"[{time.strftime('%H:%M:%S')}] WS 已连接")
                backoff = 1

                snap = await asyncio.to_thread(fetch_snapshot, proxy=proxy)
                snapshots.append(snap)
                last_snap = time.time()

                while time.time() < end_ts:
                    raw = await ws.recv()
                    recv_ts = now_ms()
                    msg = json.loads(raw)
                    stream = msg.get("stream")
                    data = msg.get("data")
                    if stream is None or data is None:
                        continue

                    if stream == config.DEPTH_STREAM:
                        depth_rows.append({
                            "recv_ts_ms": recv_ts,
                            "E": data["E"],
                            "U": data["U"],
                            "u": data["u"],
                            "b": json.dumps(data["b"]),
                            "a": json.dumps(data["a"]),
                        })
                    elif stream == config.AGG_STREAM:
                        agg_rows.append({
                            "recv_ts_ms": recv_ts,
                            "T": data["T"],
                            "p": data["p"],
                            "q": data["q"],
                            "m": data["m"],
                        })

                    if time.time() - last_snap >= config.SNAPSHOT_EVERY_SEC:
                        snap = await asyncio.to_thread(fetch_snapshot, proxy=proxy)
                        snapshots.append(snap)
                        last_snap = time.time()
                        print(f"[{time.strftime('%H:%M:%S')}] 快照 "
                              f"lastUpdateId={snap['snapshot']['lastUpdateId']}，"
                              f"depth={len(depth_rows)} agg={len(agg_rows)}")

        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] 连接断开: {e!r}，"
                  f"{backoff}s 后重连")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    # ---- 落盘：写进本次 run 的独立文件夹，不覆盖历史 ----
    pd.DataFrame(depth_rows).to_parquet(
        os.path.join(out_dir, "depth.parquet"), index=False)
    pd.DataFrame(agg_rows).to_parquet(
        os.path.join(out_dir, "aggtrade.parquet"), index=False)
    with open(os.path.join(out_dir, "snapshots.jsonl"), "w", encoding="utf-8") as f:
        for s in snapshots:
            f.write(json.dumps(s) + "\n")

    meta = {
        "run_id": run_id,
        "symbol": config.SYMBOL,
        "minutes": minutes,
        "depth_events": len(depth_rows),
        "agg_trades": len(agg_rows),
        "snapshots": len(snapshots),
        "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "proxy": proxy or "",
    }
    with open(os.path.join(out_dir, "meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    runpaths.set_latest(run_id)

    print(f"录制完成: depth={len(depth_rows)}, agg={len(agg_rows)}, "
          f"snapshots={len(snapshots)}")
    print(f"数据目录: {out_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=30)
    parser.add_argument("--proxy", default=None,
                        help="例如 http://127.0.0.1:7890；不填则直连")
    parser.add_argument("--run", default=None,
                        help="本次运行的名字；不填自动用时间戳，永不覆盖")
    args = parser.parse_args()
    asyncio.run(record(args.minutes, proxy=args.proxy, run_id=args.run))

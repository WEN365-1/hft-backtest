"""仅在双市场 REST 与 WebSocket 均可达时允许新一轮 24 小时采集启动。"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import requests

try:
    from websockets.asyncio.client import connect
except ImportError:
    from websockets import connect


def _rest_ready(url: str) -> bool:
    response = requests.get(url, timeout=8)
    response.raise_for_status()
    return True


async def _websocket_ready(url: str) -> bool:
    async with connect(url, open_timeout=8, ping_interval=None) as ws:
        await asyncio.wait_for(ws.recv(), timeout=8)
    return True


async def _all_ready() -> bool:
    checks = [
        asyncio.to_thread(_rest_ready, "https://api.binance.com/api/v3/ping"),
        asyncio.to_thread(_rest_ready, "https://fapi.binance.com/fapi/v1/ping"),
        _websocket_ready("wss://stream.binance.com:443/ws/btcusdt@depth@100ms"),
        _websocket_ready("wss://fstream.binance.com/ws/btcusdt@depth@100ms"),
    ]
    results = await asyncio.gather(*checks, return_exceptions=True)
    return all(result is True for result in results)


def should_fire(ctx) -> bool:
    status_path = Path(r"C:\Users\HP\Desktop\DBC_project\Binance\hft-backtest\output\lesson2\capture_24h_status.json")
    try:
        if status_path.exists():
            status = json.loads(status_path.read_text(encoding="utf-8"))
            is_v2 = str(status.get("dataRoot", "")).endswith("lesson2_real_24h_v2")
            if is_v2 and status.get("state") in {"running", "succeeded", "completed", "failed"}:
                return False  # 已开始或已有终态，禁止向同一目录自动重复采集。
        return asyncio.run(asyncio.wait_for(_all_ready(), timeout=20))
    except Exception:
        return False

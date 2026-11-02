"""最小实时行情源：与 ParquetReplaySource 一样通过 events() 供 SimExchange 使用。

websockets 仅在真正启动实时连接时导入，纯离线逻辑测试不依赖它。
"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Iterator

from .endpoints import endpoints


def now_ms() -> int:
    return int(time.time() * 1000)


def fetch_snapshot(market: str, symbol: str, rest_url: str | None = None,
                   proxy: str | None = None) -> dict:
    """同步 REST 快照函数；实时源通过 asyncio.to_thread 调用，避免阻塞接收循环。"""
    import requests

    url = rest_url or endpoints(market, symbol)[1]
    proxies = {"http": proxy, "https": proxy} if proxy else None
    response = requests.get(url, params={"symbol": symbol, "limit": 1000},
                            timeout=20, proxies=proxies)
    response.raise_for_status()
    data = response.json()
    return {"market": market, "symbol": symbol, "event_type": "snapshot",
            "recv_ts_ms": now_ms(), "event_ts_ms": None,
            "last_update_id": int(data["lastUpdateId"]),
            "bids": data["bids"], "asks": data["asks"]}


def message_to_event(market: str, symbol: str, raw: str | bytes,
                     recv_ts_ms: int) -> dict | None:
    """把合并流消息映射为统一 depth/trade 事件；未知消息明确忽略。"""
    msg = json.loads(raw)
    stream, data = msg.get("stream", ""), msg.get("data", {})
    if stream.endswith("@depth@100ms"):
        return {"market": market, "symbol": symbol, "event_type": "depth",
                "recv_ts_ms": recv_ts_ms, "event_ts_ms": data.get("E"),
                "U": data["U"], "u": data["u"], "pu": data.get("pu"),
                "bids": data["b"], "asks": data["a"]}
    if stream.endswith(("@aggTrade", "@trade")):
        return {"market": market, "symbol": symbol, "event_type": "trade",
                "recv_ts_ms": recv_ts_ms, "event_ts_ms": data.get("E"),
                "trade_ts_ms": data.get("T"), "p": data["p"], "q": data["q"],
                "m": data["m"]}
    return None


class BinanceWebSocketSource:
    """实时 source 示例；策略和 SimExchange 无需知道事件来自网络还是 Parquet。"""

    def __init__(self, market: str, symbol: str = "BTCUSDT",
                 proxy: str | None = None, snapshot_interval_seconds: float = 60,
                 duration_seconds: float | None = None):
        if snapshot_interval_seconds <= 0:
            raise ValueError("snapshot_interval_seconds 必须大于 0")
        if duration_seconds is not None and duration_seconds < 0:
            raise ValueError("duration_seconds 不能为负数")
        self.market, self.symbol = market, symbol
        self.proxy = proxy
        self.snapshot_interval_seconds = snapshot_interval_seconds
        self.duration_seconds = duration_seconds

    def events(self) -> Iterator[dict]:
        """在独立事件循环上驱动异步流，对上层保留同步迭代器契约。"""
        async_events = self._stream()
        loop = asyncio.new_event_loop()
        try:
            iterator = async_events.__aiter__()
            while True:
                try:
                    yield loop.run_until_complete(iterator.__anext__())
                except StopAsyncIteration:
                    return
        finally:
            loop.run_until_complete(async_events.aclose())
            loop.close()

    async def _stream(self) -> AsyncIterator[dict]:
        try:
            from websockets.asyncio.client import connect
        except ImportError:  # websockets 12/13 兼容入口
            from websockets import connect

        ws_url, rest_url = endpoints(self.market, self.symbol)
        started = time.monotonic()
        backoff = 1

        def expired() -> bool:
            return (self.duration_seconds is not None
                    and time.monotonic() - started >= self.duration_seconds)

        while not expired():
            try:
                kwargs = {"ping_interval": 20, "ping_timeout": 60, "open_timeout": 15}
                if self.proxy:
                    kwargs["proxy"] = self.proxy
                async with connect(ws_url, **kwargs) as ws:
                    yield await asyncio.to_thread(
                        fetch_snapshot, self.market, self.symbol, rest_url, self.proxy)
                    backoff = 1
                    last_snapshot = time.monotonic()
                    while not expired():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            raw = None
                        if raw:
                            event = message_to_event(self.market, self.symbol, raw, now_ms())
                            if event is not None:
                                yield event
                        if time.monotonic() - last_snapshot >= self.snapshot_interval_seconds:
                            yield await asyncio.to_thread(
                                fetch_snapshot, self.market, self.symbol, rest_url, self.proxy)
                            last_snapshot = time.monotonic()
            except Exception as exc:
                # 先通知 gap；SimExchange 会暂停推送，下一张快照到达后再恢复。
                yield {"market": self.market, "symbol": self.symbol,
                       "event_type": "gap", "recv_ts_ms": now_ms(), "reason": repr(exc)}
                if expired():
                    break
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

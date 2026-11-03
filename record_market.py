"""第二课真实行情采集：现货/U 本位、分钟快照、断线 gap、分区 Parquet。

示例：python record_market.py --market both --symbol BTCUSDT --hours 24 --data-root data/lesson2
仅使用公开行情，无需密钥。24 小时采集属于运行任务，不用合成数据冒充。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time

import requests

from sim_exchange.endpoints import endpoints
from sim_exchange.live_source import now_ms
from sim_exchange.storage import ParquetEventStore

try:
    from websockets.asyncio.client import connect
except ImportError:
    from websockets import connect


def snapshot(market: str, symbol: str, rest_url: str, proxy: str | None) -> dict:
    proxies = {"http": proxy, "https": proxy} if proxy else None
    response = requests.get(rest_url, params={"symbol": symbol, "limit": 5000 if market == "spot" else 1000},
                            timeout=20, proxies=proxies)
    response.raise_for_status()
    data = response.json()
    return {"market": market, "symbol": symbol, "event_type": "snapshot",
            "recv_ts_ms": now_ms(), "event_ts_ms": None,
            "last_update_id": int(data["lastUpdateId"]),
            "bids": data["bids"], "asks": data["asks"]}


async def record_one(market: str, symbol: str, hours: float, store: ParquetEventStore,
                     proxy: str | None = None) -> None:
    # 与实时演示共用连接/快照/重连实现，避免两套采集协议漂移。
    from sim_exchange.live_source import BinanceWebSocketSource
    source = BinanceWebSocketSource(market, symbol, proxy=proxy,
                                    duration_seconds=hours * 3600)
    rows = []
    last_flush = time.monotonic()
    try:
        async for event in source._stream():
            if event["event_type"] in {"snapshot", "gap"}:
                if rows:
                    store.write_batch(rows)
                    rows = []
                store.write_batch([event])
                print(f"{market} {event['event_type']}", flush=True)
            else:
                rows.append(event)
            if time.monotonic() - last_flush >= 60:
                if rows:
                    store.write_batch(rows)
                    rows = []
                last_flush = time.monotonic()
    finally:
        if rows:
            store.write_batch(rows)
    _, rest_url = endpoints(market, symbol)
    store.write_batch([await asyncio.to_thread(snapshot, market, symbol, rest_url, proxy)])
    print(f"{market} 已到采集终点并保存终点快照", flush=True)


async def record_usdm_trades(symbol: str, hours: float, store: ParquetEventStore,
                             proxy: str | None = None) -> None:
    """只补录 USD-M 逐笔成交，不重复写深度或快照。"""
    url = f"wss://fstream.binance.com/ws/{symbol.lower()}@trade"
    end = time.monotonic() + hours * 3600
    backoff = 1
    while time.monotonic() < end:
        rows = []
        try:
            kwargs = {"ping_interval": 20, "ping_timeout": 60}
            if proxy:
                kwargs["proxy"] = proxy
            async with connect(url, **kwargs) as ws:
                print("usdm 逐笔成交流已连接", flush=True)
                last_flush = time.monotonic()
                backoff = 1
                while time.monotonic() < end:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        raw = None
                    recv = now_ms()
                    if raw:
                        data = json.loads(raw)
                        if data.get("e") == "trade":
                            rows.append({"market": "usdm", "symbol": symbol,
                                "event_type": "trade", "recv_ts_ms": recv,
                                "event_ts_ms": data.get("E"), "trade_ts_ms": data.get("T"),
                                "p": data["p"], "q": data["q"], "m": data["m"]})
                    if time.monotonic() - last_flush >= 60:
                        store.write_batch(rows)
                        print(f"usdm 已补写 {len(rows)} 条逐笔成交", flush=True)
                        rows, last_flush = [], time.monotonic()
        except Exception as exc:
            store.write_batch(rows)
            rows = []
            store.write_batch([{"market": "usdm", "symbol": symbol,
                "event_type": "gap", "recv_ts_ms": now_ms(),
                "reason": f"trade supplement: {exc!r}"}])
            print(f"usdm 成交流断线：{exc!r}；{backoff}s 后重连", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        finally:
            if rows:
                store.write_batch(rows)


async def run_all(markets: list[str], symbol: str, hours: float,
                  store: ParquetEventStore, proxy: str | None) -> None:
    await asyncio.gather(*[record_one(m, symbol, hours, store, proxy) for m in markets])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--market", choices=["spot", "usdm", "both"], default="both")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--data-root", default="data/lesson2")
    parser.add_argument("--proxy", default=None)
    args = parser.parse_args()
    store = ParquetEventStore(args.data_root)
    markets = ["spot", "usdm"] if args.market == "both" else [args.market]
    asyncio.run(run_all(markets, args.symbol, args.hours, store, args.proxy))


if __name__ == "__main__":
    main()

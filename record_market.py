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

from sim_exchange.storage import ParquetEventStore

try:
    from websockets.asyncio.client import connect
except ImportError:
    from websockets import connect


def now_ms() -> int:
    return int(time.time() * 1000)


def endpoints(market: str, symbol: str) -> tuple[str, str]:
    s = symbol.lower()
    # 实测 USD-M 的 aggTrade 持续无消息，而同主机逐笔 trade 正常；保留相同 E/T/p/q/m。
    trade_stream = "aggTrade" if market == "spot" else "trade"
    streams = f"{s}@depth@100ms/{s}@{trade_stream}"
    if market == "spot":
        # 443 与常用 HTTPS 端口一致，网络切换后通常比 9443 更容易恢复。
        return (f"wss://stream.binance.com:443/stream?streams={streams}",
                "https://api.binance.com/api/v3/depth")
    return (f"wss://fstream.binance.com/stream?streams={streams}",
            "https://fapi.binance.com/fapi/v1/depth")


def snapshot(market: str, symbol: str, rest_url: str, proxy: str | None) -> dict:
    proxies = {"http": proxy, "https": proxy} if proxy else None
    response = requests.get(rest_url, params={"symbol": symbol, "limit": 1000},
                            timeout=20, proxies=proxies)
    response.raise_for_status()
    data = response.json()
    return {"market": market, "symbol": symbol, "event_type": "snapshot",
            "recv_ts_ms": now_ms(), "event_ts_ms": None,
            "last_update_id": int(data["lastUpdateId"]),
            "bids": data["bids"], "asks": data["asks"]}


async def record_one(market: str, symbol: str, hours: float, store: ParquetEventStore,
                     proxy: str | None = None) -> None:
    ws_url, rest_url = endpoints(market, symbol)
    end = time.monotonic() + hours * 3600
    backoff = 1
    while time.monotonic() < end:
        rows = []
        try:
            kwargs = {"ping_interval": 20, "ping_timeout": 60, "open_timeout": 15}
            if proxy:
                kwargs["proxy"] = proxy
            async with connect(ws_url, **kwargs) as ws:
                print(f"{market} 已连接，重新获取快照", flush=True)
                store.write_batch([
                    await asyncio.to_thread(snapshot, market, symbol, rest_url, proxy)
                ])
                last_flush = time.monotonic()
                backoff = 1
                while time.monotonic() < end:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        raw = None
                    recv = now_ms()
                    if raw:
                        msg = json.loads(raw)
                        stream, data = msg.get("stream", ""), msg.get("data", {})
                        if stream.endswith("@depth@100ms"):
                            rows.append({"market": market, "symbol": symbol,
                                "event_type": "depth", "recv_ts_ms": recv,
                                "event_ts_ms": data.get("E"), "U": data["U"],
                                "u": data["u"], "pu": data.get("pu"),
                                "bids": data["b"], "asks": data["a"]})
                        elif stream.endswith(("@aggTrade", "@trade")):
                            rows.append({"market": market, "symbol": symbol,
                                "event_type": "trade", "recv_ts_ms": recv,
                                "event_ts_ms": data.get("E"), "trade_ts_ms": data.get("T"),
                                "p": data["p"], "q": data["q"], "m": data["m"]})
                    if time.monotonic() - last_flush >= 60:
                        store.write_batch(rows)
                        store.write_batch([
                            await asyncio.to_thread(snapshot, market, symbol, rest_url, proxy)
                        ])
                        print(f"{market} 已写 {len(rows)} 条并保存分钟快照", flush=True)
                        rows, last_flush = [], time.monotonic()
        except Exception as exc:
            store.write_batch(rows)
            rows = []  # 防止 finally 再写一次，造成断线前事件重复。
            store.write_batch([{"market": market, "symbol": symbol,
                "event_type": "gap", "recv_ts_ms": now_ms(), "reason": repr(exc)}])
            print(f"{market} 断线：{exc!r}；{backoff}s 后重连", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        finally:
            if rows:
                store.write_batch(rows)
    # 分钟快照通常早于终点几十秒；终点再拍一张，才能严格证明快照频道覆盖全程。
    store.write_batch([
        await asyncio.to_thread(snapshot, market, symbol, rest_url, proxy)
    ])
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

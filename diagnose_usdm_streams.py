"""短期独立诊断 USD-M WebSocket 成交流；不写入正式采集目录。"""
import asyncio
import json
import time

try:
    from websockets.asyncio.client import connect
except ImportError:
    from websockets import connect


async def probe(name, url, subscribe=False, seconds=20):
    started = time.time()
    counts = {}
    samples = []
    try:
        async with connect(url, open_timeout=15, ping_interval=None) as ws:
            if subscribe:
                await ws.send(json.dumps({
                    "method": "SUBSCRIBE",
                    "params": ["btcusdt@aggTrade"],
                    "id": 1,
                }))
            while time.time() - started < seconds:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, seconds - (time.time() - started)))
                except asyncio.TimeoutError:
                    break
                msg = json.loads(raw)
                key = msg.get("stream") or msg.get("e") or ("subscription_response" if "result" in msg else "other")
                counts[key] = counts.get(key, 0) + 1
                if len(samples) < 3:
                    samples.append(raw[:500])
    except Exception as exc:
        print(name, "ERROR", repr(exc), flush=True)
        return
    print(name, "COUNTS", counts, "SAMPLES", samples, flush=True)


async def main():
    await asyncio.gather(
        probe("raw_aggTrade", "wss://fstream.binance.com/ws/btcusdt@aggTrade"),
        probe("combined_aggTrade", "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade"),
        probe("subscribe_aggTrade", "wss://fstream.binance.com/ws", subscribe=True),
        probe("raw_trade", "wss://fstream.binance.com/ws/btcusdt@trade"),
    )


if __name__ == "__main__":
    asyncio.run(main())

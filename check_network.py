"""
网络体检：一条命令判断"VPN 开对了没有"。

检查两件事：
  1. REST 能不能连上币安（拿深度快照）
  2. WebSocket 能不能连上并收到一条真实数据

用法：
  python check_network.py
  python check_network.py --proxy http://127.0.0.1:7890
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

import requests

import config

REST_PING = "https://api.binance.com/api/v3/ping"
REST_DEPTH = "https://api.binance.com/api/v3/depth"
WS_URL = ("wss://stream.binance.com:443/stream?streams="
          f"{config.DEPTH_STREAM}/{config.AGG_STREAM}")


def proxies_of(proxy: str | None):
    if not proxy:
        return None
    return {"http": proxy, "https": proxy}


def test_rest(proxy: str | None) -> bool:
    print("[1/2] 测试 REST ...")
    try:
        r = requests.get(REST_PING, timeout=15, proxies=proxies_of(proxy))
        print(f"      ping: HTTP {r.status_code}")
        r = requests.get(REST_DEPTH,
                         params={"symbol": config.SYMBOL, "limit": 5},
                         timeout=15, proxies=proxies_of(proxy))
        r.raise_for_status()
        data = r.json()
        print(f"      depth 快照 OK: lastUpdateId={data['lastUpdateId']}, "
              f"买一={data['bids'][0]}, 卖一={data['asks'][0]}")
        return True
    except Exception as e:
        print(f"      FAIL: {type(e).__name__}: {str(e)[:160]}")
        return False


async def test_ws(proxy: str | None) -> bool:
    print("[2/2] 测试 WebSocket ...")
    try:
        from websockets.asyncio.client import connect
    except ImportError:
        from websockets import connect

    kwargs = {"open_timeout": 15}
    if proxy:
        kwargs["proxy"] = proxy
    try:
        async with connect(WS_URL, **kwargs) as ws:
            raw = await asyncio.wait_for(ws.recv(), timeout=20)
            msg = json.loads(raw)
            print(f"      收到消息: stream={msg.get('stream')}")
            return True
    except Exception as e:
        print(f"      FAIL: {type(e).__name__}: {str(e)[:160]}")
        return False


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", default=None,
                        help="例如 http://127.0.0.1:7890；不填则用系统/环境代理")
    args = parser.parse_args()

    rest_ok = test_rest(args.proxy)
    ws_ok = asyncio.run(test_ws(args.proxy))

    print("\n=== 结论 ===")
    if rest_ok and ws_ok:
        print("✅ 网络就绪，可以开始录制：python recorder.py --minutes 30")
    elif rest_ok and not ws_ok:
        print("⚠️  REST 通、WS 不通：多半是代理只对 HTTP 生效。")
        print("   请用 --proxy 指定代理地址，例如：")
        print("   python check_network.py --proxy http://127.0.0.1:7890")
    else:
        print("❌ 都连不上：VPN/代理没生效。检查 VPN 是否开启、是否全局模式；")
        print("   或询问代理软件的本地端口，再用 --proxy 指定。")


if __name__ == "__main__":
    main()

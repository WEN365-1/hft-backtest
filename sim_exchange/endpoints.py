"""币安公开行情地址；保持零第三方依赖，便于单独测试订阅规则。"""
from __future__ import annotations


def endpoints(market: str, symbol: str) -> tuple[str, str]:
    """返回合并 WebSocket 流与 REST 深度快照地址。"""
    s = symbol.lower()
    streams = f"{s}@depth@100ms/{s}@aggTrade"
    if market == "spot":
        # 443 与常用 HTTPS 端口一致，网络切换后通常比 9443 更容易恢复。
        return (f"wss://stream.binance.com:443/stream?streams={streams}",
                "https://api.binance.com/api/v3/depth")
    if market == "usdm":
        return (f"wss://fstream.binance.com/public/stream?streams={s}@depth@100ms",
                "https://fapi.binance.com/fapi/v1/depth")
    raise ValueError(f"未知市场: {market!r}")


def websocket_urls(market: str, symbol: str) -> tuple[str, ...]:
    """USD-M 的深度和聚合成交必须分别走 public / market 路由。"""
    primary, _ = endpoints(market, symbol)
    if market == "spot":
        return (primary,)
    return (primary, f"wss://fstream.binance.com/market/stream?streams={symbol.lower()}@aggTrade")

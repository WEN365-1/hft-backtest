"""币安公开行情地址；保持零第三方依赖，便于单独测试订阅规则。"""
from __future__ import annotations


def endpoints(market: str, symbol: str) -> tuple[str, str]:
    """返回合并 WebSocket 流与 REST 深度快照地址。"""
    s = symbol.lower()
    # 实测 USD-M 的 aggTrade 持续无消息，而同主机逐笔 trade 正常；保留相同 E/T/p/q/m。
    trade_stream = "aggTrade" if market == "spot" else "trade"
    streams = f"{s}@depth@100ms/{s}@{trade_stream}"
    if market == "spot":
        # 443 与常用 HTTPS 端口一致，网络切换后通常比 9443 更容易恢复。
        return (f"wss://stream.binance.com:443/stream?streams={streams}",
                "https://api.binance.com/api/v3/depth")
    if market == "usdm":
        return (f"wss://fstream.binance.com/stream?streams={streams}",
                "https://fapi.binance.com/fapi/v1/depth")
    raise ValueError(f"未知市场: {market!r}")

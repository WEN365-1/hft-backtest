"""示例策略：每秒输出中间价、价差、过去一秒主动买卖量差。"""
from __future__ import annotations

from collections import deque

from .storage import replay_time_ms


class OneSecondMetricsStrategy:
    def __init__(self, name: str = "metrics", verbose: bool = True):
        self.name, self.verbose = name, verbose
        self.books: dict[tuple[str, str], dict] = {}
        self.trades: dict[tuple[str, str], deque[tuple[int, float]]] = {}
        self.samples: list[dict] = []
        self.statuses: list[dict] = []
        self._last_second: dict[tuple[str, str], int] = {}

    def on_book(self, event: dict) -> None:
        key = (event["market"], event["symbol"])
        self.books[key] = event["book"]
        self._sample(key, replay_time_ms(event))

    def on_trade(self, event: dict) -> None:
        # m=true 表示买方是 maker，因此是主动卖，记为负量。
        key = (event["market"], event["symbol"])
        qty = float(event["q"])
        self.trades.setdefault(key, deque()).append(
            (replay_time_ms(event), -qty if event["m"] else qty))
        self._sample(key, replay_time_ms(event))

    def on_status(self, event: dict) -> None:
        self.statuses.append(event)

    def _sample(self, key: tuple[str, str], now: int) -> None:
        trades = self.trades.setdefault(key, deque())
        while trades and trades[0][0] < now - 1000:
            trades.popleft()
        second = now // 1000
        book = self.books.get(key)
        if (self._last_second.get(key) == second or not book
                or not book["bids"] or not book["asks"]):
            return
        market, symbol = key
        bid, ask = book["bids"][0][0], book["asks"][0][0]
        sample = {"second": second, "market": market, "symbol": symbol,
                  "mid": (bid + ask) / 2, "spread": ask - bid,
                  "signed_volume_1s": sum(q for _, q in trades)}
        self.samples.append(sample)
        self._last_second[key] = second
        if self.verbose:
            print(f"[{self.name}] {market}.{symbol} mid={sample['mid']:.2f} "
                  f"spread={sample['spread']:.2f} "
                  f"signed_volume_1s={sample['signed_volume_1s']:.4f}")


class BrokenStrategy:
    """测试异常隔离：回调必失败，但不应中断交易所或其他策略。"""
    def on_book(self, event):
        raise RuntimeError("intentional strategy failure")

    def on_trade(self, event):
        raise RuntimeError("intentional strategy failure")

    def on_status(self, event):
        raise RuntimeError("intentional strategy failure")

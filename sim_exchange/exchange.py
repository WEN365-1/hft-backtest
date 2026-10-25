"""模拟交易所：只负责行情与接口预留，不在本课实现下单撮合。"""
from __future__ import annotations

import copy
import logging
from collections import defaultdict
from dataclasses import dataclass, field

from .orderbook import SyncedOrderBook, compare_top

log = logging.getLogger(__name__)


@dataclass
class ValidationStats:
    snapshot_comparisons: int = 0
    exact_matches: int = 0
    gaps: int = 0
    callback_errors: int = 0
    pushed: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def consistency(self) -> float:
        return self.exact_matches / self.snapshot_comparisons if self.snapshot_comparisons else 1.0


class SimExchange:
    def __init__(self, source):
        self.source = source
        self._subscriptions: list[tuple[object, frozenset[str]]] = []
        self._books: dict[tuple[str, str], SyncedOrderBook] = {}
        self._held: dict[tuple[str, str], bool] = defaultdict(bool)
        self.stats = ValidationStats()

    def subscribe(self, strategy, topics: list[str]) -> None:
        self._subscriptions.append((strategy, frozenset(topics)))

    def book(self, market: str, symbol: str, depth: int = 20) -> dict:
        key = (market, symbol)
        if key not in self._books:
            self._books[key] = SyncedOrderBook(market, symbol)
        return self._books[key].top(depth)

    def _notify(self, strategy, callback: str, payload: dict) -> None:
        # deepcopy 防止一个策略篡改其他策略看到的数据；异常只隔离当前策略。
        try:
            getattr(strategy, callback)(copy.deepcopy(payload))
        except Exception:
            self.stats.callback_errors += 1
            log.exception("策略 %r 的 %s 回调失败，已隔离", strategy, callback)

    def _publish(self, topic: str, callback: str, payload: dict) -> None:
        delivered = 0
        for strategy, topics in self._subscriptions:
            if topic in topics:
                self._notify(strategy, callback, payload)
                delivered += 1
        self.stats.pushed[callback] += delivered

    def _status(self, market: str, symbol: str, state: str, reason: str = "") -> None:
        self._publish(f"{market}.{symbol}.status", "on_status", {
            "market": market, "symbol": symbol, "state": state, "reason": reason,
        })

    def run(self) -> ValidationStats:
        for event in self.source.events():
            market, symbol, kind = event["market"], event["symbol"], event["event_type"]
            key = (market, symbol)
            book = self._books.setdefault(key, SyncedOrderBook(market, symbol))
            base_topic = f"{market}.{symbol}"

            if kind == "gap":
                book.invalidate()
                self._held[key] = True
                self.stats.gaps += 1
                self._status(market, symbol, "gap", event.get("reason") or "recorded gap")
                continue

            # 缺页后只接受快照来恢复；中间旧簿与成交都不推给策略。
            if self._held[key] and kind != "snapshot":
                continue

            if kind == "snapshot":
                if book.synced and book.last_update_id:
                    self.stats.snapshot_comparisons += 1
                    self.stats.exact_matches += int(compare_top(book, event, 20))
                was_held = self._held[key]
                book.apply_snapshot(event)
                self._held[key] = False
                if was_held:
                    self._status(market, symbol, "ready", "resynchronized")
                self._publish(f"{base_topic}.book", "on_book", {
                    **event, "book": book.top(20),
                })
            elif kind == "depth":
                result = book.apply_depth(event)
                if result == "gap":
                    self._held[key] = True
                    self.stats.gaps += 1
                    self._status(market, symbol, "gap", "depth sequence gap")
                elif result == "ok":
                    self._publish(f"{base_topic}.book", "on_book", {
                        **event, "book": book.top(20),
                    })
            elif kind == "trade":
                self._publish(f"{base_topic}.trade", "on_trade", event)
            else:
                raise ValueError(f"未知事件类型: {kind}")
        return self.stats

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
    snapshot_skipped_stale: int = 0
    snapshot_skipped_crossed: int = 0
    snapshot_pending: int = 0
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
        self._pending: dict[tuple[str, str], list[dict]] = defaultdict(list)
        self._snapshot_checks: dict[tuple[str, str], list[dict]] = defaultdict(list)
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

    def _publish_book(self, base_topic: str, event: dict, book: SyncedOrderBook) -> None:
        topic = f"{base_topic}.book"
        # 核验常无订阅者；此时避免对每条 depth 排序整本簿并构造无用载荷。
        if any(topic in topics for _, topics in self._subscriptions):
            self._publish(topic, "on_book", {**event, "book": book.top(20)})

    def _compare_snapshot(self, book: SyncedOrderBook, snapshot: dict) -> None:
        # 只有同一更新编号才代表同一市场状态，不能直接比较任意到达时刻的簿。
        assert book.last_update_id == int(snapshot["last_update_id"])
        self.stats.snapshot_comparisons += 1
        self.stats.exact_matches += int(compare_top(book, snapshot, 20))

    def _check_pending_snapshots(self, key, book: SyncedOrderBook) -> None:
        waiting = []
        for snapshot in self._snapshot_checks[key]:
            snapshot_id = int(snapshot["last_update_id"])
            if book.last_update_id < snapshot_id:
                waiting.append(snapshot)
            elif book.last_update_id == snapshot_id:
                self._compare_snapshot(book, snapshot)
            else:
                # 一个聚合 depth 跨过了快照编号，无法拆出事件中间状态，不能假装可比。
                self.stats.snapshot_skipped_crossed += 1
        self._snapshot_checks[key] = waiting

    def run(self) -> ValidationStats:
        for event in self.source.events():
            market, symbol, kind = event["market"], event["symbol"], event["event_type"]
            key = (market, symbol)
            book = self._books.setdefault(key, SyncedOrderBook(market, symbol))
            base_topic = f"{market}.{symbol}"

            if kind == "gap":
                book.invalidate()
                self._pending[key].clear()
                self._snapshot_checks[key].clear()
                self._held[key] = True
                self.stats.gaps += 1
                self._status(market, symbol, "gap", event.get("reason") or "recorded gap")
                continue

            # 缺页后暂停对外推送，但缓存增量供下一张快照衔接。
            if self._held[key] and kind != "snapshot":
                if kind == "depth":
                    self._pending[key].append(event)
                continue

            if kind == "snapshot":
                if book.synced and book.last_update_id:
                    snapshot_id = int(event["last_update_id"])
                    if book.last_update_id == snapshot_id:
                        self._compare_snapshot(book, event)
                    elif book.last_update_id < snapshot_id:
                        self._snapshot_checks[key].append(event)
                    else:
                        self.stats.snapshot_skipped_stale += 1
                    # 健康簿的周期快照只用于体检；不能用可能较旧的快照倒退重置。
                    self._publish_book(base_topic, event, book)
                    continue
                was_held = self._held[key]
                book.apply_snapshot(event)
                self._held[key] = False
                # WS 先于 REST 返回的事件在此衔接，不能因尚未快照而丢弃。
                pending = sorted(self._pending.pop(key, []),
                                 key=lambda x: (x["recv_ts_ms"], x.get("seq") or 0))
                for index, buffered in enumerate(pending):
                    result = book.apply_depth(buffered)
                    if result == "gap":
                        self._held[key] = True
                        self._pending[key] = pending[index:]
                        self.stats.gaps += 1
                        self._status(market, symbol, "gap", "initial snapshot mismatch")
                        break
                if was_held and not self._held[key]:
                    self._status(market, symbol, "ready", "resynchronized")
                if not self._held[key]:
                    self._publish_book(base_topic, event, book)
            elif kind == "depth":
                if not book.synced:
                    self._pending[key].append(event)
                    continue
                result = book.apply_depth(event)
                if result == "gap":
                    self._held[key] = True
                    self._pending[key] = [event]
                    self.stats.gaps += 1
                    self._status(market, symbol, "gap", "depth sequence gap")
                elif result == "ok":
                    self._check_pending_snapshots(key, book)
                    self._publish_book(base_topic, event, book)
            elif kind == "trade":
                self._publish(f"{base_topic}.trade", "on_trade", event)
            else:
                raise ValueError(f"未知事件类型: {kind}")
        self.stats.snapshot_pending = sum(len(x) for x in self._snapshot_checks.values())
        return self.stats

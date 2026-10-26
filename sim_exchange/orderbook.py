"""现货与 U 本位 L2 本地订单簿，严格区分两套增量同步规则。"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any


def levels(value: Any) -> list[tuple[float, float]]:
    if isinstance(value, str):
        value = json.loads(value)
    return [(float(p), float(q)) for p, q in (value or [])]


@dataclass
class SyncedOrderBook:
    market: str
    symbol: str
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_update_id: int = 0
    synced: bool = False
    awaiting_first: bool = False

    def __post_init__(self) -> None:
        if self.market not in {"spot", "usdm"}:
            raise ValueError("market 必须是 spot 或 usdm")

    def apply_snapshot(self, event: dict) -> None:
        self.bids = {p: q for p, q in levels(event.get("bids")) if q > 0}
        self.asks = {p: q for p, q in levels(event.get("asks")) if q > 0}
        self.last_update_id = int(event.get("last_update_id") or event.get("u") or 0)
        self.synced = True
        self.awaiting_first = True

    def invalidate(self) -> None:
        self.synced = False
        self.awaiting_first = False

    def apply_depth(self, event: dict) -> str:
        """返回 ok/stale/gap/unsynced；gap 后绝不继续推送旧订单簿。"""
        if not self.synced:
            return "unsynced"
        first, final = int(event["U"]), int(event["u"])
        last = self.last_update_id
        stale = final < last or (not self.awaiting_first
                                 and self.market == "spot" and final <= last)
        if stale:
            return "stale"
        if self.awaiting_first:
            # 两市场首条衔接事件都必须覆盖快照 lastUpdateId；此步不看 pu。
            continuous = first <= last <= final
        elif self.market == "spot":
            # 现货后续事件允许 last+1 落在 [U,u] 内。
            continuous = first <= last + 1 <= final
        else:
            # U 本位后续事件用 pu 串联上一事件最终编号。
            continuous = int(event["pu"]) == last
        if not continuous:
            self.invalidate()
            return "gap"
        for p, q in levels(event.get("bids", event.get("b"))):
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        for p, q in levels(event.get("asks", event.get("a"))):
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q
        self.last_update_id = final
        self.awaiting_first = False
        return "ok"

    def top(self, depth: int = 20) -> dict:
        return {
            "market": self.market,
            "symbol": self.symbol,
            "last_update_id": self.last_update_id,
            "synced": self.synced,
            "bids": sorted(self.bids.items(), reverse=True)[:depth],
            "asks": sorted(self.asks.items())[:depth],
        }

    def mid_spread(self) -> tuple[float | None, float | None]:
        if not self.bids or not self.asks:
            return None, None
        bid, ask = max(self.bids), min(self.asks)
        return (bid + ask) / 2, ask - bid


def compare_top(book: SyncedOrderBook, snapshot: dict, depth: int = 20) -> bool:
    top = book.top(depth)
    return (top["bids"] == levels(snapshot["bids"])[:depth]
            and top["asks"] == levels(snapshot["asks"])[:depth])

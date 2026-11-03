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
    known_bid_floor: float | None = None
    known_ask_ceiling: float | None = None

    def __post_init__(self) -> None:
        if self.market not in {"spot", "usdm"}:
            raise ValueError("market 必须是 spot 或 usdm")

    def apply_snapshot(self, event: dict) -> None:
        self.bids = {p: q for p, q in levels(event.get("bids")) if q > 0}
        self.asks = {p: q for p, q in levels(event.get("asks")) if q > 0}
        self.last_update_id = int(event.get("last_update_id") or event.get("u") or 0)
        self.known_bid_floor = min(self.bids, default=None)
        self.known_ask_ceiling = max(self.asks, default=None)
        self.synced = True
        self.awaiting_first = True

    def extend_snapshot_coverage(self, event: dict) -> int:
        """同编号核验完成后，扩充初始快照范围外的未知价位；不改已知范围。"""
        if not self.synced or self.last_update_id != int(event["last_update_id"]):
            raise ValueError("只能用同编号快照扩充覆盖范围")
        added = 0
        for side, boundary_name, is_bid in (
                ("bids", "known_bid_floor", True),
                ("asks", "known_ask_ceiling", False)):
            boundary = getattr(self, boundary_name)
            snapshot_levels = [(p, q) for p, q in levels(event.get(side)) if q > 0]
            local = getattr(self, side)
            for price, qty in snapshot_levels:
                outside = boundary is None or (price < boundary if is_bid else price > boundary)
                if outside and price not in local:
                    local[price] = qty
                    added += 1
            if snapshot_levels:
                edge = (min if is_bid else max)(p for p, _ in snapshot_levels)
                setattr(self, boundary_name, edge if boundary is None else
                        (min if is_bid else max)(boundary, edge))
        return added

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

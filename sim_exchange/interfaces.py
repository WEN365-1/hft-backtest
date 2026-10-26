"""下一课接口契约：本课只定义数据结构，不提前伪造撮合与账户功能。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Protocol


class MarketDataSource(Protocol):
    def events(self) -> Iterator[dict]: ...


class Strategy(Protocol):
    def on_book(self, event: dict) -> None: ...
    def on_trade(self, event: dict) -> None: ...
    def on_status(self, event: dict) -> None: ...


@dataclass(frozen=True)
class OrderRequest:
    market: str
    symbol: str
    side: str                 # BUY / SELL
    order_type: str           # MARKET / LIMIT
    quantity: float
    price: float | None = None
    client_order_id: str = ""
    post_only: bool = False


@dataclass(frozen=True)
class RiskDecision:
    accepted: bool
    reason: str = ""


@dataclass(frozen=True)
class OrderAck:
    client_order_id: str
    status: str               # NEW / PARTIAL / FILLED / CANCELED / REJECTED
    filled_quantity: float
    average_price: float | None
    exchange_ts_ms: int


@dataclass(frozen=True)
class AccountSnapshot:
    cash: float
    available_cash: float
    position: float
    average_entry_price: float | None
    realized_pnl: float

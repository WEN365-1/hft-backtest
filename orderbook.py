"""
本地订单簿：把第 6 课的"官方 7 步同步流程"写成代码。

核心思想（漫画比喻）：
  - apply_snapshot = 买一套全集（REST 深度快照），记下 lastUpdateId（期号）
  - apply_diff     = 拆一个包裹（WS 增量事件），按 U/u 检查有没有缺页
  - qty == 0       = 这个价位从簿里消失（被吃光或被撤单），删掉
"""

from __future__ import annotations


class LocalOrderBook:
    """维护一份和交易所一致的 BTCUSDT 订单簿（L2）。"""

    def __init__(self):
        self.bids: dict[float, float] = {}   # 价格 -> 挂单量
        self.asks: dict[float, float] = {}
        self.last_update_id: int = 0         # 当前账本"到第几期"

    # ---------- 同步：官方 7 步中的第 5 步 ----------
    def apply_snapshot(self, snap: dict) -> None:
        """用 REST 快照重建本地簿。snap 里有 lastUpdateId/bids/asks。"""
        self.bids = {float(p): float(q) for p, q in snap["bids"] if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in snap["asks"] if float(q) > 0}
        self.last_update_id = int(snap["lastUpdateId"])

    # ---------- 同步：官方 7 步中的第 6、7 步 ----------
    def apply_diff(self, ev: dict) -> str:
        """
        应用一条增量事件。
        返回：
          "stale" = 旧包裹（u <= 当前期号），忽略
          "gap"   = 缺页（U > 当前期号 + 1），调用方必须重同步
          "ok"    = 正常贴上
        """
        U, u = int(ev["U"]), int(ev["u"])

        if u <= self.last_update_id:
            return "stale"
        if U > self.last_update_id + 1:
            return "gap"

        # b = bids 增量, a = asks 增量；qty=0 表示删掉该价位
        for price, qty in ev["b"]:
            price, qty = float(price), float(qty)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for price, qty in ev["a"]:
            price, qty = float(price), float(qty)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.last_update_id = u
        return "ok"

    # ---------- 行情指标（第 3 课） ----------
    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    def mid_price(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2

    def spread_bp(self) -> float | None:
        bb, ba = self.best_bid(), self.best_ask()
        mid = self.mid_price()
        if bb is None or ba is None or not mid:
            return None
        return (ba - bb) / mid * 10000

    def imbalance(self, levels: int = 3) -> float | None:
        """
        前 N 档不平衡度 = (买量 - 卖量) / (买量 + 卖量)，范围 [-1, 1]。
        这就是策略的信号来源。
        """
        if len(self.bids) < levels or len(self.asks) < levels:
            return None
        top_bids = sorted(self.bids.items(), key=lambda x: -x[0])[:levels]
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:levels]
        bid_vol = sum(q for _, q in top_bids)
        ask_vol = sum(q for _, q in top_asks)
        if bid_vol + ask_vol == 0:
            return None
        return (bid_vol - ask_vol) / (bid_vol + ask_vol)

    # ---------- 市价成交模拟：走档（第 3 课） ----------
    def walk_buy(self, qty: float) -> tuple[float, float]:
        """市价买入 qty 个：从最低卖价开始吃，返回 (成交均价, 实际成交量)。"""
        remaining, cost, filled = qty, 0.0, 0.0
        for price, avail in sorted(self.asks.items()):
            take = min(remaining, avail)
            cost += take * price
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break
        return (cost / filled if filled > 0 else 0.0, filled)

    def walk_sell(self, qty: float) -> tuple[float, float]:
        """市价卖出 qty 个：从最高买价开始吃，返回 (成交均价, 实际成交量)。"""
        remaining, proceeds, filled = qty, 0.0, 0.0
        for price, avail in sorted(self.bids.items(), key=lambda x: -x[0]):
            take = min(remaining, avail)
            proceeds += take * price
            filled += take
            remaining -= take
            if remaining <= 1e-12:
                break
        return (proceeds / filled if filled > 0 else 0.0, filled)

    def top_levels(self, n: int = 20) -> dict:
        """取前 n 档，用于和 REST 快照比对体检。"""
        bids = sorted(self.bids.items(), key=lambda x: -x[0])[:n]
        asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return {"bids": bids, "asks": asks}


def compare_books(local: LocalOrderBook, snap: dict, n: int = 20) -> int:
    """和 REST 快照比对前 n 档，返回不一致的档位数（0 = 完全一致）。"""
    diff = 0
    local_top = local.top_levels(n)
    for side in ("bids", "asks"):
        local_map = dict(local_top[side])
        snap_map = {float(p): float(q) for p, q in snap[side][:n]}
        for price in set(local_map) | set(snap_map):
            if abs(local_map.get(price, 0.0) - snap_map.get(price, 0.0)) > 1e-12:
                diff += 1
    return diff

# 第二课：模拟交易所与成交模拟笔记

> 范围：本课交付行情采集、存储、回放、订单簿和订阅推送；下单、撮合、风控、账户只定义接口。演示结果来自合成数据，不能冒充真实 24 小时录制。

## 1. 本课做了什么

- `record_market.py`：现货与 U 本位 WebSocket 采集 `depth@100ms`、`aggTrade`；保存 `E`、`T`、`recv_ts_ms`；分钟快照；断线写 `gap`，指数退避重连并重新快照。
- `sim_exchange/storage.py`：统一事件 Schema，Parquet + ZSTD；按 `market/symbol/date=YYYY-MM-DD/hour=HH` 分区；UUID 批文件追加，不覆盖。
- `sim_exchange/sources.py`：PyArrow 分批读取 + k 路归并，按本地接收时间输出；内存不随总数据时长线性增长。
- `sim_exchange/orderbook.py`：现货检查 `U <= last+1 <= u`，U 本位检查 `pu == last`；数量 0 删除价位。
- `sim_exchange/exchange.py`：缺页通知、暂停、快照恢复；多策略订阅、深拷贝隔离、回调异常隔离；快照前 20 档一致率统计。
- `sim_exchange/interfaces.py`：预留 `OrderRequest`、`RiskDecision`、`OrderAck`、`AccountSnapshot`。
- `demo_sim_exchange.py`：可重复的双市场、双策略、断线恢复演示。
- `tests/`：关键逻辑单元测试。

## 2. 市价单和限价单如何判断成交

### 市价单 / 吃单（liquidity-taking）

真实交易所按价格优先、时间优先，从对手方最优档逐档吃，直到数量完成或流动性耗尽；可能部分成交，均价包含走档滑点。我们的下一版应使用当前可见 L2 深度走档，并限制不能超过可见深度；不能默认任意数量都在卖一/买一全部成交。

hftbacktest 的 `NoPartialFillExchange` 对吃单直接按最优价全成，`PartialFillExchange` 会按订单簿数量执行；文档特别提醒，回放不会改变市场，大单即使数量成交也可能不现实，因为未模拟自身市场冲击。

### 限价单 / 挂单（liquidity-making）

- 可立即成交：买单限价 ≥ 卖一，卖单限价 ≤ 买一，先按吃单规则处理。
- 被动成交：价格被穿过通常可判成交；价格仅被触及时，必须结合排队位置与该价位主动成交量。买单由主动卖成交，卖单由主动买成交。
- hftbacktest 的不部分成交模型要求到队首后整单成交；部分成交模型允许在队首按剩余成交量分批成交。

## 3. 只有 L2 时如何估计排队位置

新限价单到达某价位时，假设当时该价位已有总量都排在前面：`queue_ahead = displayed_qty_at_price`。之后：

1. 同价位真实成交优先扣减 `queue_ahead`，再扣完才轮到我的订单；
2. 同价位数量减少可能来自撤单/改价，也可能来自成交，需避免重复扣减；
3. 新增量排在我后面，不改变我的前方队列；
4. L2 没有订单 ID，无法知道减量发生在队首、中间还是队尾，所以只能建模，不能“精确还原”。

撤单假设：

| 模型 | 同价位减量发生在哪里 | 对成交概率的影响 | 用途 |
|---|---|---|---|
| RiskAverse / 保守队尾撤单 | 全部在我后面 | 撤单不让我前进，只有成交让我前进；成交最少 | 压力情景、下界 |
| 队首撤单 | 全部在我前面 | 撤单大幅让我前进；成交最多，通常过于乐观 | 乐观边界 |
| ProbQueue / 概率撤单 | 按当前相对队列位置分配前后撤单概率 | 介于两者之间 | 基准情景 |
| Power / Log 变体 | 用不同函数描述相对位置与撤单概率 | 改变队列前进速度 | 敏感性分析 |

应至少报“保守 / 概率 / 乐观”三档结果，并用实盘小单的等待时间、成交率校准，而不是挑收益最高的一档。

## 4. 行情延迟和下单延迟

- **行情延迟（feed latency）**：交易所发送事件到本地收到的差。策略看到的是旧市场；若用交易所事件时间直接决策，会引入前视。
- **下单进入延迟（order entry latency）**：本地发单到撮合引擎处理的差。订单更晚到达，市价成交价格变化，限价单位置更靠后，撤单也可能来不及。
- **订单响应延迟（order response latency）**：撮合引擎处理到本地收到回报的差。策略更晚知道成交/持仓，可能重复下单或延迟风控。
- 延迟越大通常越不利，但不能机械宣称每次收益都下降：随机市场中单次噪声可正可负，应比较多时段分布。官方教程中不同延迟假设确实会显著改变同一策略的收益、夏普和回撤。

## 5. 我们撮合模块的计划（伪代码）

```text
on_order_request(req, local_ts):
    risk = risk_engine.check(req, account, current_book)
    if not risk.accepted: return reject(risk.reason)
    arrival_ts = local_ts + sample_order_entry_latency()
    schedule(arrival_ts, req)

on_exchange_time(now):
    activate orders whose arrival_ts <= now       # 未到交易所的订单不得成交
    for order in activated_orders:
        if marketable(order):
            fills = walk_visible_book(order)       # 分档、限量、可部分成交
        else:
            order.queue_ahead = current_qty(order.price)
            order.state = RESTING

on_trade(trade):                                   # 只处理已激活的挂单
    for order at trade.price in time_priority:
        consumed = min(order.queue_ahead, trade.remaining_qty)
        order.queue_ahead -= consumed
        trade.remaining_qty -= consumed
        if order.queue_ahead == 0:
            fill = min(order.remaining_qty, trade.remaining_qty)
            emit_partial_fill(fill)

on_depth(delta):
    estimate_cancel_before_order(queue_model, delta)  # 不与 trade 重复扣量
    update_book(delta)

on_cancel(req):
    schedule_cancel_with_latency(req)               # 到交易所前仍可能成交

on_fill(fill):
    apply_taker_or_maker_fee()
    update_order_manager_and_account_atomically()
    schedule_fill_response(response_latency)
```

下一课测试重点：价格时间优先、跨档、部分成交、排队、撤单与成交竞争、延迟窗口、手续费、账户原子更新、自身成交不参与市场冲击的边界。

## 6. 演示验证口径

- 快照一致率 = 回放中每次新快照到达前，本地簿与该快照前 20 档完全一致的次数 / 有效比较次数。
- 快照到达前的比较才有意义；重置后再比会恒等于 100%。
- 快照与增量存在毫秒级错位，真实数据不必强求 100%；低于 95% 应排查丢包、时间对齐与同步规则。
- 双策略一致性要比较完整事件/样本序列，不能只比较最后一条。

## 7. 参考资料

- hftbacktest Order Fill: https://hftbacktest.readthedocs.io/en/latest/order_fill.html
- hftbacktest Latency Models: https://hftbacktest.readthedocs.io/en/latest/latency_models.html
- hftbacktest Impact of Order Latency: https://hftbacktest.readthedocs.io/en/latest/tutorials/Impact%20of%20Order%20Latency.html
- Binance Spot WebSocket / REST API: https://developers.binance.com/docs/binance-spot-api-docs/
- Binance USD-M Futures API: https://developers.binance.com/docs/derivatives/usds-margined-futures/

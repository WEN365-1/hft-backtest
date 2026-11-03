# 高频回测成交模拟学习笔记

我阅读了hftbacktest的Order Fill和Impact of Order Latency，按作业中的三个问题整理如下理解，最后给出我对下一步撮合模块的设计。本文件是学习笔记与伪代码，撮合执行尚未实现。

## 一 市价单和限价单分别怎么判断成交

### 市价单和可立即成交的限价单

我将这类订单按主动成交处理：买单从最低卖价开始，卖单从最高买价开始，逐档消耗对手方数量，直到订单完成或可见深度用完。成交均价可能因跨档而偏离最优报价。

限价单也可能主动成交：买价达到或超过卖一、卖价达到或低于买一时，应先按主动成交处理；不能把所有限价单都看成被动挂单。

hftbacktest不允许部分成交的模型对主动吃单按最优价全成；部分成交模型会考虑盘口数量。我需要注意，两者基于历史行情回放，模拟订单不会真的改变后续市场，所以大单成交和自身市场冲击仍有局限。

### 被动挂着的限价单

我不能只凭“市场碰到限价”就判整单成交。同一价格可能有更早到达的订单排在前面，主动成交量必须先消耗前方队列，才轮到我的订单。

我的判断顺序是：订单已经到达交易所 → 价格与主动成交方向合适 → 前方队列消耗完 → 按剩余成交量处理全部或部分成交。买单只由主动卖成交，卖单只由主动买成交。

## 二 只有L2数据时怎么估计排队位置

L2只给价格对应的总数量，没有逐笔委托编号和精确排队顺序。我只能建立队列模型，不能声称精确还原。

订单到达时，我准备把该价位已有的可见数量作为前方队列量queue_ahead。后续同价位主动成交先扣前方数量，再轮到我的订单；后加的挂单排在我的后面。

数量减少也可能来自撤单，因此需要区分成交与撤单，避免同一减量被重复扣除。不同撤单假设如下：

| 假设 | 对前方队列的处理 | 对成交判断的影响 |
|---|---|---|
| 保守队尾撤单 | 撤单都发生在我后面，前方只因真实成交减少 | 成交估计较少，可作保守情景。 |
| 队首撤单 | 撤单都发生在我前面 | 前进较快，是偏乐观的边界。 |
| 概率撤单 | 根据相对排队位置，把撤单分配到前后 | 介于两者之间，需要校准概率函数。 |

我准备同时比较保守、概率和乐观情景，再用实际等待时间和成交率校准，而不是只采用收益最高的假设。

## 三 行情延迟和下单延迟对结果有什么影响

| 延迟 | 我的理解 | 对模拟的影响 |
|---|---|---|
| 行情延迟 | 交易所产生消息到本地收到消息的时间 | 策略使用的是较旧市场；不能提前按尚未收到的数据决策。 |
| 下单进入延迟 | 本地发单到交易所处理的时间 | 到达价格可能改变，限价单排队更晚，撤单在途仍可能成交。 |
| 响应延迟 | 交易所处理到本地收到回报的时间 | 策略更晚知道成交和持仓，影响后续下单与风险判断。 |

我的设计是分别建模这三种延迟。订单未到交易所不能成交，撤单未到交易所不能立即生效，成交回报未到本地也不能让策略提前知道结果。

延迟会改变成交机会与风险，但不能保证每一笔收益都单调下降；我需要比较多时段结果和压力情景。

## 四 我对下一步撮合模块的设计

我准备按“风控检查 → 安排到达时间 → 激活订单 → 主动成交或进入队列 → 成交与撤单竞争 → 更新账户并延迟回报”的顺序实现。

```text
on_order_request(req, local_ts):
    risk = risk_engine.check(req, account, current_book)
    if not risk.accepted: return reject(risk.reason)
    arrival_ts = local_ts + sample_order_entry_latency()
    schedule(arrival_ts, req)

on_exchange_time(now):
    activate orders whose arrival_ts <= now       # 未到交易所的订单不得成交
    for order in newly_activated_orders:
        if marketable(order):
            fills = walk_visible_book(order)       # 分档、限量、可部分成交
        else:
            order.queue_ahead = current_qty(order.price)
            order.state = RESTING

on_trade(trade):                                   # 只处理已激活的挂单
    for order at trade.price in time_priority:
        if not ((order.side == BUY and trade.aggressor_side == SELL)
                or (order.side == SELL and trade.aggressor_side == BUY)):
            continue                              # 仅处理主动方与挂单相反的成交
        consumed = min(order.queue_ahead, trade.remaining_qty)
        order.queue_ahead -= consumed
        trade.remaining_qty -= consumed
        if order.queue_ahead == 0:
            fill = min(order.remaining_qty, trade.remaining_qty)
            order.remaining_qty -= fill
            trade.remaining_qty -= fill
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

这段伪代码只描述设计顺序。下一步我需要重点验证价格时间优先、跨档、部分成交、多个本方订单的队列共享、成交与深度更新去重、撤单竞争、手续费与账户原子更新。

## 参考资料

- [hftbacktest Order Fill](https://hftbacktest.readthedocs.io/en/latest/order_fill.html)
- [hftbacktest Impact of Order Latency](https://hftbacktest.readthedocs.io/en/latest/tutorials/Impact%20of%20Order%20Latency.html)
- [hftbacktest Latency Models](https://hftbacktest.readthedocs.io/en/latest/latency_models.html)

## 备注

行情模块的最终结果、代码入口和验证证据统一列在[作业成果说明](SUBMISSION_README.md)；版本修复和历史基线记录放在PR复核文件中，不与这份概念学习笔记混写。

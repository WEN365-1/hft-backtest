# 作业二提交前复核

复核日期：2026-11-03（Asia/Shanghai）。结论：离线工程测试和同批历史数据复核通过，可供导师审阅；下述历史粒度偏离、断连范围和现网限制必须一并披露。

## 当前可复查结果

| 项目 | 本次结果 | 证据 |
|---|---|---|
| 测试 | 28 passed | output/lesson2/submission_test_results.txt |
| 真实接收范围 | 2026-10-28 01:10:51.944 至 10-29 01:10:58.172；24.00173小时 | real_24h_validation.json 首尾毫秒时间 |
| 六个必需频道 | 各实际接收覆盖至少23.99小时，无补录 | channel_coverage / trade_source_counts |
| 同编号前20档比较 | 1457/1457，100% | final_acceptance.json |
| 分市场 | 现货1426/1426；U本位31/31；分别检验至少95% | consistency_by_market |
| 跨过/待比较 | 1393张跨过、2张待比较，均不计分母 | snapshot_skipped_crossed / snapshot_pending |
| 原始基线 | 1455/1457，约99.86%，完整保留 | output/lesson2/baseline_20261103/ |
| 覆盖补全 | 同编号比较后补入3396个原快照边界外价位 | coverage_levels_added |
| gap口径 | 录制10条（两市场各5），另检出1次编号连续性缺口，总11 | gap_counts / gap_details |
| 真实断连 | 历史独立WS连接主动关闭后重连；当前代码重放该数据仍同步 | controlled_disconnect_validation.json / submission_disconnect_replay.json |
| 合成演示 | 前20档2/2、双策略样本一致、每策略302样本、2次gap恢复 | demo_validation.json |
| 本次实时短测 | 未通过：REST快照HTTP451 | submission_live_smoke.json |

真实24小时是既有录制批次，不是本次重新录满24小时；HTTP451不被写成成功。新机器若没有原始Parquet，只能复查提供的报告和运行测试/合成演示，不能凭空复现真实全量结果。

## 已处理的问题

1. 有限快照覆盖：两次旧差异分别缺4、8个卖价，全部在重建1000档快照之外，之后无相应已录增量。当前先完成同编号比较并记账，再仅补充原已知边界外价位；不重置健康簿，不覆盖已知范围的错量和缺失。测试特意验证第一次差异仍须记失败、未来快照不得提前引入、已知错误不得被快照掩盖。
2. 数据统计：不再把零次比较报告成100%；按市场分别检验一致率；输出差异详情、覆盖补充数和分类型gap计数。
3. 采集路由：官方已将U本位深度与聚合成交分到/public和/market；新代码使用双连接，并由接收队列保留实际到达顺序；录制与实时演示复用同一采集实现。现货新快照取5000档，U本位1000档。新连接离线测试通过；独立现网WS检查的深度、aggTrade各收到3条真实消息，核心字段齐全。完整实时源仍受REST快照HTTP451限制。
4. 首条边界：保留当前现货保守初始化政策U<=last<=u；U=last+1时重取快照，后续才检查U<=last+1<=u。该行为与数学上可连续的边界分开陈述，不再直接宣布为确定漏洞。U本位后续pu不等不允许区间兜底。
5. 文档：统一测试数量、真实数据首尾时间、原基线与新结果；删除同编号差异由毫秒错位解释的无证据结论；标明按订阅公平、普通异常隔离和事件触发采样的边界。
6. 笔记与图：撮合计划只初始化新激活订单的队列，补上成交余量扣减；图中成交/状态由模拟交易所推送，策略目前只计算指标，PnL/执行等后续内容明确标注。

## 仍需主动披露的边界

- 历史v2录制的U本位成交是更细的逐笔trade，而作业原文要求aggTrade；不能把旧数据改称aggTrade。当前新采集器已按官方路由使用aggTrade，但尚未取得新的24小时该口径录制。
- 真实主动断连验证为WebSocket连接级，不是整机断网。若导师按字面要求整机断网实验，仍需补做。
- 新实时连接在本机重测遇HTTP451，不能宣称本次新现网联调成功；历史录制及当前离线结果分别保留。
- 有限深度即使扩大到5000档也不是全市场覆盖保证；跨过编号的快照无法比较，U本位有效比较仅31次，应同时给出分母。
- 采样是每市场/交易对每秒最多一次，事件触发，非秒末汇总；无事件不输出。示例状态回调只记录，缺口内成交不能靠快照补回。
- 回调捕获普通异常，不提供进程/超时隔离；慢回调或死循环仍可能阻塞。
- 下单、撮合、账户和风控只有接口及设计，没有真实执行实现。

## 官方核对来源

- 币安现货同步与有限快照范围：https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly
- U本位同步：https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly
- U本位分路由公告：https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice
- hftbacktest成交模型：https://hftbacktest.readthedocs.io/en/latest/order_fill.html
- 延迟教程：https://hftbacktest.readthedocs.io/en/latest/tutorials/Impact%20of%20Order%20Latency.html

## 追加交付核验

真实前60秒双策略各收到3624次回调、各形成121条样本，全部回调内容指纹和全部样本一致；故意修改数据并抛出一次异常的第三策略不影响它们。证据为 `submission_two_strategies.json`。原始批次5772个Parquet、542709834字节，清单指纹记录于 `submission_audit.json`，原始文件不进提交。源码证据指纹按UTF-8文本统一LF计算，兼容Windows检出换行。

## Grok并行审核的后续核对

详见 `GROK_AUDIT_RECONCILIATION.md`。原7个未提交改动已提交推送；当前28项测试通过。补测中，独立/public深度和/market aggTrade通道各收3条真实消息，见 `submission_websocket_channels.json`。使用本机已有代理进行完整实时源尝试仍遇REST HTTP451，见 `submission_live_proxy_smoke.json`。因此：接口实现通过、WS通道短测通过、完整实时源未通过、新aggTrade口径24小时未做。不能混为“新采集已全部验收”。

# 作业二当前提交复核

复核日期：2026-11-03。完整记录保留在PR；老师包以SUBMISSION_README.md介绍成果和边界。独立审核原件保持原样，c4b171b及以前的结论是对应版本的历史快照，不作为当前代码的唯一说明。

## 当前验证结果

| 项目 | 结果与范围 | 证据 |
|---|---|---|
| 测试 | 33 passed | submission_test_results.txt |
| 既有真实24小时单遍复算 | 1457次同编号有效比较全部一致；另1393张跨过、2张待比较不计分母，U本位31次 | real_24h_validation.json |
| 分市场 | 现货1426/1426，U本位31/31 | consistency_by_market |
| 优化前后对账 | 29个数据字段完全相同 | submission_replay_regression.json |
| 修复前基线 | 1,455/1,457≈99.86%单列归档，不改历史数据 | baseline_20261103/ |
| 原始文件 | 5772个、542709834字节，逐文件及聚合指纹重算匹配；算法已公开 | fingerprint_data.py / submission_audit.json |
| 现货完整实时演示 | 官方.vision，204次成交回调、126次簿回调、14样本，最终同步，gap=0、异常0 | submission_spot_vision_live.json |
| 现货当前主动断连 | 真正关闭WS，1次gap；重连后2快照、201 depth、148 trade，最终同步 | submission_spot_controlled_disconnect.json |
| U本位通道 | /public深度、/market aggTrade各3条真实消息、字段齐全 | submission_websocket_channels.json |
| U本位完整实时源 | 直连与代理REST快照仍451，未通过 | submission_live_smoke.json / submission_live_proxy_smoke.json |
| 双策略真实60秒 | 各3624次回调、121样本，完整内容指纹和样本相同；篡改并报错第三策略未影响它们 | submission_two_strategies.json |
| 合成演示 | 2/2比较、2次gap恢复，两策略各302样本 | demo_validation.json |

真实24小时接收范围为2026-10-28 01:10:51.944至10-29 01:10:58.172（北京时间），六个必需频道各至少23.99小时、无补录。终点两市场同步。抽查全匹配不能称全时刻准确率100%。

## 独立复核意见处理

1. 文案：旧1455/99.86%均标为修复前基线；当前结果带有效分母、跨过/待比较数量与U本位31次。内部Q&A改成“历史v2是trade、当前为何aggTrade”。
2. 交付分层：讲义移到仓库外_内部材料_不交；老师包用白名单，不含答辩卡、第一课资产或完整内部复核。完整复核仍留PR，成果导航不以审核为首项。
3. 交接：正文中的固定旧HEAD、20/28项、历史成交粒度和学习进度已更新，历史交接另存。
4. 网络：现货改官方.vision端点并实际跑通；U本位只确认WS通道，REST451与新24小时未做继续披露。
5. 采集：代理支持CLI与BINANCE_PROXY；自动化入口只允许独立空目录；快照统一委托同一实现。
6. 验证性能：计数和模拟器共享一次流；capture采完只调用一次finalize完整核验，29字段不变。
7. 指纹：公开原166fea聚合值的精确NUL/LF编码算法与可复算脚本，重新对5772个文件验证。
8. 撮合计划：买单仅由主动卖成交、卖单仅由主动买成交，显式过滤；仍是设计而非已实现撮合。
9. 集成修正：采集改为复用实时源后，主动断连脚本的连接注入点也改到真实构造器，实网重测通过。

## 保留边界

历史U本位24小时是逐笔trade，不改名、不伪造新的aggTrade订阅记录；当前没有新的aggTrade口径24小时采集。本机U本位REST451未解决。

既有原始录制证明两市场连接/快照请求异常并恢复，各约72秒取得新快照，全市场约79.285秒；这不是整机断网证明。没有执行人工关闭整机网络。详见docs/NETWORK_RECOVERY_TIMELINE.md。

有限深度、回调超时隔离缺失、分钟缓冲损失和事件触发采样边界见使用说明。下单、撮合、风控、账户仍是接口/设计。

## 证据一致性与独立复算

verify_submission.py仅核对报告、固定历史批次断言与源码指纹的一致性；不能代替validate_replay.py读取原始Parquet。当前完整真实回放已经重新执行，结果与独立审核的旧版本29个数据字段相同；第三方仍需原始数据才能独立复算。

官方端点依据：https://developers.binance.com/en/docs/products/spot/faqs/market_data_only

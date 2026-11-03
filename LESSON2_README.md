# 作业二使用说明

本课实现行情采集、分区存储、流式回放、本地订单簿和策略订阅。下单、撮合、风控、账户只有接口和设计，不是已实现执行模块。老师包只包含第二课成果；仓库根目录的orderbook.py、backtester.py等为第一课资产，第二课请看sim_exchange/。

## 安装与离线运行

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest -q
.venv\Scripts\python verify_submission.py
.venv\Scripts\python demo_sim_exchange.py --data-root data/lesson2_demo_review
```

当前33项测试通过。合成演示前20档比较2/2，两次gap恢复，两个策略各302条样本且完整样本/状态一致；合成数据不能当作真实24小时录制。演示目录仅用于合成数据，勿指向原始行情目录。

verify_submission.py是交付报告、基线和源码指纹的一致性检查，不是独立计算真实行情。完整真实复算必须有原始Parquet。

## 当前采集接口和实时演示

- 现货公开行情：`wss://data-stream.binance.vision:443/stream?...`与`https://data-api.binance.vision/api/v3/depth`，来自官方Market Data Only接口。
- U本位：深度走`wss://fstream.binance.com/public`，聚合成交aggTrade走`/market`，双连接统一按实际接收顺序提供消息。
- 现货新快照5000档，U本位1000档；录制、周期、重连与终点快照统一调用live_source.fetch_snapshot，不再保留两套字段/深度实现。
- 交易所E、成交T和本地接收时间都保留。每分钟快照，重连后再取，终点补一张；异常写gap并退避重连。

```powershell
python demo_live_source.py --market spot --duration 15 --verbose --report output/lesson2/my_spot_demo.json
```

当前本机现货完整短测通过：最终同步、买卖盘非空，gap=0，回调异常=0；实际204次成交回调、126次簿回调、14条指标，见submission_spot_vision_live.json。请求15秒接收时长，总耗时另含连接与收尾。

U本位WS两路各收到3条真实消息，核心字段齐全；但本机fapi REST快照直连/代理仍451，不能演示完整U本位建簿，也没有新的aggTrade口径24小时录制。细节与命令见docs/LIVE_DEMO.md。

## 新24小时采集

```powershell
python record_market.py --market both --symbol BTCUSDT --hours 24 --data-root data/new_aggtrade_run --proxy http://127.0.0.1:7890
# 自动化入口，支持--proxy或BINANCE_PROXY；只接受新的空目录
python capture_24h.py --data-root data/new_aggtrade_24h --proxy http://127.0.0.1:7890
```

不要复用历史v2目录。capture_24h默认按UTC时间生成独立目录；采完由finalize统一执行测试与一次全量回放，不再先验证一遍、又重复验证一遍。当前本机U本位REST451尚未解决，不能把上述命令写成已经完成的新24小时实验。

## 存储与回放

Parquet按市场/交易对/UTC日期/小时分区，批次文件追加而不覆盖。分钟缓冲在进程崩溃时可能丢失尚未写盘的约一分钟数据。回放先裁剪小时分区、分批读取并归并；当前小时文件数仍影响文件句柄和堆规模。

```powershell
python validate_replay.py --data-root data/lesson2_real_24h_v2 --require-pass --require-24h
python finalize_lesson2.py
```

实时录制按recv_ts_ms回放；明确标记的旧REST补录才按replay_ts_ms=T参加研究回放。v2没有历史补录。验证器现在在同一次事件遍历内计数并运行模拟交易所，单遍优化后与c4b171b报告29个数据结果字段全部一致。

## 真实v2批次结果

原始接收范围：2026-10-28 01:10:51.944至2026-10-29 01:10:58.172（北京时间），总跨度24.00173小时，六个必需频道各实际覆盖至少23.99小时。首尾跨度不等于中间无缺口。

当前1457次同编号有效比较全部一致（另1393张跨过编号、2张待比较不计分母，U本位31次有效比较）。现货1426/1426，U本位31/31；其余5张快照用于初始化/重建，不参与比较。33项测试通过，终点双市场同步、普通策略回调异常0。

修复前基线1,455/1,457≈99.86%在baseline_20261103/归档。两次旧差异分别缺4和8个卖方价位，均在重建1000档快照之外。当前先按同编号比较并记账，再扩充原覆盖边界外的未知价位；不重置健康簿、不覆盖已知范围错误。共补入3396个价位，原始录制不变。

历史v2的U本位成交为逐笔trade，不是aggTrade；当前采集代码改变不改写历史数据，也不等于已经重新录满24小时。旧失败批次、旧补录与v2独立，不拼接冒充通过；旧结果见legacy_attempt_summary.md。

## 异常与恢复证据

录制gap共10条（两市场各5），模拟器另检出1次编号连续性缺口，总11。既有录制中两市场出现SSLError/ConnectionResetError，随后各约72秒取得新快照，从最早异常到两市场均有新快照为79.285秒；逐时刻证据见docs/NETWORK_RECOVERY_TIMELINE.md。它支持采集连接异常自动恢复，不能证明整机网络被关闭。

```powershell
python diagnose_disconnect.py --output output/lesson2/my_connection_disconnect.json
```

此脚本关闭首条真实WS连接，不关整机网络。当前.vision重测：1次gap，重连后2张快照、201条depth、148条trade，最终同步；证据为submission_spot_controlled_disconnect.json。历史2026-10-28连接级测试的248条depth、154条trade另存controlled_disconnect_validation.json，不混为本次数据。

## 同步、推送和指标边界

- 现货首条采用U<=last<=u的保守重取政策，U=last+1会重新同步；正常后续U<=last+1<=u。U本位首条覆盖快照，后续只看pu==last，不等时不能用区间兜底。
- 同订阅内容顺序一致；深拷贝隔离数据；普通回调异常被记录并隔离。没有进程/超时隔离，卡住不返回仍会阻塞。
- 中间价、价差和过去1秒主动量差按市场/交易对分别维护；m=true为主动卖，记负。每秒最多一次、由事件触发，不是秒末汇总，无事件不输出。
- 缺口期间漏掉的成交不能靠新快照补回；示例状态回调只记录通知。
- 有限快照深度不是全市场覆盖保证；抽查全匹配也不是每一时刻都100%准确。

## 指纹与老师包

```powershell
python fingerprint_data.py --data-root data/lesson2_real_24h_v2 --output local_data_fingerprint.json
python build_submission.py --output ../lesson2_teacher.zip
```

聚合指纹精确算法：按相对POSIX路径字典序排列，每个文件编码一行`相对路径 + NUL + 十进制字节数 + NUL + 小写文件sha256 + LF`，以UTF-8编码拼接后取sha256。5772个文件、542709834字节，重新计算仍为166fea19a753b2f41243a4b4cb78ab8ea477d97d7085456868203987d336c4f2。完整逐文件清单只留本地。

老师包由白名单构建，仅含第二课代码、测试、结构图、成交笔记、项目说明与必要统计证据；不包含原始行情、环境和第一课资产。完整审计记录保留在PR：https://github.com/WEN365-1/hft-backtest/pull/1

## 正式文件导航

- sim_exchange/：双市场行情交易所；record_market.py：采集；capture_24h.py：新24小时入口。
- validate_replay.py：单遍全量复算；finalize_lesson2.py：测试与最终验收。
- demo_live_source.py、diagnose_disconnect.py：实网演示与连接级恢复。
- LESSON2_NOTES.md、docs/system_architecture.svg：笔记与结构图。
- verify_submission.py：已有证据一致性核对；fingerprint_data.py：原始文件指纹。
- output/lesson2/：证据与修复前小型统计基线；原始数据不提交。

官方现货公开行情说明：https://developers.binance.com/en/docs/products/spot/faqs/market_data_only

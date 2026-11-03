# 作业二使用说明

成果与代码导航见[作业成果说明](SUBMISSION_README.md)。本页给出安装、运行和复算方法。

## 一 安装与快速验证

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest -q
.venv\Scripts\python verify_submission.py
.venv\Scripts\python demo_sim_exchange.py --data-root data/lesson2_demo_review
```

当前33项测试通过。合成演示前20档比较2/2、两次gap恢复，两个策略各302条样本且内容一致。演示目录只用于合成数据，请勿指向原始行情目录。

`verify_submission.py`核对报告、基线与源码指纹的一致性；真实行情全量复算使用第四节命令。

## 二 实时演示与连接恢复

```powershell
python demo_live_source.py --market spot --duration 15 --verbose --report output/lesson2/my_spot_demo.json
python diagnose_disconnect.py --output output/lesson2/my_connection_disconnect.json
```

我已验证现货完整实时链路：最终同步、买卖盘非空、gap=0、回调异常=0。主动关闭一条真实WS连接后，采集器记录gap、退避重连、取得新快照并恢复同步。该测试属于连接级断连。

当前采集接口：

- 现货：官方公开行情`data-stream.binance.vision:443` WS和`data-api.binance.vision` REST；新快照5000档。
- U本位：`fstream.binance.com/public`深度与`/market`聚合成交双连接；快照1000档。本机REST返回451，完整U本位实时演示受此限制。
- 保留交易所E、成交T和本地接收时间；每分钟取快照，重连与录制终点再取快照。

详细命令及环境说明见[实时演示说明](docs/LIVE_DEMO.md)。

## 三 采集与存储

```powershell
python record_market.py --market both --symbol BTCUSDT --hours 24 --data-root data/new_aggtrade_run --proxy http://127.0.0.1:7890
python capture_24h.py --data-root data/new_aggtrade_24h --proxy http://127.0.0.1:7890
```

代理按运行环境配置，也支持`BINANCE_PROXY`。`capture_24h.py`默认按UTC时间生成新目录，拒绝混写已有非空目录；采完统一执行测试与一次全量回放。本机U本位REST限制解除后才能完成新的双市场24小时实验。

Parquet按市场、交易对、UTC日期和小时分区，批次追加。分钟缓冲未落盘前，进程崩溃可能丢失约一分钟数据。回放按时间范围裁剪小时分区，分批读取并归并，按本地接收时间推送。

## 四 真实数据回放与结果复算

```powershell
python validate_replay.py --data-root data/lesson2_real_24h_v2 --require-pass --require-24h
python finalize_lesson2.py
```

完整复算需要本地原始Parquet，按作业要求不随提交包上传。

| 验证项 | 结果 |
|---|---|
| 真实双市场录制 | 总跨度24.00173小时，六个必需频道各至少23.99小时 |
| 前20档同编号有效比较 | 1457/1457；现货1426/1426，U本位31/31 |
| 双策略一致性 | 真实60秒片段各3624次回调、121条样本，内容一致 |
| 最终状态 | 双市场同步，普通策略回调异常0 |

比较时另有1393张快照跨过编号、2张待比较，不计分母；5张用于初始化或重建。结果仅表示有效抽查一致率。

## 五 结构图与学习笔记

- [系统结构图](docs/system_architecture.svg)：九个模块、数据流向以及回测和实盘的共用与替换关系。
- [成交模拟学习笔记](LESSON2_NOTES.md)：市价/限价成交、L2排队假设、延迟，以及下一步撮合伪代码。

下单、撮合、风控、账户目前为接口与设计，执行模块留到下一次作业。

## 六 数据指纹与提交包

```powershell
python fingerprint_data.py --data-root data/lesson2_real_24h_v2 --output local_data_fingerprint.json
python build_submission.py --output ../lesson2_teacher.zip
```

指纹算法按相对POSIX路径字典序排列，每个文件编码为`相对路径 + NUL + 十进制字节数 + NUL + 小写文件sha256 + LF`，UTF-8拼接后取sha256。当前5772个Parquet共542709834字节，聚合指纹为`166fea19a753b2f41243a4b4cb78ab8ea477d97d7085456868203987d336c4f2`。

老师包只含作业二代码、测试、图、笔记、说明和必要统计证据；完整复核记录见[PR](https://github.com/WEN365-1/hft-backtest/pull/1)。

## 备注

1. 真实数据接收范围为北京时间2026-10-28 01:10:51.944至10-29 01:10:58.172。首尾跨度不表示中间没有缺口。历史批次U本位成交为逐笔trade，当前采集器为aggTrade，尚未完成新口径24小时录制。
2. 录制gap共10条，回放另检出1次编号连续性缺口。既有连接异常与本次主动断连的证据分别留存，没有做人工整机断网实验，见[网络恢复时间线](docs/NETWORK_RECOVERY_TIMELINE.md)。
3. 现货首条采用`U<=last<=u`的保守重取政策；后续用`U<=last+1<=u`。U本位首条衔接后只用`pu==last`，不等则重新同步。
4. 回调数据用深拷贝隔离，普通异常被记录；回调卡住仍会阻塞。指标按市场/交易对每秒最多生成一次，由事件触发；无新事件不输出，缺口内成交不能靠快照补回。
5. 修复前1,455/1,457≈99.86%的基线保存在`output/lesson2/baseline_20261103/`。两次旧差异源于有限快照外未知价位进入前20档；最终实现先比较记账，再扩充原边界外未知深度，不覆盖已知范围错误。历史变更和回归对账留在PR复核记录。
6. 现货端点依据[Binance官方公开行情说明](https://developers.binance.com/en/docs/products/spot/faqs/market_data_only)。老师包仅包含本次作业二成果。

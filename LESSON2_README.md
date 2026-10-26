# 第二课使用说明

## 快速验证（无需联网）

```bash
pip install -r requirements.txt
python -m pytest -q
python demo_sim_exchange.py
# 如需观察每秒指标：
python demo_sim_exchange.py --verbose
```

演示输出：`output/lesson2/demo_validation.json`。合成数据只验证工程机制，不用于评价策略收益。

当前离线验证：15 项测试通过；双市场前 20 档一致率 100%；2 次 gap 均暂停并恢复；双策略完整样本一致。

## 真实 24 小时采集

```bash
python record_market.py --market both --symbol BTCUSDT --hours 24 --data-root data/lesson2_real
# 网络受限时按本机情况添加：--proxy http://127.0.0.1:7890
```

运行期间可主动断网一次，观察日志中的 gap、退避重连、重连快照。真实 24 小时录制需要持续运行 24 小时；本仓库不把三分钟合成演示声称为真实录制结果。

频道说明：现货使用 `aggTrade`，U 本位使用更细粒度的逐笔 `trade`，两者统一保存 `E/T/p/q/m`。2026-10-26 的正式主采集从 07:58 开始；因 U 本位 `aggTrade` 实测持续无消息，逐笔成交流从 10:53 起补录，补录前的成交缺口不得隐瞒，也不能把补录称为完整 24 小时 aggTrade。

采集结束后一键生成核验报告：

```bash
python validate_replay.py --data-root data/lesson2_real --require-pass
# 额外要求双市场 depth/snapshot/trade 各自覆盖完整 24 小时：
python validate_replay.py --data-root data/lesson2_real --require-pass --require-24h
```

报告包含实际时间跨度、每个频道独立的首尾时间与覆盖小时数、gap 明细、最终同步状态和前 20 档一致率。一致率只在本地簿与快照的 `lastUpdateId` 相同时计算；旧快照、被聚合事件跨过的快照和仍等待对齐的快照分别统计，不混入分母。`--require-pass` 检查双市场成交流、最终同步和 95% 一致率；`--require-24h` 额外拒绝任何不足 23.99 小时的必需频道。首尾跨度不等于中间无缺失，仍需结合 gap 和补录记录判断。

## 真实主动断连验证

```bash
python diagnose_disconnect.py
```

该测试在真实现货 WebSocket 收到行情后主动关闭首条连接，不注入伪 gap，也不影响整机网络。2026-10-26 的实测产生 1 次真实 gap，随后自动重连并重新快照；最后 gap 之后收到 1 张快照、246 条 depth、62 条 trade，最终同步为 true。证据见 `output/lesson2/controlled_disconnect_validation.json`。这是独立连接级断连测试，不冒充 24 小时主采集发生了断线，也不声称做过整机断网。

## 回放真实数据

```python
from sim_exchange import SimExchange, ParquetReplaySource
from sim_exchange.strategies import OneSecondMetricsStrategy

source = ParquetReplaySource("data/lesson2_real", start_ms=None, end_ms=None)
ex = SimExchange(source)
strategy = OneSecondMetricsStrategy()
ex.subscribe(strategy, [
    "usdm.BTCUSDT.book", "usdm.BTCUSDT.trade", "usdm.BTCUSDT.status",
    "spot.BTCUSDT.book", "spot.BTCUSDT.trade", "spot.BTCUSDT.status",
])
stats = ex.run()
print(stats.consistency, ex.book("usdm", "BTCUSDT", depth=20))
```

实时接入只需提供同样按 `recv_ts_ms` 顺序产生事件的 `events()` 数据源，策略回调不用改。

## 文件导航

| 路径 | 内容 |
|---|---|
| `sim_exchange/` | 模拟交易所包 |
| `record_market.py` | 双市场真实采集 |
| `demo_sim_exchange.py` | 离线可重复演示 |
| `tests/test_sim_exchange.py` | 15 项关键测试 |
| `LESSON2_NOTES.md` | 成交、排队、延迟与撮合伪代码 |
| `docs/system_architecture.svg` | 系统结构图 |
| `第二课_学习与汇报.html` | 术语 + 大白话学习与汇报材料 |

## 已知边界

- 本课不实现下单、撮合、账户和风控，只预留接口。
- 流式读取按文件 batch 控制内存；大量小文件仍会提高打开文件与堆维护成本，生产环境应做分区裁剪与文件压缩合并。
- Parquet 是列式格式，批次越小写入开销越大；当前“分钟写盘”在崩溃时最多损失尚未 flush 的约一分钟数据。
- 演示为合成数据；真实一致率、断线恢复与 24 小时稳定性须以真实录制日志复核。

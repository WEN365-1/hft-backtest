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

当前离线验证：10 项测试通过；双市场前 20 档一致率 100%；2 次 gap 均暂停并恢复；双策略完整样本一致。

## 真实 24 小时采集

```bash
python record_market.py --market both --symbol BTCUSDT --hours 24 --data-root data/lesson2_real
# 网络受限时按本机情况添加：--proxy http://127.0.0.1:7890
```

运行期间可主动断网一次，观察日志中的 gap、退避重连、重连快照。真实 24 小时录制需要持续运行 24 小时；本仓库不把三分钟合成演示声称为真实录制结果。

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
| `tests/test_sim_exchange.py` | 10 项关键测试 |
| `LESSON2_NOTES.md` | 成交、排队、延迟与撮合伪代码 |
| `docs/system_architecture.svg` | 系统结构图 |
| `第二课_学习与汇报.html` | 术语 + 大白话学习与汇报材料 |

## 已知边界

- 本课不实现下单、撮合、账户和风控，只预留接口。
- 流式读取按文件 batch 控制内存；大量小文件仍会提高打开文件与堆维护成本，生产环境应做分区裁剪与文件压缩合并。
- Parquet 是列式格式，批次越小写入开销越大；当前“分钟写盘”在崩溃时最多损失尚未 flush 的约一分钟数据。
- 演示为合成数据；真实一致率、断线恢复与 24 小时稳定性须以真实录制日志复核。

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

当前离线验证：16 项测试通过；双市场前 20 档一致率 100%；2 次 gap 均暂停并恢复；双策略完整样本一致。

## 真实 24 小时采集

```bash
python record_market.py --market both --symbol BTCUSDT --hours 24 --data-root data/lesson2_real
# 网络受限时按本机情况添加：--proxy http://127.0.0.1:7890
```

运行期间可主动断网一次，观察日志中的 gap、退避重连、重连快照。真实 24 小时录制需要持续运行 24 小时；本仓库不把三分钟合成演示声称为真实录制结果。

频道说明：现货使用 `aggTrade`，U 本位使用更细粒度的实时逐笔 `trade`，两者统一保存 `E/T/p/q/m`。2026-10-26 的旧批次中，U 本位实时逐笔成交流从 10:53 开始，此前缺口已用 222,914 条币安真实 REST `aggTrades` 补齐；但现货在约 5.8 小时后持续重连失败，旧批次因此**未通过**双市场 24 小时验收。补录清单见 `output/lesson2/usdm_trade_backfill_manifest.json`，旧批次失败报告已保存为 `output/lesson2/real_24h_validation_legacy.json`。修复后现货改用 443 端口、快照请求改为异步、终点增加快照；新批次写入独立目录 `data/lesson2_real_24h_v2`，不与旧数据拼接。

如需复现历史补录（已有完整清单时重复执行会直接退出，避免重复写数）：

```bash
python backfill_usdm_trades.py --data-root data/lesson2_real_24h
```

采集结束后一键生成核验报告：

```bash
# 新批次严格实时口径：六个必需频道都必须实际接收满 24 小时。
python validate_replay.py --data-root data/lesson2_real_24h_v2 --require-pass --require-24h
# 旧批次仅用于失败复盘；即使允许 REST 补录，也会因现货不足 24 小时而拒绝。
python validate_replay.py --data-root data/lesson2_real_24h --require-pass --require-replay-24h
# 主采集状态成功后，再跑测试、覆盖最终报告并生成验收摘要：
python finalize_lesson2.py
```

新批次已设置为每小时检查双市场 REST 与 WebSocket，只有网络可用时才自动开始 24 小时采集；当前状态为 `waiting_for_network`，不是正在录制。`finalize_lesson2.py` 会检查同批次采集状态，未成功会拒绝出最终报告；通过标准为六个必需频道实时覆盖均 ≥23.99 小时、同编号前 20 档一致率 ≥95%、双市场最终同步、无回调异常、补录（如有）与清单一致、真实断连后已恢复。输出为 `output/lesson2/real_24h_validation.json` 和 `output/lesson2/final_acceptance.json`。

报告分别包含实时接收与研究回放的首尾时间、覆盖小时数、成交来源数量、gap 明细、最终同步状态和前 20 档一致率。一致率只在本地簿与快照的 `lastUpdateId` 相同时计算；旧快照、被聚合事件跨过的快照和仍等待对齐的快照分别统计，不混入分母。`--require-pass` 检查双市场成交流、最终同步和 95% 一致率；`--require-24h` 只认可实时接收，`--require-replay-24h` 才允许明确标记的补录。首尾跨度不等于中间无缺失，仍需结合 gap 和补录清单判断。

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
| `backfill_usdm_trades.py` | USD-M 真实历史成交补录与来源清单 |
| `validate_replay.py` | 实时 / 研究回放双口径核验 |
| `demo_sim_exchange.py` | 离线可重复演示 |
| `tests/test_sim_exchange.py` | 16 项关键测试 |
| `LESSON2_NOTES.md` | 成交、排队、延迟与撮合伪代码 |
| `docs/system_architecture.svg` | 系统结构图 |
| `第二课_学习与汇报.html` | 术语 + 大白话学习与汇报材料 |

## 已知边界

- 本课不实现下单、撮合、账户和风控，只预留接口。
- 流式读取按文件 batch 控制内存；大量小文件仍会提高打开文件与堆维护成本，生产环境应做分区裁剪与文件压缩合并。
- Parquet 是列式格式，批次越小写入开销越大；当前“分钟写盘”在崩溃时最多损失尚未 flush 的约一分钟数据。
- 演示为合成数据；真实一致率、断线恢复与 24 小时稳定性须以真实录制日志复核。

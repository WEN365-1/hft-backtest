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

当前离线验证：28 项测试通过；双市场前 20 档一致率 100%；2 次 gap 均暂停并恢复；双策略完整样本一致。新增测试覆盖小时分区裁剪、回放速度和实时源接口替换。

## 真实 24 小时采集

```bash
python record_market.py --market both --symbol BTCUSDT --hours 24 --data-root data/lesson2_real
# 网络受限时按本机情况添加：--proxy http://127.0.0.1:7890
```

运行期间可主动断网一次，观察日志中的 gap、退避重连、重连快照。真实 24 小时录制需要持续运行 24 小时；本仓库不把三分钟合成演示声称为真实录制结果。

当前采集器：现货使用合并流；U 本位深度走 `/public`，`aggTrade` 走 `/market`，两条连接按实际接收顺序汇入同一事件队列，统一保存 `E/T/p/q/m`。现货新快照取 5000 档，U 本位取 1000 档。历史 v2 批次的 U 本位使用逐笔 `trade`，这是当时相对作业 `aggTrade` 要求的偏离，原始数据没有改写。2026-10-26 的旧批次中，U 本位实时逐笔成交流从 10:53 开始，此前缺口已用 222,914 条币安真实 REST `aggTrades` 补齐；但现货在约 5.8 小时后持续重连失败，旧批次因此**未通过**双市场 24 小时验收。补录清单见 `output/lesson2/usdm_trade_backfill_manifest.json`，旧批次失败摘要保持为 `output/lesson2/legacy_attempt_summary.md`；约 291 KB 的完整旧报告只留本地，不进入 PR。修复后现货改用 443 端口、快照请求改为异步、终点增加快照；新批次写入独立目录 `data/lesson2_real_24h_v2`，不与旧数据拼接。

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

历史v2批次已完成真实录制；接收范围为2026-10-28 01:10:51.944至2026-10-29 01:10:58.172（Asia/Shanghai）。2026-11-03使用当前代码复核，结果为 `status=passed`。六个必需频道的实时覆盖均 ≥23.99 小时，总体跨度 24.00173 小时，未使用历史补录。现货写入 depth 857,475 条、trade 800,124 条、snapshot 1,429 张；U 本位写入 depth 846,380 条、trade 2,287,977 条、snapshot 1,428 张。同编号前 20 档比较 1,457 次，1,457 次一致，一致率 100%（2026-11-03 使用同一原始批次复核；原基线 1,455/1,457≈99.86% 已归档）；双市场最终均同步、回调异常为 0。主采集期间两个市场各记录 5 条采集异常 gap（含连接和快照请求异常），模拟器另检出 1 次编号连续性缺口，合计 11，随后均恢复。最终报告为 `output/lesson2/real_24h_validation.json` 和 `output/lesson2/final_acceptance.json`。

报告分别包含实时接收与研究回放的首尾时间、覆盖小时数、成交来源数量、gap 明细、最终同步状态和前 20 档一致率。一致率只在本地簿与快照的 `lastUpdateId` 相同时计算；旧快照、被聚合事件跨过的快照和仍等待对齐的快照分别统计，不混入分母。`--require-pass` 检查双市场成交流、最终同步和 95% 一致率；`--require-24h` 只认可实时接收，`--require-replay-24h` 才允许明确标记的补录。首尾跨度不等于中间无缺失，仍需结合 gap 和补录清单判断。

## 真实主动断连验证

```bash
python diagnose_disconnect.py
```

该测试在真实现货 WebSocket 收到行情后主动关闭首条连接，不注入伪 gap，也不影响整机网络。2026-10-28 使用修复后的 443 通道复测，产生 1 次真实 gap，随后自动重连并重新快照；最后 gap 之后收到 2 张快照、248 条 depth、154 条 trade，最终同步为 true。证据见 `output/lesson2/controlled_disconnect_validation.json`。这是独立连接级断连测试，不冒充 24 小时主采集发生了断线，也不声称做过整机断网。

## 回放真实数据

```python
from sim_exchange import SimExchange, ParquetReplaySource
from sim_exchange.strategies import OneSecondMetricsStrategy

# speed=0 尽快回放；speed=1 按接收时差原速，speed=2 表示两倍速。
source = ParquetReplaySource("data/lesson2_real_24h_v2", speed=0)
ex = SimExchange(source)
strategy = OneSecondMetricsStrategy()
ex.subscribe(strategy, [
    "usdm.BTCUSDT.book", "usdm.BTCUSDT.trade", "usdm.BTCUSDT.status",
    "spot.BTCUSDT.book", "spot.BTCUSDT.trade", "spot.BTCUSDT.status",
])
stats = ex.run()
print(stats.consistency, ex.book("usdm", "BTCUSDT", depth=20))
```

回放器先按 `start_ms/end_ms` 裁剪小时分区，每次只归并当前小时的文件，再按文件 batch 读取；24 小时总文件数不再决定同时打开的文件数。存储层保证分区小时与 `replay_time_ms` 对应，因此不会为了裁剪牺牲时间顺序。

## 切换真实实时源

```bash
# 只读公开行情；默认演示 60 秒，网络受限时按本机情况加 --proxy。
python demo_live_source.py --market spot --duration 60 --verbose
```

`BinanceWebSocketSource` 与 `ParquetReplaySource` 都实现同步 `events()`。实时源负责连接、分钟快照、gap 和退避重连；`SimExchange`、订单簿与 `OneSecondMetricsStrategy` 完全不变。`sim_exchange/endpoints.py` 是零第三方依赖的订阅地址模块，纯逻辑测试不再需要导入 `record_market.py` 或加载 `websockets`。

## 文件导航

| 路径 | 内容 |
|---|---|
| `sim_exchange/` | 模拟交易所包 |
| `sim_exchange/live_source.py` | 可替换的真实 WebSocket 实时源 |
| `sim_exchange/endpoints.py` | 无第三方依赖的行情地址与成交频道规则 |
| `record_market.py` | 双市场真实采集 |
| `backfill_usdm_trades.py` | USD-M 真实历史成交补录与来源清单 |
| `validate_replay.py` | 实时 / 研究回放双口径核验 |
| `demo_sim_exchange.py` | 离线可重复演示 |
| `demo_live_source.py` | 只换 source 的真实实时演示 |
| `tests/test_sim_exchange.py` | 28 项关键测试 |
| `LESSON2_NOTES.md` | 成交、排队、延迟与撮合伪代码 |
| `docs/system_architecture.svg` | 系统结构图 |
| `第二课_学习与汇报.html` | 术语 + 大白话学习与汇报材料 |

## 已知边界

- 本课不实现下单、撮合、账户和风控，只预留接口。
- 回放已按小时裁剪并分批归并；单小时内大量小文件仍有打开与堆维护成本，生产环境可继续压缩合并。
- Parquet 是列式格式，批次越小写入开销越大；当前“分钟写盘”在崩溃时最多损失尚未 flush 的约一分钟数据。
- 演示为合成数据；真实一致率、断线恢复与 24 小时稳定性须以真实录制日志复核。实时源示例的接口已离线测试，但每次实网表现仍取决于当时网络与币安服务。

## 提交前复核补充 2026-11-03

- 全量原始数据复核仍为 1457 次同编号比较，现为 1457 次一致；现货 1426/1426，U 本位 31/31，分别通过 95% 检查。被跨过的 1393 张、待比较的 2 张不计入分母。
- 两次原始差异来自 1000 档快照之外未知卖价进入前 20 档，非已证实的毫秒错位。当前在同编号核验结果记账后，仅扩充原覆盖边界外的未知深度，不重置健康簿，也不覆盖已知范围的错误。真实回放补充 3396 个价位。
- 原报告保存在 `output/lesson2/baseline_20261103/`，新报告位于原有验收入口。原始 Parquet 不变。
- `submission_live_smoke.json` 记录本次真实实时重测：REST 快照返回 HTTP 451，未通过新现网冒烟。新路由的双连接归并已通过离线测试，不冒充已完成新的现网或24小时验证。
- 现货首条仍采用保守重取快照政策 `U <= last <= u`；首条 `U=last+1` 会重新同步。正常后续采用 `U <= last+1 <= u`。U 本位首条覆盖快照，后续只检查 `pu==last`，无区间兜底。
- 每个市场/交易对每秒最多一条指标，事件触发、不是秒末汇总；无新事件不输出。普通回调异常隔离不等于超时或进程隔离。
- 新机器没有原始录制时，可以直接运行测试及合成演示；真实全量复核需要本地 `data/lesson2_real_24h_v2`，按作业要求不随提交包提供。

补充现网证据：`submission_websocket_channels.json` 中两路各收到3条真实消息、核心字段齐全；`submission_live_proxy_smoke.json` 的完整实时源仍因REST快照451失败。通道成功不能替代完整实时源或新24小时验收，详见 `GROK_AUDIT_RECONCILIATION.md`。

# hft-backtest

币安 BTCUSDT 高频回测入门项目：录制订单簿与成交流 → 重建本地订单簿 → 事件驱动回测不平衡度策略 → 对比费用与延迟的影响。

## 项目结构

| 文件 | 作用 |
|---|---|
| `config.py` | 全部可调参数（阈值、持仓时长、费率、延迟） |
| `recorder.py` | 录制 WS `depth@100ms` + `aggTrade`，每条带本地接收时间戳，每 60s 拍 REST 快照 |
| `orderbook.py` | 本地订单簿：快照 + 增量同步（官方 7 步）、不平衡度、走档成交 |
| `verify_orderbook.py` | 订单簿重建体检：定期与 REST 快照比对前 20 档，统计不一致次数，缺页自动重同步 |
| `backtester.py` | 事件驱动回测器：两路数据按时间归并回放，市价开平仓，计费 + 延迟 |
| `experiment.py` | 三组对比实验：无费用无延迟 / 有费用 / 有费用 + 50ms 延迟 |
| `make_sample_data.py` | 生成模拟行情，供无法连接币安时离线跑通全流程 |
| `notes.md` | 概念题、计算题、API 题的学习笔记 |

## 环境

```bash
pip install -r requirements.txt
```

## 运行

```bash
# 1) 没有网络/无法连币安时：先生成模拟数据
python make_sample_data.py

# 2) 有网络时：录真实数据（默认 30 分钟；受限网络加 --proxy）
python recorder.py --minutes 30 --proxy http://127.0.0.1:7890

# 3) 重建订单簿并体检（默认用最近一次运行）
python verify_orderbook.py

# 4) 跑对比实验（默认用最近一次运行）
python experiment.py

# 想回看某一次运行：加 --run
python experiment.py --run btcusdt_20261020_114313
```

## 运行目录（永不覆盖）

每次录制自动生成一个带时间戳的文件夹：

```
data/runs/btcusdt_20261020_114313/
    depth.parquet
    aggtrade.parquet
    snapshots.jsonl
    meta.json
output/runs/btcusdt_20261020_114313/
    comparison.csv
    trades_*.csv
    pnl_comparison.png
```

`data/latest_run.txt` 记录最近一次 run id；不写 `--run` 时就使用它。

## 策略

- **信号**：订单簿前 3 档不平衡度 `(买量 - 卖量) / (买量 + 卖量)`
- **开仓**：不平衡度 > 阈值（默认 0.30）且空仓 → 市价买入（吃卖档，taker）
- **平仓**：持仓满固定时长（默认 5 秒）→ 市价卖出（吃买档，taker）
- **成本**：U 本位合约 taker 0.05%，双边收取
- **延迟**：信号在 t 发出，成交价取 t + latency 之后第一条数据的盘口

## 结果解读（模拟数据）

模拟数据是随机游走，**本身不含可预测性**，因此：

- 无费用场景的盈亏接近 0（本样本为 -68 USDT，属随机噪声）；
- 加费用后每笔平均亏约 60 USDT ≈ 1 BTC × 60000 × 0.05% × 2，**费用完全主导结果**；
- 50ms 延迟在随机游走数据上的期望影响为 0，只有方差；真实市场存在动量与逆向选择，延迟会系统性变差。

**结论**：高频策略的生死线是"单笔毛利能否覆盖双边成本"。回测若漏算费用、延迟、排队，收益会被系统性高估。

## 数据与密钥

- `data/`、`*.parquet`、`*.csv` 已在 `.gitignore` 中排除；
- 本项目只使用公开行情接口，不需要 API Key；
- 若后续接入下单接口，密钥只能放环境变量，绝不进仓库。

## 免责声明

本项目仅用于学习与研究，不构成任何投资建议。模拟数据为人工生成，不代表真实市场。

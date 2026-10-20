# 抓取操作手册（手动执行版）

> 目标：录 30 分钟 BTCUSDT 真实行情（depth@100ms + aggTrade），落盘为 Parquet。

## 第 0 步：准备

1. 笔记本**插上电源**，电源计划设为"从不睡眠"（控制面板 → 电源选项）。
2. 关闭下载/视频等占带宽的程序。
3. 打开 VPN / 代理，切到 **全局模式（Global / TUN 模式）**。
   - Clash 类：打开"全局"或"TUN 模式"，本地端口通常是 `7890`
   - 游戏加速器（如雷神）通常只加速游戏，**不一定代理 Python 流量**——如果体检失败，换 Clash/v2ray 类工具
4. 打开终端，进入项目目录：

```bash
cd C:\Users\Administrator\Desktop\DBC_project\hft-backtest
```

## 第 1 步：网络体检（必须先做）

```bash
python check_network.py
```

如果代理是本地端口且系统代理没生效，加 `--proxy`：

```bash
python check_network.py --proxy http://127.0.0.1:7890
```

**结果对照表**：

| 输出 | 含义 | 怎么办 |
|---|---|---|
| ✅ 网络就绪 | 两条通道都通 | 进入第 2 步 |
| REST 451 | 请求到了币安，但被地区限制拒绝 | **换节点：香港/新加坡/日本可用；美国、中国大陆节点会被 451** |
| REST 通、WS 不通 | 代理只对 HTTP 生效 | 加 `--proxy http://127.0.0.1:端口` 重试 |
| 两个都超时 | VPN 没生效 | 检查 VPN 是否开启、是否全局模式 |
| ConnectionResetError | 连接被重置 | 换节点/换全局模式重试 |

## 第 2 步：先录 2 分钟试跑（强烈建议）

```bash
python recorder.py --minutes 2
```

（若有代理）`python recorder.py --minutes 2 --proxy http://127.0.0.1:7890`

**正常输出长这样**：

```
[14:03:01] WS 已连接
[14:04:01] 快照 lastUpdateId=...，depth=6000 agg=3500
录制完成: depth=..., agg=..., snapshots=...
```

看到 `WS 已连接` 就说明通了。结束后会打印本次的 run id，数据在
`data/runs/<run_id>/` 下（depth.parquet / aggtrade.parquet / snapshots.jsonl / meta.json）。

## 第 3 步：正式录 30 分钟

```bash
python recorder.py --minutes 30
```

- **不要关窗口**，不要点休眠；
- 断线会自动重连并补快照（会打印"连接断开...重连"）；
- 30 分钟后自动落盘。

## 第 4 步：验收数据

```bash
python verify_orderbook.py
```

正常结果：缺页 0 次、快照比对不一致数接近 0。

```bash
python experiment.py
```

输出对比表 + `output/runs/<run_id>/pnl_comparison.png`（每次运行独立文件夹，不覆盖）。

## 第 5 步：把结果发我

把这三样贴给我：
1. `recorder.py` 最后一行"录制完成"的数字；
2. `verify_orderbook.py` 的汇总；
3. `experiment.py` 的对比表。

我帮你解读，并据此写反思。

## 常见问题

| 现象 | 原因 | 处理 |
|---|---|---|
| `451` | 地区限制 | VPN 全局/TUN 模式，换节点 |
| `TimeoutError: timed out during opening handshake` | WS 不通 | 用 `--proxy` 指定端口 |
| `ConnectionResetError` | 被重置 | 换节点；确认全局模式 |
| 录到一半断了 | 网络抖动 | 正常，会自动重连；重连后继续录 |
| 磁盘/内存 | 数据量 | 30 分钟通常几十 MB，无需担心 |

# 作业二 模拟交易所与成交模拟

本项目交付行情采集、Parquet分区存储、流式回放、本地订单簿和策略订阅推送，并附系统结构图、关键测试、成交模拟笔记和下一课撮合设计。下单、撮合、账户与风控只有接口及设计，本课没有执行实现。

## 项目导航

1. `sim_exchange/`：可复用行情模拟交易所；第二课订单簿实现为`sim_exchange/orderbook.py`。
2. `docs/system_architecture.svg`：模块、数据流及回测/实盘边界。
3. `LESSON2_NOTES.md`：市价/限价成交、排队假设、延迟与撮合伪代码。
4. `LESSON2_README.md`：运行方式、数据口径与完整文件导航。
5. `tests/`：关键逻辑测试；`output/lesson2/`：真实与合成验证证据。
6. `docs/LIVE_DEMO.md`：现货实时演示；`docs/NETWORK_RECOVERY_TIMELINE.md`：既有批次网络异常恢复时间线。

## 已验证结果

33项测试通过。既有真实24小时批次的1457次同编号有效比较全部一致（另有1393张跨过编号、2张待比较不计分母；其中U本位31次有效比较）。修复前基线1,455/1,457≈99.86%已单独归档；原始录制不变，当前结果不是新的24小时采集。

现货.vision完整实时短测成功，最终同步、gap=0；U本位两个WS通道也收到真实消息。合成演示两策略各302样本且一致，2次缺口恢复。合成数据不用于替代真实录制。

## 需要披露的范围

- 历史v2的U本位24小时成交为逐笔trade；当前采集器改用aggTrade，没有把历史数据改名，也没有新的aggTrade口径24小时录制。
- 本机U本位REST快照仍451；现货完整演示通过不代表U本位完整建簿通过。
- 已有两市场采集网络异常自动恢复时间线和独立WS主动断连；二者都不能证明人工整机断网实验已经做过。

## 运行

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest -q
.venv\Scripts\python verify_submission.py
.venv\Scripts\python demo_sim_exchange.py --data-root data/lesson2_demo_review
```

`verify_submission.py`核对已有报告和源码指纹的一致性，不是独立计算真实数据。真实全量复算必须有本地原始Parquet，运行`validate_replay.py --data-root data/lesson2_real_24h_v2 --require-pass --require-24h`；原始行情不随包提供。

`fingerprint_data.py`公开逐文件及聚合指纹算法。`build_submission.py`按白名单生成本作业的成果包。完整复核记录可在PR中查看：https://github.com/WEN365-1/hft-backtest/pull/1

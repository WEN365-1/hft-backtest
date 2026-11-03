# 作业二 模拟交易所与成交模拟

本次提交包括行情采集、分区存储、流式回放、本地订单簿、策略订阅推送、测试、系统结构图和成交模拟笔记。下单、撮合、账户和风控仅预留接口，未实现执行逻辑。

## 建议审阅顺序

1. `SUBMISSION_REVIEW.md`：本次复核、真实证据与未完成的字面要求。
2. `LESSON2_README.md`：安装、运行和数据口径。
3. `docs/system_architecture.svg`：模块及数据流。
4. `sim_exchange/`、`record_market.py`、`tests/test_sim_exchange.py`：实现和28项测试。
5. `LESSON2_NOTES.md`：成交模型、排队假设、延迟和下一课撮合设计。
6. `output/lesson2/final_acceptance.json` 与 `real_24h_validation.json`：当前全量复核；`baseline_20261103/` 保存修复前报告。

## 结果

28项测试通过；合成演示双策略各302条样本且一致，2次缺口恢复。既有真实24小时批次按当前代码重新回放，同编号前20档比较1457/1457，现货1426/1426、U本位31/31；1393张跨过编号、2张待比较不计分母。

原1455/1457的两处差异已追到有限快照覆盖不足。修复通过已到达的同编号快照在比较后扩充未知深度，未改原始录制，也没有重置后再比凑100%。

## 需要说明

历史24小时U本位使用逐笔trade，保留相对aggTrade要求的偏离。新采集器已改/public深度、/market聚合成交双连接，但本次现网短测因REST HTTP451未通过；不冒充新24小时采集。独立主动断连是连接级，不是整机断网。

## 最快运行

在此目录中执行：

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python -m pytest -q
.venv\Scripts\python verify_submission.py
.venv\Scripts\python demo_sim_exchange.py --data-root data/lesson2_demo_review
```

演示目录只用于合成数据，勿指向真实录制目录。数据文件按作业要求不随包提供；真实全量复核命令见使用说明，有本地原始录制时才能执行。

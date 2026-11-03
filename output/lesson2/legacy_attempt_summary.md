# 旧批次失败摘要（不进入最终验收）

- 旧批次：2026-10-26，`data/lesson2_real_24h`。
- 结果：**未通过**，不能与后续 v2 批次拼接或算作成功。
- 原因：现货 depth、trade、snapshot 均只覆盖约 5.80 小时，之后持续重连失败；U 本位早期实时成交缺口虽以 222,914 条真实 REST aggTrades 补录，但补录不能替代实时 24 小时采集。
- 旧批次同编号快照比较 374 次，373 次一致，前 20 档一致率约 99.73%；一致率达标不能弥补覆盖不足。
- 修复：现货改用 443 端口、REST 快照改为异步、终点增加快照。
- 新批次：`data/lesson2_real_24h_v2` 独立重录，最终 `status=passed`；详见 `final_acceptance.json` 与 `real_24h_validation.json`。

完整旧报告 `real_24h_validation_legacy.json` 约 291 KB，仅保留在本机用于排查，不纳入 PR，避免评审噪声。

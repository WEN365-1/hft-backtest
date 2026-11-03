# 现货实时行情演示

当前现货使用币安官方公开行情专用端点：`data-api.binance.vision`取快照，`data-stream.binance.vision:443`取深度和聚合成交。它们不需要密钥，只用于行情，不涉及账户或下单。

在仓库或老师包的hft-backtest目录运行：

```powershell
python demo_live_source.py --market spot --duration 15 --verbose --report output/lesson2/my_spot_demo.json
```

网络需要本机已有代理时显式追加`--proxy http://127.0.0.1:7890`；程序不会修改系统代理或关闭网络。15秒是接收循环请求时长，连接建立和收尾可能使总耗时稍长。

本机本次实际运行结果见`submission_spot_vision_live.json`：订单簿最终同步，买卖盘非空，gap=0、回调异常=0；收到204次成交回调、126次簿回调、14条指标样本。这是短时实网演示，不代替24小时稳定性验证。

连接级主动断连验证：

```powershell
python diagnose_disconnect.py --output output/lesson2/my_connection_disconnect.json
```

脚本在首条真实连接建立12秒后关闭该WS连接，以1012触发重连，运行约45秒；不会关闭整机网络，也不覆盖历史断连报告。

U本位的/public深度和/market aggTrade通道已各收到真实消息，但本机fapi REST快照仍返回451。因此本机现场可以展示U本位通道消息，不能把它说成完整U本位建簿已演示成功；也没有新aggTrade口径24小时录制。

若以后有满足访问条件的环境，需要新24小时采集时：

```powershell
python capture_24h.py --proxy http://127.0.0.1:7890 --data-root data/new_aggtrade_24h
```

也可设置`BINANCE_PROXY`。新采集只接受空的新目录，采完统一执行一次全量回放和测试；不要复用历史v2目录。

官方端点说明：https://developers.binance.com/en/docs/products/spot/faqs/market_data_only

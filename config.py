"""
全局配置：所有可调参数集中在这里。
改参数不用翻代码 —— 这是回测项目的标准做法。
"""

# ---------- 交易标的与数据流 ----------
SYMBOL = "BTCUSDT"
# 币安 WS 的频道名要小写
DEPTH_STREAM = "btcusdt@depth@100ms"   # 订单簿增量（100ms 一推）
AGG_STREAM = "btcusdt@aggTrade"        # 聚合成交

# ---------- 策略参数（第 4 章"不平衡度阈值策略"） ----------
IMBALANCE_LEVELS = 3        # 用前几档算不平衡度（作业口径：前 3 档）
IMBALANCE_THRESHOLD = 0.30  # 不平衡度超过这个值就开仓（可调）
HOLD_SECONDS = 5.0          # 固定持仓时长（秒）
ORDER_QTY = 1.0             # 每次交易数量（BTC）

# ---------- 成本参数（第 2 课） ----------
TAKER_FEE = 0.0005          # U 本位合约 taker 0.05%
LATENCY_MS = 0               # 下单延迟（毫秒）。对比实验里用 50

# ---------- 文件路径 ----------
DATA_DIR = "data"
RUNS_DIR = "data/runs"                  # 每次录制一个带时间戳的子文件夹
LATEST_POINTER = "data/latest_run.txt"  # 记录最近一次运行的 run id
OUTPUT_RUNS_DIR = "output/runs"         # 每次回测结果一个子文件夹

# ---------- 录制参数 ----------
RECORD_SECONDS = 30 * 60    # 录 30 分钟
SNAPSHOT_EVERY_SEC = 60     # 每 60 秒拍一张 REST 快照（体检 + 重锚定）

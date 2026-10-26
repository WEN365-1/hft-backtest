"""真实 WebSocket 主动断连测试：收到公网行情后关闭首条连接，验证原采集器恢复。"""
from __future__ import annotations

import asyncio
import json
from collections import Counter
from datetime import datetime
from pathlib import Path

import record_market
from sim_exchange import ParquetEventStore, ParquetReplaySource
from validate_replay import validate


async def exercise(root: Path):
    """只关闭第一条真实连接；第二次及以后使用原始连接，不注入伪事件。"""
    original_connect = record_market.connect
    first_connection = True
    disconnect_started = None

    class ControlledConnection:
        def __init__(self, url, kwargs, close_after):
            self.cm = original_connect(url, **kwargs)
            self.ws = None
            self.close_after = close_after
            self.timer = None

        async def __aenter__(self):
            nonlocal disconnect_started
            self.ws = await self.cm.__aenter__()
            if self.close_after:
                async def close_real_socket():
                    nonlocal disconnect_started
                    await asyncio.sleep(12)
                    disconnect_started = datetime.now().astimezone().isoformat()
                    await self.ws.close(code=1012, reason="controlled reconnect test")
                self.timer = asyncio.create_task(close_real_socket())
            return self.ws

        async def __aexit__(self, exc_type, exc, tb):
            if self.timer:
                self.timer.cancel()
            return await self.cm.__aexit__(exc_type, exc, tb)

    def controlled_connect(url, **kwargs):
        nonlocal first_connection
        close_after = first_connection
        first_connection = False
        return ControlledConnection(url, kwargs, close_after)

    record_market.connect = controlled_connect
    try:
        store = ParquetEventStore(root)
        await record_market.record_one("spot", "BTCUSDT", 45 / 3600, store, None)
    finally:
        record_market.connect = original_connect
    return disconnect_started, datetime.now().astimezone().isoformat()


def main():
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    root = Path(f"data/lesson2_disconnect_{stamp}")
    output = Path("output/lesson2/controlled_disconnect_validation.json")
    disconnect_started, test_ended = asyncio.run(exercise(root))
    report = validate(str(root), str(output))
    events = list(ParquetReplaySource(root).events())
    gap_times = [e["recv_ts_ms"] for e in events if e["event_type"] == "gap"]
    last_gap = max(gap_times) if gap_times else None
    after = Counter(e["event_type"] for e in events
                    if last_gap is not None and e["recv_ts_ms"] > last_gap)
    recovered = bool(last_gap is not None and after["snapshot"] > 0 and after["depth"] > 0
                     and report["final_synced"].get("spot.BTCUSDT"))
    report.update({
        "test_type": "controlled_websocket_disconnect_not_whole_machine_network",
        "disconnect_started_at": disconnect_started,
        "test_ended_at": test_ended,
        "close_trigger": "真实首条 WebSocket 收到行情后，以 1012 主动关闭；未注入伪 gap",
        "events_after_last_gap": dict(after),
        "reconnected_and_resynced": recovered,
    })
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: report[k] for k in (
        "test_type", "gaps_detected", "events_after_last_gap",
        "final_synced", "reconnected_and_resynced")}, ensure_ascii=False, indent=2))
    assert report["gaps_detected"] >= 1, "主动断连未产生 gap"
    assert recovered, "断连后未完成重连、快照与同步恢复"


if __name__ == "__main__":
    main()

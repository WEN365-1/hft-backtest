from __future__ import annotations

import pyarrow.parquet as pq

from sim_exchange import (BinanceWebSocketSource, ParquetEventStore,
                          ParquetReplaySource, SimExchange)
from sim_exchange.endpoints import endpoints, websocket_urls
from sim_exchange.live_source import message_to_event
from sim_exchange.orderbook import SyncedOrderBook
from sim_exchange.strategies import OneSecondMetricsStrategy


def test_market_specific_trade_streams():
    spot_url, spot_rest = endpoints("spot", "BTCUSDT")
    usdm_url, _ = endpoints("usdm", "BTCUSDT")
    assert "btcusdt@aggTrade" in spot_url
    assert "data-stream.binance.vision:443" in spot_url
    assert spot_rest == "https://data-api.binance.vision/api/v3/depth"
    assert ":9443" not in spot_url
    assert "/public/" in usdm_url
    assert "btcusdt@depth@100ms" in usdm_url
    depth_url, trade_url = websocket_urls("usdm", "BTCUSDT")
    assert depth_url == usdm_url
    assert "/market/" in trade_url and "btcusdt@aggTrade" in trade_url
    assert websocket_urls("spot", "BTCUSDT") == (spot_url,)


def snap(market="spot", ts=0, uid=100):
    return {"market": market, "symbol": "BTCUSDT", "event_type": "snapshot",
            "recv_ts_ms": ts, "last_update_id": uid,
            "bids": [[99, 2], [98, 3]], "asks": [[101, 1], [102, 4]]}


def depth(market="spot", ts=1, uid=101, pu=None):
    return {"market": market, "symbol": "BTCUSDT", "event_type": "depth",
            "recv_ts_ms": ts, "U": uid - 1, "u": uid, "pu": uid - 1 if pu is None else pu,
            "bids": [[99, 2.5]], "asks": []}


def test_spot_sync_and_zero_deletes_level():
    book = SyncedOrderBook("spot", "BTCUSDT")
    book.apply_snapshot(snap())
    assert book.apply_depth(depth()) == "ok"
    event = depth(uid=102)
    event.update(U=102, u=102, bids=[[99, 0]])
    assert book.apply_depth(event) == "ok"
    assert 99 not in book.bids


def test_usdm_requires_pu_not_spot_rule():
    book = SyncedOrderBook("usdm", "BTCUSDT")
    book.apply_snapshot(snap("usdm"))
    assert book.apply_depth(depth("usdm", uid=102, pu=99)) == "gap"
    assert not book.synced


def test_stale_depth_does_not_move_book():
    book = SyncedOrderBook("spot", "BTCUSDT")
    book.apply_snapshot(snap())
    assert book.apply_depth(depth(uid=99)) == "stale"
    assert book.last_update_id == 100


def test_store_partition_and_never_overwrite(tmp_path):
    store = ParquetEventStore(tmp_path)
    p1 = store.write_batch([snap(ts=3_600_000)])[0]
    p2 = store.write_batch([snap(ts=3_600_001)])[0]
    assert p1 != p2 and p1.exists() and p2.exists()
    assert p1.parts[-5:-1] == ("spot", "BTCUSDT", "date=1970-01-01", "hour=01")
    assert len(pq.read_table(p1).to_pylist()) == 1


def test_streaming_replay_merges_local_receive_order(tmp_path):
    store = ParquetEventStore(tmp_path)
    for ts in (30, 10, 20):
        event = snap(ts=ts)
        store.write_batch([event])
    times = [x["recv_ts_ms"] for x in ParquetReplaySource(tmp_path).events()]
    assert times == [10, 20, 30]


def test_replay_time_filter(tmp_path):
    store = ParquetEventStore(tmp_path)
    store.write_batch([snap(ts=t) for t in (10, 20, 30)])
    source = ParquetReplaySource(tmp_path, start_ms=15, end_ms=25)
    assert [x["recv_ts_ms"] for x in source.events()] == [20]


def test_backfill_uses_replay_time_and_preserves_receive_time(tmp_path):
    store = ParquetEventStore(tmp_path)
    historical = {"market": "usdm", "symbol": "BTCUSDT", "event_type": "trade",
                  "recv_ts_ms": 7_200_000, "replay_ts_ms": 3_600_000,
                  "trade_ts_ms": 3_600_000, "p": "100", "q": "1", "m": False,
                  "source": "binance_rest_aggTrades_backfill"}
    path = store.write_batch([historical])[0]
    store.write_batch([snap("usdm", ts=3_600_500)])
    assert "hour=01" in path.parts  # 不按实际下载小时 02 错放研究回放分区。
    events = list(ParquetReplaySource(tmp_path, start_ms=3_600_000,
                                      end_ms=3_600_000).events())
    assert len(events) == 1
    assert events[0]["recv_ts_ms"] == 7_200_000
    assert events[0]["replay_ts_ms"] == 3_600_000
    assert events[0]["source"] == "binance_rest_aggTrades_backfill"
    from validate_replay import validate
    report = validate(str(tmp_path), str(tmp_path / "validation.json"))
    assert report["backfill_integrity"]["rows"] == 1
    assert report["backfill_integrity"]["valid_rows"] == 1
    assert report["backfill_integrity"]["invalid_rows"] == 0


def test_gap_pauses_then_snapshot_recovers():
    class Source:
        def events(self):
            yield snap()
            yield depth()
            yield {"market": "spot", "symbol": "BTCUSDT", "event_type": "gap",
                   "recv_ts_ms": 2, "reason": "test"}
            yield depth(ts=3, uid=102)
            yield snap(ts=4, uid=200)
            yield depth(ts=5, uid=201)
    class Strategy:
        def __init__(self): self.books, self.status = [], []
        def on_book(self, e): self.books.append(e["book"]["last_update_id"])
        def on_status(self, e): self.status.append(e["state"])
    s = Strategy()
    ex = SimExchange(Source())
    ex.subscribe(s, ["spot.BTCUSDT.book", "spot.BTCUSDT.status"])
    ex.run()
    assert s.status == ["gap", "ready"]
    assert 102 not in s.books and s.books[-1] == 201


def test_two_strategies_identical_and_error_isolated():
    class Source:
        def events(self):
            yield snap()
            yield depth()
    class Bad:
        def on_book(self, e): raise RuntimeError("boom")
    a, b = OneSecondMetricsStrategy(verbose=False), OneSecondMetricsStrategy(verbose=False)
    ex = SimExchange(Source())
    for s in (a, b, Bad()):
        ex.subscribe(s, ["spot.BTCUSDT.book"])
    stats = ex.run()
    assert a.samples == b.samples
    assert stats.callback_errors == 2 and stats.pushed["on_book"] == 6


def test_subscriber_cannot_mutate_other_subscribers_payload():
    class Source:
        def events(self): yield snap()
    class Evil:
        def on_book(self, e): e["book"]["bids"].clear()
    class Good:
        def __init__(self): self.received = None
        def on_book(self, e): self.received = e
    good = Good()
    ex = SimExchange(Source())
    ex.subscribe(Evil(), ["spot.BTCUSDT.book"])
    ex.subscribe(good, ["spot.BTCUSDT.book"])
    ex.run()
    assert good.received["book"]["bids"]


def test_top20_consistency_metric():
    class Source:
        def events(self):
            yield snap()
            yield depth()
            event = snap(ts=2, uid=101)
            event["bids"] = [[99, 2.5], [98, 3]]
            yield event
    ex = SimExchange(Source())
    stats = ex.run()
    assert stats.snapshot_comparisons == 1
    assert stats.exact_matches == 1 and stats.consistency == 1.0


def test_snapshot_comparison_waits_for_same_update_id():
    class Source:
        def events(self):
            yield snap()
            yield depth()
            expected = snap(ts=2, uid=102)
            expected["bids"] = [[99, 2.5], [98, 3]]
            yield expected  # 本地编号 101，先等待，不比较不同状态。
            yield snap(ts=3, uid=100)  # 本地已超过旧快照，明确跳过。
            yield depth(ts=4, uid=102)
    stats = SimExchange(Source()).run()
    assert stats.snapshot_comparisons == 1
    assert stats.exact_matches == 1
    assert stats.snapshot_skipped_stale == 1
    assert stats.snapshot_pending == 0


def test_usdm_first_event_covers_snapshot_before_pu_rule():
    book = SyncedOrderBook("usdm", "BTCUSDT")
    book.apply_snapshot(snap("usdm", uid=100))
    first = depth("usdm", uid=101, pu=95)  # 首条不看 pu，U=100 覆盖快照编号。
    assert book.apply_depth(first) == "ok"
    following = depth("usdm", uid=102, pu=100)
    assert book.apply_depth(following) == "gap"  # 后续必须 pu=101。


def test_depth_before_snapshot_is_buffered_and_applied():
    class Source:
        def events(self):
            early = depth("usdm", ts=0, uid=101, pu=95)
            yield early
            yield snap("usdm", ts=1, uid=100)
    ex = SimExchange(Source())
    stats = ex.run()
    assert stats.gaps == 0
    assert ex.book("usdm", "BTCUSDT")["last_update_id"] == 101
    assert ex.book("usdm", "BTCUSDT")["bids"][0] == (99.0, 2.5)


def test_first_spot_event_can_end_exactly_at_snapshot_id():
    book = SyncedOrderBook("spot", "BTCUSDT")
    book.apply_snapshot(snap(uid=100))
    event = depth(uid=100)  # U=99, u=100：首条覆盖快照，不是旧事件。
    assert book.apply_depth(event) == "ok"
    following = depth(uid=101)
    assert book.apply_depth(following) == "ok"


def test_replay_prunes_hours_before_opening_files(tmp_path, monkeypatch):
    store = ParquetEventStore(tmp_path)
    store.write_batch([snap(ts=10), snap(ts=3_600_010)])
    source = ParquetReplaySource(tmp_path, start_ms=3_600_000, end_ms=3_600_020)
    opened = []
    original = source._file_rows

    def tracking(path, batch_size=128):
        opened.append(path)
        return original(path, batch_size)

    monkeypatch.setattr(source, "_file_rows", tracking)
    assert [x["recv_ts_ms"] for x in source.events()] == [3_600_010]
    assert len(opened) == 1 and "hour=01" in opened[0].parts


def test_replay_speed_uses_replay_time_without_real_sleep(tmp_path, monkeypatch):
    store = ParquetEventStore(tmp_path)
    store.write_batch([snap(ts=0), snap(ts=100)])
    sleeps = []
    monkeypatch.setattr("sim_exchange.sources.time.monotonic", lambda: 1_000.0)
    monkeypatch.setattr("sim_exchange.sources.time.sleep", sleeps.append)
    source = ParquetReplaySource(tmp_path, speed=2)
    assert len(list(source.events())) == 2
    assert len(sleeps) == 1 and abs(sleeps[0] - 0.05) < 1e-9


def test_live_message_uses_same_unified_trade_schema():
    raw = ('{"stream":"btcusdt@trade","data":'
           '{"E":1,"T":2,"p":"100","q":"0.2","m":false}}')
    event = message_to_event("usdm", "BTCUSDT", raw, 3)
    assert event == {"market": "usdm", "symbol": "BTCUSDT", "event_type": "trade",
                     "recv_ts_ms": 3, "event_ts_ms": 1, "trade_ts_ms": 2,
                     "p": "100", "q": "0.2", "m": False}


def test_live_source_contract_runs_without_changing_exchange_or_strategy():
    class FakeLiveSource(BinanceWebSocketSource):
        def __init__(self):
            pass  # 不建立网络连接，只验证实时源的同步 events() 桥接契约。

        async def _stream(self):
            yield snap()
            yield depth()

    strategy = OneSecondMetricsStrategy(verbose=False)
    ex = SimExchange(FakeLiveSource())
    ex.subscribe(strategy, ["spot.BTCUSDT.book"])
    stats = ex.run()
    assert stats.callback_errors == 0
    assert ex.book("spot", "BTCUSDT")["last_update_id"] == 101


def test_unknown_depth_is_extended_only_after_comparison():
    class Source:
        def events(self):
            yield snap()
            extended = snap(ts=1)
            extended["asks"].append([103, 2])
            yield extended
            yield depth(ts=2)
            extended = snap(ts=3, uid=101)
            extended["bids"][0][1] = 2.5
            extended["asks"].append([103, 2])
            yield extended
    ex = SimExchange(Source())
    stats = ex.run()
    assert stats.snapshot_comparisons == 2
    assert stats.exact_matches == 1  # 第一次必须先记录不一致，不能抄快照后再比较。
    assert stats.coverage_levels_added == 1
    assert ex.book("spot", "BTCUSDT")["asks"][-1] == (103.0, 2.0)


def test_coverage_extension_does_not_repair_known_range_errors():
    book = SyncedOrderBook("spot", "BTCUSDT")
    book.apply_snapshot(snap())
    bad = snap()
    bad["asks"] = [[101, 9], [102, 4], [103, 2]]
    assert book.extend_snapshot_coverage(bad) == 1
    assert book.asks[101] == 1  # 已知范围内错量必须继续暴露。
    assert book.asks[103] == 2
    book.asks.pop(102)
    assert book.extend_snapshot_coverage(bad) == 0
    assert 102 not in book.asks  # 已知范围内缺失也不能用快照悄悄修掉。


def test_future_snapshot_cannot_extend_current_book():
    book = SyncedOrderBook("spot", "BTCUSDT")
    book.apply_snapshot(snap())
    future = snap(uid=101)
    future["asks"].append([103, 2])
    import pytest
    with pytest.raises(ValueError):
        book.extend_snapshot_coverage(future)
    assert 103 not in book.asks


def test_unknown_bid_extension_keeps_known_quantities():
    book = SyncedOrderBook("spot", "BTCUSDT")
    book.apply_snapshot(snap())
    extended = snap()
    extended["bids"] = [[99, 7], [98, 3], [97, 4]]
    assert book.extend_snapshot_coverage(extended) == 1
    assert book.bids[99] == 2 and book.bids[97] == 4
    assert book.known_bid_floor == 97


def test_no_comparisons_is_not_a_perfect_consistency():
    class Source:
        def events(self):
            yield snap()
    assert SimExchange(Source()).run().consistency is None


def test_usdm_numeric_jump_is_not_a_gap_but_pu_mismatch_is():
    book = SyncedOrderBook("usdm", "BTCUSDT")
    book.apply_snapshot(snap("usdm"))
    assert book.apply_depth(depth("usdm", uid=101, pu=95)) == "ok"
    event = depth("usdm", uid=110, pu=101)
    event["U"] = 105
    assert book.apply_depth(event) == "ok"
    event = depth("usdm", uid=112, pu=109)
    event["U"] = 110  # 即使编号区间覆盖下一号，pu不等也不能兜底。
    assert book.apply_depth(event) == "gap"


def test_initial_spot_boundary_uses_conservative_resnapshot_policy():
    book = SyncedOrderBook("spot", "BTCUSDT")
    book.apply_snapshot(snap())
    event = depth(uid=102)
    event["U"] = 101
    assert book.apply_depth(event) == "gap"
    assert not book.synced


def test_live_usdm_merges_separate_public_and_market_connections(monkeypatch):
    import asyncio
    import json
    import websockets.asyncio.client
    opened = []

    class Connection:
        def __init__(self, url):
            self.url, self.sent = url, False
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
        async def recv(self):
            if self.sent:
                await asyncio.Event().wait()
            self.sent = True
            if "/public/" in self.url:
                return json.dumps({"stream": "btcusdt@depth@100ms", "data":
                    {"E": 1, "U": 100, "u": 101, "pu": 99, "b": [[99, 2]], "a": []}})
            return json.dumps({"stream": "btcusdt@aggTrade", "data":
                {"E": 2, "T": 2, "p": "101", "q": "0.2", "m": False}})

    def connect(url, **kwargs):
        opened.append(url)
        return Connection(url)
    monkeypatch.setattr(websockets.asyncio.client, "connect", connect)
    monkeypatch.setattr("sim_exchange.live_source.fetch_snapshot", lambda *args: snap("usdm", ts=3))

    async def collect():
        stream = BinanceWebSocketSource("usdm", duration_seconds=5)._stream()
        events = []
        try:
            async for event in stream:
                events.append(event)
                if len(events) == 3:
                    break
        finally:
            await stream.aclose()
        return events

    events = asyncio.run(collect())
    assert len(opened) == 2
    assert {event["event_type"] for event in events} == {"snapshot", "depth", "trade"}
    assert "/public/" in opened[0] and "/market/" in opened[1]
    received_times = [event["recv_ts_ms"] for event in events]
    assert received_times == sorted(received_times)


def test_validator_reads_each_event_stream_once(tmp_path, monkeypatch):
    import validate_replay
    calls = []
    class Source:
        def __init__(self, root):
            pass
        def events(self):
            calls.append(True)
            yield snap()
            yield depth()
    monkeypatch.setattr(validate_replay, "ParquetReplaySource", Source)
    result = validate_replay.validate("unused", str(tmp_path / "report.json"))
    assert len(calls) == 1
    assert result["event_counts"] == {"spot.BTCUSDT.depth": 1, "spot.BTCUSDT.snapshot": 1}
    assert result["final_synced"]["spot.BTCUSDT"]


def test_recording_terminal_snapshot_delegates_to_live_implementation(monkeypatch):
    import record_market
    calls = []
    expected = snap()
    def fetch(*args):
        calls.append(args)
        return expected
    monkeypatch.setattr(record_market, "fetch_snapshot", fetch)
    assert record_market.snapshot("spot", "BTCUSDT", "https://example.invalid", "proxy") is expected
    assert calls == [("spot", "BTCUSDT", "https://example.invalid", "proxy")]


def test_capture_forwards_configured_proxy_and_finalizes_once(tmp_path, monkeypatch):
    import capture_24h
    import record_market
    import finalize_lesson2
    seen, states, finalizations = [], [], []
    async def record(markets, symbol, hours, store, proxy):
        seen.append((markets, symbol, hours, proxy))
    def finalize(root):
        finalizations.append(root)
        return {"status": "passed"}
    monkeypatch.setattr(capture_24h, "PROJECT", tmp_path)
    monkeypatch.setattr(capture_24h, "write_status", lambda state, **kwargs: states.append(state))
    monkeypatch.setattr(record_market, "run_all", record)
    monkeypatch.setattr(finalize_lesson2, "validate", finalize)
    monkeypatch.setenv("BINANCE_PROXY", "http://configured.invalid:1234")
    capture_24h.run(data_root="data/new_run")
    assert seen[0][-1] == "http://configured.invalid:1234"
    assert states == ["running", "completed", "succeeded"]
    assert len(finalizations) == 1
    capture_24h.run(proxy="http://explicit.invalid:5678", data_root="data/another_run")
    assert seen[-1][-1] == "http://explicit.invalid:5678"


def test_capture_refuses_existing_data_directory(tmp_path):
    import capture_24h
    import pytest
    (tmp_path / "existing.parquet").write_bytes(b"original evidence")
    with pytest.raises(ValueError, match="新目录"):
        capture_24h.run(data_root=str(tmp_path))
    assert (tmp_path / "existing.parquet").read_bytes() == b"original evidence"


def test_original_data_fingerprint_is_reproducible_and_detects_tampering(tmp_path):
    import hashlib
    import pytest
    from fingerprint_data import fingerprint, verify
    (tmp_path / "b.parquet").write_bytes(b"second")
    (tmp_path / "a.parquet").write_bytes(b"first")
    report = fingerprint(tmp_path)
    expected_bytes = b"".join(
        f"{item['file']}\0{item['bytes']}\0{item['sha256']}\n".encode()
        for item in report["manifest"])
    assert report["summary"]["manifest_sha256"] == hashlib.sha256(expected_bytes).hexdigest()
    verify(fingerprint(tmp_path), report)
    (tmp_path / "a.parquet").write_bytes(b"changed")
    with pytest.raises(ValueError):
        verify(fingerprint(tmp_path), report)

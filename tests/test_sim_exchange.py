from __future__ import annotations

import pyarrow.parquet as pq

from sim_exchange import ParquetEventStore, ParquetReplaySource, SimExchange
from sim_exchange.orderbook import SyncedOrderBook
from sim_exchange.strategies import OneSecondMetricsStrategy
from record_market import endpoints


def test_market_specific_trade_streams():
    spot_url, _ = endpoints("spot", "BTCUSDT")
    usdm_url, _ = endpoints("usdm", "BTCUSDT")
    assert "btcusdt@aggTrade" in spot_url
    assert "stream.binance.com:443" in spot_url
    assert ":9443" not in spot_url
    assert "btcusdt@trade" in usdm_url
    assert "btcusdt@aggTrade" not in usdm_url


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

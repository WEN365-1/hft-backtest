from __future__ import annotations

import pyarrow.parquet as pq

from sim_exchange import ParquetEventStore, ParquetReplaySource, SimExchange
from sim_exchange.orderbook import SyncedOrderBook
from sim_exchange.strategies import OneSecondMetricsStrategy


def snap(market="spot", ts=0, uid=100):
    return {"market": market, "symbol": "BTCUSDT", "event_type": "snapshot",
            "recv_ts_ms": ts, "last_update_id": uid,
            "bids": [[99, 2], [98, 3]], "asks": [[101, 1], [102, 4]]}


def depth(market="spot", ts=1, uid=101, pu=None):
    return {"market": market, "symbol": "BTCUSDT", "event_type": "depth",
            "recv_ts_ms": ts, "U": uid, "u": uid, "pu": uid - 1 if pu is None else pu,
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
    assert book.apply_depth(depth(uid=100)) == "stale"
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

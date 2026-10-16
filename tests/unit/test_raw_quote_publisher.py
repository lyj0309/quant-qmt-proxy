"""Cold baseline, bounded bursts and source generations without QMT."""

from typing import cast

import grpc
import pytest

from app.services.raw_quote_publisher import (
    HEADER,
    MAGIC,
    ROW,
    NativeBatch,
    QueueLimits,
    RawQuotePublisher,
)


class Context:
    def is_active(self) -> bool:
        return True

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        raise RuntimeError(f"{code}: {details}")


def quote(symbols: tuple[str, ...] = ("600000.SH",), volume: int = 10) -> NativeBatch:
    return {
        symbol: {"time": 1788840000000, "volume": volume, "amount": volume * 10.0}
        for symbol in symbols
    }


def context() -> grpc.ServicerContext:
    return cast(grpc.ServicerContext, Context())


def test_callback_is_not_a_complete_baseline() -> None:
    publisher = RawQuotePublisher()
    with pytest.raises(RuntimeError, match="explicit snapshot"):
        publisher.publish(quote(), 1)
    with pytest.raises(ValueError, match="missing required symbols"):
        publisher.initialize(quote(), 1, ("600000.SH", "000001.SZ"))
    assert not publisher.stats().ready
    publisher.initialize(quote(("600000.SH", "000001.SZ")), 1)
    with pytest.raises(RuntimeError, match="no baseline"):
        next(publisher.stream(MAGIC, context()))
    publisher.publish(quote(volume=11), 2)  # callback during subscription setup
    publisher.activate()
    stream = publisher.stream(MAGIC, context())
    try:
        baseline = next(stream)
        assert HEADER.unpack_from(baseline)[-1] == 2
        assert ROW.unpack_from(baseline, HEADER.size)[5] == 11
    finally:
        stream.close()


def test_burst_larger_than_old_four_batch_queue_is_lossless() -> None:
    publisher = RawQuotePublisher()
    publisher.initialize(quote(), 1)
    publisher.activate()
    stream = publisher.stream(MAGIC, context())
    try:
        next(stream)
        for index in range(64):
            publisher.publish(quote(volume=20 + index), index + 2)
        assert publisher.stats().peak_consumer_batches == 64
        assert [HEADER.unpack_from(next(stream))[3] for _ in range(64)] == list(range(1, 65))
        assert publisher.stats().pending_bytes == 0
        assert publisher.stats().overflows == 0
    finally:
        stream.close()


@pytest.mark.parametrize(
    "limits", [QueueLimits(max_batches=2), QueueLimits(max_bytes=2 * (HEADER.size + ROW.size))]
)
def test_each_limit_fails_closed_and_reconnects_with_latest_cache(limits: QueueLimits) -> None:
    publisher = RawQuotePublisher(limits)
    publisher.initialize(quote(), 1)
    publisher.activate()
    stream = publisher.stream(MAGIC, context())
    next(stream)
    for volume in range(11, 14):
        publisher.publish(quote(volume=volume), volume)
    assert publisher.stats().overflows == 1
    assert publisher.stats().pending_bytes == 0
    with pytest.raises(RuntimeError, match="queue overflow"):
        next(stream)
    recovered = publisher.stream(MAGIC, context())
    try:
        baseline = next(recovered)
        assert HEADER.unpack_from(baseline)[3] == 3
        assert ROW.unpack_from(baseline, HEADER.size)[5] == 13
    finally:
        recovered.close()


def test_late_retired_source_callback_cannot_modify_new_epoch() -> None:
    publisher = RawQuotePublisher()
    retired = publisher.initialize(quote(), 1)
    publisher.activate()
    publisher.invalidate()
    current = publisher.initialize(quote(volume=30), 2)
    publisher.activate()
    assert retired != current
    publisher.publish(quote(volume=999), 3, retired)
    stream = publisher.stream(MAGIC, context())
    try:
        baseline = next(stream)
        assert HEADER.unpack_from(baseline)[3] == 0
        assert ROW.unpack_from(baseline, HEADER.size)[5] == 30
    finally:
        stream.close()


def test_rejected_receipt_requires_explicit_reinitialization() -> None:
    publisher = RawQuotePublisher()
    publisher.initialize(quote(), 1)
    publisher.activate()
    with pytest.raises(ValueError, match="receipt"):
        publisher.publish(quote(), 0)
    assert not publisher.stats().ready
    with pytest.raises(RuntimeError, match="explicit snapshot"):
        publisher.publish(quote(), 3)


@pytest.mark.parametrize("bad_price", [float("inf"), float("-inf"), float("nan"), "bad"])
def test_bad_snapshot_row_is_quarantined_not_fabricated(bad_price: float | str) -> None:
    publisher = RawQuotePublisher()
    snapshot = dict(quote())
    snapshot["110075.SH"] = {"time": 1788840000000, "lastPrice": bad_price}
    publisher.initialize(snapshot, 1)
    publisher.activate()
    assert publisher.stats().quarantined_rows == 1
    stream = publisher.stream(MAGIC, context())
    try:
        frame = next(stream)
        assert HEADER.unpack_from(frame)[-1] == 1
        assert ROW.unpack_from(frame, HEADER.size)[0].rstrip(b"\0") == b"600000.SH"
    finally:
        stream.close()
    with pytest.raises(ValueError, match="invalid required symbols"):
        publisher.initialize(snapshot, 2, ("110075.SH",))
    assert not publisher.stats().ready


def test_quarantine_transitions_reseed_and_native_callback_generation_survives() -> None:
    publisher = RawQuotePublisher()
    epoch = publisher.initialize(quote(("600000.SH", "000001.SZ")), 1)
    publisher.activate()
    stream = publisher.stream(MAGIC, context())
    first = next(stream)
    bad: NativeBatch = {"600000.SH": {"time": 1788840000000, "lastPrice": float("inf")}}
    publisher.publish(bad, 2, epoch)
    with pytest.raises(RuntimeError, match="source reset"):
        next(stream)
    assert publisher.stats().ready
    assert publisher.stats().baseline_rows == 1
    stream = publisher.stream(MAGIC, context())
    baseline = next(stream)
    assert HEADER.unpack_from(first)[2] != HEADER.unpack_from(baseline)[2]
    publisher.publish(bad, 3, epoch)
    assert HEADER.unpack_from(next(stream))[-1] == 0  # no repeated reset
    publisher.publish(quote(volume=99), 4, epoch)
    recovered = next(stream)
    assert HEADER.unpack_from(recovered)[2] == HEADER.unpack_from(baseline)[2]
    assert ROW.unpack_from(recovered, HEADER.size)[5] == 99
    assert publisher.stats().quarantined_rows == 0
    try:
        publisher.publish(quote(volume=100), 5, epoch)
        update = next(stream)
        assert ROW.unpack_from(update, HEADER.size)[5] == 100
    finally:
        stream.close()


def test_all_invalid_snapshot_stays_unavailable() -> None:
    publisher = RawQuotePublisher()
    with pytest.raises(ValueError, match="no valid rows"):
        publisher.initialize({"600000.SH": {"time": 0}}, 1)
    assert not publisher.stats().ready


@pytest.mark.parametrize(("batches", "byte_limit"), [(0, 1024), (1, HEADER.size - 1)])
def test_invalid_queue_limits(batches: int, byte_limit: int) -> None:
    with pytest.raises(ValueError, match="queue limits"):
        QueueLimits(batches, byte_limit)

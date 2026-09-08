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


def test_rejected_source_requires_explicit_reinitialization() -> None:
    publisher = RawQuotePublisher()
    publisher.initialize(quote(), 1)
    publisher.activate()
    with pytest.raises(ValueError, match="native time"):
        publisher.publish({"600000.SH": {"time": 0}}, 2)
    assert not publisher.stats().ready
    with pytest.raises(RuntimeError, match="explicit snapshot"):
        publisher.publish(quote(), 3)


@pytest.mark.parametrize(("batches", "byte_limit"), [(0, 1024), (1, HEADER.size - 1)])
def test_invalid_queue_limits(batches: int, byte_limit: int) -> None:
    with pytest.raises(ValueError, match="queue limits"):
        QueueLimits(batches, byte_limit)

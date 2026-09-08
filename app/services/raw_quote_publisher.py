"""Bounded immutable native-callback frames for authenticated raw gRPC streams."""

from __future__ import annotations

import math
import struct
import threading
import time
import uuid

from collections import deque
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum, IntEnum

import grpc

MAGIC = b"QBR1"
HEADER = struct.Struct("<4sB3x16sQQI")
ROW = struct.Struct("<16sqqqqqdI5q5q5q5q")
MAX_ROWS = 65536
NativeValue = int | float | str | list[int] | list[float] | tuple[int, ...] | tuple[float, ...]
NativeBatch = Mapping[str, Mapping[str, NativeValue]]


class BatchKind(IntEnum):
    BASELINE = 1
    UPDATE = 2
    HEARTBEAT = 3


class SourcePhase(Enum):
    EMPTY = "empty"
    PREPARED = "prepared"
    LIVE = "live"


class StreamFailure(Enum):
    OVERFLOW = "queue overflow"
    RESET = "source reset"


@dataclass(frozen=True)
class QueueLimits:
    """Bound pending data, excluding one in-flight frame and transport buffers."""

    max_batches: int = 128
    max_bytes: int = 32 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.max_batches < 1 or self.max_bytes < HEADER.size:
            raise ValueError("invalid raw quote queue limits")


@dataclass(frozen=True)
class PublisherStats:
    ready: bool
    baseline_rows: int
    consumers: int
    pending_batches: int
    pending_bytes: int
    peak_consumer_batches: int
    peak_consumer_bytes: int
    overflows: int


@dataclass(eq=False)
class Consumer:
    """All fields are protected by the publisher condition lock."""

    pending: deque[bytes] = field(default_factory=deque)
    pending_bytes: int = 0
    failure: StreamFailure | None = None

    def fail(self, reason: StreamFailure) -> None:
        self.failure = reason
        self.pending.clear()
        self.pending_bytes = 0


def number(row: Mapping[str, NativeValue], key: str) -> float:
    value = row.get(key, 0)
    if not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"invalid native quote field: {key}")
    return float(value)


def levels(row: Mapping[str, NativeValue], key: str, scale: int) -> tuple[int, ...]:
    value = row.get(key, ())
    if not isinstance(value, (tuple, list)) or len(value) > 5:
        raise ValueError(f"invalid native quote levels: {key}")
    return tuple(round(item * scale) for item in value) + (0,) * (5 - len(value))


def encode_row(symbol: str, row: Mapping[str, NativeValue], received: int) -> bytes:
    """Copy native callback data before returning ownership to xtdata."""
    encoded = symbol.upper().encode("ascii")
    if not 0 < len(encoded) < 16 or b"\x00" in encoded:
        raise ValueError("invalid native quote symbol")
    timestamp_ms = int(number(row, "time"))
    volume = number(row, "volume")
    amount = number(row, "amount")
    if not 1_000_000_000_000 <= timestamp_ms < 10_000_000_000_000 or received <= 0:
        raise ValueError("native time must be epoch milliseconds with a valid receipt")
    if volume < 0 or amount < 0:
        raise ValueError("invalid native timestamp or cumulative counters")
    return ROW.pack(
        encoded,
        timestamp_ms * 1_000_000,
        received,
        round(number(row, "lastPrice") * 1e6),
        round(number(row, "lastClose") * 1e6),
        int(volume),
        amount,
        int(number(row, "stockStatus")),
        *levels(row, "bidPrice", 1_000_000),
        *levels(row, "askPrice", 1_000_000),
        *levels(row, "bidVol", 1),
        *levels(row, "askVol", 1),
    )


class RawQuotePublisher:
    """One callback sequence owner with atomic baseline/stream attachment."""

    def __init__(self, limits: QueueLimits | None = None) -> None:
        self._lock = threading.Condition()
        self._limits = limits or QueueLimits()
        self._epoch = uuid.uuid4().bytes
        self._sequence = 0
        self._latest: dict[str, bytes] = {}
        self._consumers: set[Consumer] = set()
        self._phase = SourcePhase.EMPTY
        self._peak_batches = 0
        self._peak_bytes = 0
        self._overflows = 0

    def stats(self) -> PublisherStats:
        with self._lock:
            return PublisherStats(
                self._phase is SourcePhase.LIVE,
                len(self._latest),
                len(self._consumers),
                sum(len(c.pending) for c in self._consumers),
                sum(c.pending_bytes for c in self._consumers),
                self._peak_batches,
                self._peak_bytes,
                self._overflows,
            )

    def initialize(
        self,
        snapshot: NativeBatch,
        received_at_ns: int,
        required_symbols: Sequence[str] = (),
    ) -> bytes:
        """Load an explicit snapshot BEFORE subscribing; callbacks cannot certify it.

        For explicit subscriptions every required symbol must exist. Market-wide
        callers use the SDK full-market snapshot, not the first callback's keys.
        Activation is separate so a failed subscription never publishes readiness.
        """
        self.invalidate()
        with self._lock:
            epoch = self._epoch
        if not snapshot or not set(required_symbols).issubset(snapshot):
            raise ValueError("raw quote snapshot empty or missing required symbols")
        if len(snapshot) > MAX_ROWS:
            raise ValueError("raw quote universe capacity exceeded")
        encoded = {
            symbol: encode_row(symbol, row, received_at_ns) for symbol, row in snapshot.items()
        }
        with self._lock:
            if self._epoch != epoch:
                raise RuntimeError("raw source reset during snapshot initialization")
            self._latest = encoded
            self._phase = SourcePhase.PREPARED
            return epoch

    def activate(self) -> None:
        """Open consumption only after snapshot load and successful subscription."""
        with self._lock:
            if self._phase is not SourcePhase.PREPARED:
                raise RuntimeError("raw quote source has no prepared baseline")
            self._phase = SourcePhase.LIVE

    def invalidate(self) -> None:
        """Discard incomplete state and force all clients to establish a baseline."""
        with self._lock:
            self._epoch = uuid.uuid4().bytes
            self._sequence = 0
            self._latest.clear()
            self._phase = SourcePhase.EMPTY
            for consumer in self._consumers:
                consumer.fail(StreamFailure.RESET)
            self._lock.notify_all()

    def publish(
        self, payload: NativeBatch, received_at_ns: int, epoch: bytes | None = None
    ) -> None:
        """Encode once per callback, sharing immutable bytes with all clients."""
        try:
            with self._lock:
                if epoch is not None and epoch != self._epoch:
                    return  # Late callback from a retired native subscription.
                if self._phase is SourcePhase.EMPTY:
                    raise RuntimeError("raw quote source requires explicit snapshot initialization")
                encoded = {
                    symbol: encode_row(symbol, row, received_at_ns)
                    for symbol, row in payload.items()
                }
                new_symbols = sum(symbol not in self._latest for symbol in encoded)
                if len(self._latest) + new_symbols > MAX_ROWS:
                    raise ValueError("raw quote universe capacity exceeded")
                self._latest.update(encoded)
                self._sequence += 1
                frame = self._frame(BatchKind.UPDATE, received_at_ns, tuple(encoded.values()))
                for consumer in self._consumers:
                    if consumer.failure is not None:
                        continue
                    if (
                        len(consumer.pending) >= self._limits.max_batches
                        or consumer.pending_bytes + len(frame) > self._limits.max_bytes
                    ):
                        consumer.fail(StreamFailure.OVERFLOW)
                        self._overflows += 1
                        continue
                    consumer.pending.append(frame)
                    consumer.pending_bytes += len(frame)
                    self._peak_batches = max(self._peak_batches, len(consumer.pending))
                    self._peak_bytes = max(self._peak_bytes, consumer.pending_bytes)
                self._lock.notify_all()
        except (ValueError, TypeError, OverflowError, struct.error):
            self.invalidate()
            raise

    def _frame(self, kind: BatchKind, received: int, rows: tuple[bytes, ...]) -> bytes:
        return HEADER.pack(
            MAGIC, kind, self._epoch, self._sequence, received, len(rows)
        ) + b"".join(rows)

    def stream(self, request: bytes, context: grpc.ServicerContext) -> Generator[bytes, None, None]:
        """Attach cursor and cache atomically; heartbeat proves transport liveness only."""
        if request != MAGIC:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unsupported raw quote version")
        consumer = Consumer()
        with self._lock:
            if self._phase is not SourcePhase.LIVE:
                context.abort(grpc.StatusCode.UNAVAILABLE, "raw quote source has no baseline")
            if len(self._consumers) >= 2:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "raw quote consumer limit")
            self._consumers.add(consumer)
            baseline = self._frame(BatchKind.BASELINE, time.time_ns(), tuple(self._latest.values()))
        try:
            yield baseline
            while context.is_active():
                with self._lock:
                    if not consumer.pending and consumer.failure is None:
                        self._lock.wait(timeout=1)
                    if consumer.failure is not None:
                        context.abort(
                            grpc.StatusCode.RESOURCE_EXHAUSTED
                            if consumer.failure is StreamFailure.OVERFLOW
                            else grpc.StatusCode.UNAVAILABLE,
                            f"raw quote continuity lost: {consumer.failure.value}",
                        )
                    if consumer.pending:
                        frame = consumer.pending.popleft()
                        consumer.pending_bytes -= len(frame)
                    else:
                        # Cursor and empty-queue observation share the publish lock.
                        frame = self._frame(BatchKind.HEARTBEAT, time.time_ns(), ())
                yield frame
        finally:
            with self._lock:
                self._consumers.discard(consumer)


def identity(payload: bytes) -> bytes:
    return payload

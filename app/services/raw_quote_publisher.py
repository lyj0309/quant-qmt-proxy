"""Bounded immutable native-callback frames for authenticated raw gRPC streams."""

from __future__ import annotations

import math
import queue
import struct
import threading
import time
import uuid

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from enum import IntEnum

import grpc

MAGIC = b"QBR1"
HEADER = struct.Struct("<4sB3x16sQQI")
ROW = struct.Struct("<16sqqqqqdI5q5q5q5q")
MAX_ROWS = 65536
NativeValue = int | float | list[int] | list[float] | tuple[int, ...] | tuple[float, ...]
NativeBatch = Mapping[str, Mapping[str, NativeValue]]


class BatchKind(IntEnum):
    BASELINE = 1
    UPDATE = 2
    HEARTBEAT = 3


@dataclass(eq=False)
class Consumer:
    """Overflow disconnects a consumer; it never silently skips callbacks."""

    pending: queue.Queue[bytes] = field(default_factory=lambda: queue.Queue(maxsize=4))
    failed: threading.Event = field(default_factory=threading.Event)


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

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._epoch = uuid.uuid4().bytes
        self._sequence = 0
        self._latest: dict[str, bytes] = {}
        self._consumers: set[Consumer] = set()
        self._ready = False

    def invalidate(self) -> None:
        """Discard incomplete state and force all clients to establish a baseline."""
        with self._lock:
            self._epoch = uuid.uuid4().bytes
            self._sequence = 0
            self._latest.clear()
            self._ready = False
            for consumer in self._consumers:
                consumer.failed.set()

    def publish(self, payload: NativeBatch, received_at_ns: int) -> None:
        """Encode once per callback, sharing immutable bytes with all clients."""
        try:
            with self._lock:
                encoded = {
                    symbol: encode_row(symbol, row, received_at_ns)
                    for symbol, row in payload.items()
                }
                new_symbols = sum(symbol not in self._latest for symbol in encoded)
                if len(self._latest) + new_symbols > MAX_ROWS:
                    raise ValueError("raw quote universe capacity exceeded")
                self._latest.update(encoded)
                self._sequence += 1
                self._ready = True
                frame = self._frame(BatchKind.UPDATE, received_at_ns, tuple(encoded.values()))
                for consumer in self._consumers:
                    try:
                        consumer.pending.put_nowait(frame)
                    except queue.Full:
                        consumer.failed.set()
        except (ValueError, TypeError, OverflowError, struct.error):
            self.invalidate()
            raise

    def _frame(self, kind: BatchKind, received: int, rows: tuple[bytes, ...]) -> bytes:
        return HEADER.pack(
            MAGIC, kind, self._epoch, self._sequence, received, len(rows)
        ) + b"".join(rows)

    def stream(self, request: bytes, context: grpc.ServicerContext) -> Iterator[bytes]:
        """Attach cursor and cache atomically; heartbeat proves transport liveness only."""
        if request != MAGIC:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unsupported raw quote version")
        consumer = Consumer()
        with self._lock:
            if not self._ready:
                context.abort(grpc.StatusCode.UNAVAILABLE, "raw quote source has no baseline")
            if len(self._consumers) >= 2:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "raw quote consumer limit")
            self._consumers.add(consumer)
            baseline = self._frame(BatchKind.BASELINE, time.time_ns(), tuple(self._latest.values()))
        try:
            yield baseline
            while context.is_active():
                if consumer.failed.is_set():
                    context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "raw quote continuity lost")
                try:
                    frame = consumer.pending.get(timeout=1)
                except queue.Empty:
                    with self._lock:
                        # Queue inspection and heartbeat cursor must be atomic
                        # with publication, otherwise a heartbeat could skip data.
                        if not consumer.pending.empty():
                            continue
                        frame = self._frame(BatchKind.HEARTBEAT, time.time_ns(), ())
                if consumer.failed.is_set():
                    context.abort(grpc.StatusCode.UNAVAILABLE, "raw source reset")
                yield frame
        finally:
            with self._lock:
                self._consumers.discard(consumer)


def identity(payload: bytes) -> bytes:
    return payload

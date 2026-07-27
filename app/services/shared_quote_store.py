from __future__ import annotations

import mmap
import os
import struct
import threading
import time

from collections.abc import Callable
from pathlib import Path
from typing import Any

MAGIC = b"QMTQSHM1"
SCHEMA_VERSION = 1
HEADER_SIZE = 4096
SLOT_SIZE = 512
SYMBOL_SIZE = 16
PRICE_SCALE = 1_000_000

HEADER_STRUCT = struct.Struct("<8sIIIIQQQQ")
SLOT_HEADER_STRUCT = struct.Struct("<16sQ")
QUOTE_STRUCT = struct.Struct("<qqqqqdI5q5q5q5q")
QUOTE_COPY_SIZE = 224

STATE_INITIALIZING = 0
STATE_READY = 1


def _price_units(value: Any) -> int:
    try:
        return round(float(value or 0.0) * PRICE_SCALE)
    except (TypeError, ValueError, OverflowError):
        return 0


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError, OverflowError):
        return 0


def _levels(
    values: Any,
    converter: Callable[[Any], int] | None = None,
) -> tuple[int, ...]:
    items = values if isinstance(values, list | tuple) else ()
    convert = converter or _price_units
    normalized = [convert(item) for item in items[:5]]
    normalized.extend([0] * (5 - len(normalized)))
    return tuple(normalized)


class SharedQuoteStore:
    """Single-writer latest-quote table backed by a fixed-size mmap file."""

    def __init__(self, path: Path, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("shared quote capacity must be positive")
        self.path = path
        self.capacity = capacity
        self.generation = time.time_ns()
        self._lock = threading.Lock()
        self._symbols: dict[str, int] = {}
        self._versions: list[int] = [0] * capacity
        self._closed = False

        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        self._file = temporary_path.open("w+b")
        self._file.truncate(HEADER_SIZE + capacity * SLOT_SIZE)
        self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_WRITE)
        self._write_header(state=STATE_INITIALIZING, quote_count=0)
        self._mmap.flush()
        self._mmap.close()
        self._file.close()
        os.replace(temporary_path, path)
        self._file = path.open("r+b")
        self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_WRITE)

    def mark_ready(self) -> None:
        with self._lock:
            self._write_header(state=STATE_READY, quote_count=len(self._symbols))

    def heartbeat(self) -> None:
        with self._lock:
            if not self._closed:
                self._write_header(
                    state=STATE_READY,
                    quote_count=len(self._symbols),
                )

    def write(self, symbol: str, event_time_ms: int, data: dict[str, Any]) -> bool:
        normalized_symbol = symbol.strip().upper()
        encoded_symbol = normalized_symbol.encode("ascii")
        if not normalized_symbol or len(encoded_symbol) >= SYMBOL_SIZE:
            return False

        with self._lock:
            if self._closed:
                return False
            slot_index = self._symbols.get(normalized_symbol)
            added_symbol = slot_index is None
            if slot_index is None:
                slot_index = len(self._symbols)
                if slot_index >= self.capacity:
                    return False
                self._symbols[normalized_symbol] = slot_index
                slot_offset = HEADER_SIZE + slot_index * SLOT_SIZE
                self._mmap[slot_offset : slot_offset + SYMBOL_SIZE] = encoded_symbol.ljust(
                    SYMBOL_SIZE, b"\0"
                )

            next_version = self._versions[slot_index] + 1
            copy_index = next_version & 1
            slot_offset = HEADER_SIZE + slot_index * SLOT_SIZE
            payload_offset = (
                slot_offset + SLOT_HEADER_STRUCT.size + copy_index * QUOTE_COPY_SIZE
            )
            payload = QUOTE_STRUCT.pack(
                _integer(data.get("time_ms") or event_time_ms) * 1_000_000,
                time.time_ns(),
                _price_units(data.get("last_price")),
                _price_units(data.get("last_close")),
                _integer(data.get("volume")),
                float(data.get("amount") or 0.0),
                _integer(data.get("stock_status")),
                *_levels(data.get("bid_price")),
                *_levels(data.get("ask_price")),
                *_levels(data.get("bid_vol"), _integer),
                *_levels(data.get("ask_vol"), _integer),
            )
            self._mmap[payload_offset : payload_offset + len(payload)] = payload
            struct.pack_into("<Q", self._mmap, slot_offset + SYMBOL_SIZE, next_version)
            self._versions[slot_index] = next_version
            if added_symbol:
                self._write_header(state=STATE_READY, quote_count=len(self._symbols))
            return True

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._mmap.flush()
            self._mmap.close()
            self._file.close()

    def _write_header(self, *, state: int, quote_count: int) -> None:
        HEADER_STRUCT.pack_into(
            self._mmap,
            0,
            MAGIC,
            SCHEMA_VERSION,
            HEADER_SIZE,
            SLOT_SIZE,
            self.capacity,
            self.generation,
            time.time_ns(),
            os.getpid(),
            (state << 32) | quote_count,
        )

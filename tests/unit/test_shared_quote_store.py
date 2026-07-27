from __future__ import annotations

import mmap
import struct

from pathlib import Path

from app.services.shared_quote_store import (
    HEADER_SIZE,
    HEADER_STRUCT,
    MAGIC,
    PRICE_SCALE,
    QUOTE_COPY_SIZE,
    QUOTE_STRUCT,
    SCHEMA_VERSION,
    SLOT_HEADER_STRUCT,
    SLOT_SIZE,
    STATE_READY,
    SharedQuoteStore,
)


def _read_quote(path: Path) -> tuple[str, int, tuple[int | float, ...]]:
    with path.open("rb") as handle:
        mapped = mmap.mmap(handle.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            header = HEADER_STRUCT.unpack_from(mapped, 0)
            assert header[0] == MAGIC
            assert header[1] == SCHEMA_VERSION
            assert header[2] == HEADER_SIZE
            assert header[3] == SLOT_SIZE
            state_and_count = header[8]
            assert state_and_count >> 32 == STATE_READY
            assert state_and_count & 0xFFFFFFFF == 1

            slot_offset = HEADER_SIZE
            raw_symbol, version_before = SLOT_HEADER_STRUCT.unpack_from(
                mapped, slot_offset
            )
            copy_index = version_before & 1
            payload_offset = (
                slot_offset + SLOT_HEADER_STRUCT.size + copy_index * QUOTE_COPY_SIZE
            )
            payload = QUOTE_STRUCT.unpack_from(mapped, payload_offset)
            version_after = struct.unpack_from("<Q", mapped, slot_offset + 16)[0]
            assert version_before == version_after
            return (
                raw_symbol.rstrip(b"\0").decode("ascii"),
                version_after,
                payload,
            )
        finally:
            mapped.close()


def test_shared_quote_store_overwrites_latest_slot(tmp_path: Path) -> None:
    path = tmp_path / "quotes.mmap"
    store = SharedQuoteStore(path, capacity=8)
    store.mark_ready()
    store.write(
        "110084.SH",
        1_700_000_000_000,
        {
            "time_ms": 1_700_000_000_001,
            "last_price": 121.123,
            "last_close": 120.0,
            "volume": 1234,
            "amount": 456789.5,
            "stock_status": 7,
            "bid_price": [121.122, 121.121],
            "ask_price": [121.124, 121.125],
            "bid_vol": [10, 20],
            "ask_vol": [30, 40],
        },
    )
    store.write(
        "110084.SH",
        1_700_000_000_002,
        {
            "last_price": 121.456,
            "last_close": 120.0,
            "volume": 1300,
            "amount": 500000.0,
            "bid_price": [121.455],
            "ask_price": [121.457],
        },
    )

    symbol, version, payload = _read_quote(path)
    store.close()

    assert symbol == "110084.SH"
    assert version == 2
    assert payload[2] == round(121.456 * PRICE_SCALE)
    assert payload[3] == 120 * PRICE_SCALE
    assert payload[4] == 1300
    assert payload[7] == round(121.455 * PRICE_SCALE)
    assert payload[12] == round(121.457 * PRICE_SCALE)


def test_shared_quote_store_reports_capacity_overflow(tmp_path: Path) -> None:
    store = SharedQuoteStore(tmp_path / "quotes.mmap", capacity=1)
    store.write("000001.SZ", 1, {})

    written = store.write("600000.SH", 2, {})
    store.close()

    assert written is False


def test_shared_quote_store_rejects_writes_after_close(tmp_path: Path) -> None:
    store = SharedQuoteStore(tmp_path / "quotes.mmap", capacity=1)
    store.close()

    assert store.write("000001.SZ", 1, {}) is False

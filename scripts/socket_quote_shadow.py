"""Loopback TCP shadow of native QMT batches; no trading service."""

import json
import socket
import struct
import sys
import threading
import time

from pathlib import Path
from typing import cast

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from xtquant import xtdata

from app.services.raw_quote_publisher import MAGIC, NativeBatch, RawQuotePublisher


class Context:
    """Adapter for the publisher's stream contract without a gRPC network stack."""

    def __init__(self, stop: threading.Event) -> None:
        self.stop = stop

    def is_active(self) -> bool:
        return not self.stop.is_set()

    def abort(self, code: grpc.StatusCode, details: str) -> None:
        raise RuntimeError(f"{code}: {details}")


def main() -> None:
    publisher = RawQuotePublisher()
    counts: list[int] = []
    offsets: list[float] = []
    encodes: list[float] = []
    failures: list[str] = []
    stop = threading.Event()
    wall_start, cpu_start = time.monotonic(), time.process_time()

    def callback(payload: NativeBatch) -> None:
        started = time.monotonic()
        try:
            publisher.publish(payload, time.time_ns())
            counts.append(len(payload))
            offsets.append(started - wall_start)
            encodes.append((time.monotonic() - started) * 1000)
        except (ValueError, TypeError, OverflowError, struct.error) as exc:
            failures.append(str(exc))

    xtdata.enable_hello = False
    subscription = xtdata.subscribe_whole_quote(["SH", "SZ"], callback=callback)
    if subscription < 0:
        raise RuntimeError("shadow subscription failed")
    threading.Thread(target=xtdata.run, daemon=True).start()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 50060))
    listener.listen(1)
    listener.settimeout(1)

    def serve() -> None:
        while not stop.is_set():
            try:
                conn, _ = listener.accept()
            except TimeoutError:
                continue
            with conn:
                conn.settimeout(5)
                stream = publisher.stream(MAGIC, cast(grpc.ServicerContext, Context(stop)))
                try:
                    for frame in stream:
                        conn.sendall(struct.pack("<I", len(frame)))
                        conn.sendall(frame)
                except OSError:
                    pass  # Normal reader shutdown ends this diagnostic connection.
                except RuntimeError as exc:
                    failures.append(str(exc))
                finally:
                    stream.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    print("SOCKET_SHADOW_READY", flush=True)
    try:
        time.sleep(45)
    finally:
        stop.set()
        xtdata.unsubscribe_quote(subscription)
        thread.join(timeout=6)
        listener.close()
    print(
        json.dumps(
            {
                "batches": len(counts),
                "max_rows": max(counts, default=0),
                "rows_per_batch": counts,
                "callback_offsets_sec": offsets,
                "encode_ms": encodes,
                "errors": failures[:5],
                "cpu_core_pct": 100
                * (time.process_time() - cpu_start)
                / (time.monotonic() - wall_start),
            }
        )
    )


if __name__ == "__main__":
    main()

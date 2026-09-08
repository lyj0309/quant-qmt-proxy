"""Temporary loopback-only quote shadow; no trading service or broker session."""

import json
import sys
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from xtquant import xtdata

from app.services.raw_quote_publisher import NativeBatch, RawQuotePublisher, identity


def main() -> None:
    publisher = RawQuotePublisher()
    errors: list[str] = []
    counts: list[int] = []
    arrivals: list[float] = []
    encodes: list[float] = []
    wall_start, cpu_start = time.monotonic(), time.process_time()

    def callback(payload: NativeBatch) -> None:
        arrived = time.monotonic()
        try:
            publisher.publish(payload, time.time_ns())
            counts.append(len(payload))
            arrivals.append(arrived)
            encodes.append((time.monotonic() - arrived) * 1000)
        except (ValueError, TypeError, OverflowError) as exc:
            errors.append(str(exc))

    xtdata.enable_hello = False
    subscription = xtdata.subscribe_whole_quote(["SH", "SZ"], callback=callback)
    if subscription < 0:
        raise RuntimeError("shadow subscription failed")
    threading.Thread(target=xtdata.run, daemon=True).start()
    with ThreadPoolExecutor(max_workers=2) as pool:
        server = grpc.server(pool)
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "qmt.data.RawQuoteService",
                    {
                        "Stream": grpc.unary_stream_rpc_method_handler(
                            publisher.stream,
                            request_deserializer=identity,
                            response_serializer=identity,
                        )
                    },
                ),
            )
        )
        if not server.add_insecure_port("127.0.0.1:50059"):
            raise RuntimeError("shadow loopback bind failed")
        server.start()
        print("RAW_SHADOW_READY", flush=True)
        try:
            time.sleep(45)
        finally:
            xtdata.unsubscribe_quote(subscription)
            publisher.invalidate()
            server.stop(0).wait()
    print(
        json.dumps(
            {
                "batches": len(counts),
                "max_rows": max(counts, default=0),
                "rows_per_batch": counts,
                "callback_offsets_sec": [a - wall_start for a in arrivals],
                "encode_ms": encodes,
                "cpu_core_pct": 100
                * (time.process_time() - cpu_start)
                / (time.monotonic() - wall_start),
                "errors": errors[:5],
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()

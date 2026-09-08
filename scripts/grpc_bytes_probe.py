"""Bounded transport-only probe. No QMT, protobuf messages or account imports.

Environment-selected roles avoid adding CLI dependencies to the Wine interpreter.
Run each experiment in a fresh process; GRPC_EXPERIMENTS is read before import.
"""

import json
import os
import struct
import time

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from enum import Enum

import grpc

ADDRESS = "127.0.0.1:50061"
METHOD = "/diagnostic.Bytes/Stream"
REQUEST = struct.Struct("<III")
LIMIT = 8 * 1024 * 1024


class Role(str, Enum):
    SERVER = "server"
    CLIENT = "client"


def identity(value: bytes) -> bytes:
    return value


def parse_request(request: bytes) -> tuple[int, int, int]:
    if len(request) != REQUEST.size:
        raise ValueError("invalid request size")
    size, count, pause_ms = REQUEST.unpack(request)
    if not 1 <= size <= LIMIT or not 1 <= count <= 100 or not 0 <= pause_ms <= 100:
        raise ValueError("request outside probe bounds")
    return int(size), int(count), int(pause_ms)


def stream(request: bytes, context: grpc.ServicerContext) -> Iterator[bytes]:
    try:
        size, count, pause_ms = parse_request(request)
    except ValueError as exc:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return
    print(json.dumps({"sending_bytes": size, "count": count}), flush=True)
    for index in range(count):
        if not context.is_active():
            return
        yield bytes([index % 251]) * size
        time.sleep(pause_ms / 1000)


def serve() -> None:
    with ThreadPoolExecutor(max_workers=2) as pool:
        server = grpc.server(pool, options=(("grpc.max_send_message_length", LIMIT),))
        server.add_generic_rpc_handlers(
            (
                grpc.method_handlers_generic_handler(
                    "diagnostic.Bytes",
                    {
                        "Stream": grpc.unary_stream_rpc_method_handler(
                            stream,
                            request_deserializer=identity,
                            response_serializer=identity,
                        )
                    },
                ),
            )
        )
        if not server.add_insecure_port(ADDRESS):
            raise RuntimeError("probe bind failed")
        server.start()
        print(
            json.dumps(
                {
                    "ready": True,
                    "grpc": grpc.__version__,
                    "experiments": os.environ.get("GRPC_EXPERIMENTS", "default"),
                }
            ),
            flush=True,
        )
        try:
            server.wait_for_termination(timeout=45)
        finally:
            server.stop(0).wait()


def consume() -> None:
    sizes = tuple(
        int(value)
        for value in os.environ.get("PROBE_SIZES", "11856,262144,1189476,6111996").split(",")
    )
    pause_ms = int(os.environ.get("PROBE_PAUSE_MS", "10"))
    read_pause_ms = int(os.environ.get("PROBE_READ_PAUSE_MS", "0"))
    count = int(os.environ.get("PROBE_COUNT", "5"))
    with grpc.insecure_channel(
        ADDRESS, options=(("grpc.max_receive_message_length", LIMIT),)
    ) as channel:
        grpc.channel_ready_future(channel).result(timeout=8)
        call = channel.unary_stream(
            METHOD, request_serializer=identity, response_deserializer=identity
        )
        for size in sizes:
            request = REQUEST.pack(size, count, pause_ms)
            parse_request(request)
            received = 0
            started = time.perf_counter()
            try:
                for payload in call(request, timeout=10):
                    if payload != bytes([received % 251]) * size:
                        raise ValueError("payload integrity mismatch")
                    received += 1
                    time.sleep(read_pause_ms / 1000)
                if received != count:
                    raise ValueError("message count mismatch")
            except grpc.RpcError as exc:
                print(
                    json.dumps({"bytes": size, "received": received, "error": str(exc)}), flush=True
                )
                raise
            print(
                json.dumps(
                    {
                        "bytes": size,
                        "received": received,
                        "elapsed_ms": 1000 * (time.perf_counter() - started),
                    }
                ),
                flush=True,
            )


if __name__ == "__main__":
    role = Role(os.environ.get("PROBE_ROLE", Role.SERVER.value))
    if role is Role.SERVER:
        serve()
    else:
        consume()

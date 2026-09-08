from concurrent.futures import ThreadPoolExecutor

import grpc
import pytest

from scripts.grpc_bytes_probe import LIMIT, METHOD, REQUEST, identity, parse_request, stream


@pytest.mark.parametrize(("size", "count", "pause"), [(1, 1, 0), (LIMIT, 100, 100)])
def test_probe_request_bounds(size: int, count: int, pause: int) -> None:
    assert parse_request(REQUEST.pack(size, count, pause)) == (size, count, pause)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        REQUEST.pack(0, 1, 0),
        REQUEST.pack(LIMIT + 1, 1, 0),
        REQUEST.pack(1, 101, 0),
        REQUEST.pack(1, 1, 101),
    ],
)
def test_probe_rejects_unbounded_requests(payload: bytes) -> None:
    with pytest.raises(ValueError, match=r"invalid request size|outside probe bounds"):
        parse_request(payload)


def test_probe_loopback_payload_and_invalid_request() -> None:
    with ThreadPoolExecutor(max_workers=1) as pool:
        server = grpc.server(pool)
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
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                call = channel.unary_stream(
                    METHOD, request_serializer=identity, response_deserializer=identity
                )
                assert list(call(REQUEST.pack(8192, 3, 0), timeout=3)) == [
                    bytes([i]) * 8192 for i in range(3)
                ]
                with pytest.raises(grpc.RpcError) as error:
                    list(call(b"", timeout=3))
                assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        finally:
            server.stop(0).wait()

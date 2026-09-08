"""Output selection tests without QMT, network or broker sessions."""

from collections.abc import Callable
from pathlib import Path
from unittest.mock import Mock

import pytest

from app.config import QuoteTransport, Settings, XTQuantDataConfig, XTQuantMode, load_config
from app.services import xtdata_subscription_hub as hub_module
from app.services.raw_quote_publisher import NativeBatch
from app.services.xtdata_gateway import XtDataGateway
from app.services.xtdata_subscription_hub import SubscriptionRecord, XtDataSubscriptionHub
from app.utils.exceptions import DataServiceException


def test_yaml_load_preserves_raw_transport(tmp_path: Path) -> None:
    config = tmp_path / "raw.yml"
    config.write_text(
        "xtquant:\n  data:\n    quote_transport: raw_grpc\n"
        "modes:\n  mock:\n    xtquant_mode: mock\n"
    )
    settings = load_config(str(config), app_mode="mock", local_config_file=None)
    assert settings.xtquant.data.quote_transport is QuoteTransport.RAW_GRPC


def test_raw_internal_source_rejects_legacy_consumer() -> None:
    settings = Settings()
    settings.xtquant.data = XTQuantDataConfig(quote_transport=QuoteTransport.RAW_GRPC)
    hub = XtDataSubscriptionHub(settings, XtDataGateway(settings))
    hub._subscriptions["raw"] = SubscriptionRecord(
        "raw",
        "whole_quote",
        True,
        "tick",
        shared_quote_sink=True,
    )
    with pytest.raises(DataServiceException, match="requires the raw gRPC stream"):
        hub._register_consumer("raw")


def test_shutdown_invalidates_raw_stream(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings()
    hub = XtDataSubscriptionHub(settings, XtDataGateway(settings))
    invalidate = Mock()
    monkeypatch.setattr(hub.raw_quotes, "invalidate", invalidate)
    hub.shutdown()
    invalidate.assert_called_once()


class NativeAdapter:
    """Only capture the subscription callback; no external activity."""

    callback: Callable[[NativeBatch], None] | None = None

    def subscribe_whole_quote(
        self, markets: list[str], callback: Callable[[NativeBatch], None]
    ) -> int:
        self.callback = callback
        return 1


@pytest.mark.parametrize(
    ("transport", "raw", "normalized"),
    [
        (QuoteTransport.MMAP, 0, 1),
        (QuoteTransport.RAW_GRPC, 1, 0),
        (QuoteTransport.DUAL, 1, 1),
    ],
)
def test_native_callback_selects_only_configured_sinks(
    transport: QuoteTransport, raw: int, normalized: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = Settings()
    settings.xtquant.mode = XTQuantMode.DEV
    settings.xtquant.data = XTQuantDataConfig(
        quote_transport=transport,
        shared_quote_path="unused.mmap",
    )
    hub = XtDataSubscriptionHub(settings, XtDataGateway(settings))
    native = NativeAdapter()
    monkeypatch.setattr(hub_module, "xtdata", native)
    monkeypatch.setattr(hub_module, "XTQUANT_DATA_AVAILABLE", True)
    monkeypatch.setattr(hub, "_start_runtime_if_needed", lambda: None)
    encode, normalize = Mock(), Mock(return_value=())
    monkeypatch.setattr(hub.raw_quotes, "publish", encode)
    monkeypatch.setattr(hub, "_iter_normalized_payload", normalize)
    record = SubscriptionRecord("test", "whole_quote", True, "tick", shared_quote_sink=True)
    hub._subscribe_native_whole_quote(record)
    assert native.callback is not None
    native.callback({})
    assert encode.call_count == raw
    assert normalize.call_count == normalized


def test_raw_only_starts_without_mmap_path(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings()
    settings.xtquant.data = XTQuantDataConfig(quote_transport=QuoteTransport.RAW_GRPC)
    hub = XtDataSubscriptionHub(settings, XtDataGateway(settings))
    store = Mock(side_effect=AssertionError("raw-only must not create mmap"))
    monkeypatch.setattr(hub_module, "SharedQuoteStore", store)
    monkeypatch.setattr(hub, "_run_shared_quote_publisher", lambda: None)
    try:
        hub.start_shared_quote_publisher()
        assert hub._shared_quote_thread is not None
        store.assert_not_called()
    finally:
        hub.shutdown()


def test_default_is_legacy_and_dual_requires_path(tmp_path: Path) -> None:
    assert XTQuantDataConfig().quote_transport is QuoteTransport.MMAP
    with pytest.raises(ValueError, match="requires shared_quote_path"):
        XTQuantDataConfig(quote_transport=QuoteTransport.DUAL)
    assert (
        XTQuantDataConfig(
            quote_transport=QuoteTransport.DUAL, shared_quote_path=str(tmp_path / "quotes.mmap")
        ).quote_transport
        is QuoteTransport.DUAL
    )

import threading
import time

import pytest

from app.config import Settings
from app.models.api_requests import SubmitStockOrderRequestModel
from app.services.credit import credit_symbol_matches
from app.services import (
    trading_event_hub as event_hub_module,
    trading_session_manager as manager_module,
)
from app.services.contracts import (
    CancelStockOrderCommand,
    OpenSessionCommand,
    SubmitStockOrderCommand,
)
from app.services.trading_event_hub import TradingEventHub
from app.services.trading_session_manager import TradingSessionManager
from app.utils.exceptions import TradingServiceException


class FakeCreditDetail:
    account_id = "CREDIT-001"
    account_type = 3
    status = 0
    update_time = 1
    calc_config = 0
    frozen_cash = 100.0
    balance = 200000.0
    available = 80000.0
    position_profit = 1000.0
    market_value = 120000.0
    fetch_balance = 0.0
    stock_value = 120000.0
    fund_value = 0.0
    total_debt = 50000.0
    enable_bail_balance = 70000.0
    per_assurescale_value = 3.0
    assure_asset = 150000.0
    fin_debt = 40000.0
    fin_deal_avl = 39000.0
    fin_fee = 1000.0
    slo_debt = 10000.0
    slo_market_value = 9500.0
    slo_fee = 500.0
    other_fare = 0.0
    fin_max_quota = 200000.0
    fin_enable_quota = 100000.0
    fin_used_quota = 100000.0
    slo_max_quota = 100000.0
    slo_enable_quota = 80000.0
    slo_used_quota = 20000.0
    slo_sell_balance = 10000.0
    used_slo_sell_balance = 5000.0
    surplus_slo_sell_balance = 5000.0


class FakeGateway:
    def __init__(self, qmt_userdata_path: str, account_id: str, account_type: str = "STOCK", event_handler=None):
        self.qmt_userdata_path = qmt_userdata_path
        self.account_id = account_id
        self.account_type = account_type
        self.event_handler = event_handler
        self.connected = False
        self.order_calls = []
        self.cancel_calls = []

    def connect(self):
        self.connected = True

    def disconnect(self):
        self.connected = False

    def query_stock_asset(self):
        class Asset:
            cash = 100000.0
            frozen_cash = 1000.0
            market_value = 250000.0
            total_asset = 351000.0
            fetch_balance = 99000.0

        return Asset()

    def query_stock_positions(self):
        return []

    def query_stock_orders(self, cancelable_only: bool = False):
        return []

    def query_stock_trades(self):
        return []

    def query_credit_detail(self):
        return [FakeCreditDetail()]

    def query_credit_compacts(self):
        return []

    def query_credit_subjects(self):
        return []

    def query_credit_slo_codes(self):
        return []

    def query_credit_assures(self):
        return []

    def query_new_purchase_limit(self):
        return {"SH": 7300, "SZ": 4100, "KCB": 6200}

    def query_ipo_data(self):
        return {
            "754001.SH": {
                "name": "测试发债",
                "type": "BOND",
                "minPurchaseNum": 10,
                "maxPurchaseNum": 10000,
                "purchaseDate": "20260806",
                "issuePrice": 100.0,
            }
        }

    def order_stock(
        self,
        stock_code: str,
        order_type: int,
        order_volume: int,
        price_type: int,
        price: float,
        strategy_name: str = "",
        order_remark: str = "",
    ) -> int:
        self.order_calls.append(
            {
                "stock_code": stock_code,
                "order_type": order_type,
                "order_volume": order_volume,
                "price_type": price_type,
                "price": price,
                "strategy_name": strategy_name,
                "order_remark": order_remark,
            }
        )
        return 7788

    def cancel_order_stock(self, order_id: int) -> int:
        self.cancel_calls.append(order_id)
        return 0

    def cancel_order_stock_sysid(self, market: str, order_sysid: str) -> int:
        self.cancel_calls.append((market, order_sysid))
        return 0


class CallbackDuringConnectGateway(FakeGateway):
    def connect(self):
        self.connected = True
        if self.event_handler:
            class Asset:
                account_id = "SIM-001"
                cash = 88888.0
                frozen_cash = 12.0
                market_value = 12345.0
                total_asset = 101245.0
                fetch_balance = 88000.0

            self.event_handler("stock_asset", Asset())


class CallbackDuringDisconnectGateway(FakeGateway):
    def disconnect(self):
        self.connected = False
        if self.event_handler:
            class Trade:
                account_id = "SIM-001"
                stock_code = "000001.SZ"
                instrument_name = "PingAn"
                order_type = 23
                traded_id = "T-001"
                traded_time = "20250102103000"
                traded_price = 12.3
                traded_volume = 100
                traded_amount = 1230.0
                order_id = "7788"
                order_sysid = ""
                strategy_name = ""
                order_remark = ""
                direction = ""
                offset_flag = ""
                commission = 0.0
                secu_account = "SIM-001"

            self.event_handler("stock_trade", Trade())


class DisconnectFailureGateway(FakeGateway):
    def disconnect(self):
        self.connected = False
        raise RuntimeError("disconnect failed")


class BlockingDisconnectGateway(FakeGateway):
    release_event = threading.Event()

    def disconnect(self):
        self.connected = False
        self.release_event.wait(timeout=5.0)


def build_settings(mode: str, *, accounts: list[dict] | None = None, enable_prod_orders: bool = False) -> Settings:
    return Settings(
        xtquant={
            "mode": mode,
            "data": {"qmt_userdata_path": "C:/fake-qmt"},
            "trading": {
                "accounts": accounts or [],
                "enable_prod_orders": enable_prod_orders,
                "disconnect_timeout_seconds": 0.05,
            },
        },
    )


def simulated_account():
    return {
        "name": "simulated-dev",
        "account_id": "SIM-001",
        "account_type": "STOCK",
        "account_kind": "simulated",
        "allowed_modes": ["dev"],
        "enabled": True,
    }


def real_account():
    return {
        "name": "real-prod",
        "account_id": "PROD-001",
        "account_type": "STOCK",
        "account_kind": "real",
        "allowed_modes": ["prod"],
        "enabled": True,
    }


def credit_account():
    return {
        "name": "credit-dev",
        "account_id": "CREDIT-001",
        "account_type": "CREDIT",
        "account_kind": "simulated",
        "allowed_modes": ["dev"],
        "enabled": True,
    }


def test_open_session_uses_real_gateway_in_dev_for_registered_simulated_account(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(build_settings("dev", accounts=[simulated_account()]), TradingEventHub())
    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))

    assert session["is_real"] is True
    assert session["environment"] == "dev"
    assert session["account_kind"] == "simulated"
    assert session["orders_enabled"] is True
    assert manager.get_stock_asset(session["session_id"])["total_asset"] == 351000.0
    assert manager.get_new_purchase_limits(session["session_id"]) == [
        {"market": "KCB", "quantity": 6200},
        {"market": "SH", "quantity": 7300},
        {"market": "SZ", "quantity": 4100},
    ]
    assert manager.get_ipo_data(session["session_id"])[0] == {
        "stock_code": "754001.SH",
        "name": "测试发债",
        "instrument_type": "BOND",
        "min_purchase_quantity": 10,
        "max_purchase_quantity": 10000,
        "purchase_date": "20260806",
        "issue_price": 100.0,
    }


def test_open_session_rejects_unregistered_real_account_in_dev(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(build_settings("dev", accounts=[real_account()]), TradingEventHub())

    with pytest.raises(TradingServiceException) as exc:
        manager.open_session(OpenSessionCommand(account_id="PROD-001"))
    assert exc.value.error_code == "ACCOUNT_PROFILE_NOT_ALLOWED"


def test_submit_order_calls_gateway_in_dev_simulated_mode(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(build_settings("dev", accounts=[simulated_account()]), TradingEventHub())
    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))
    order = manager.submit_stock_order(
        SubmitStockOrderCommand(
            session_id=session["session_id"],
            stock_code="000001.SZ",
            side=23,
            price_type=11,
            volume=100,
            price=12.34,
        )
    )

    stored_session = manager._sessions[session["session_id"]]
    gateway = stored_session.gateway

    assert order["order_id"] == "7788"
    assert gateway is not None
    assert gateway.order_calls[0]["stock_code"] == "000001.SZ"
    assert gateway.order_calls[0]["order_volume"] == 100
    assert gateway.order_calls[0]["price"] == 12.34


def test_credit_session_exposes_detail_and_accepts_financing_action(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)
    manager = TradingSessionManager(
        build_settings("dev", accounts=[credit_account()]), TradingEventHub()
    )
    session = manager.open_session(
        OpenSessionCommand(account_id="CREDIT-001", account_type="CREDIT")
    )

    detail = manager.get_credit_detail(session["session_id"])[0]
    assert detail["total_debt"] == 50000.0
    assert detail["maintenance_collateral_ratio"] == 3.0
    manager.submit_stock_order(
        SubmitStockOrderCommand(
            session_id=session["session_id"],
            stock_code="000001.SZ",
            side=27,
            price_type=11,
            volume=100,
            price=12.34,
        )
    )
    gateway = manager._sessions[session["session_id"]].gateway
    assert gateway is not None
    assert gateway.order_calls[0]["order_type"] == 27


def test_stock_session_rejects_credit_order_action(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)
    manager = TradingSessionManager(
        build_settings("dev", accounts=[simulated_account()]), TradingEventHub()
    )
    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))

    with pytest.raises(TradingServiceException) as exc:
        manager.submit_stock_order(
            SubmitStockOrderCommand(
                session_id=session["session_id"],
                stock_code="000001.SZ",
                side=27,
                price_type=11,
                volume=100,
                price=12.34,
            )
        )
    assert exc.value.error_code == "CREDIT_ACCOUNT_REQUIRED"


def test_rest_credit_action_requires_matching_side():
    with pytest.raises(ValueError, match="requires side=BUY"):
        SubmitStockOrderRequestModel(
            stock_code="000001.SZ",
            side="SELL",
            price_type=11,
            volume=100,
            price=12.34,
            credit_action=27,
        )


def test_credit_symbol_filter_matches_bare_xtquant_instrument_id():
    assert credit_symbol_matches("000001", frozenset({"000001.SZ"})) is True


def test_prod_real_account_is_readonly_by_default(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(build_settings("prod", accounts=[real_account()]), TradingEventHub())
    session = manager.open_session(OpenSessionCommand(account_id="PROD-001"))
    assert session["orders_enabled"] is False

    with pytest.raises(TradingServiceException) as exc:
        manager.submit_stock_order(
            SubmitStockOrderCommand(
                session_id=session["session_id"],
                stock_code="000001.SZ",
                side=23,
                price_type=11,
                volume=100,
                price=12.34,
            )
        )
    assert exc.value.error_code == "ORDERS_DISABLED"


def test_prod_real_account_can_submit_when_prod_orders_enabled(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(
        build_settings("prod", accounts=[real_account()], enable_prod_orders=True),
        TradingEventHub(),
    )
    session = manager.open_session(OpenSessionCommand(account_id="PROD-001"))
    assert session["orders_enabled"] is True

    order = manager.submit_stock_order(
        SubmitStockOrderCommand(
            session_id=session["session_id"],
            stock_code="000001.SZ",
            side=23,
            price_type=11,
            volume=100,
            price=12.34,
        )
    )

    stored_session = manager._sessions[session["session_id"]]
    gateway = stored_session.gateway

    assert order["order_id"] == "7788"
    assert gateway is not None
    assert gateway.order_calls[0]["stock_code"] == "000001.SZ"


def test_cancel_by_sysid_normalizes_market_to_xt_enum(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(build_settings("dev", accounts=[simulated_account()]), TradingEventHub())
    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))

    result = manager.cancel_stock_order(
        CancelStockOrderCommand(
            session_id=session["session_id"],
            market="SH",
            order_sysid="SYSID-001",
        )
    )
    assert result.accepted is True
    assert result.confirmed is True
    assert result.still_cancelable is False

    gateway = manager._sessions[session["session_id"]].gateway
    assert gateway is not None
    assert gateway.cancel_calls[-1] == (0, "SYSID-001")


def test_cancel_by_sysid_rejects_invalid_market(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", FakeGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(build_settings("dev", accounts=[simulated_account()]), TradingEventHub())
    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))

    with pytest.raises(TradingServiceException) as exc:
        manager.cancel_stock_order(
            CancelStockOrderCommand(
                session_id=session["session_id"],
                market="BJ",
                order_sysid="SYSID-002",
            )
        )

    assert exc.value.error_code == "INVALID_MARKET"


def test_mock_order_publishes_stream_event():
    hub = TradingEventHub()
    manager = TradingSessionManager(build_settings("mock"), hub)
    session = manager.open_session(OpenSessionCommand(account_id="mock-account"))

    stream = manager.stream_events(session["session_id"], stop_checker=lambda: True)

    def submit_order() -> None:
        time.sleep(0.05)
        manager.submit_stock_order(
            SubmitStockOrderCommand(
                session_id=session["session_id"],
                stock_code="000001.SZ",
                side=23,
                price_type=11,
                volume=100,
                price=10.5,
            )
        )

    worker = threading.Thread(target=submit_order, daemon=True)
    worker.start()
    event = next(stream)
    stream.close()
    worker.join(timeout=1)

    assert event["event_type"] == "order_update"
    assert event["payload"]["stock_code"] == "000001.SZ"


def test_close_session_ends_trading_event_stream():
    hub = TradingEventHub()
    manager = TradingSessionManager(build_settings("mock"), hub)
    session = manager.open_session(OpenSessionCommand(account_id="mock-account"))

    stream = manager.stream_events(session["session_id"], stop_checker=lambda: True)

    def close_session() -> None:
        time.sleep(0.05)
        manager.close_session(session["session_id"])

    worker = threading.Thread(target=close_session, daemon=True)
    worker.start()

    with pytest.raises(StopIteration):
        next(stream)

    worker.join(timeout=1)
    stream.close()


def test_close_session_after_stream_creation_still_ends_without_hanging():
    hub = TradingEventHub()
    manager = TradingSessionManager(build_settings("mock"), hub)
    session = manager.open_session(OpenSessionCommand(account_id="mock-account"))

    stream = manager.stream_events(session["session_id"], stop_checker=lambda: True)
    assert manager.close_session(session["session_id"]) is True

    with pytest.raises(StopIteration):
        next(stream)

    stream.close()


def test_open_session_accepts_callback_before_gateway_connect_returns(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", CallbackDuringConnectGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(build_settings("dev", accounts=[simulated_account()]), TradingEventHub())

    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))
    asset = manager.get_stock_asset(session["session_id"])

    assert session["session_id"] in manager._sessions
    assert asset["cash"] == 100000.0


def test_close_session_ignores_late_gateway_callbacks(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", CallbackDuringDisconnectGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    manager = TradingSessionManager(build_settings("dev", accounts=[simulated_account()]), TradingEventHub())
    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))

    assert manager.close_session(session["session_id"]) is True
    assert session["session_id"] not in manager._sessions


def test_close_session_cleans_up_even_when_disconnect_fails(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", DisconnectFailureGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    hub = TradingEventHub()
    manager = TradingSessionManager(build_settings("dev", accounts=[simulated_account()]), hub)
    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))
    closed_sessions: list[str] = []
    original_close = hub.close_session_streams

    def capture_close(session_id: str) -> None:
        closed_sessions.append(session_id)
        original_close(session_id)

    monkeypatch.setattr(hub, "close_session_streams", capture_close)

    assert manager.close_session(session["session_id"]) is True
    assert session["session_id"] not in manager._sessions
    assert closed_sessions == [session["session_id"]]


def test_close_session_returns_promptly_when_disconnect_blocks(monkeypatch):
    monkeypatch.setattr(manager_module, "XTTraderGateway", BlockingDisconnectGateway)
    monkeypatch.setattr(manager_module, "XTQUANT_TRADER_AVAILABLE", True)

    BlockingDisconnectGateway.release_event.clear()
    hub = TradingEventHub()
    manager = TradingSessionManager(build_settings("dev", accounts=[simulated_account()]), hub)
    session = manager.open_session(OpenSessionCommand(account_id="SIM-001"))

    started = time.perf_counter()
    try:
        assert manager.close_session(session["session_id"]) is True
        duration = time.perf_counter() - started
        assert duration < 0.5
        assert session["session_id"] not in manager._sessions
    finally:
        BlockingDisconnectGateway.release_event.set()


def test_stock_asset_callback_uses_payload_without_sync_requery(monkeypatch):
    manager = TradingSessionManager(build_settings("mock"), TradingEventHub())
    session = manager.open_session(OpenSessionCommand(account_id="mock-account"))
    stored_session = manager._sessions[session["session_id"]]

    def fail_query(_session):
        raise AssertionError("asset callback should not perform a synchronous query")

    monkeypatch.setattr(manager, "_query_asset", fail_query)

    class Asset:
        account_id = "mock-account"
        cash = 123456.0
        frozen_cash = 111.0
        market_value = 222222.0
        total_asset = 345678.0
        fetch_balance = 120000.0

    manager._handle_gateway_event(session["session_id"], "stock_asset", Asset())

    assert stored_session.asset is not None
    assert stored_session.asset["cash"] == 123456.0


def test_terminal_order_cancel_error_is_idempotent_order_update():
    hub = TradingEventHub()
    manager = TradingSessionManager(build_settings("mock"), hub)
    session = manager.open_session(OpenSessionCommand(account_id="mock-account"))
    session_id = session["session_id"]
    stored_session = manager._sessions[session_id]
    stored_session.orders["7788"] = {
        "account_id": "mock-account",
        "stock_code": "113650.SH",
        "instrument_name": "",
        "order_id": "7788",
        "order_sysid": "SYS-7788",
        "order_time_ms": 0,
        "order_type": 23,
        "order_volume": 10,
        "price_type": 11,
        "price": 126.53,
        "traded_volume": 10,
        "traded_price": 126.53,
        "order_status_code": 56,
        "status_msg": "filled",
        "strategy_name": "",
        "order_remark": "",
        "direction": "",
        "offset_flag": "",
        "secu_account": "mock-account",
    }
    _, events = hub.register(session_id)

    class CancelError:
        account_id = "mock-account"
        order_id = 7788
        order_sysid = "SYS-7788"
        error_id = -54
        error_msg = "[COUNTER] [251020][委托状态错误不能撤单] [old_entrust_status=8]"

    manager._handle_gateway_event(session_id, "cancel_error", CancelError())

    event = events.get_nowait()
    assert event["event_type"] == "order_update"
    assert event["payload"]["order_id"] == "7788"
    assert event["payload"]["order_status_code"] == 56


def test_open_order_cancel_error_remains_error_event():
    hub = TradingEventHub()
    manager = TradingSessionManager(build_settings("mock"), hub)
    session = manager.open_session(OpenSessionCommand(account_id="mock-account"))
    session_id = session["session_id"]
    stored_session = manager._sessions[session_id]
    stored_session.orders["7788"] = {
        "order_id": "7788",
        "order_sysid": "SYS-7788",
        "order_status_code": 50,
    }
    _, events = hub.register(session_id)

    class CancelError:
        account_id = "mock-account"
        order_id = 7788
        order_sysid = "SYS-7788"
        error_id = 1001
        error_msg = "real cancel failure"

    manager._handle_gateway_event(session_id, "cancel_error", CancelError())

    event = events.get_nowait()
    assert event["event_type"] == "cancel_error"
    assert event["payload"]["error_id"] == 1001


def test_trading_event_queue_overflow_emits_warning(monkeypatch):
    hub = TradingEventHub(maxsize=1)
    consumer_id, consumer_queue = hub.register("session-test")
    warnings: list[str] = []

    monkeypatch.setattr(event_hub_module.logger, "warning", lambda message: warnings.append(message))

    hub.publish("session-test", {"seq": 1})
    hub.publish("session-test", {"seq": 2})

    assert warnings
    assert "trading event queue overflow" in warnings[0]
    assert "session_id=session-test" in warnings[0]
    assert consumer_queue.get_nowait()["seq"] == 2
    hub.unregister("session-test", consumer_id)


def test_to_epoch_ms_handles_unknown_real_order_time_values_without_crashing():
    manager = TradingSessionManager(build_settings("mock"), TradingEventHub())

    assert manager._to_epoch_ms("20250426193015") > 0
    assert manager._to_epoch_ms("20250426") > 0
    assert manager._to_epoch_ms(1714123456) == 1714123456000
    assert manager._to_epoch_ms(1714123456789) == 1714123456789
    assert manager._to_epoch_ms("99999999999999") == 0
    assert manager._to_epoch_ms("not-a-time") == 0

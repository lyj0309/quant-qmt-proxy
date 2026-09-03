from __future__ import annotations

import threading
import time
import uuid

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import Any

from app.config import AccountKind, Settings, XTQuantMode, XTQuantTradingAccountConfig
from app.services.contracts import (
    CancelStockOrderCommand,
    CancelStockOrderResult,
    OpenSessionCommand,
    OrderSnapshotPayload,
    SubmitStockOrderCommand,
)
from app.services.credit import (
    CreditAssurePayload,
    CreditCompactPayload,
    CreditDetailPayload,
    CreditOrderAction,
    CreditSloCodePayload,
    CreditSubjectPayload,
    convert_credit_assure,
    convert_credit_compact,
    convert_credit_detail,
    convert_credit_slo_code,
    convert_credit_subject,
    credit_symbol_matches,
)
from app.services.trading_event_hub import TradingEventHub
from app.services.xttrader_gateway import XTQUANT_TRADER_AVAILABLE, XTTraderGateway
from app.utils.exceptions import TradingServiceException
from app.utils.helpers import validate_stock_code
from app.utils.logger import logger


@dataclass
class TradingSession:
    session_id: str
    account_id: str
    account_type: str
    mode: str
    environment: str
    is_real: bool
    account_profile: str | None
    account_kind: str
    orders_enabled: bool
    opened_at_ms: int
    gateway: XTTraderGateway | None = None
    orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    trades: list[dict[str, Any]] = field(default_factory=list)
    asset: dict[str, Any] | None = None
    accept_events: bool = True


@dataclass(frozen=True)
class CancelConfirmationResult:
    confirmed: bool
    latest_order: OrderSnapshotPayload | None
    still_cancelable: bool


class XtOrderStatus(IntEnum):
    """Terminal xtquant order states needed for idempotent cancellation."""

    PARTIALLY_FILLED_CANCELLED = 53
    CANCELLED = 54
    FILLED = 56
    REJECTED = 57


class TradingSessionManager:
    CANCEL_MARKET_MAP = {
        "SH": 0,
        "SHA": 0,
        "SHANGHAI": 0,
        "0": 0,
        "SZ": 1,
        "SZA": 1,
        "SHENZHEN": 1,
        "1": 1,
    }

    def __init__(self, settings: Settings, event_hub: TradingEventHub | None = None):
        self.settings = settings
        self.event_hub = event_hub or TradingEventHub()
        self._lock = threading.RLock()
        self._sessions: dict[str, TradingSession] = {}
        self._mock_order_counter = 1000

    def open_session(self, command: OpenSessionCommand) -> dict[str, Any]:
        session_id = f"session_{command.account_id}_{uuid.uuid4().hex[:10]}"
        mode = self.settings.xtquant.mode
        is_real = mode in {XTQuantMode.DEV, XTQuantMode.PROD}

        account_profile_name: str | None = None
        account_kind = AccountKind.MOCK.value

        if is_real:
            profile = self._resolve_account_profile(command.account_id, command.account_type)
            account_profile_name = profile.name
            account_kind = profile.account_kind.value

        session = TradingSession(
            session_id=session_id,
            account_id=command.account_id,
            account_type=self._normalize_account_type(command.account_type),
            mode=mode.value,
            environment=mode.value,
            is_real=is_real,
            account_profile=account_profile_name,
            account_kind=account_kind,
            orders_enabled=self._orders_enabled(mode, account_kind),
            opened_at_ms=int(time.time() * 1000),
        )
        with self._lock:
            self._sessions[session_id] = session
        try:
            if is_real:
                gateway = self._connect_gateway(command.account_id, command.account_type, session_id)
                with self._lock:
                    session.gateway = gateway
            session.asset = self._query_asset(session)
        except Exception:
            with self._lock:
                existing = self._sessions.pop(session_id, None)
                if existing:
                    existing.accept_events = False
                    gateway = existing.gateway
                else:
                    gateway = None
            if gateway:
                try:
                    gateway.disconnect()
                except Exception as exc:
                    logger.warning(
                        f"failed to cleanup gateway after session open error: session_id={session_id}, error={exc}"
                    )
            raise
        logger.info(
            f"opened trading session: session_id={session_id}, account_id={session.account_id}, account_type={session.account_type}, mode={session.mode}, account_kind={session.account_kind}, orders_enabled={session.orders_enabled}"
        )
        return self._serialize_session(session)

    def close_session(self, session_id: str) -> bool:
        with self._lock:
            session = self._sessions.get(session_id)
        if not session:
            return False
        session.accept_events = False
        self.event_hub.close_session_streams(session_id)
        with self._lock:
            self._sessions.pop(session_id, None)

        disconnect_error: Exception | None = None
        disconnect_timed_out = False
        if session.gateway:
            def disconnect_worker() -> None:
                nonlocal disconnect_error
                try:
                    session.gateway.disconnect()
                except Exception as exc:
                    disconnect_error = exc
                    logger.warning(
                        f"gateway disconnect failed during session close: session_id={session_id}, account_id={session.account_id}, error={exc}"
                    )

            disconnect_thread = threading.Thread(
                target=disconnect_worker,
                daemon=True,
                name=f"xttrader-disconnect-{session_id[:8]}",
            )
            disconnect_thread.start()
            disconnect_thread.join(timeout=self.settings.xtquant.trading.disconnect_timeout_seconds)
            disconnect_timed_out = disconnect_thread.is_alive()
            if disconnect_timed_out:
                logger.warning(
                    f"gateway disconnect timed out during session close: session_id={session_id}, account_id={session.account_id}, timeout_seconds={self.settings.xtquant.trading.disconnect_timeout_seconds}"
                )
        logger.info(f"closed trading session: session_id={session_id}, account_id={session.account_id}")
        if disconnect_error:
            logger.info(
                f"session cleanup completed after disconnect error: session_id={session_id}, account_id={session.account_id}"
            )
        if disconnect_timed_out:
            logger.info(
                f"session cleanup completed with background disconnect still running: session_id={session_id}, account_id={session.account_id}"
            )
        return True

    def get_session(self, session_id: str) -> dict[str, Any]:
        return self._serialize_session(self._get_session(session_id))

    def get_stock_asset(self, session_id: str) -> dict[str, Any]:
        session = self._get_session(session_id)
        asset = self._query_asset(session)
        with self._lock:
            session.asset = asset
        return asset

    def get_stock_positions(self, session_id: str) -> list[dict[str, Any]]:
        session = self._get_session(session_id)
        if session.gateway:
            positions = session.gateway.query_stock_positions() or []
            return [self._convert_position(item) for item in positions]
        return [self._mock_position(session.account_id)]

    def get_stock_orders(self, session_id: str, cancelable_only: bool = False) -> list[dict[str, Any]]:
        session = self._get_session(session_id)
        if session.gateway:
            orders = [self._convert_order(item) for item in (session.gateway.query_stock_orders(cancelable_only) or [])]
            with self._lock:
                session.orders = {order["order_id"]: order for order in orders}
            return orders
        with self._lock:
            return list(session.orders.values())

    def get_stock_trades(self, session_id: str) -> list[dict[str, Any]]:
        session = self._get_session(session_id)
        if session.gateway:
            trades = [self._convert_trade(item) for item in (session.gateway.query_stock_trades() or [])]
            with self._lock:
                session.trades = trades
            return trades
        with self._lock:
            return list(session.trades)

    def get_credit_detail(self, session_id: str) -> list[CreditDetailPayload]:
        """Return credit assets for one CREDIT session."""
        session = self._require_credit_session(session_id)
        if session.gateway is None:
            return []
        return [
            convert_credit_detail(item)
            for item in session.gateway.query_credit_detail()
        ]

    def get_credit_compacts(
        self,
        session_id: str,
        instrument_ids: tuple[str, ...] = (),
    ) -> list[CreditCompactPayload]:
        """Return outstanding credit contracts, optionally filtered by symbol."""
        session = self._require_credit_session(session_id)
        if session.gateway is None:
            return []
        requested = self._credit_instrument_filter(instrument_ids)
        return [
            convert_credit_compact(item)
            for item in session.gateway.query_credit_compacts()
            if credit_symbol_matches(item.instrument_id, requested)
        ]

    def get_credit_subjects(
        self,
        session_id: str,
        instrument_ids: tuple[str, ...] = (),
    ) -> list[CreditSubjectPayload]:
        """Return financing and short-selling eligibility by instrument."""
        session = self._require_credit_session(session_id)
        if session.gateway is None:
            return []
        requested = self._credit_instrument_filter(instrument_ids)
        return [
            convert_credit_subject(item)
            for item in session.gateway.query_credit_subjects()
            if credit_symbol_matches(item.instrument_id, requested)
        ]

    def get_credit_slo_codes(
        self,
        session_id: str,
        instrument_ids: tuple[str, ...] = (),
    ) -> list[CreditSloCodePayload]:
        """Return available securities-lending inventory."""
        session = self._require_credit_session(session_id)
        if session.gateway is None:
            return []
        requested = self._credit_instrument_filter(instrument_ids)
        return [
            convert_credit_slo_code(item)
            for item in session.gateway.query_credit_slo_codes()
            if credit_symbol_matches(item.instrument_id, requested)
        ]

    def get_credit_assures(
        self,
        session_id: str,
        instrument_ids: tuple[str, ...] = (),
    ) -> list[CreditAssurePayload]:
        """Return collateral eligibility and conversion ratios."""
        session = self._require_credit_session(session_id)
        if session.gateway is None:
            return []
        requested = self._credit_instrument_filter(instrument_ids)
        return [
            convert_credit_assure(item)
            for item in session.gateway.query_credit_assures()
            if credit_symbol_matches(item.instrument_id, requested)
        ]

    def get_new_purchase_limits(self, session_id: str) -> list[dict[str, Any]]:
        session = self._get_session(session_id)
        if not session.gateway:
            return []
        raw_limits = session.gateway.query_new_purchase_limit() or {}
        return [
            {"market": str(market).strip().upper(), "quantity": int(quantity)}
            for market, quantity in sorted(raw_limits.items())
            if str(market).strip() and int(quantity) >= 0
        ]

    def get_ipo_data(self, session_id: str) -> list[dict[str, Any]]:
        session = self._get_session(session_id)
        if not session.gateway:
            return []
        raw_instruments = session.gateway.query_ipo_data() or {}
        return [
            {
                "stock_code": str(stock_code).strip().upper(),
                "name": str(info.get("name", "")).strip(),
                "instrument_type": str(info.get("type", "")).strip().upper(),
                "min_purchase_quantity": int(info.get("minPurchaseNum", 0) or 0),
                "max_purchase_quantity": int(info.get("maxPurchaseNum", 0) or 0),
                "purchase_date": str(info.get("purchaseDate", "")).strip(),
                "issue_price": float(info.get("issuePrice", 0.0) or 0.0),
            }
            for stock_code, info in sorted(raw_instruments.items())
            if str(stock_code).strip() and isinstance(info, dict)
        ]

    def submit_stock_order(self, command: SubmitStockOrderCommand) -> dict[str, Any]:
        session = self._get_session(command.session_id)
        if not validate_stock_code(command.stock_code):
            raise TradingServiceException(f"invalid stock code: {command.stock_code}", "INVALID_STOCK_CODE")
        self._validate_order_action(session, command.side)

        if session.mode == XTQuantMode.MOCK.value:
            order = self._build_mock_order(session, command)
            self._publish_event(command.session_id, "order_update", order)
            logger.info(
                f"submitted mock order: session_id={command.session_id}, account_id={session.account_id}, stock_code={command.stock_code}, volume={command.volume}, price_type={command.price_type}"
            )
            return order

        if not session.orders_enabled:
            logger.warning(
                f"order rejected by session policy: session_id={command.session_id}, account_id={session.account_id}, mode={session.mode}, account_kind={session.account_kind}"
            )
            raise TradingServiceException(
                "order placement is disabled for this session",
                "ORDERS_DISABLED",
            )

        if not session.gateway:
            raise TradingServiceException("session is not connected to xttrader", "TRADER_NOT_CONNECTED")

        order_id = session.gateway.order_stock(
            stock_code=command.stock_code,
            order_type=command.side,
            order_volume=command.volume,
            price_type=command.price_type,
            price=float(command.price or 0.0),
            strategy_name=command.strategy_name,
            order_remark=command.order_remark,
        )
        order = {
            "account_id": session.account_id,
            "stock_code": command.stock_code,
            "instrument_name": "",
            "order_id": str(order_id),
            "order_sysid": "",
            "order_time_ms": int(time.time() * 1000),
            "order_type": command.side,
            "order_volume": command.volume,
            "price_type": command.price_type,
            "price": float(command.price or 0.0),
            "traded_volume": 0,
            "traded_price": 0.0,
            "order_status_code": 50,
            "status_msg": "submitted",
            "strategy_name": command.strategy_name,
            "order_remark": command.order_remark,
            "direction": "",
            "offset_flag": "",
            "secu_account": session.account_id,
        }
        with self._lock:
            session.orders[order["order_id"]] = order
        self._publish_event(command.session_id, "order_update", order)
        logger.info(
            f"submitted real order: session_id={command.session_id}, account_id={session.account_id}, order_id={order['order_id']}, stock_code={command.stock_code}, volume={command.volume}, price_type={command.price_type}"
        )
        return order

    def cancel_stock_order(self, command: CancelStockOrderCommand) -> CancelStockOrderResult:
        session = self._get_session(command.session_id)
        session_order = self._lookup_session_order(session, command)

        if session.mode == XTQuantMode.MOCK.value:
            if session_order is not None:
                with self._lock:
                    session_order["order_status_code"] = 54
                    session_order["status_msg"] = "cancelled"
                    session.orders[session_order["order_id"]] = session_order
                    self._publish_event(command.session_id, "order_update", session_order)
            logger.info(
                f"cancelled mock order: session_id={command.session_id}, account_id={session.account_id}, order_id={command.order_id or ''}, order_sysid={command.order_sysid or ''}"
            )
            return CancelStockOrderResult(
                accepted=True,
                confirmed=True,
                latest_order=session_order,
                still_cancelable=False,
                message="mock cancel confirmed",
            )

        if not session.orders_enabled:
            logger.warning(
                f"cancel rejected by session policy: session_id={command.session_id}, account_id={session.account_id}, mode={session.mode}, account_kind={session.account_kind}"
            )
            raise TradingServiceException(
                "order cancellation is disabled for this session",
                "ORDERS_DISABLED",
            )

        if not session.gateway:
            raise TradingServiceException("session is not connected to xttrader", "TRADER_NOT_CONNECTED")

        if command.order_id:
            result = session.gateway.cancel_order_stock(int(command.order_id))
        else:
            if not command.market or not command.order_sysid:
                raise TradingServiceException(
                    "market and order_sysid are required when cancelling by sysid",
                    "CANCEL_TARGET_REQUIRED",
                )
            normalized_market = self._normalize_cancel_market(command.market)
            result = session.gateway.cancel_order_stock_sysid(normalized_market, command.order_sysid)

        accepted = result == 0
        confirmation = CancelConfirmationResult(
            confirmed=False,
            latest_order=session_order,
            still_cancelable=session_order is not None,
        )
        if accepted:
            confirmation = self._confirm_cancel_result(session, command, session_order)
        if confirmation.confirmed and confirmation.latest_order is not None:
            with self._lock:
                session.orders[confirmation.latest_order["order_id"]] = confirmation.latest_order
            self._publish_event(command.session_id, "order_update", confirmation.latest_order)
        message = (
            "cancel request accepted and confirmed"
            if accepted and confirmation.confirmed
            else "cancel request accepted but final state is not confirmed yet"
            if accepted
            else "cancel request rejected by xttrader"
        )
        logger.info(
            f"cancel order result: session_id={command.session_id}, account_id={session.account_id}, accepted={accepted}, confirmed={confirmation.confirmed}, still_cancelable={confirmation.still_cancelable}, order_id={command.order_id or ''}, order_sysid={command.order_sysid or ''}"
        )
        return CancelStockOrderResult(
            accepted=accepted,
            confirmed=confirmation.confirmed,
            latest_order=confirmation.latest_order,
            still_cancelable=confirmation.still_cancelable,
            message=message,
        )

    def _lookup_session_order(
        self,
        session: TradingSession,
        command: CancelStockOrderCommand,
    ) -> dict[str, Any] | None:
        with self._lock:
            for order in session.orders.values():
                if self._matches_cancel_target(order, command):
                    return dict(order)
        return None

    def _matches_cancel_target(
        self,
        order: dict[str, Any],
        command: CancelStockOrderCommand,
    ) -> bool:
        if command.order_id:
            return str(order.get("order_id", "")) == str(command.order_id)
        if command.order_sysid:
            return str(order.get("order_sysid", "")) == str(command.order_sysid)
        return False

    def _confirm_cancel_result(
        self,
        session: TradingSession,
        command: CancelStockOrderCommand,
        session_order: OrderSnapshotPayload | None,
        timeout_sec: float = 2.0,
        poll_interval_sec: float = 0.2,
    ) -> CancelConfirmationResult:
        if not session.gateway:
            return CancelConfirmationResult(
                confirmed=False,
                latest_order=session_order,
                still_cancelable=session_order is not None,
            )
        deadline = time.monotonic() + timeout_sec
        latest_order = session_order
        while time.monotonic() <= deadline:
            full_orders = [
                self._convert_order(item)
                for item in (session.gateway.query_stock_orders(cancelable_only=False) or [])
            ]
            with self._lock:
                session.orders = {order["order_id"]: order for order in full_orders}
            matched_full = next(
                (order for order in full_orders if self._matches_cancel_target(order, command)),
                None,
            )
            if matched_full is not None:
                latest_order = matched_full
                if self._is_cancelled_order(matched_full):
                    return CancelConfirmationResult(
                        confirmed=True,
                        latest_order=matched_full,
                        still_cancelable=False,
                    )
            cancelable_orders = [
                self._convert_order(item)
                for item in (session.gateway.query_stock_orders(cancelable_only=True) or [])
            ]
            still_cancelable = any(
                self._matches_cancel_target(order, command) for order in cancelable_orders
            )
            if not still_cancelable:
                if matched_full is not None:
                    return CancelConfirmationResult(
                        confirmed=True,
                        latest_order=matched_full,
                        still_cancelable=False,
                    )
                if session_order is not None:
                    confirmed = dict(session_order)
                    confirmed["order_status_code"] = 54
                    confirmed["status_msg"] = "cancelled"
                    return CancelConfirmationResult(
                        confirmed=True,
                        latest_order=confirmed,
                        still_cancelable=False,
                    )
                return CancelConfirmationResult(
                    confirmed=True,
                    latest_order=latest_order,
                    still_cancelable=False,
                )
            time.sleep(poll_interval_sec)
        return CancelConfirmationResult(
            confirmed=False,
            latest_order=latest_order,
            still_cancelable=True,
        )

    def _is_cancelled_order(self, order: dict[str, Any]) -> bool:
        status_code = int(order.get("order_status_code", 0) or 0)
        status_msg = str(order.get("status_msg", "")).strip().lower()
        return (
            status_code
            in {
                XtOrderStatus.CANCELLED.value,
                XtOrderStatus.PARTIALLY_FILLED_CANCELLED.value,
            }
            or "cancel" in status_msg
            or "撤" in status_msg
        )

    def _is_terminal_order(self, order: OrderSnapshotPayload) -> bool:
        status_code = int(order.get("order_status_code", 0) or 0)
        return status_code in {status.value for status in XtOrderStatus}

    def _find_session_order(
        self,
        session: TradingSession,
        order_id: str,
        order_sysid: str,
    ) -> OrderSnapshotPayload | None:
        with self._lock:
            if order_id and order_id in session.orders:
                return dict(session.orders[order_id])
            for order in session.orders.values():
                if order_sysid and str(order.get("order_sysid", "")) == order_sysid:
                    return dict(order)
        return None

    def stream_events(
        self,
        session_id: str,
        stop_checker: Callable[[], bool] | None = None,
    ) -> Iterator[dict[str, Any]]:
        self._get_session(session_id)
        stream = self.event_hub.stream(session_id, stop_checker=stop_checker)
        if not self._get_session_if_active(session_id):
            stream.close()
        return stream

    def shutdown(self) -> None:
        with self._lock:
            session_ids = list(self._sessions.keys())
        for session_id in session_ids:
            self.close_session(session_id)

    def _connect_gateway(self, account_id: str, account_type: str, session_id: str) -> XTTraderGateway:
        if not XTQUANT_TRADER_AVAILABLE or not self.settings.xtquant.data.qmt_userdata_path:
            raise TradingServiceException(
                "xttrader is unavailable; check xtquant installation and qmt_userdata_path",
                "XTTRADER_UNAVAILABLE",
            )
        gateway = XTTraderGateway(
            qmt_userdata_path=self.settings.xtquant.data.qmt_userdata_path,
            account_id=account_id,
            account_type=account_type,
            event_handler=lambda event_type, payload: self._handle_gateway_event(session_id, event_type, payload),
        )
        try:
            gateway.connect()
        except RuntimeError as exc:
            raise TradingServiceException(str(exc), "XTTRADER_UNAVAILABLE") from exc
        return gateway

    def _resolve_account_profile(self, account_id: str, account_type: str) -> XTQuantTradingAccountConfig:
        normalized_account_type = self._normalize_account_type(account_type)
        mode = self.settings.xtquant.mode

        for profile in self.settings.xtquant.trading.accounts:
            if not profile.enabled:
                continue
            if profile.account_id != account_id:
                continue
            if self._normalize_account_type(profile.account_type) != normalized_account_type:
                continue
            if mode not in profile.allowed_modes:
                continue
            if mode == XTQuantMode.DEV and profile.account_kind != AccountKind.SIMULATED:
                continue
            if mode == XTQuantMode.PROD and profile.account_kind != AccountKind.REAL:
                continue
            return profile

        raise TradingServiceException(
            f"account {account_id} is not registered for {mode.value}",
            "ACCOUNT_PROFILE_NOT_ALLOWED",
        )

    def _orders_enabled(self, mode: XTQuantMode, account_kind: str) -> bool:
        if mode == XTQuantMode.MOCK:
            return True
        if mode == XTQuantMode.DEV:
            return account_kind == AccountKind.SIMULATED.value
        if mode == XTQuantMode.PROD:
            # pytest 中的 prod 自动化始终只读；真实 prod 放单能力仍由运行时配置控制。
            return account_kind == AccountKind.REAL.value and self.settings.xtquant.trading.enable_prod_orders
        return False

    def _normalize_account_type(self, value: str | None) -> str:
        return (value or "STOCK").strip().upper()

    def _require_credit_session(self, session_id: str) -> TradingSession:
        session = self._get_session(session_id)
        if session.account_type != "CREDIT":
            raise TradingServiceException(
                "credit query requires a CREDIT account session",
                "CREDIT_ACCOUNT_REQUIRED",
            )
        return session

    @staticmethod
    def _credit_instrument_filter(values: tuple[str, ...]) -> frozenset[str]:
        return frozenset(
            value.strip().upper()
            for value in values
            if value.strip()
        )

    @staticmethod
    def _validate_order_action(session: TradingSession, action_value: int) -> None:
        if session.account_type == "CREDIT":
            try:
                CreditOrderAction(action_value)
            except ValueError as exc:
                raise TradingServiceException(
                    f"unsupported CREDIT order action: {action_value}",
                    "INVALID_CREDIT_ORDER_ACTION",
                ) from exc
            return
        if action_value not in {
            CreditOrderAction.COLLATERAL_BUY.value,
            CreditOrderAction.COLLATERAL_SELL.value,
        }:
            raise TradingServiceException(
                "credit order action requires a CREDIT account session",
                "CREDIT_ACCOUNT_REQUIRED",
            )

    def _normalize_cancel_market(self, value: str | int) -> int:
        if isinstance(value, int):
            if value in {0, 1}:
                return value
            raise TradingServiceException("market must be 0/1 or SH/SZ", "INVALID_MARKET")

        normalized = str(value).strip().upper()
        if normalized in self.CANCEL_MARKET_MAP:
            return self.CANCEL_MARKET_MAP[normalized]
        raise TradingServiceException("market must be 0/1 or SH/SZ", "INVALID_MARKET")

    def _get_session(self, session_id: str) -> TradingSession:
        with self._lock:
            session = self._sessions.get(session_id)
        if not session or not session.accept_events:
            raise TradingServiceException("trading session does not exist", "SESSION_NOT_FOUND")
        return session

    def _query_asset(self, session: TradingSession) -> dict[str, Any]:
        if session.gateway:
            asset = session.gateway.query_stock_asset()
            return self._convert_asset(asset, session.account_id)
        return {
            "account_id": session.account_id,
            "cash": 950000.0,
            "frozen_cash": 50000.0,
            "market_value": 800000.0,
            "total_asset": 1800000.0,
            "fetch_balance": 900000.0,
        }

    def _serialize_session(self, session: TradingSession) -> dict[str, Any]:
        return {
            "session_id": session.session_id,
            "account_id": session.account_id,
            "account_type": session.account_type,
            "is_real": session.is_real,
            "mode": session.mode,
            "environment": session.environment,
            "account_profile": session.account_profile,
            "account_kind": session.account_kind,
            "orders_enabled": session.orders_enabled,
            "opened_at_ms": session.opened_at_ms,
        }

    def _build_mock_order(self, session: TradingSession, command: SubmitStockOrderCommand) -> dict[str, Any]:
        with self._lock:
            order_id = f"mock_{self._mock_order_counter}"
            self._mock_order_counter += 1
        order = {
            "account_id": session.account_id,
            "stock_code": command.stock_code,
            "instrument_name": f"Mock {command.stock_code}",
            "order_id": order_id,
            "order_sysid": "",
            "order_time_ms": int(time.time() * 1000),
            "order_type": command.side,
            "order_volume": command.volume,
            "price_type": command.price_type,
            "price": float(command.price or 0.0),
            "traded_volume": 0,
            "traded_price": 0.0,
            "order_status_code": 50,
            "status_msg": "submitted",
            "strategy_name": command.strategy_name,
            "order_remark": command.order_remark,
            "direction": "",
            "offset_flag": "",
            "secu_account": session.account_id,
        }
        with self._lock:
            session.orders[order_id] = order
        return order

    def _mock_position(self, account_id: str) -> dict[str, Any]:
        return {
            "account_id": account_id,
            "stock_code": "000001.SZ",
            "instrument_name": "Mock Position",
            "volume": 10000,
            "can_use_volume": 10000,
            "frozen_volume": 0,
            "on_road_volume": 0,
            "yesterday_volume": 10000,
            "open_price": 12.5,
            "avg_price": 12.5,
            "last_price": 13.2,
            "market_value": 132000.0,
            "profit_rate": 0.056,
            "direction": "LONG",
            "secu_account": account_id,
        }

    def _convert_order(self, order: Any) -> dict[str, Any]:
        return {
            "account_id": str(getattr(order, "account_id", "")),
            "stock_code": str(getattr(order, "stock_code", "")),
            "instrument_name": str(getattr(order, "instrument_name", "")),
            "order_id": str(getattr(order, "order_id", "")),
            "order_sysid": str(getattr(order, "order_sysid", "")),
            "order_time_ms": self._to_epoch_ms(getattr(order, "order_time", None)),
            "order_type": int(getattr(order, "order_type", 0) or 0),
            "order_volume": int(getattr(order, "order_volume", 0) or 0),
            "price_type": int(getattr(order, "price_type", 0) or 0),
            "price": float(getattr(order, "price", 0.0) or 0.0),
            "traded_volume": int(getattr(order, "traded_volume", 0) or 0),
            "traded_price": float(getattr(order, "traded_price", 0.0) or 0.0),
            "order_status_code": int(getattr(order, "order_status", 0) or 0),
            "status_msg": str(getattr(order, "status_msg", "")),
            "strategy_name": str(getattr(order, "strategy_name", "")),
            "order_remark": str(getattr(order, "order_remark", "")),
            "direction": str(getattr(order, "direction", "")),
            "offset_flag": str(getattr(order, "offset_flag", "")),
            "secu_account": str(getattr(order, "secu_account", "")),
        }

    def _convert_trade(self, trade: Any) -> dict[str, Any]:
        return {
            "account_id": str(getattr(trade, "account_id", "")),
            "stock_code": str(getattr(trade, "stock_code", "")),
            "instrument_name": str(getattr(trade, "instrument_name", "")),
            "order_type": int(getattr(trade, "order_type", 0) or 0),
            "traded_id": str(getattr(trade, "traded_id", "")),
            "traded_time_ms": self._to_epoch_ms(getattr(trade, "traded_time", None)),
            "traded_price": float(getattr(trade, "traded_price", 0.0) or 0.0),
            "traded_volume": int(getattr(trade, "traded_volume", 0) or 0),
            "traded_amount": float(getattr(trade, "traded_amount", 0.0) or 0.0),
            "order_id": str(getattr(trade, "order_id", "")),
            "order_sysid": str(getattr(trade, "order_sysid", "")),
            "strategy_name": str(getattr(trade, "strategy_name", "")),
            "order_remark": str(getattr(trade, "order_remark", "")),
            "direction": str(getattr(trade, "direction", "")),
            "offset_flag": str(getattr(trade, "offset_flag", "")),
            "commission": float(getattr(trade, "commission", 0.0) or 0.0),
            "secu_account": str(getattr(trade, "secu_account", "")),
        }

    def _convert_position(self, position: Any) -> dict[str, Any]:
        return {
            "account_id": str(getattr(position, "account_id", "")),
            "stock_code": str(getattr(position, "stock_code", "")),
            "instrument_name": str(getattr(position, "instrument_name", "")),
            "volume": int(getattr(position, "volume", 0) or 0),
            "can_use_volume": int(getattr(position, "can_use_volume", 0) or 0),
            "frozen_volume": int(getattr(position, "frozen_volume", 0) or 0),
            "on_road_volume": int(getattr(position, "on_road_volume", 0) or 0),
            "yesterday_volume": int(getattr(position, "yesterday_volume", 0) or 0),
            "open_price": float(getattr(position, "open_price", 0.0) or 0.0),
            "avg_price": float(getattr(position, "avg_price", 0.0) or 0.0),
            "last_price": float(getattr(position, "last_price", 0.0) or 0.0),
            "market_value": float(getattr(position, "market_value", 0.0) or 0.0),
            "profit_rate": float(getattr(position, "profit_rate", 0.0) or 0.0),
            "direction": str(getattr(position, "direction", "")),
            "secu_account": str(getattr(position, "secu_account", "")),
        }

    def _convert_asset(self, asset: Any, account_id: str) -> dict[str, Any]:
        return {
            "account_id": str(getattr(asset, "account_id", "") or account_id),
            "cash": float(getattr(asset, "cash", 0.0) or 0.0),
            "frozen_cash": float(getattr(asset, "frozen_cash", 0.0) or 0.0),
            "market_value": float(getattr(asset, "market_value", 0.0) or 0.0),
            "total_asset": float(getattr(asset, "total_asset", 0.0) or 0.0),
            "fetch_balance": float(getattr(asset, "fetch_balance", 0.0) or 0.0),
        }

    def _get_session_if_active(self, session_id: str) -> TradingSession | None:
        with self._lock:
            session = self._sessions.get(session_id)
            if not session or not session.accept_events:
                return None
            return session

    def _handle_gateway_event(self, session_id: str, event_type: str, payload: Any) -> None:
        session = self._get_session_if_active(session_id)
        if not session:
            logger.info(f"ignored gateway event for inactive session: session_id={session_id}, event_type={event_type}")
            return

        if event_type == "account_status":
            event_payload = {
                "account_id": str(getattr(payload, "account_id", "")),
                "account_type": int(getattr(payload, "account_type", 0) or 0),
                "status_code": int(getattr(payload, "status", 0) or 0),
            }
            self._publish_event(session_id, "account_status", event_payload)
            return
        if event_type == "stock_asset":
            asset = self._convert_asset(payload, session.account_id)
            with self._lock:
                if session.accept_events:
                    session.asset = asset
            self._publish_event(session_id, "asset_update", asset)
            return
        if event_type == "stock_order":
            order = self._convert_order(payload)
            with self._lock:
                if not session.accept_events:
                    return
                session.orders[order["order_id"]] = order
            self._publish_event(session_id, "order_update", order)
            return
        if event_type == "stock_trade":
            trade = self._convert_trade(payload)
            with self._lock:
                if not session.accept_events:
                    return
                session.trades.append(trade)
            self._publish_event(session_id, "trade_update", trade)
            return
        if event_type == "stock_position":
            self._publish_event(session_id, "position_update", self._convert_position(payload))
            return
        if event_type == "order_error":
            self._publish_event(
                session_id,
                "order_error",
                {
                    "account_id": str(getattr(payload, "account_id", "")),
                    "order_id": str(getattr(payload, "order_id", "")),
                    "error_id": int(getattr(payload, "error_id", 0) or 0),
                    "error_msg": str(getattr(payload, "error_msg", "")),
                    "strategy_name": str(getattr(payload, "strategy_name", "")),
                    "order_remark": str(getattr(payload, "order_remark", "")),
                },
            )
            return
        if event_type == "cancel_error":
            order_id = str(getattr(payload, "order_id", ""))
            order_sysid = str(getattr(payload, "order_sysid", ""))
            error_id = int(getattr(payload, "error_id", 0) or 0)
            error_msg = str(getattr(payload, "error_msg", ""))
            latest_order = self._find_session_order(
                session,
                order_id,
                order_sysid,
            )
            if latest_order is not None and self._is_terminal_order(latest_order):
                logger.info(
                    "ignored cancel error for terminal order: "
                    f"session_id={session_id}, order_id={order_id}, "
                    f"order_sysid={order_sysid}, "
                    f"order_status_code={latest_order['order_status_code']}, "
                    f"error_id={error_id}, error_msg={error_msg}"
                )
                self._publish_event(session_id, "order_update", latest_order)
                return
            logger.error(
                "xttrader cancel failed: "
                f"session_id={session_id}, order_id={order_id}, "
                f"order_sysid={order_sysid}, error_id={error_id}, "
                f"error_msg={error_msg}"
            )
            self._publish_event(
                session_id,
                "cancel_error",
                {
                    "account_id": str(getattr(payload, "account_id", "")),
                    "order_id": order_id,
                    "order_sysid": order_sysid,
                    "error_id": error_id,
                    "error_msg": error_msg,
                },
            )

    def _publish_event(self, session_id: str, event_type: str, payload: dict[str, Any]) -> None:
        self.event_hub.publish(
            session_id,
            {
                "event_time_ms": int(time.time() * 1000),
                "event_type": event_type,
                "payload": payload,
            },
        )

    def _to_epoch_ms(self, value: Any) -> int:
        if isinstance(value, datetime):
            return int(value.timestamp() * 1000)
        if value in (None, ""):
            return 0
        value_str = str(value)
        looks_like_calendar_value = len(value_str) in {8, 14} and value_str.isdigit()
        for fmt, expected_length in (("%Y%m%d%H%M%S", 14), ("%Y%m%d", 8)):
            if len(value_str) != expected_length or not value_str.isdigit():
                continue
            year = int(value_str[:4])
            if year < 2000 or year > 2100:
                continue
            try:
                return int(datetime.strptime(value_str, fmt).timestamp() * 1000)
            except (OSError, OverflowError, ValueError):
                continue
        if not looks_like_calendar_value:
            try:
                numeric = float(value)
                if numeric > 1_000_000_000_000:
                    return int(numeric)
                if numeric > 1_000_000_000:
                    return int(numeric * 1000)
            except (TypeError, ValueError):
                pass
        logger.warning(f"unable to normalize xttrader time value: raw={value!r}")
        return 0

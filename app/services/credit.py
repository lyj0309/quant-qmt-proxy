"""Strongly typed credit-account contracts and XTQuant field adapters."""

from __future__ import annotations

from enum import IntEnum
from typing import Protocol, TypedDict


class CreditOrderAction(IntEnum):
    """XTQuant credit-account order actions."""

    COLLATERAL_BUY = 23
    COLLATERAL_SELL = 24
    FINANCING_BUY = 27
    SHORT_SELL = 28
    BUY_TO_REPAY_SECURITIES = 29
    DIRECT_REPAY_SECURITIES = 30
    SELL_TO_REPAY_CASH = 31
    DIRECT_REPAY_CASH = 32
    SPECIAL_FINANCING_BUY = 40
    SPECIAL_SHORT_SELL = 41
    SPECIAL_BUY_TO_REPAY_SECURITIES = 42
    SPECIAL_DIRECT_REPAY_SECURITIES = 43
    SPECIAL_SELL_TO_REPAY_CASH = 44
    SPECIAL_DIRECT_REPAY_CASH = 45

    @property
    def expected_side(self) -> str:
        """Return the REST order side corresponding to this QMT action."""
        if self in {
            self.COLLATERAL_BUY,
            self.FINANCING_BUY,
            self.BUY_TO_REPAY_SECURITIES,
            self.SPECIAL_FINANCING_BUY,
            self.SPECIAL_BUY_TO_REPAY_SECURITIES,
        }:
            return "BUY"
        return "SELL"


MARKET_CREDIT_ACTIONS = frozenset(
    {
        CreditOrderAction.COLLATERAL_BUY,
        CreditOrderAction.COLLATERAL_SELL,
        CreditOrderAction.FINANCING_BUY,
        CreditOrderAction.SHORT_SELL,
        CreditOrderAction.BUY_TO_REPAY_SECURITIES,
        CreditOrderAction.SELL_TO_REPAY_CASH,
        CreditOrderAction.SPECIAL_FINANCING_BUY,
        CreditOrderAction.SPECIAL_SHORT_SELL,
        CreditOrderAction.SPECIAL_BUY_TO_REPAY_SECURITIES,
        CreditOrderAction.SPECIAL_SELL_TO_REPAY_CASH,
    }
)
DIRECT_CREDIT_ACTIONS = frozenset(set(CreditOrderAction) - MARKET_CREDIT_ACTIONS)


class CreditDetailRaw(Protocol):
    account_id: str
    account_type: int
    status: int
    update_time: int
    calc_config: int
    frozen_cash: float
    balance: float
    available: float
    position_profit: float
    market_value: float
    fetch_balance: float
    stock_value: float
    fund_value: float
    total_debt: float
    enable_bail_balance: float
    per_assurescale_value: float
    assure_asset: float
    fin_debt: float
    fin_deal_avl: float
    fin_fee: float
    slo_debt: float
    slo_market_value: float
    slo_fee: float
    other_fare: float
    fin_max_quota: float
    fin_enable_quota: float
    fin_used_quota: float
    slo_max_quota: float
    slo_enable_quota: float
    slo_used_quota: float
    slo_sell_balance: float
    used_slo_sell_balance: float
    surplus_slo_sell_balance: float


class CreditCompactRaw(Protocol):
    account_id: str
    account_type: int
    compact_type: int
    cashgroup_prop: int
    exchange_id: int
    open_date: int
    business_vol: int
    real_compact_vol: int
    ret_end_date: int
    business_balance: float
    businessFare: float
    real_compact_balance: float
    real_compact_fare: float
    repaid_fare: float
    repaid_balance: float
    instrument_id: str
    compact_id: str
    position_str: str


class CreditSubjectRaw(Protocol):
    account_id: str
    account_type: int
    slo_status: int
    fin_status: int
    exchange_id: int
    slo_ratio: float
    fin_ratio: float
    instrument_id: str


class CreditSloCodeRaw(Protocol):
    account_id: str
    account_type: int
    cashgroup_prop: int
    exchange_id: int
    enable_amount: int
    instrument_id: str


class CreditAssureRaw(Protocol):
    account_id: str
    account_type: int
    assure_status: int
    exchange_id: int
    assure_ratio: float
    instrument_id: str


class CreditDetailPayload(TypedDict):
    account_id: str
    account_type: int
    status_code: int
    update_time: int
    calc_config: int
    frozen_cash: float
    balance: float
    available: float
    position_profit: float
    market_value: float
    fetch_balance: float
    stock_value: float
    fund_value: float
    total_debt: float
    enable_bail_balance: float
    maintenance_collateral_ratio: float
    assure_asset: float
    financing_debt: float
    financing_principal: float
    financing_fee: float
    securities_lending_debt: float
    securities_lending_market_value: float
    securities_lending_fee: float
    other_fee: float
    financing_max_quota: float
    financing_available_quota: float
    financing_used_quota: float
    securities_lending_max_quota: float
    securities_lending_available_quota: float
    securities_lending_used_quota: float
    short_sale_cash: float
    used_short_sale_cash: float
    remaining_short_sale_cash: float


class CreditCompactPayload(TypedDict):
    account_id: str
    account_type: int
    compact_type: int
    cashgroup_prop: int
    exchange_id: int
    open_date: int
    business_volume: int
    outstanding_volume: int
    due_date: int
    business_balance: float
    business_fee: float
    outstanding_balance: float
    outstanding_fee: float
    repaid_fee: float
    repaid_balance: float
    instrument_id: str
    compact_id: str
    position_str: str


class CreditSubjectPayload(TypedDict):
    account_id: str
    account_type: int
    short_sell_status: int
    financing_status: int
    exchange_id: int
    short_sell_margin_ratio: float
    financing_margin_ratio: float
    instrument_id: str


class CreditSloCodePayload(TypedDict):
    account_id: str
    account_type: int
    cashgroup_prop: int
    exchange_id: int
    available_quantity: int
    instrument_id: str


class CreditAssurePayload(TypedDict):
    account_id: str
    account_type: int
    assure_status: int
    exchange_id: int
    assure_ratio: float
    instrument_id: str


def convert_credit_detail(item: CreditDetailRaw) -> CreditDetailPayload:
    """Convert one XTQuant credit-account detail without reflection."""
    return {
        "account_id": str(item.account_id),
        "account_type": int(item.account_type),
        "status_code": int(item.status),
        "update_time": int(item.update_time),
        "calc_config": int(item.calc_config),
        "frozen_cash": float(item.frozen_cash),
        "balance": float(item.balance),
        "available": float(item.available),
        "position_profit": float(item.position_profit),
        "market_value": float(item.market_value),
        "fetch_balance": float(item.fetch_balance),
        "stock_value": float(item.stock_value),
        "fund_value": float(item.fund_value),
        "total_debt": float(item.total_debt),
        "enable_bail_balance": float(item.enable_bail_balance),
        "maintenance_collateral_ratio": float(item.per_assurescale_value),
        "assure_asset": float(item.assure_asset),
        "financing_debt": float(item.fin_debt),
        "financing_principal": float(item.fin_deal_avl),
        "financing_fee": float(item.fin_fee),
        "securities_lending_debt": float(item.slo_debt),
        "securities_lending_market_value": float(item.slo_market_value),
        "securities_lending_fee": float(item.slo_fee),
        "other_fee": float(item.other_fare),
        "financing_max_quota": float(item.fin_max_quota),
        "financing_available_quota": float(item.fin_enable_quota),
        "financing_used_quota": float(item.fin_used_quota),
        "securities_lending_max_quota": float(item.slo_max_quota),
        "securities_lending_available_quota": float(item.slo_enable_quota),
        "securities_lending_used_quota": float(item.slo_used_quota),
        "short_sale_cash": float(item.slo_sell_balance),
        "used_short_sale_cash": float(item.used_slo_sell_balance),
        "remaining_short_sale_cash": float(item.surplus_slo_sell_balance),
    }


def convert_credit_compact(item: CreditCompactRaw) -> CreditCompactPayload:
    return {
        "account_id": str(item.account_id),
        "account_type": int(item.account_type),
        "compact_type": int(item.compact_type),
        "cashgroup_prop": int(item.cashgroup_prop),
        "exchange_id": int(item.exchange_id),
        "open_date": int(item.open_date),
        "business_volume": int(item.business_vol),
        "outstanding_volume": int(item.real_compact_vol),
        "due_date": int(item.ret_end_date),
        "business_balance": float(item.business_balance),
        "business_fee": float(item.businessFare),
        "outstanding_balance": float(item.real_compact_balance),
        "outstanding_fee": float(item.real_compact_fare),
        "repaid_fee": float(item.repaid_fare),
        "repaid_balance": float(item.repaid_balance),
        "instrument_id": str(item.instrument_id),
        "compact_id": str(item.compact_id),
        "position_str": str(item.position_str),
    }


def convert_credit_subject(item: CreditSubjectRaw) -> CreditSubjectPayload:
    return {
        "account_id": str(item.account_id),
        "account_type": int(item.account_type),
        "short_sell_status": int(item.slo_status),
        "financing_status": int(item.fin_status),
        "exchange_id": int(item.exchange_id),
        "short_sell_margin_ratio": float(item.slo_ratio),
        "financing_margin_ratio": float(item.fin_ratio),
        "instrument_id": str(item.instrument_id),
    }


def convert_credit_slo_code(item: CreditSloCodeRaw) -> CreditSloCodePayload:
    return {
        "account_id": str(item.account_id),
        "account_type": int(item.account_type),
        "cashgroup_prop": int(item.cashgroup_prop),
        "exchange_id": int(item.exchange_id),
        "available_quantity": int(item.enable_amount),
        "instrument_id": str(item.instrument_id),
    }


def convert_credit_assure(item: CreditAssureRaw) -> CreditAssurePayload:
    return {
        "account_id": str(item.account_id),
        "account_type": int(item.account_type),
        "assure_status": int(item.assure_status),
        "exchange_id": int(item.exchange_id),
        "assure_ratio": float(item.assure_ratio),
        "instrument_id": str(item.instrument_id),
    }


def credit_symbol_matches(instrument_id: str, requested_symbols: frozenset[str]) -> bool:
    """Match QMT's bare or suffixed instrument id against requested symbols."""
    if not requested_symbols:
        return True
    normalized = instrument_id.strip().upper()
    bare = normalized.split(".", maxsplit=1)[0]
    return normalized in requested_symbols or any(
        requested.split(".", maxsplit=1)[0] == bare
        for requested in requested_symbols
    )

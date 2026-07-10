"""Execution-feasibility checks applied before costly analysis and scoring."""

from dataclasses import dataclass
from typing import Dict, Iterable, List

import config
from common.models import TickerData


@dataclass(frozen=True)
class ExecutionDecision:
    executable: bool
    rejection_reasons: List[str]
    spread_bps: float | None = None
    expected_slippage_bps: float | None = None


def assess_execution(
    ticker: TickerData, raw_ticker: dict | None, orderbook: dict | None
) -> ExecutionDecision:
    raw_ticker = raw_ticker or {}
    if raw_ticker.get("market_warning") not in (None, "NONE"):
        return ExecutionDecision(False, ["market_warning"])
    if float(raw_ticker.get("acc_trade_price_24h") or 0) < config.EXECUTION_MIN_DAILY_TURNOVER_KRW:
        return ExecutionDecision(False, ["daily_turnover_below_minimum"])
    units = (orderbook or {}).get("orderbook_units") or []
    if not units:
        return ExecutionDecision(False, ["orderbook_unavailable"])
    best = units[0]
    bid, ask = float(best.get("bid_price") or 0), float(best.get("ask_price") or 0)
    if bid <= 0 or ask <= 0 or ask < bid:
        return ExecutionDecision(False, ["orderbook_invalid"])
    mid = (bid + ask) / 2
    spread_bps = (ask - bid) / mid * 10_000
    bid_depth = sum(float(unit.get("bid_price") or 0) * float(unit.get("bid_size") or 0) for unit in units)
    ask_depth = sum(float(unit.get("ask_price") or 0) * float(unit.get("ask_size") or 0) for unit in units)
    if min(bid_depth, ask_depth) < config.EXECUTION_NOTIONAL_KRW:
        return ExecutionDecision(False, ["orderbook_depth_below_notional"], spread_bps)
    buy = _vwap(units, "ask_price", "ask_size", config.EXECUTION_NOTIONAL_KRW)
    sell = _vwap(units, "bid_price", "bid_size", config.EXECUTION_NOTIONAL_KRW)
    if buy is None or sell is None:
        return ExecutionDecision(False, ["orderbook_depth_below_notional"], spread_bps)
    slippage_bps = max((buy - ask) / mid * 10_000, (bid - sell) / mid * 10_000)
    if spread_bps > config.EXECUTION_MAX_SPREAD_BPS:
        return ExecutionDecision(False, ["spread_above_maximum"], spread_bps, slippage_bps)
    if slippage_bps > config.EXECUTION_MAX_SLIPPAGE_BPS:
        return ExecutionDecision(False, ["slippage_above_maximum"], spread_bps, slippage_bps)
    observed_move_bps = abs(ticker.price_change_10m or 0) * 100
    if observed_move_bps <= config.ESTIMATED_ROUND_TRIP_COST_BPS + spread_bps + 2 * slippage_bps:
        return ExecutionDecision(False, ["move_does_not_cover_estimated_costs"], spread_bps, slippage_bps)
    return ExecutionDecision(True, [], spread_bps, slippage_bps)


def _vwap(units: Iterable[dict], price_key: str, size_key: str, notional: float) -> float | None:
    remaining = notional
    for unit in units:
        price, size = float(unit.get(price_key) or 0), float(unit.get(size_key) or 0)
        if price <= 0 or size <= 0:
            continue
        available = price * size
        taken = min(remaining, available)
        remaining -= taken
        if remaining <= 0:
            return _filled_vwap(units, price_key, size_key, notional)
    return None


def _filled_vwap(units: Iterable[dict], price_key: str, size_key: str, notional: float) -> float:
    remaining, quantity = notional, 0.0
    for unit in units:
        price, size = float(unit.get(price_key) or 0), float(unit.get(size_key) or 0)
        taken = min(remaining, price * size)
        quantity += taken / price
        remaining -= taken
        if remaining <= 0:
            break
    return notional / quantity

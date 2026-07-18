"""Execution-feasibility checks applied before costly analysis and scoring."""

from dataclasses import dataclass
from enum import StrEnum
from typing import List, Sequence

import config
from common.models import (
    MarketTicker,
    OrderBookSnapshot,
    OrderBookUnit,
    RejectionCode,
    TickerData,
)


@dataclass(frozen=True)
class ExecutionDecision:
    executable: bool
    rejection_reasons: List[RejectionCode]
    spread_bps: float | None = None
    expected_slippage_bps: float | None = None


class ExecutionSide(StrEnum):
    BID = "bid"
    ASK = "ask"


def assess_execution(
    ticker: TickerData,
    raw_ticker: MarketTicker | None,
    orderbook: OrderBookSnapshot | None,
) -> ExecutionDecision:
    if raw_ticker and raw_ticker.market_event.blocks_execution:
        return ExecutionDecision(False, [RejectionCode.MARKET_WARNING])
    if (
        raw_ticker is None
        or raw_ticker.acc_trade_price_24h
        < config.EXECUTION_MIN_DAILY_TURNOVER_KRW
    ):
        return ExecutionDecision(False, [RejectionCode.DAILY_TURNOVER_BELOW_MINIMUM])
    if orderbook is None:
        return ExecutionDecision(False, [RejectionCode.ORDERBOOK_UNAVAILABLE])
    units = orderbook.orderbook_units
    best = units[0]
    bid, ask = best.bid_price, best.ask_price
    if ask < bid:
        return ExecutionDecision(False, [RejectionCode.ORDERBOOK_INVALID])
    mid = (bid + ask) / 2
    spread_bps = (ask - bid) / mid * 10_000
    bid_depth = sum(unit.bid_price * unit.bid_size for unit in units)
    ask_depth = sum(unit.ask_price * unit.ask_size for unit in units)
    if min(bid_depth, ask_depth) < config.EXECUTION_NOTIONAL_KRW:
        return ExecutionDecision(
            False, [RejectionCode.ORDERBOOK_DEPTH_BELOW_NOTIONAL], spread_bps
        )
    buy = _vwap(units, ExecutionSide.ASK, config.EXECUTION_NOTIONAL_KRW)
    sell = _vwap(units, ExecutionSide.BID, config.EXECUTION_NOTIONAL_KRW)
    if buy is None or sell is None:
        return ExecutionDecision(
            False, [RejectionCode.ORDERBOOK_DEPTH_BELOW_NOTIONAL], spread_bps
        )
    slippage_bps = max((buy - ask) / mid * 10_000, (bid - sell) / mid * 10_000)
    if spread_bps > config.EXECUTION_MAX_SPREAD_BPS:
        return ExecutionDecision(
            False, [RejectionCode.SPREAD_ABOVE_MAXIMUM], spread_bps, slippage_bps
        )
    if slippage_bps > config.EXECUTION_MAX_SLIPPAGE_BPS:
        return ExecutionDecision(
            False, [RejectionCode.SLIPPAGE_ABOVE_MAXIMUM], spread_bps, slippage_bps
        )
    observed_move_bps = abs(ticker.price_change_10m or 0) * 100
    if observed_move_bps <= config.ESTIMATED_ROUND_TRIP_COST_BPS + spread_bps + 2 * slippage_bps:
        return ExecutionDecision(
            False,
            [RejectionCode.MOVE_DOES_NOT_COVER_ESTIMATED_COSTS],
            spread_bps,
            slippage_bps,
        )
    return ExecutionDecision(True, [], spread_bps, slippage_bps)


def _unit_price_and_size(
    unit: OrderBookUnit, side: ExecutionSide
) -> tuple[float, float]:
    if side is ExecutionSide.ASK:
        return unit.ask_price, unit.ask_size
    return unit.bid_price, unit.bid_size


def _vwap(
    units: Sequence[OrderBookUnit], side: ExecutionSide, notional: float
) -> float | None:
    remaining = notional
    for unit in units:
        price, size = _unit_price_and_size(unit, side)
        available = price * size
        taken = min(remaining, available)
        remaining -= taken
        if remaining <= 0:
            return _filled_vwap(units, side, notional)
    return None


def _filled_vwap(
    units: Sequence[OrderBookUnit], side: ExecutionSide, notional: float
) -> float:
    remaining, quantity = notional, 0.0
    for unit in units:
        price, size = _unit_price_and_size(unit, side)
        taken = min(remaining, price * size)
        quantity += taken / price
        remaining -= taken
        if remaining <= 0:
            break
    return notional / quantity

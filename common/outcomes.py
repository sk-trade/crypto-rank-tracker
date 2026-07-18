"""Fixed, execution-aware outcome definitions for signal evaluation."""

from dataclasses import dataclass
from typing import Iterable

import config
from common.models import Direction


@dataclass(frozen=True)
class PerformanceTarget:
    """The one immutable primary target used to evaluate all signal variants."""

    holding_period_minutes: int
    execution_timeframe_minutes: int
    estimated_round_trip_cost_bps: float

    @property
    def holding_period_bars(self) -> int:
        return self.holding_period_minutes // self.execution_timeframe_minutes


PRIMARY_PERFORMANCE_TARGET = PerformanceTarget(
    holding_period_minutes=config.PRIMARY_HOLDING_PERIOD_MINUTES,
    execution_timeframe_minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES,
    estimated_round_trip_cost_bps=config.ESTIMATED_ROUND_TRIP_COST_BPS,
)


def directional_net_return(
    entry_price: float,
    exit_price: float,
    direction: Direction,
    round_trip_cost_bps: float = PRIMARY_PERFORMANCE_TARGET.estimated_round_trip_cost_bps,
) -> float:
    """Return a directional holding-period return after the fixed round-trip cost."""
    if entry_price <= 0 or exit_price <= 0:
        raise ValueError("entry_price and exit_price must be positive")
    if direction is Direction.LONG:
        gross_return = exit_price / entry_price - 1
    elif direction is Direction.SHORT:
        gross_return = entry_price / exit_price - 1
    else:
        raise ValueError(f"Unsupported direction: {direction}")
    return gross_return - round_trip_cost_bps / 10_000


def favorable_and_adverse_excursions(
    entry_price: float,
    highs: Iterable[float],
    lows: Iterable[float],
    direction: Direction,
) -> tuple[float, float]:
    """Return directional MFE and MAE over the fixed holding window before costs."""
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")
    high_values = list(highs)
    low_values = list(lows)
    if not high_values or not low_values:
        raise ValueError("highs and lows must not be empty")
    if min(high_values) <= 0 or min(low_values) <= 0:
        raise ValueError("highs and lows must be positive")

    if direction is Direction.LONG:
        return max(high_values) / entry_price - 1, min(low_values) / entry_price - 1
    if direction is Direction.SHORT:
        return entry_price / min(low_values) - 1, entry_price / max(high_values) - 1
    raise ValueError(f"Unsupported direction: {direction}")

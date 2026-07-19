import pytest

from common.models import Direction
from common.outcomes import (
    PRIMARY_PERFORMANCE_TARGET,
    directional_net_return,
    favorable_and_adverse_excursions,
)


def test_primary_target_is_fixed_to_one_hour_from_the_next_ten_minute_open():
    assert PRIMARY_PERFORMANCE_TARGET.holding_period_minutes == 60
    assert PRIMARY_PERFORMANCE_TARGET.execution_timeframe_minutes == 10
    assert PRIMARY_PERFORMANCE_TARGET.holding_period_bars == 6
    assert PRIMARY_PERFORMANCE_TARGET.estimated_round_trip_cost_bps == 10.0


def test_directional_returns_deduct_the_fixed_round_trip_cost_for_both_directions():
    assert directional_net_return(100.0, 110.0, Direction.LONG) == pytest.approx(
        0.099
    )
    assert directional_net_return(100.0, 90.0, Direction.SHORT) == pytest.approx(
        (100 / 90) - 1 - 0.001
    )


def test_mfe_and_mae_follow_the_signal_direction():
    assert favorable_and_adverse_excursions(
        100.0, [105.0, 112.0], [98.0, 94.0], Direction.LONG
    ) == pytest.approx((0.12, -0.06))
    assert favorable_and_adverse_excursions(
        100.0, [105.0, 112.0], [98.0, 94.0], Direction.SHORT
    ) == pytest.approx((100 / 94 - 1, 100 / 112 - 1))


def test_outcome_calculation_rejects_invalid_prices_or_directions():
    with pytest.raises(ValueError):
        directional_net_return(0.0, 100.0, "long")
    with pytest.raises(ValueError):
        favorable_and_adverse_excursions(100.0, [], [], "long")

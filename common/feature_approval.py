"""Fail-closed OOS approval policy for prospective signal features."""

from dataclasses import dataclass
from enum import StrEnum
import math
from typing import Mapping

from common.models import LiquidityTier, MarketRegime


@dataclass(frozen=True)
class FeatureEvaluation:
    net_expected_return_delta: float
    fixed_alert_hit_rate_delta: float
    liquidity_deltas: Mapping[LiquidityTier, float]
    regime_deltas: Mapping[MarketRegime, float]
    confidence_interval: tuple[float, float]


class FeatureApprovalCode(StrEnum):
    INVALID_EVALUATION_METRICS = "invalid_evaluation_metrics"
    NET_EXPECTED_RETURN_NOT_IMPROVED = "net_expected_return_not_improved"
    FIXED_ALERT_HIT_RATE_NOT_IMPROVED = "fixed_alert_hit_rate_not_improved"
    INCREMENTAL_VALUE_NOT_SIGNIFICANT = "incremental_value_not_significant"
    LIQUIDITY_DIRECTION_NOT_STABLE = "liquidity_direction_not_stable"
    REGIME_DIRECTION_NOT_STABLE = "regime_direction_not_stable"
    LIQUIDITY_STRATA_INCOMPLETE = "liquidity_strata_incomplete"
    REGIME_STRATA_INCOMPLETE = "regime_strata_incomplete"


@dataclass(frozen=True)
class FeatureApprovalResult:
    approved: bool
    reasons: tuple[FeatureApprovalCode, ...]


_REQUIRED_LIQUIDITY_TIERS = frozenset(
    {LiquidityTier.LOW, LiquidityTier.MEDIUM, LiquidityTier.HIGH}
)
_REQUIRED_MARKET_REGIMES = frozenset(
    {
        MarketRegime.HIGH_VOLATILITY,
        MarketRegime.TRENDING_BULL,
        MarketRegime.TRENDING_BEAR,
        MarketRegime.MEAN_REVERSION,
    }
)


def _finite_number(value: object) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def approve_feature(evaluation: FeatureEvaluation) -> FeatureApprovalResult:
    interval = evaluation.confidence_interval
    metrics = [
        evaluation.net_expected_return_delta,
        evaluation.fixed_alert_hit_rate_delta,
        *evaluation.liquidity_deltas.values(),
        *evaluation.regime_deltas.values(),
        *interval,
    ]
    if (
        len(interval) != 2
        or not all(_finite_number(value) for value in metrics)
        or not -1 <= evaluation.fixed_alert_hit_rate_delta <= 1
        or interval[0] > interval[1]
    ):
        return FeatureApprovalResult(
            approved=False,
            reasons=(FeatureApprovalCode.INVALID_EVALUATION_METRICS,),
        )

    reasons = []
    if evaluation.net_expected_return_delta <= 0:
        reasons.append(FeatureApprovalCode.NET_EXPECTED_RETURN_NOT_IMPROVED)
    if evaluation.fixed_alert_hit_rate_delta <= 0:
        reasons.append(FeatureApprovalCode.FIXED_ALERT_HIT_RATE_NOT_IMPROVED)
    if evaluation.confidence_interval[0] <= 0:
        reasons.append(FeatureApprovalCode.INCREMENTAL_VALUE_NOT_SIGNIFICANT)
    if set(evaluation.liquidity_deltas) != _REQUIRED_LIQUIDITY_TIERS:
        reasons.append(FeatureApprovalCode.LIQUIDITY_STRATA_INCOMPLETE)
    if not evaluation.liquidity_deltas or any(
        value <= 0 for value in evaluation.liquidity_deltas.values()
    ):
        reasons.append(FeatureApprovalCode.LIQUIDITY_DIRECTION_NOT_STABLE)
    if set(evaluation.regime_deltas) != _REQUIRED_MARKET_REGIMES:
        reasons.append(FeatureApprovalCode.REGIME_STRATA_INCOMPLETE)
    if not evaluation.regime_deltas or any(
        value <= 0 for value in evaluation.regime_deltas.values()
    ):
        reasons.append(FeatureApprovalCode.REGIME_DIRECTION_NOT_STABLE)
    return FeatureApprovalResult(approved=not reasons, reasons=tuple(reasons))

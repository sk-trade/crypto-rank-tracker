"""Fail-closed OOS approval policy for prospective signal features."""

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class FeatureEvaluation:
    net_expected_return_delta: float
    fixed_alert_hit_rate_delta: float
    liquidity_deltas: Mapping[str, float]
    regime_deltas: Mapping[str, float]
    confidence_interval: tuple[float, float]


def approve_feature(evaluation: FeatureEvaluation) -> tuple[bool, list[str]]:
    reasons = []
    if evaluation.net_expected_return_delta <= 0:
        reasons.append("net_expected_return_not_improved")
    if evaluation.fixed_alert_hit_rate_delta <= 0:
        reasons.append("fixed_alert_hit_rate_not_improved")
    if evaluation.confidence_interval[0] <= 0:
        reasons.append("incremental_value_not_significant")
    if not evaluation.liquidity_deltas or any(value <= 0 for value in evaluation.liquidity_deltas.values()):
        reasons.append("liquidity_direction_not_stable")
    if not evaluation.regime_deltas or any(value <= 0 for value in evaluation.regime_deltas.values()):
        reasons.append("regime_direction_not_stable")
    return not reasons, reasons

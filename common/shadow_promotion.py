"""Promotion policy for a frozen model and threshold shadow run."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ShadowRun:
    model_version: str
    threshold: float
    independent_events: int
    net_expected_return: float
    hit_rate: float


def promote_shadow_run(
    run: ShadowRun, *, frozen_model_version: str, frozen_threshold: float,
    minimum_events: int, minimum_net_expected_return: float, minimum_hit_rate: float,
) -> tuple[bool, list[str]]:
    reasons = []
    if run.model_version != frozen_model_version or run.threshold != frozen_threshold:
        reasons.append("model_or_threshold_not_frozen")
    if run.independent_events < minimum_events:
        reasons.append("insufficient_independent_shadow_events")
    if run.net_expected_return < minimum_net_expected_return:
        reasons.append("shadow_net_expected_return_below_minimum")
    if run.hit_rate < minimum_hit_rate:
        reasons.append("shadow_hit_rate_below_minimum")
    return not reasons, reasons

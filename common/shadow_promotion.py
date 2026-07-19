"""Promotion policy for a frozen model and threshold shadow run."""

from dataclasses import dataclass
from enum import StrEnum
import math


@dataclass(frozen=True)
class ShadowRun:
    model_version: str
    threshold: float
    independent_events: int
    net_expected_return: float
    hit_rate: float


class ShadowPromotionCode(StrEnum):
    INVALID_SHADOW_METRICS = "invalid_shadow_metrics"
    MODEL_OR_THRESHOLD_NOT_FROZEN = "model_or_threshold_not_frozen"
    INSUFFICIENT_INDEPENDENT_EVENTS = "insufficient_independent_shadow_events"
    NET_EXPECTED_RETURN_BELOW_MINIMUM = "shadow_net_expected_return_below_minimum"
    HIT_RATE_BELOW_MINIMUM = "shadow_hit_rate_below_minimum"


@dataclass(frozen=True)
class ShadowPromotionResult:
    approved: bool
    reasons: tuple[ShadowPromotionCode, ...]


def promote_shadow_run(
    run: ShadowRun, *, frozen_model_version: str, frozen_threshold: float,
    minimum_events: int, minimum_net_expected_return: float, minimum_hit_rate: float,
) -> ShadowPromotionResult:
    finite_metrics = (
        run.threshold,
        run.net_expected_return,
        run.hit_rate,
        frozen_threshold,
        minimum_net_expected_return,
        minimum_hit_rate,
    )
    if (
        not run.model_version
        or not all(math.isfinite(value) for value in finite_metrics)
        or isinstance(run.independent_events, bool)
        or isinstance(minimum_events, bool)
        or run.independent_events < 0
        or minimum_events < 1
        or not 0 <= run.hit_rate <= 1
        or not 0 <= minimum_hit_rate <= 1
    ):
        return ShadowPromotionResult(
            approved=False,
            reasons=(ShadowPromotionCode.INVALID_SHADOW_METRICS,),
        )

    reasons = []
    if run.model_version != frozen_model_version or run.threshold != frozen_threshold:
        reasons.append(ShadowPromotionCode.MODEL_OR_THRESHOLD_NOT_FROZEN)
    if run.independent_events < minimum_events:
        reasons.append(ShadowPromotionCode.INSUFFICIENT_INDEPENDENT_EVENTS)
    if run.net_expected_return < minimum_net_expected_return:
        reasons.append(ShadowPromotionCode.NET_EXPECTED_RETURN_BELOW_MINIMUM)
    if run.hit_rate < minimum_hit_rate:
        reasons.append(ShadowPromotionCode.HIT_RATE_BELOW_MINIMUM)
    return ShadowPromotionResult(approved=not reasons, reasons=tuple(reasons))

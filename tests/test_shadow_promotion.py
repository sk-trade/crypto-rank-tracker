from common.shadow_promotion import (
    ShadowPromotionCode,
    ShadowRun,
    promote_shadow_run,
)


def test_shadow_promotion_requires_frozen_and_sufficiently_performant_run():
    run = ShadowRun("baseline-v1", 0.7, 100, 0.1, 0.6)
    result = promote_shadow_run(run, frozen_model_version="baseline-v1", frozen_threshold=0.7, minimum_events=100, minimum_net_expected_return=0.05, minimum_hit_rate=0.55)
    assert result.approved
    assert result.reasons == ()


def test_shadow_promotion_fails_closed_for_changed_or_weak_run():
    run = ShadowRun("baseline-v2", 0.7, 99, -0.1, 0.4)
    result = promote_shadow_run(run, frozen_model_version="baseline-v1", frozen_threshold=0.7, minimum_events=100, minimum_net_expected_return=0.05, minimum_hit_rate=0.55)
    assert not result.approved
    assert result.reasons == (
        ShadowPromotionCode.MODEL_OR_THRESHOLD_NOT_FROZEN,
        ShadowPromotionCode.INSUFFICIENT_INDEPENDENT_EVENTS,
        ShadowPromotionCode.NET_EXPECTED_RETURN_BELOW_MINIMUM,
        ShadowPromotionCode.HIT_RATE_BELOW_MINIMUM,
    )


def test_shadow_promotion_rejects_non_finite_metrics():
    run = ShadowRun("baseline-v1", 0.7, 100, float("nan"), float("nan"))

    result = promote_shadow_run(
        run,
        frozen_model_version="baseline-v1",
        frozen_threshold=0.7,
        minimum_events=100,
        minimum_net_expected_return=0.05,
        minimum_hit_rate=0.55,
    )

    assert result.reasons == (ShadowPromotionCode.INVALID_SHADOW_METRICS,)

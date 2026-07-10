from common.shadow_promotion import ShadowRun, promote_shadow_run


def test_shadow_promotion_requires_frozen_and_sufficiently_performant_run():
    run = ShadowRun("baseline-v1", 0.7, 100, 0.1, 0.6)
    assert promote_shadow_run(run, frozen_model_version="baseline-v1", frozen_threshold=0.7, minimum_events=100, minimum_net_expected_return=0.05, minimum_hit_rate=0.55) == (True, [])


def test_shadow_promotion_fails_closed_for_changed_or_weak_run():
    run = ShadowRun("baseline-v2", 0.7, 99, -0.1, 0.4)
    approved, reasons = promote_shadow_run(run, frozen_model_version="baseline-v1", frozen_threshold=0.7, minimum_events=100, minimum_net_expected_return=0.05, minimum_hit_rate=0.55)
    assert not approved
    assert len(reasons) == 4

from common.feature_approval import FeatureEvaluation, approve_feature


def test_feature_requires_significant_and_stable_incremental_oos_value():
    approved, reasons = approve_feature(FeatureEvaluation(0.1, 0.05, {"HIGH": 0.1, "LOW": 0.01}, {"BULL": 0.1, "BEAR": 0.02}, (0.01, 0.2)))
    assert approved and not reasons


def test_feature_is_rejected_when_any_required_oos_check_fails():
    approved, reasons = approve_feature(FeatureEvaluation(0.1, 0.05, {"HIGH": 0.1, "LOW": -0.01}, {"BULL": 0.1}, (-0.01, 0.2)))
    assert not approved
    assert "incremental_value_not_significant" in reasons
    assert "liquidity_direction_not_stable" in reasons

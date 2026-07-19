from common.feature_approval import (
    FeatureApprovalCode,
    FeatureEvaluation,
    approve_feature,
)
from common.models import LiquidityTier, MarketRegime


def test_feature_requires_significant_and_stable_incremental_oos_value():
    result = approve_feature(
        FeatureEvaluation(
            0.1,
            0.05,
            {
                LiquidityTier.HIGH: 0.1,
                LiquidityTier.MEDIUM: 0.05,
                LiquidityTier.LOW: 0.01,
            },
            {
                MarketRegime.HIGH_VOLATILITY: 0.03,
                MarketRegime.TRENDING_BULL: 0.1,
                MarketRegime.TRENDING_BEAR: 0.02,
                MarketRegime.MEAN_REVERSION: 0.01,
            },
            (0.01, 0.2),
        )
    )
    assert result.approved
    assert result.reasons == ()


def test_feature_is_rejected_when_any_required_oos_check_fails():
    result = approve_feature(FeatureEvaluation(0.1, 0.05, {LiquidityTier.HIGH: 0.1, LiquidityTier.LOW: -0.01}, {MarketRegime.TRENDING_BULL: 0.1}, (-0.01, 0.2)))
    assert not result.approved
    assert FeatureApprovalCode.INCREMENTAL_VALUE_NOT_SIGNIFICANT in result.reasons
    assert FeatureApprovalCode.LIQUIDITY_DIRECTION_NOT_STABLE in result.reasons


def test_feature_approval_rejects_non_finite_or_incomplete_evidence():
    invalid = approve_feature(
        FeatureEvaluation(
            float("nan"),
            0.05,
            {LiquidityTier.HIGH: 0.1},
            {MarketRegime.TRENDING_BULL: 0.1},
            (0.01, 0.2),
        )
    )
    incomplete = approve_feature(
        FeatureEvaluation(
            0.1,
            0.05,
            {LiquidityTier.HIGH: 0.1},
            {MarketRegime.TRENDING_BULL: 0.1},
            (0.01, 0.2),
        )
    )

    assert invalid.reasons == (FeatureApprovalCode.INVALID_EVALUATION_METRICS,)
    assert FeatureApprovalCode.LIQUIDITY_STRATA_INCOMPLETE in incomplete.reasons
    assert FeatureApprovalCode.REGIME_STRATA_INCOMPLETE in incomplete.reasons

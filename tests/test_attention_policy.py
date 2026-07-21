import datetime
import math

import pytest

from common.attention_policy import (
    ARTIFACT_HASH,
    FEATURE_INDEX,
    FEATURE_NAMES,
    FEATURES,
    frozen_feature_vector,
    score_frozen_candidate,
    score_frozen_features,
)
from common.models import (
    AttentionCandidate,
    AttentionEvidence,
    AttentionLane,
    AttentionStage,
    EvidenceFamily,
    EvidenceVerdict,
    StructureDirection,
)


UTC = datetime.timezone.utc


def test_frozen_artifact_matches_sealed_golden_scores():
    means = [float(feature["mean"]) for feature in FEATURES]
    stds = [float(feature["std"]) for feature in FEATURES]

    fixtures = [
        (means, 0.4682649030954108, 0.7682649030954107),
        (
            [mean + std for mean, std in zip(means, stds, strict=True)],
            0.5164326059027246,
            0.5164326059027246,
        ),
        (
            [mean - std for mean, std in zip(means, stds, strict=True)],
            0.4200972002880968,
            0.7200972002880968,
        ),
        (
            [
                mean + (std if index % 2 == 0 else -std)
                for index, (mean, std) in enumerate(zip(means, stds, strict=True))
            ],
            0.36686689346527707,
            0.666866893465277,
        ),
    ]

    assert ARTIFACT_HASH == (
        "sha256:cabde76be658c5668cc52a0dbfc877f843945199344c8e3663edbd53ed897866"
    )
    for vector, expected_ridge, expected_adjusted in fixtures:
        score = score_frozen_features(vector)
        assert score.ridge == pytest.approx(expected_ridge, abs=1e-12)
        assert score.adjusted == pytest.approx(expected_adjusted, abs=1e-12)


@pytest.mark.parametrize(
    ("feature", "boundary"),
    [("abs_price_change_1h", 2.0), ("abs_price_change_4h", 5.0)],
)
def test_early_bonus_uses_strict_boundaries(feature, boundary):
    vector = [float(item["mean"]) for item in FEATURES]
    vector[FEATURE_INDEX[feature]] = boundary

    score = score_frozen_features(vector)

    assert score.adjusted == score.ridge


def test_frozen_candidate_extracts_the_exact_named_feature_contract():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    candidate = AttentionCandidate(
        market="KRW-FIXTURE",
        attention_rank=3,
        lane=AttentionLane.FOCUS,
        quality_score=0.61,
        ranking_score=-4.25,
        primary_exposures_60m=101,
        score_version="fixture",
        context_available=True,
        market_rank=8,
        market_rank_delta=None,
        stage=AttentionStage.FAILED,
        episode_id="fixture-episode",
        first_seen_at=observed_at - datetime.timedelta(minutes=70),
        observed_at=observed_at,
        consecutive_observations=7,
        current_price=123.45,
        price_change_10m=-60.0,
        price_change_1h=-1.75,
        price_change_4h=5.5,
        relative_volume=24.0,
        conditional_volume_z=12.5,
        price_surprise=-25.0,
        residual_momentum=-55.0,
        signal_score=12.0,
        structure_direction=StructureDirection.BEARISH,
        material_change=False,
        evidence=[
            AttentionEvidence(
                family=EvidenceFamily.ACTIVITY,
                verdict=EvidenceVerdict.SUPPORTING,
                summary="activity",
            ),
            AttentionEvidence(
                family=EvidenceFamily.PRICE_STRUCTURE,
                verdict=EvidenceVerdict.MIXED,
                summary="structure",
            ),
            AttentionEvidence(
                family=EvidenceFamily.CONTEXT,
                verdict=EvidenceVerdict.SUPPORTING,
                summary="context",
            ),
            AttentionEvidence(
                family=EvidenceFamily.EXECUTION,
                verdict=EvidenceVerdict.SUPPORTING,
                summary="execution must be excluded",
            ),
        ],
    )
    expected = {
        "v3_rank": 4.0,
        "inverse_v3_rank": 0.25,
        "attention_rank": 3.0,
        "quality_score": 0.61,
        "ranking_score": -4.25,
        "conditional_volume_z": 10.0,
        "log_relative_volume": math.log(25.0),
        "price_surprise": -20.0,
        "price_change_10m": -50.0,
        "abs_price_change_10m": 50.0,
        "price_change_1h": -1.75,
        "abs_price_change_1h": 1.75,
        "price_change_4h": 5.5,
        "abs_price_change_4h": 5.5,
        "residual_momentum": -50.0,
        "abs_residual_momentum": 50.0,
        "signal_score": 10.0,
        "signal_score_present": 1.0,
        "log_market_rank": math.log(9.0),
        "market_rank_delta": 0.0,
        "market_rank_delta_present": 0.0,
        "log_consecutive_observations": math.log(8.0),
        "primary_exposures_60m": 100.0,
        "context_available": 1.0,
        "material_change": 0.0,
        "stage_confirmed": 0.0,
        "stage_building": 0.0,
        "stage_discovered": 0.0,
        "stage_other": 1.0,
        "lane_focus_now": 1.0,
        "lane_early_watch": 0.0,
        "structure_bullish": 0.0,
        "structure_bearish": 1.0,
        "evidence_support_count": 2.0,
        "evidence_mixed_count": 1.0,
        "activity_supporting": 1.0,
        "price_structure_supporting": 0.0,
        "context_supporting": 1.0,
    }

    vector = frozen_feature_vector(candidate, v3_rank=4)

    assert dict(zip(FEATURE_NAMES, vector, strict=True)) == expected
    score = score_frozen_candidate(candidate, v3_rank=4)
    assert score.ridge == pytest.approx(9.336726438380342, abs=1e-12)
    assert score.adjusted == pytest.approx(9.336726438380342, abs=1e-12)

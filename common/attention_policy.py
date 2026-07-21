"""Frozen, auditable reranker promoted from historical attention replay."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from common.models import AttentionCandidate, EvidenceFamily


_ARTIFACT_PATH = (
    Path(__file__).with_name("model_artifacts") / "ridge_early_bonus_0p3.json"
)


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _load_artifact() -> dict[str, object]:
    artifact = json.loads(_ARTIFACT_PATH.read_text(encoding="utf-8"))
    claimed_hash = str(artifact["artifact_hash"])
    payload = dict(artifact)
    payload.pop("artifact_hash")
    actual_hash = "sha256:" + hashlib.sha256(_canonical(payload)).hexdigest()
    if claimed_hash != actual_hash:
        raise RuntimeError("frozen attention artifact hash mismatch")

    features = artifact.get("features")
    if not isinstance(features, list) or len(features) != 38:
        raise RuntimeError("frozen attention artifact must contain 38 features")
    for position, feature in enumerate(features):
        if not isinstance(feature, dict) or feature.get("position") != position:
            raise RuntimeError("frozen attention artifact feature order mismatch")
    return artifact


ARTIFACT = _load_artifact()
ARTIFACT_HASH = str(ARTIFACT["artifact_hash"])
CANDIDATE_NAME = str(ARTIFACT["candidate"])
CANDIDATE_POOL_LIMIT = 5
FEATURES = tuple(ARTIFACT["features"])
FEATURE_NAMES = tuple(str(feature["name"]) for feature in FEATURES)
FEATURE_INDEX = {name: index for index, name in enumerate(FEATURE_NAMES)}
INTERCEPT = float(ARTIFACT["intercept"])
EARLY_BONUS = float(ARTIFACT["early_bonus"]["value"])


@dataclass(frozen=True)
class FrozenAttentionScore:
    ridge: float
    adjusted: float


def _safe_float(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return default
    result = float(value)
    return result if math.isfinite(result) else default


def _clipped(value: object, minimum: float, maximum: float) -> float:
    return min(max(_safe_float(value), minimum), maximum)


def frozen_feature_vector(
    candidate: AttentionCandidate,
    v3_rank: int,
) -> tuple[float, ...]:
    """Reproduce the exact 38-feature vector used by the sealed evaluator."""
    evidence = {
        item.family.value: item.verdict.value
        for item in candidate.evidence
        if item.family is not EvidenceFamily.EXECUTION
    }
    support_count = sum(verdict == "supporting" for verdict in evidence.values())
    mixed_count = sum(verdict == "mixed" for verdict in evidence.values())
    stage = candidate.stage.value
    lane = candidate.lane.value
    direction = (
        candidate.structure_direction.value
        if candidate.structure_direction is not None
        else "other"
    )
    signal_score = candidate.signal_score
    market_rank_delta = candidate.market_rank_delta
    consecutive = max(1, candidate.consecutive_observations)

    values = (
        float(v3_rank),
        1.0 / max(v3_rank, 1),
        float(candidate.attention_rank),
        _clipped(candidate.quality_score, -5.0, 5.0),
        _clipped(candidate.ranking_score, -5.0, 5.0),
        _clipped(candidate.conditional_volume_z, -10.0, 10.0),
        math.log1p(max(0.0, _clipped(candidate.relative_volume, 0.0, 1000.0))),
        _clipped(candidate.price_surprise, -20.0, 20.0),
        _clipped(candidate.price_change_10m, -50.0, 50.0),
        abs(_clipped(candidate.price_change_10m, -50.0, 50.0)),
        _clipped(candidate.price_change_1h, -100.0, 100.0),
        abs(_clipped(candidate.price_change_1h, -100.0, 100.0)),
        _clipped(candidate.price_change_4h, -200.0, 200.0),
        abs(_clipped(candidate.price_change_4h, -200.0, 200.0)),
        _clipped(candidate.residual_momentum, -50.0, 50.0),
        abs(_clipped(candidate.residual_momentum, -50.0, 50.0)),
        _clipped(signal_score, -10.0, 10.0),
        float(
            isinstance(signal_score, (int, float))
            and not isinstance(signal_score, bool)
        ),
        math.log1p(max(0.0, _safe_float(candidate.market_rank))),
        _clipped(market_rank_delta, -500.0, 500.0),
        float(
            isinstance(market_rank_delta, (int, float))
            and not isinstance(market_rank_delta, bool)
        ),
        math.log1p(consecutive),
        _clipped(candidate.primary_exposures_60m, 0.0, 100.0),
        float(candidate.context_available),
        float(candidate.material_change),
        float(stage == "confirmed"),
        float(stage == "building"),
        float(stage == "discovered"),
        float(stage not in {"confirmed", "building", "discovered"}),
        float(lane == "focus_now"),
        float(lane == "early_watch"),
        float(direction == "bullish"),
        float(direction == "bearish"),
        float(support_count),
        float(mixed_count),
        float(evidence.get("activity") == "supporting"),
        float(evidence.get("price_structure") == "supporting"),
        float(evidence.get("context") == "supporting"),
    )
    if len(values) != len(FEATURES):
        raise AssertionError("frozen attention feature length mismatch")
    return values


def score_frozen_features(values: Sequence[float]) -> FrozenAttentionScore:
    if len(values) != len(FEATURES):
        raise ValueError("frozen attention score requires 38 features")
    ridge = INTERCEPT + sum(
        ((float(value) - float(feature["mean"])) / float(feature["std"]))
        * float(feature["slope"])
        for value, feature in zip(values, FEATURES, strict=True)
    )
    early = (
        values[FEATURE_INDEX["abs_price_change_1h"]] < 2.0
        and values[FEATURE_INDEX["abs_price_change_4h"]] < 5.0
    )
    return FrozenAttentionScore(
        ridge=ridge,
        adjusted=ridge + (EARLY_BONUS if early else 0.0),
    )


def score_frozen_candidate(
    candidate: AttentionCandidate,
    v3_rank: int,
) -> FrozenAttentionScore:
    return score_frozen_features(frozen_feature_vector(candidate, v3_rank))

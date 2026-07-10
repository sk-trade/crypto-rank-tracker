"""Leakage-resistant temporal split helpers for scan outcomes."""

import datetime
from dataclasses import dataclass
from typing import Iterable, List

import numpy as np

from common.models import ScanOutcome


@dataclass(frozen=True)
class TemporalFold:
    train: List[ScanOutcome]
    validation: List[ScanOutcome]


@dataclass(frozen=True)
class EvaluationReport:
    net_expected_return: float
    hit_rate: float
    confidence_interval: tuple[float, float]


def purged_walk_forward(
    outcomes: Iterable[ScanOutcome], folds: int = 3, embargo: datetime.timedelta = datetime.timedelta(hours=1)
) -> List[TemporalFold]:
    ordered = sorted(outcomes, key=lambda item: item.entry_candle_start)
    if folds < 1 or len(ordered) < folds + 1:
        raise ValueError("insufficient outcomes for walk-forward validation")
    block = len(ordered) // (folds + 1)
    result = []
    for index in range(1, folds + 1):
        validation = ordered[index * block : (index + 1) * block]
        if not validation:
            continue
        boundary = validation[0].entry_candle_start - embargo
        # Remove every training label whose realized holding interval touches validation.
        train = [item for item in ordered[: index * block] if item.exit_candle_start < boundary]
        if train:
            result.append(TemporalFold(train, validation))
    return result


def final_holdout(outcomes: Iterable[ScanOutcome], fraction: float = 0.2) -> tuple[List[ScanOutcome], List[ScanOutcome]]:
    ordered = sorted(outcomes, key=lambda item: item.entry_candle_start)
    split = int(len(ordered) * (1 - fraction))
    if split < 1 or split == len(ordered):
        raise ValueError("insufficient outcomes for holdout")
    return ordered[:split], ordered[split:]


def evaluate_outcomes(outcomes: Iterable[ScanOutcome], bootstrap_samples: int = 1_000) -> EvaluationReport:
    values = np.array([outcome.directional_net_return for outcome in outcomes], dtype=float)
    if len(values) < 2:
        raise ValueError("insufficient outcomes for evaluation")
    generator = np.random.default_rng(0)
    samples = generator.choice(values, size=(bootstrap_samples, len(values)), replace=True).mean(axis=1)
    return EvaluationReport(
        net_expected_return=float(values.mean()),
        hit_rate=float((values > 0).mean()),
        confidence_interval=(float(np.quantile(samples, 0.025)), float(np.quantile(samples, 0.975))),
    )

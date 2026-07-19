"""A small point-in-time logistic baseline for offline comparison only."""

from dataclasses import dataclass
from typing import Iterable

import numpy as np

from common.models import ScanEvent, ScanOutcome

FEATURES = (
    "price_surprise", "conditional_log_rvol_z_score", "cross_sectional_log_rvol_z_score",
    "residual_momentum_score", "execution_spread_bps", "expected_slippage_bps",
)
MIN_TRAINING_ROWS = 30


@dataclass(frozen=True)
class LogisticBaseline:
    means: np.ndarray
    scales: np.ndarray
    coefficients: np.ndarray

    def predict(self, feature_snapshot: dict) -> float:
        values = _feature_vector(feature_snapshot)
        if values is None:
            raise ValueError("missing baseline feature")
        x = (values - self.means) / self.scales
        return float(1 / (1 + np.exp(-np.clip(np.r_[1.0, x] @ self.coefficients, -30, 30))))


def fit_baseline(events: Iterable[ScanEvent], outcomes: Iterable[ScanOutcome]) -> LogisticBaseline:
    outcomes_by_event = {outcome.event_id: outcome for outcome in outcomes}
    rows, labels = [], []
    for event in events:
        outcome = outcomes_by_event.get(event.event_id)
        values = _feature_vector(event.feature_snapshot)
        if outcome is None or values is None:
            continue
        rows.append(values)
        labels.append(int(outcome.directional_net_return > 0))
    if len(rows) < MIN_TRAINING_ROWS or len(set(labels)) != 2:
        raise ValueError("insufficient balanced point-in-time training rows")
    x = np.array(rows, dtype=float)
    means = x.mean(axis=0)
    scales = x.std(axis=0, ddof=1)
    if np.any(scales == 0):
        raise ValueError("constant baseline feature")
    x = np.c_[np.ones(len(x)), (x - means) / scales]
    y = np.array(labels, dtype=float)
    weights = np.zeros(x.shape[1])
    for _ in range(500):
        probabilities = 1 / (1 + np.exp(-np.clip(x @ weights, -30, 30)))
        gradient = x.T @ (probabilities - y) / len(y) + 0.001 * np.r_[0.0, weights[1:]]
        weights -= 0.1 * gradient
    return LogisticBaseline(means, scales, weights)


def _feature_vector(snapshot: dict) -> np.ndarray | None:
    try:
        values = np.array([float(snapshot[name]) for name in FEATURES], dtype=float)
    except (KeyError, TypeError, ValueError):
        return None
    return values if np.isfinite(values).all() else None

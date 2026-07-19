import datetime

import pytest

from common.models import ScanOutcome
from common.time_validation import evaluate_outcomes, final_holdout, purged_walk_forward


def _outcome(index):
    start = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(hours=2 * index)
    return ScanOutcome(event_id=str(index), market="KRW-A", entry_candle_start=start, exit_candle_start=start + datetime.timedelta(hours=1), entry_price=1, exit_price=1, directional_net_return=0.1, mfe=0, mae=0)


def test_walk_forward_purges_overlapping_training_outcomes_and_embargoes_boundary():
    folds = purged_walk_forward([_outcome(index) for index in range(20)], folds=2)
    assert folds
    for fold in folds:
        boundary = fold.validation[0].entry_candle_start - datetime.timedelta(hours=1)
        assert all(item.exit_candle_start < boundary for item in fold.train)


def test_final_holdout_is_later_than_training_data():
    train, holdout = final_holdout([_outcome(index) for index in range(10)])
    assert max(item.entry_candle_start for item in train) < min(item.entry_candle_start for item in holdout)


def test_final_holdout_purges_overlapping_labels_and_applies_embargo():
    base = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    outcomes = [
        _outcome(index).model_copy(
            update={
                "entry_candle_start": base + datetime.timedelta(minutes=10 * index),
                "exit_candle_start": base + datetime.timedelta(minutes=10 * index + 60),
            }
        )
        for index in range(20)
    ]

    train, holdout = final_holdout(outcomes)
    boundary = holdout[0].entry_candle_start - datetime.timedelta(hours=1)

    assert all(item.exit_candle_start < boundary for item in train)


def test_evaluation_reports_cost_adjusted_expectation_hit_rate_and_bootstrap_interval():
    report = evaluate_outcomes([_outcome(index).model_copy(update={"directional_net_return": value}) for index, value in enumerate([-0.2, 0.1, 0.3, -0.1])])
    assert report.net_expected_return == pytest.approx(0.025)
    assert report.hit_rate == 0.5
    assert report.confidence_interval[0] <= report.net_expected_return <= report.confidence_interval[1]

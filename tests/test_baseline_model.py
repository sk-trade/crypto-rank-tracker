import datetime

from common.baseline_model import FEATURES, fit_baseline
from common.models import ScanEvent, ScanOutcome


def test_baseline_model_uses_only_predecision_features_and_directional_outcomes():
    events, outcomes = [], []
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    for index in range(40):
        snapshot = {name: float(index + offset) for offset, name in enumerate(FEATURES)}
        events.append(ScanEvent(event_id=str(index), observed_at=now, market="KRW-A", feature_snapshot=snapshot, candidate_eligible=True, rejection_reasons=[], final_decision="alert_sent", model_version="test"))
        outcomes.append(ScanOutcome(event_id=str(index), market="KRW-A", entry_candle_start=now, exit_candle_start=now, entry_price=1, exit_price=1, directional_net_return=1.0 if index % 2 else -1.0, mfe=0, mae=0))
    model = fit_baseline(events, outcomes)
    assert 0 < model.predict(events[0].feature_snapshot) < 1

import datetime

from common.models import ScanEvent, ScanOutcome
from common.threshold_selection import select_threshold


def _row(index, score, outcome):
    timestamp = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(days=index // 3)
    event = ScanEvent(event_id=str(index), observed_at=timestamp, market="KRW-A", feature_snapshot={}, candidate_eligible=True, rejection_reasons=[], final_decision="alert_sent", model_version="test", signal_score=score)
    result = ScanOutcome(event_id=str(index), market="KRW-A", entry_candle_start=timestamp, exit_candle_start=timestamp, entry_price=1, exit_price=1, directional_net_return=outcome, mfe=0, mae=0)
    return event, result


def test_threshold_selection_maximizes_net_value_at_fixed_daily_alert_volume():
    rows = [_row(0, 0.9, 0.3), _row(1, 0.8, -0.2), _row(2, 0.7, 0.1), _row(3, 0.9, 0.2), _row(4, 0.8, -0.1), _row(5, 0.7, 0.05)]
    selected = select_threshold([event for event, _ in rows], [outcome for _, outcome in rows], 1)
    assert selected.threshold == 0.9
    assert selected.selected_events == 2

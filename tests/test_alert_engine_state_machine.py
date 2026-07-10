import datetime

from common.models import Alert, AlertHistory, CandleData, SignalCandidate, TickerData
from common.notification.engine import AlertEngine
from common.notification.main import _update_alert_history


UTC = datetime.timezone.utc


def _ticker(closes: list[float]) -> TickerData:
    return TickerData(
        market="KRW-BTC",
        candle_history=[
            CandleData(
                market="KRW-BTC",
                timestamp=datetime.datetime(2026, 6, 18, tzinfo=UTC)
                + datetime.timedelta(minutes=10 * index),
                open_price=close,
                high_price=close,
                low_price=close,
                close_price=close,
                volume=1.0,
            )
            for index, close in enumerate(closes)
        ],
    )


def _candidate(price: float) -> SignalCandidate:
    return SignalCandidate(
        market="KRW-BTC",
        signal_score=0.8,
        price_change=0.1,
        rvol=1.0,
        rvol_z_score=1.0,
        contexts=[],
        current_price=price,
    )


def _history(direction: str, level: float, last_price: float) -> AlertHistory:
    now = datetime.datetime.now(UTC)
    return AlertHistory(
        market="KRW-BTC",
        last_alert_timestamp=now,
        last_signal_type="BREAKOUT_START",
        last_price=last_price,
        last_rvol=1.0,
        initial_timestamp=now,
        initial_price=last_price,
        structure_level=level,
        structure_direction=direction,
    )


def test_breakout_start_requires_close_above_prior_resistance():
    signal_type, _, level = AlertEngine()._get_alert_type_and_priority(
        _candidate(101.0), _ticker([100.0] * 20 + [101.0]), {}
    )

    assert signal_type == "BREAKOUT_START"
    assert level == 100.0


def test_positive_move_below_resistance_is_not_a_breakout_start():
    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        _candidate(105.0), _ticker([100.0] * 19 + [110.0] + [105.0]), {}
    )

    assert signal_type is None


def test_acceleration_requires_an_existing_breakout_and_further_extension():
    history = {"KRW-BTC": _history("bullish", level=100.0, last_price=102.0)}

    signal_type, _, level = AlertEngine()._get_alert_type_and_priority(
        _candidate(104.0), _ticker([]), history
    )

    assert signal_type == "MOMENTUM_ACCELERATION"
    assert level == 100.0


def test_failure_requires_reentry_through_the_breakout_level():
    history = {"KRW-BTC": _history("bullish", level=100.0, last_price=104.0)}

    signal_type, _, level = AlertEngine()._get_alert_type_and_priority(
        _candidate(100.0), _ticker([]), history
    )

    assert signal_type == "BULL_MOMENTUM_FAILED"
    assert level == 100.0


def test_alert_history_persists_breakout_structure_and_clears_it_after_failure():
    ticker = _ticker([100.0] * 20 + [101.0])
    breakout = Alert(
        candidate=_candidate(101.0),
        ticker_data=ticker,
        signal_type="BREAKOUT_START",
        priority=3,
        structure_level=100.0,
    )
    history = _update_alert_history({}, [breakout])

    assert history["KRW-BTC"].structure_direction == "bullish"
    assert history["KRW-BTC"].structure_level == 100.0

    failed = Alert(
        candidate=_candidate(100.0),
        ticker_data=ticker,
        signal_type="BULL_MOMENTUM_FAILED",
        priority=3,
        structure_level=100.0,
    )
    history = _update_alert_history(history, [failed])

    assert history["KRW-BTC"].structure_direction is None
    assert history["KRW-BTC"].structure_level is None

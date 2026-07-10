import datetime

from common.models import AlertHistory, SignalCandidate, TickerData
from common.notification.engine import AlertEngine


def _history(signal_type: str, last_price: float, now: datetime.datetime) -> AlertHistory:
    return AlertHistory(
        market="KRW-BTC",
        last_alert_timestamp=now - datetime.timedelta(minutes=30),
        last_signal_type=signal_type,
        last_price=last_price,
        last_rvol=1.0,
        initial_timestamp=now - datetime.timedelta(hours=2),
        initial_price=last_price,
    )


def _candidate(current_price: float, price_change: float = 1.2) -> SignalCandidate:
    return SignalCandidate(
        market="KRW-BTC",
        signal_score=0.75,
        price_change=price_change,
        rvol=1.0,
        rvol_z_score=1.0,
        contexts=[],
        current_price=current_price,
    )


def _ticker() -> TickerData:
    return TickerData(
        market="KRW-BTC",
        candle_history=[],
        price_change_10m=0.0,
        price_change_1h=0.0,
    )


def _process_signal_type(previous_signal_type: str, current_price: float) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    alerts = AlertEngine().process_signals(
        candidates=[_candidate(current_price)],
        enriched_tickers={"KRW-BTC": _ticker()},
        history={
            "KRW-BTC": _history(previous_signal_type, 100.0, now),
        },
    )

    assert len(alerts) == 1
    return alerts[0].signal_type


def test_process_signals_classifies_downtrend_follow_up_as_bear_sustained():
    assert (
        _process_signal_type("DOWNTREND_ACCELERATION", 98.0)
        == "BEAR_MOMENTUM_SUSTAINED"
    )


def test_process_signals_classifies_momentum_reversal_as_bull_failed():
    assert (
        _process_signal_type("MOMENTUM_ACCELERATION", 97.0)
        == "BULL_MOMENTUM_FAILED"
    )


def test_process_signals_classifies_followup_bull_sustained_as_bull_sustained():
    assert (
        _process_signal_type("BULL_MOMENTUM_SUSTAINED", 104.0)
        == "BULL_MOMENTUM_SUSTAINED"
    )


def test_process_signals_classifies_followup_bull_failed_as_bull_failed():
    assert (
        _process_signal_type("BULL_MOMENTUM_FAILED", 97.0)
        == "BULL_MOMENTUM_FAILED"
    )


def test_process_signals_classifies_followup_bear_sustained_as_bear_sustained():
    assert (
        _process_signal_type("BEAR_MOMENTUM_SUSTAINED", 96.0)
        == "BEAR_MOMENTUM_SUSTAINED"
    )


def test_process_signals_classifies_followup_bear_failed_as_bear_failed():
    assert (
        _process_signal_type("BEAR_MOMENTUM_FAILED", 102.0)
        == "BEAR_MOMENTUM_FAILED"
    )


def test_prior_downtrend_acceleration_with_continued_lower_price_is_bear_sustained():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(98.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("DOWNTREND_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "BEAR_MOMENTUM_SUSTAINED"


def test_prior_downtrend_acceleration_with_rebound_is_bear_failed():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(102.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("DOWNTREND_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "BEAR_MOMENTUM_FAILED"


def test_prior_momentum_acceleration_with_continued_upside_is_bull_sustained():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(102.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("MOMENTUM_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "BULL_MOMENTUM_SUSTAINED"


def test_prior_momentum_acceleration_with_reversal_down_is_bull_failed():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(97.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("MOMENTUM_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "BULL_MOMENTUM_FAILED"

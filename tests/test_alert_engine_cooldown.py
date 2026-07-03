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


def _candidate(current_price: float) -> SignalCandidate:
    return SignalCandidate(
        market="KRW-BTC",
        confidence=0.75,
        price_change=0.0,
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

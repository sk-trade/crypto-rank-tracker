import asyncio
import datetime

import pytest

import config
from common.models import AlertHistory, SignalCandidate, TickerData
from common.notification.engine import AlertEngine
from common.state_manager import load_alert_history
from common.storage_client import StateLoadError


def _history(
    signal_type: str,
    last_price: float,
    now: datetime.datetime,
    minutes_ago: int = 30,
) -> AlertHistory:
    bearish = "BEAR" in signal_type or "DOWNTREND" in signal_type or "BREAKDOWN" in signal_type
    return AlertHistory(
        market="KRW-BTC",
        last_alert_timestamp=now - datetime.timedelta(minutes=minutes_ago),
        last_signal_type=signal_type,
        last_price=last_price,
        last_rvol=1.0,
        initial_timestamp=now - datetime.timedelta(hours=2),
        initial_price=last_price,
        structure_level=100.0,
        structure_direction="bearish" if bearish else "bullish",
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


def test_alert_history_wrong_shape_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    (tmp_path / config.ALERT_HISTORY_FILE_NAME).write_text("[]", encoding="utf-8")

    with pytest.raises(StateLoadError, match="JSON object"):
        asyncio.run(load_alert_history())


def test_process_signals_classifies_downtrend_follow_up_as_acceleration():
    assert (
        _process_signal_type("DOWNTREND_ACCELERATION", 98.0)
        == "DOWNTREND_ACCELERATION"
    )


def test_process_signals_classifies_momentum_reversal_as_bull_failed():
    assert (
        _process_signal_type("MOMENTUM_ACCELERATION", 97.0)
        == "BULL_MOMENTUM_FAILED"
    )


def test_process_signals_classifies_followup_bull_sustained_as_bull_sustained():
    assert (
        _process_signal_type("BULL_MOMENTUM_SUSTAINED", 104.0)
        == "MOMENTUM_ACCELERATION"
    )


def test_process_signals_classifies_followup_bull_failed_as_bull_failed():
    assert (
        _process_signal_type("BULL_MOMENTUM_FAILED", 97.0)
        == "BULL_MOMENTUM_FAILED"
    )


def test_process_signals_classifies_followup_bear_sustained_as_bear_sustained():
    assert (
        _process_signal_type("BEAR_MOMENTUM_SUSTAINED", 96.0)
        == "DOWNTREND_ACCELERATION"
    )


def test_process_signals_classifies_followup_bear_failed_as_bear_failed():
    assert (
        _process_signal_type("BEAR_MOMENTUM_FAILED", 102.0)
        == "BEAR_MOMENTUM_FAILED"
    )


def test_prior_downtrend_acceleration_with_continued_lower_price_is_acceleration():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(98.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("DOWNTREND_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "DOWNTREND_ACCELERATION"


def test_prior_downtrend_acceleration_with_rebound_is_bear_failed():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(102.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("DOWNTREND_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "BEAR_MOMENTUM_FAILED"


def test_prior_momentum_acceleration_with_continued_upside_is_acceleration():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(102.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("MOMENTUM_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "MOMENTUM_ACCELERATION"


def test_prior_momentum_acceleration_with_reversal_down_is_bull_failed():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(97.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("MOMENTUM_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "BULL_MOMENTUM_FAILED"


def test_bullish_continuation_below_threshold_is_suppressed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(100.99),
        ticker=_ticker(),
        history={"KRW-BTC": _history("BREAKOUT_START", 100.0, now)},
    )

    assert signal_type is None


def test_bearish_continuation_at_threshold_is_allowed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(99.0),
        ticker=_ticker(),
        history={"KRW-BTC": _history("BREAKDOWN_START", 100.0, now)},
    )

    assert signal_type == "DOWNTREND_ACCELERATION"


def test_small_continuation_is_allowed_after_cooldown_expires():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(100.01),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history(
                "BREAKOUT_START",
                100.0,
                now,
                minutes_ago=61,
            )
        },
    )

    assert signal_type == "MOMENTUM_ACCELERATION"


def test_structure_failure_is_allowed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(99.0),
        ticker=_ticker(),
        history={"KRW-BTC": _history("BREAKOUT_START", 100.0, now)},
    )

    assert signal_type == "BULL_MOMENTUM_FAILED"

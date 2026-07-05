from common.models import Alert, SignalCandidate, TickerData
from common.notification.formatter import NotificationFormatter


def _alert(signal_type: str, price_change: float = -2.5) -> Alert:
    return Alert(
        candidate=SignalCandidate(
            market="KRW-BTC",
            confidence=0.75,
            price_change=price_change,
            rvol=2.0,
            rvol_z_score=1.5,
            contexts=[],
            current_price=100.0,
        ),
        ticker_data=TickerData(market="KRW-BTC", candle_history=[]),
        signal_type=signal_type,
        priority=1,
    )


def _header_for(signal_type: str, price_change: float = -2.5) -> str:
    rendered = NotificationFormatter()._format_single_alert(
        alert=_alert(signal_type, price_change),
        reverse_sector_map={},
        market_regime={"regime": "TEST"},
    )

    return rendered.splitlines()[0]


def test_formatter_labels_bearish_acceleration_alert():
    assert "하락 모멘텀 가속" in _header_for("DOWNTREND_ACCELERATION")


def test_formatter_labels_cooldown_follow_up_alerts():
    expected_titles = {
        "BULL_MOMENTUM_SUSTAINED": "상승 모멘텀 지속",
        "BULL_MOMENTUM_FAILED": "상승 모멘텀 실패",
        "BEAR_MOMENTUM_SUSTAINED": "하락 모멘텀 지속",
        "BEAR_MOMENTUM_FAILED": "하락 모멘텀 실패",
    }

    for signal_type, title in expected_titles.items():
        assert title in _header_for(signal_type)

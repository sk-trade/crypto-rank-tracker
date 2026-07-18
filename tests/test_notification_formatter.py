from common.models import (
    Alert,
    DataQualityIssue,
    MarketRegime,
    MarketRegimeSnapshot,
    RejectionCode,
    SignalCandidate,
    SignalType,
    TickerData,
)
from common.notification.formatter import NotificationFormatter


def _alert(signal_type: SignalType, price_change: float = -2.5) -> Alert:
    return Alert(
        candidate=SignalCandidate(
            market="KRW-BTC",
            signal_score=0.75,
            price_change=price_change,
            rvol=2.0,
            rvol_z_score=1.5,
            current_price=100.0,
        ),
        ticker_data=TickerData(market="KRW-BTC", candle_history=[]),
        signal_type=signal_type,
        priority=1,
    )


def _header_for(signal_type: SignalType, price_change: float = -2.5) -> str:
    rendered = NotificationFormatter()._format_single_alert(
        alert=_alert(signal_type, price_change),
        reverse_sector_map={},
        market_regime=MarketRegimeSnapshot(regime=MarketRegime.TRENDING_BULL),
    )

    return rendered.splitlines()[0]


def test_formatter_labels_bearish_acceleration_alert():
    assert "하락 모멘텀 가속" in _header_for(
        SignalType.DOWNTREND_ACCELERATION
    )


def test_formatter_labels_an_uncalibrated_signal_score_without_a_percentage():
    header = _header_for(SignalType.BREAKOUT_START)

    assert "Signal score: 0.75" in header
    assert "신뢰도" not in header


def test_data_quality_alert_does_not_claim_the_market_has_no_events():
    message = NotificationFormatter().format_data_quality_alert(
        [
            DataQualityIssue(
                code=RejectionCode.CANDLE_COVERAGE_BELOW_MINIMUM,
                message="10-minute candle coverage is below the configured minimum.",
            )
        ]
    )

    assert "데이터 품질 장애" in message
    assert "특이사항 없음" not in message


def test_formatter_labels_cooldown_follow_up_alerts():
    expected_titles = {
        SignalType.BULL_MOMENTUM_FAILED: "상승 모멘텀 실패",
        SignalType.BEAR_MOMENTUM_FAILED: "하락 모멘텀 실패",
    }

    for signal_type, title in expected_titles.items():
        assert title in _header_for(signal_type)

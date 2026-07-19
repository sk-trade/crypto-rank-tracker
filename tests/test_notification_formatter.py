import datetime

from common.attention import build_attention_queue
from common.models import (
    Alert,
    AttentionStage,
    CandleData,
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


def test_attention_queue_shows_progression_and_concrete_evidence_without_a_score():
    observed_at = datetime.datetime(2026, 7, 19, tzinfo=datetime.timezone.utc)
    candles = [
        CandleData(
            market="KRW-KAITO",
            timestamp=observed_at - datetime.timedelta(minutes=10 * (20 - index)),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.0,
            volume=100.0,
        )
        for index in range(21)
    ]
    ticker = TickerData(
        market="KRW-KAITO",
        candle_history=candles,
        price_change_10m=0.4,
        price_change_1h=1.2,
        relative_volume=10.8,
        conditional_log_rvol_z_score=5.4,
        price_surprise=2.5,
        rolling_turnover=100_000_000,
    )
    queue, _ = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {ticker.market: 60},
        [],
        [],
    )

    rendered = NotificationFormatter()._format_attention_queue(
        queue, {ticker.market: ["AI"]}
    )

    assert queue[0].stage is AttentionStage.DISCOVERED
    assert "관심종목 큐" in rendered
    assert "KAITO" in rendered
    assert "24h #42 ↑18" in rendered
    assert "RVOL 10.80x" in rendered
    assert "Signal score" not in rendered

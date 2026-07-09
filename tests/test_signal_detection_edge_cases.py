import datetime

from common.models import CandleData, SignalCandidate, TickerData
from common.signals.detector import detect_anomalies, filter_market_wide_events


def _candle(price: float = 100.0, volume: float = 1000.0) -> CandleData:
    return CandleData(
        market="KRW-BTC",
        timestamp=datetime.datetime(2026, 6, 18, 0, 0, tzinfo=datetime.timezone.utc),
        open_price=price,
        high_price=price,
        low_price=price,
        close_price=price,
        volume=volume,
    )


def test_filter_market_wide_events_returns_empty_list_for_no_tickers():
    assert filter_market_wide_events([], {}) == []


def test_detect_anomalies_allows_missing_rvol_z_score_for_price_only_candidate():
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle(price=110.0)],
        price_change_10m=6.5,
        relative_volume=None,
        rvol_z_score=None,
        trend_1h_stable="UP",
        is_above_ma50_daily=True,
    )

    candidates = detect_anomalies(
        {"KRW-BTC": ticker},
        {"KRW-BTC": 1},
        {},
        {},
    )

    assert candidates == [
        SignalCandidate(
            market="KRW-BTC",
            confidence=candidates[0].confidence,
            price_change=6.5,
            rvol=0.0,
            rvol_z_score=0.0,
            contexts=[],
            current_price=110.0,
        )
    ]


def test_detect_anomalies_uses_deep_dive_final_confidence_when_available():
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle(price=110.0, volume=2500.0)],
        price_change_10m=1.6,
        relative_volume=4.5,
        rvol_z_score=3.5,
        final_confidence=0.82,
    )

    candidates = detect_anomalies(
        {"KRW-BTC": ticker},
        {"KRW-BTC": 1},
        {},
        {},
    )

    assert len(candidates) == 1
    assert candidates[0].confidence == 0.82

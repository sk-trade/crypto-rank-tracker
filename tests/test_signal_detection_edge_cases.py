import datetime

import pytest

from common.models import CandleData, SignalCandidate, TickerData
from common.signals.detector import (
    calculate_signal_score,
    detect_anomalies,
    filter_market_wide_events,
)


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
        price_surprise=3.25,
        liquidity_tier="HIGH",
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
            signal_score=candidates[0].signal_score,
            price_change=6.5,
            rvol=0.0,
            rvol_z_score=0.0,
            contexts=[],
            current_price=110.0,
        )
    ]


def test_detect_anomalies_uses_the_single_calculated_signal_score():
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle(price=110.0, volume=2500.0)],
        price_change_10m=6.5,
        price_surprise=3.25,
        liquidity_tier="HIGH",
        relative_volume=4.5,
        rvol_z_score=5.0,
    )

    candidates = detect_anomalies(
        {"KRW-BTC": ticker},
        {"KRW-BTC": 1},
        {},
        {},
    )

    assert len(candidates) == 1
    assert candidates[0].signal_score == pytest.approx(0.525)


def test_signal_score_is_not_capped_at_an_arbitrary_value():
    ticker = TickerData(
        market="KRW-BTC",
        price_change_10m=10.0,
        price_surprise=5.0,
        liquidity_tier="HIGH",
        rvol_z_score=20.0,
        trend_1h_stable="UP",
        is_above_ma50_daily=True,
        decoupling_score=4.0,
    )

    assert calculate_signal_score(ticker, sector_corr=1.0, rank=1) == pytest.approx(1.0)

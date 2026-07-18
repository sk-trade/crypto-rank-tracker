import datetime

import pytest

from common.models import (
    CandleData,
    LiquidityTier,
    SignalCandidate,
    TickerData,
    TrendState,
)
from common.signals.detector import (
    calculate_signal_score,
    detect_anomalies,
    filter_market_wide_events,
)


def _candle(
    price: float = 100.0,
    volume: float = 1000.0,
    market: str = "KRW-BTC",
) -> CandleData:
    return CandleData(
        market=market,
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
        liquidity_tier=LiquidityTier.HIGH,
        conditional_log_rvol_z_score=0.0,
        relative_volume=None,
        rvol_z_score=None,
        trend_1h_stable=TrendState.UP,
        is_above_ma50_daily=True,
    )

    candidates = detect_anomalies(
        ["KRW-BTC"],
        {"KRW-BTC": ticker},
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
            current_price=110.0,
        )
    ]


def test_detect_anomalies_uses_the_single_calculated_signal_score():
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle(price=110.0, volume=2500.0)],
        price_change_10m=6.5,
        price_surprise=3.25,
        liquidity_tier=LiquidityTier.HIGH,
        conditional_log_rvol_z_score=5.0,
        relative_volume=4.5,
        rvol_z_score=5.0,
    )

    candidates = detect_anomalies(
        ["KRW-BTC"],
        {"KRW-BTC": ticker},
        {},
        {},
    )

    assert len(candidates) == 1
    assert candidates[0].signal_score == pytest.approx(0.525)


def test_detect_anomalies_scores_with_conditional_volume_not_legacy_rvol():
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle()],
        price_change_10m=1.0,
        price_surprise=3.0,
        liquidity_tier=LiquidityTier.HIGH,
        conditional_log_rvol_z_score=10.0,
        rvol_z_score=0.0,
    )

    candidates = detect_anomalies(
        ["KRW-BTC"],
        {"KRW-BTC": ticker},
        {},
        {},
    )

    assert len(candidates) == 1
    assert candidates[0].signal_score == pytest.approx(0.5)
    assert candidates[0].rvol_z_score == 10.0


def test_detect_anomalies_does_not_score_with_legacy_rvol():
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle()],
        price_change_10m=1.0,
        price_surprise=3.0,
        liquidity_tier=LiquidityTier.HIGH,
        conditional_log_rvol_z_score=0.0,
        rvol_z_score=10.0,
    )

    candidates = detect_anomalies(
        ["KRW-BTC"],
        {"KRW-BTC": ticker},
        {},
        {},
    )

    assert candidates == []


def test_detect_anomalies_uses_non_candidate_sector_peers_as_context():
    candidate = TickerData(
        market="KRW-A",
        candle_history=[_candle(market="KRW-A")],
        price_change_10m=1.0,
        price_surprise=2.0,
        liquidity_tier=LiquidityTier.HIGH,
        conditional_log_rvol_z_score=20.0,
        trend_1h_stable=TrendState.UP,
        is_above_ma50_daily=True,
    )
    universe = {"KRW-A": candidate}
    for market in ["KRW-B", "KRW-C", "KRW-D"]:
        universe[market] = TickerData(market=market, price_change_10m=1.0)

    candidates = detect_anomalies(
        ["KRW-A"],
        universe,
        {"Layer 1": ["KRW-A", "KRW-B", "KRW-C", "KRW-D"]},
        {market: ["Layer 1"] for market in universe},
    )

    assert len(candidates) == 1
    assert candidates[0].signal_score == pytest.approx(0.7)


def test_signal_score_is_not_capped_at_an_arbitrary_value():
    ticker = TickerData(
        market="KRW-BTC",
        price_change_10m=10.0,
        price_surprise=5.0,
        liquidity_tier=LiquidityTier.HIGH,
        conditional_log_rvol_z_score=20.0,
        trend_1h_stable=TrendState.UP,
        is_above_ma50_daily=True,
        decoupling_score=4.0,
    )

    assert calculate_signal_score(ticker, sector_corr=1.0) == pytest.approx(1.0)

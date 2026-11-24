"""Higher-timeframe enrichment and BTC market-regime classification."""

from itertools import pairwise

import numpy as np

import config
from common.models import (
    CandleData,
    MarketRegime,
    MarketRegimeSnapshot,
    TickerData,
    TrendState,
)


def classify_market_regime(
    enriched_tickers: dict[str, TickerData],
) -> MarketRegimeSnapshot:
    """Classify the current market from completed BTC hourly candles."""
    btc_ticker = enriched_tickers.get("KRW-BTC")
    required_candles = max(
        config.REGIME_ATR_LONG_PERIOD + 1,
        config.REGIME_RSI_PERIOD + 1,
    )
    if not btc_ticker or len(btc_ticker.hourly_candles) < required_candles:
        return MarketRegimeSnapshot(regime=MarketRegime.UNKNOWN)

    hourly_candles = btc_ticker.hourly_candles
    close_prices = np.array(
        [candle.close_price for candle in hourly_candles],
        dtype=float,
    )
    price_changes = np.diff(close_prices)
    recent_changes = price_changes[-config.REGIME_RSI_PERIOD :]
    average_gain = np.mean(np.maximum(recent_changes, 0))
    average_loss = np.mean(np.maximum(-recent_changes, 0))
    if average_loss == 0:
        relative_strength_index = 100.0 if average_gain > 0 else 50.0
    else:
        relative_strength_index = 100 - (100 / (1 + average_gain / average_loss))

    true_ranges = np.array(
        [
            max(
                candle.high_price - candle.low_price,
                abs(candle.high_price - previous.close_price),
                abs(candle.low_price - previous.close_price),
            )
            for previous, candle in pairwise(hourly_candles)
        ],
        dtype=float,
    )
    long_average_true_range = np.mean(true_ranges[-config.REGIME_ATR_LONG_PERIOD :])
    short_average_true_range = np.mean(true_ranges[-config.REGIME_ATR_SHORT_PERIOD :])
    atr_ratio = (
        short_average_true_range / long_average_true_range
        if long_average_true_range > 0
        else 1.0
    )

    if atr_ratio > 1.8:
        regime = MarketRegime.HIGH_VOLATILITY
    elif relative_strength_index > 60 and atr_ratio < 1.5:
        regime = MarketRegime.TRENDING_BULL
    elif relative_strength_index < 40 and atr_ratio < 1.5:
        regime = MarketRegime.TRENDING_BEAR
    else:
        regime = MarketRegime.MEAN_REVERSION
    return MarketRegimeSnapshot(
        regime=regime,
        rsi=float(relative_strength_index),
        atr_ratio=float(atr_ratio),
    )


def enrich_tickers_with_higher_timeframes(
    tickers_to_enrich: dict[str, TickerData],
    hourly_candles_by_market: dict[str, list[CandleData]],
    daily_candles_by_market: dict[str, list[CandleData]],
) -> dict[str, TickerData]:
    """Attach completed hourly/daily candles and derive stable trend context."""
    enriched_tickers = tickers_to_enrich.copy()

    for market, ticker in enriched_tickers.items():
        ticker.hourly_candles = hourly_candles_by_market.get(market, [])
        ticker.daily_candles = daily_candles_by_market.get(market, [])

        if len(ticker.daily_candles) >= 200:
            daily_closes = [candle.close_price for candle in ticker.daily_candles]
            daily_ma_50 = np.mean(daily_closes[-50:])
            daily_ma_200 = np.mean(daily_closes[-200:])
            ticker.is_above_ma50_daily = bool(daily_closes[-1] > daily_ma_50)
            ticker.is_above_ma200_daily = bool(daily_closes[-1] > daily_ma_200)

        if len(ticker.hourly_candles) >= 24:
            hourly_closes = [candle.close_price for candle in ticker.hourly_candles]
            hourly_ma_6 = np.mean(hourly_closes[-6:])
            hourly_ma_24 = np.mean(hourly_closes[-24:])
            if hourly_ma_6 > hourly_ma_24 * 1.005:
                ticker.trend_1h_stable = TrendState.UP
            elif hourly_ma_6 < hourly_ma_24 * 0.995:
                ticker.trend_1h_stable = TrendState.DOWN
            else:
                ticker.trend_1h_stable = TrendState.NEUTRAL

    return enriched_tickers

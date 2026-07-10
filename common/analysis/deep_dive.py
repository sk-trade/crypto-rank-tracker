# common/analysis/deep_dive.py

import logging
from typing import Dict, List, Any

import numpy as np

import config
from common.models import TickerData, Alert, SignalCandidate # Alert, SignalCandidate 추가

logger = logging.getLogger(config.APP_LOGGER_NAME)


def get_market_regime(enriched_tickers: Dict[str, TickerData]) -> Dict[str, Any]:
    """BTC의 1시간봉 데이터로 현재 시장 체제를 분석합니다."""
    btc_ticker = enriched_tickers.get("KRW-BTC")
    required_candles = max(config.REGIME_ATR_LONG_PERIOD + 1, config.REGIME_RSI_PERIOD + 1)
    if not btc_ticker or len(btc_ticker.hourly_candles) < required_candles:
        return {"regime": "UNKNOWN"}

    candles = btc_ticker.hourly_candles
    closes = np.array([candle.close_price for candle in candles])
    changes = np.diff(closes)
    recent_changes = changes[-config.REGIME_RSI_PERIOD :]
    average_gain = np.mean(np.maximum(recent_changes, 0))
    average_loss = np.mean(np.maximum(-recent_changes, 0))
    if average_loss == 0:
        rsi = 100.0 if average_gain > 0 else 50.0
    else:
        rsi = 100 - (100 / (1 + average_gain / average_loss))

    true_ranges = np.array(
        [
            max(
                candle.high_price - candle.low_price,
                abs(candle.high_price - previous.close_price),
                abs(candle.low_price - previous.close_price),
            )
            for previous, candle in zip(candles, candles[1:])
        ]
    )
    atr_24h = np.mean(true_ranges[-config.REGIME_ATR_LONG_PERIOD :])
    atr_6h = np.mean(true_ranges[-config.REGIME_ATR_SHORT_PERIOD :])
    vol_ratio = atr_6h / atr_24h if atr_24h > 0 else 1.0

    if vol_ratio > 1.8:
        return {"regime": "HIGH_VOLATILITY", "rsi": rsi, "atr_ratio": vol_ratio}
    if rsi > 60 and vol_ratio < 1.5:
        return {"regime": "TRENDING_BULL", "rsi": rsi, "atr_ratio": vol_ratio}
    if rsi < 40 and vol_ratio < 1.5:
        return {"regime": "TRENDING_BEAR", "rsi": rsi, "atr_ratio": vol_ratio}
    
    return {"regime": "MEAN_REVERSION", "rsi": rsi, "atr_ratio": vol_ratio}


def enrich_deep_dive_tickers(
    deep_dive_subset: Dict[str, TickerData],
    candles_60m: Dict[str, List],
    candles_daily: Dict[str, List],
    all_lightweight_tickers: Dict[str, TickerData],
) -> Dict[str, TickerData]:
    """
    상위 시간대 데이터를 병합하고 추세(Trend)를 결정합니다.
    """
    enriched_tickers = deep_dive_subset.copy()
    
    for market, ticker in enriched_tickers.items():
        # 데이터 주입
        ticker.hourly_candles = candles_60m.get(market, [])
        ticker.daily_candles = candles_daily.get(market, [])
        
        # [Daily 분석] MA50, MA200
        if len(ticker.daily_candles) >= 200:
            closes = [c.close_price for c in ticker.daily_candles]
            ma50 = np.mean(closes[-50:])
            ma200 = np.mean(closes[-200:])
            ticker.is_above_ma50_daily = closes[-1] > ma50
            ticker.is_above_ma200_daily = closes[-1] > ma200
            
        # [Hourly 분석] 단기 추세 (MA6 vs MA24)
        if len(ticker.hourly_candles) >= 24:
            h_closes = [c.close_price for c in ticker.hourly_candles]
            ma6 = np.mean(h_closes[-6:])
            ma24 = np.mean(h_closes[-24:])
            
            if ma6 > ma24 * 1.005: ticker.trend_1h_stable = "UP"
            elif ma6 < ma24 * 0.995: ticker.trend_1h_stable = "DOWN"
            else: ticker.trend_1h_stable = "NEUTRAL"
            
    return enriched_tickers

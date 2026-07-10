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
    if not btc_ticker or not btc_ticker.hourly_candles or len(btc_ticker.hourly_candles) < 24:
        return {"regime": "UNKNOWN"}

    hourly_closes = np.array([c.close_price for c in btc_ticker.hourly_candles])
    returns = np.diff(np.log(hourly_closes))
    
    # 변동성 (ATR 단순화)
    atr_24h = np.mean([c.high_price - c.low_price for c in btc_ticker.hourly_candles[-24:]])
    atr_6h = np.mean([c.high_price - c.low_price for c in btc_ticker.hourly_candles[-6:]])
    vol_ratio = atr_6h / atr_24h if atr_24h > 0 else 1.0
    
    # 모멘텀 (RSI 단순화)
    gains = returns[returns > 0].sum()
    losses = -returns[returns < 0].sum()
    rs = (gains / 24) / (losses / 24) if losses > 0 else 100
    rsi = 100 - (100 / (1 + rs))

    if vol_ratio > 1.8:
        return {"regime": "HIGH_VOLATILITY"}
    if rsi > 60 and vol_ratio < 1.5:
        return {"regime": "TRENDING_BULL"}
    if rsi < 40 and vol_ratio < 1.5:
        return {"regime": "TRENDING_BEAR"}
    
    return {"regime": "MEAN_REVERSION"}


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
    
    # 디커플링 기준 (BTC/ETH 평균)
    btc = all_lightweight_tickers.get("KRW-BTC")
    eth = all_lightweight_tickers.get("KRW-ETH")
    major_change = 0.0
    if btc and btc.price_change_10m is not None:
        major_change = btc.price_change_10m
    
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
            
        # 디커플링 계산
        if ticker.price_change_10m is not None:
            ticker.decoupling_score = ticker.price_change_10m - major_change

    return enriched_tickers

#common/analysis/utils

import logging
from typing import Any, Dict, List

import numpy as np

import config
from common.models import CandleData, TickerData

logger = logging.getLogger(config.APP_LOGGER_NAME)



def analyze_candle_anatomy(ticker: TickerData) -> TickerData:
    """캔들의 형태(몸통, 꼬리)를 분석하여 질적 정보를 추가합니다."""
    if not ticker.candle_history or len(ticker.candle_history) < 20:
        return ticker

    last_candle = ticker.candle_history[-1]
    avg_volume = np.mean([c.volume for c in ticker.candle_history[-20:-1]])

    if last_candle.volume < avg_volume * 0.8:
        ticker.candle_shape = {"type": "LOW_VOLUME", "reliability": "LOW"}
        return ticker

    o, h, l, c = (
        last_candle.open_price,
        last_candle.high_price,
        last_candle.low_price,
        last_candle.close_price,
    )
    candle_range = h - l
    if candle_range == 0:
        ticker.candle_shape = {"type": "DOJI", "reliability": "LOW"}
        return ticker

    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    reliability = "HIGH" if last_candle.volume > avg_volume * 2 else "MEDIUM"

    shape_type = "NORMAL"
    if upper_wick > body * 2 and upper_wick > candle_range * 0.4:
        shape_type = "STRONG_REJECTION_UP"
    elif lower_wick > body * 2 and lower_wick > candle_range * 0.4:
        shape_type = "STRONG_SUPPORT_DOWN"
    elif body > candle_range * 0.75:
        shape_type = "STRONG_MOMENTUM"

    ticker.candle_shape = {"type": shape_type, "reliability": reliability}
    return ticker


def calculate_rankings(raw_tickers: List[Dict[str, Any]]) -> Dict[str, int]:
    """24시간 누적 거래대금을 기준으로 마켓 순위를 계산합니다."""
    valid_tickers = [
        t
        for t in raw_tickers
        if t.get("acc_trade_price_24h") is not None and t["acc_trade_price_24h"] > 0
    ]
    sorted_tickers = sorted(
        valid_tickers, key=lambda t: t["acc_trade_price_24h"], reverse=True
    )
    return {t["market"]: rank for rank, t in enumerate(sorted_tickers, 1)}
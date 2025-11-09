#common/analysis

import logging
from typing import Any, Dict, List

import numpy as np

import config
from common.models import CandleData, TickerData

logger = logging.getLogger(config.APP_LOGGER_NAME)


def process_and_enrich_candles(
    candles_data: Dict[str, List[CandleData]]
) -> Dict[str, TickerData]:
    """수집된 캔들 데이터로부터 다양한 파생 지표를 계산하여 TickerData 객체를 생성합니다."""
    enriched_tickers = {}

    # 1단계: 각 티커별로 개별 기술적 지표 계산
    for market, candle_list in candles_data.items():
        if len(candle_list) < 20: 
            continue

        try:
            ticker = TickerData(market=market, candle_history=candle_list)
            candles = ticker.candle_history
            last_candle = candles[-1]

            # --- 가격 변동률 지표 ---
            if len(candles) >= 2:
                ticker.price_change_10m = (
                    last_candle.close_price / candles[-2].close_price - 1
                ) * 100
            if len(candles) >= 7:
                ticker.price_change_1h = (
                    last_candle.close_price / candles[-7].close_price - 1
                ) * 100
                highest_high_1h = max(c.high_price for c in candles[-7:-1])
                if last_candle.close_price > highest_high_1h:
                    ticker.is_breaking_1h_high = True
            if len(candles) >= 25:
                ticker.price_change_4h = (
                    last_candle.close_price / candles[-25].close_price - 1
                ) * 100

            # --- 거래량 관련 지표 (RVOL 등) ---
            if len(candles) >= 154:  # 24시간(144개) + 버퍼
                baseline_volumes = [c.volume for c in candles[-154:-10]]
                median_volume_24h = np.median(baseline_volumes)

                if median_volume_24h > 1:
                    ticker.relative_volume = float(
                        last_candle.volume / median_volume_24h
                    )
                    if len(candles) >= 7:
                        ticker.rvol_1h_avg = float(
                            np.mean([c.volume for c in candles[-7:]]) / median_volume_24h
                        )
                    if len(candles) >= 46:
                        median_volume_6h = np.median([c.volume for c in candles[-46:-10]])
                        ticker.volume_acceleration = float(
                            median_volume_6h / median_volume_24h
                        )
                    if len(candles) >= 24:
                        rvols_4h = np.array(
                            [c.volume / median_volume_24h for c in candles[-24:]]
                        )
                        weights = np.arange(1, 25)
                        ticker.rvol_consistency_score = float(
                            np.sum((rvols_4h > 1.5) * weights) / np.sum(weights)
                        )
            if len(candles) >= 145 and candles[-145].volume > 0:
                ticker.rvol_vs_yesterday = last_candle.volume / candles[-145].volume

            # --- 추세 및 변동성 지표 ---
            if len(candles) >= 72:
                ma_1h = np.mean([c.close_price for c in candles[-6:]])
                ma_4h = np.mean([c.close_price for c in candles[-24:]])
                if ma_1h > ma_4h * 1.001:  # 0.1% 버퍼로 추세 안정성 확보
                    ticker.trend_1h = "UP"
                elif ma_1h < ma_4h * 0.999:
                    ticker.trend_1h = "DOWN"
            if len(candles) >= 150:
                ma_4h = np.mean([c.close_price for c in candles[-24:]])
                ma_12h = np.mean([c.close_price for c in candles[-72:]])
                if ma_4h > ma_12h * 1.002:  # 0.2% 버퍼로 추세 안정성 확보
                    ticker.trend_4h = "UP"
                elif ma_4h < ma_12h * 0.998:
                    ticker.trend_4h = "DOWN"

            # --- 볼린저 밴드 상태 ---
            if len(candles) >= 20:
                closes = np.array([c.close_price for c in candles[-20:]])
                ma20, std20 = np.mean(closes), np.std(closes)
                upper, lower = ma20 + 2 * std20, ma20 - 2 * std20

                if last_candle.close_price > upper:
                    ticker.bb_status = "BREAKOUT_UPPER"
                elif last_candle.close_price < lower:
                    ticker.bb_status = "BREAKOUT_LOWER"

                if len(candles) >= 100 and ma20 > 0:
                    bandwidth = (upper - lower) / ma20
                    past_bws = [
                        (np.std(closes[i : i + 20]) * 4) / np.mean(closes[i : i + 20])
                        for i in range(len(closes) - 20)
                    ]
                    if past_bws and bandwidth < np.percentile(past_bws, 10):
                        ticker.bb_status = "SQUEEZE"

            # --- 변동성 등급 (희귀도) ---
            if len(candles) > 20:
                changes = np.diff(np.log([c.close_price for c in candles])) * 100
                last_change_abs = abs(changes[-1])
                p80, p90, p95 = np.percentile(np.abs(changes[:-1]), [80, 90, 95])
                if last_change_abs > p95:
                    ticker.volatility_tier = "EXTREME"
                elif last_change_abs > p90:
                    ticker.volatility_tier = "VERY_HIGH"
                elif last_change_abs > p80:
                    ticker.volatility_tier = "HIGH"

            ticker = analyze_candle_anatomy(ticker)
            enriched_tickers[market] = ticker

        except Exception as e:
            logger.warning(f"{market} 지표 계산 중 오류 발생: {e}")
            continue

    # 2단계: BTC/ETH 기반으로 디커플링 상태 계산 (전체 티커에 적용)
    btc_ticker = enriched_tickers.get("KRW-BTC")
    eth_ticker = enriched_tickers.get("KRW-ETH")

    if not btc_ticker or btc_ticker.price_change_10m is None:
        logger.warning("BTC 데이터가 없어 디커플링 분석을 건너뜁니다.")
        return enriched_tickers

    major_avg_change = btc_ticker.price_change_10m
    if eth_ticker and eth_ticker.price_change_10m is not None:
        major_avg_change = (btc_ticker.price_change_10m + eth_ticker.price_change_10m) / 2

    for market, ticker in enriched_tickers.items():
        if market in ["KRW-BTC", "KRW-ETH"] or ticker.price_change_10m is None:
            continue

        deviation = ticker.price_change_10m - major_avg_change
        ticker.decoupling_score = deviation

        if abs(deviation) > 2.0 and np.sign(ticker.price_change_10m) != np.sign(
            major_avg_change
        ):
            ticker.decoupling_status = "STRONG_DECOUPLE"
        elif deviation > 2.5:
            ticker.decoupling_status = "AMPLIFIED_BULL"
        elif deviation < -2.5:
            ticker.decoupling_status = "AMPLIFIED_BEAR"
        else:
            ticker.decoupling_status = "COUPLED"

    return enriched_tickers


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
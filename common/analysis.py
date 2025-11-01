#analysis
import logging
import numpy as np
from typing import Any, Dict, List

import config
from common.models import CandleData, TickerData

logger = logging.getLogger(config.APP_LOGGER_NAME)


def process_and_enrich_candles(
    candles_data: Dict[str, List[CandleData]]
) -> Dict[str, TickerData]:
    """
    수집된 캔들 데이터로부터 파생 지표를 계산하여 TickerData 객체로 만듭니다.
    """
    enriched_tickers = {}
    
    for market, candle_list in candles_data.items():
        if len(candle_list) < 20:
            continue

        try:
            ticker = TickerData(market=market, candle_history=candle_list)
            candles = ticker.candle_history
            last_candle = candles[-1]
            current_volume = last_candle.volume
            
            # --- 1. 단기(10분) 지표 ---
            if len(candles) >= 2:
                prev_candle = candles[-2]
                ticker.price_change_10m = (last_candle.close_price / prev_candle.close_price - 1) * 100

            # --- 2. 타임라인(1시간, 4시간) 지표 ---
            # 1시간 지표 (캔들 7개 필요 = 현재 + 과거 6개)
            if len(candles) >= 7:
                one_hour_ago_price = candles[-7].close_price
                ticker.price_change_1h = (last_candle.close_price / one_hour_ago_price - 1) * 100
                
                highest_high_1h = max(c.high_price for c in candles[-7:-1])
                if last_candle.close_price > highest_high_1h:
                    ticker.is_breaking_1h_high = True

            # 4시간 지표 (캔들 25개 필요 = 현재 + 과거 24개)
            if len(candles) >= 25:
                four_hours_ago_price = candles[-25].close_price
                ticker.price_change_4h = (last_candle.close_price / four_hours_ago_price - 1) * 100

            # --- 3. 거래량 관련 지표 (RVOL) ---
            median_volume_24h = None
            if len(candles) >= 154: # 24시간(144) + 버퍼
                baseline_volumes_24h = [c.volume for c in candles[-154:-10]]
                median_volume_24h = np.median(baseline_volumes_24h)

            if median_volume_24h is not None and median_volume_24h > 1:
                ticker.relative_volume = float(current_volume / median_volume_24h)
                
                # 1시간 평균 RVOL 계산
                if len(candles) >= 7:
                    volumes_1h = [c.volume for c in candles[-7:]]
                    ticker.rvol_1h_avg = float(np.mean(volumes_1h) / median_volume_24h)
                
                # 거래량 가속도 (6h 중앙값 / 24h 중앙값)
                if len(candles) >= 46:
                    median_volume_6h = np.median([c.volume for c in candles[-46:-10]])
                    ticker.volume_acceleration = float(median_volume_6h / median_volume_24h)
                
                # 거래량 꾸준함 점수 (가중 평균 방식, 4시간 관찰)
                if len(candles) >= 24:
                    rvols_4h = np.array([c.volume / median_volume_24h for c in candles[-24:]])
                    weights = np.arange(1, 25)
                    weighted_sum = np.sum((rvols_4h > 1.5) * weights)
                    total_weight = np.sum(weights)
                    if total_weight > 0:
                        ticker.rvol_consistency_score = float(weighted_sum / total_weight)
            else: # 기준선 계산 불가 시 기본값 처리
                ticker.relative_volume = 1.0

            # 어제 동시간대비 RVOL
            if len(candles) >= 145:
                yesterday_volume = candles[-145].volume
                if yesterday_volume > 0:
                    ticker.rvol_vs_yesterday = current_volume / yesterday_volume

            # --- 추세 및 변동성 지표 ---
            # '가상' 다중 시간 프레임 추세
            if len(candles) >= 72:
                ma_1h = np.mean([c.close_price for c in candles[-6:]])
                ma_4h = np.mean([c.close_price for c in candles[-24:]])
                if ma_1h > ma_4h * 1.001: ticker.trend_1h = "UP"
                elif ma_1h < ma_4h * 0.999: ticker.trend_1h = "DOWN"
            
            if len(candles) >= 150:
                ma_4h = np.mean([c.close_price for c in candles[-24:]])
                ma_12h = np.mean([c.close_price for c in candles[-72:]])
                if ma_4h > ma_12h * 1.002: ticker.trend_4h = "UP"
                elif ma_4h < ma_12h * 0.998: ticker.trend_4h = "DOWN"

            # 볼린저 밴드 상태
            if len(candles) >= 20:
                closes = np.array([c.close_price for c in candles[-20:]])
                ma20 = np.mean(closes)
                std20 = np.std(closes)
                upper_band = ma20 + 2 * std20
                lower_band = ma20 - 2 * std20
                
                if last_candle.close_price > upper_band: ticker.bb_status = "BREAKOUT_UPPER"
                elif last_candle.close_price < lower_band: ticker.bb_status = "BREAKOUT_LOWER"
                
                bandwidth = (upper_band - lower_band) / ma20 if ma20 > 0 else 0
                if len(candles) >= 100:
                    historical_bandwidths = []
                    for i in range(len(candles) - 100, len(candles) - 20):
                        past_closes = np.array([c.close_price for c in candles[i:i+20]])
                        past_ma = np.mean(past_closes)
                        past_std = np.std(past_closes)
                        if past_ma > 0: historical_bandwidths.append(((past_ma + 2*past_std) - (past_ma - 2*past_std)) / past_ma)
                    if historical_bandwidths and bandwidth < np.percentile(historical_bandwidths, 10):
                        ticker.bb_status = "SQUEEZE"

            # 변동성 등급 (희귀도)
            if len(candles) > 20:
                historical_changes = [(candles[i].close_price / candles[i-1].close_price - 1) * 100 for i in range(1, len(candles))]
                last_change_abs = abs(historical_changes[-1])
                valid_historical_changes = [abs(c) for c in historical_changes[:-1] if c is not None]
                if valid_historical_changes:
                    p80, p90, p95 = np.percentile(valid_historical_changes, [80, 90, 95])
                    if last_change_abs > p95: ticker.volatility_tier = "EXTREME"
                    elif last_change_abs > p90: ticker.volatility_tier = "VERY_HIGH"
                    elif last_change_abs > p80: ticker.volatility_tier = "HIGH"

            enriched_tickers[market] = ticker

        except Exception as e:
            logger.warning(f"{market} 지표 계산 중 오류 발생: {e}")
            continue
            
    return enriched_tickers

def calculate_rankings(raw_tickers: List[Dict[str, Any]]) -> Dict[str, int]:
    """24시간 거래대금 기준 순위를 계산합니다."""
    # 거래대금이 있는 티커만 필터링
    valid_tickers = [
        t for t in raw_tickers 
        if t.get('acc_trade_price_24h') is not None and t['acc_trade_price_24h'] > 0
    ]
    
    # 거래대금 기준으로 내림차순 정렬
    sorted_tickers = sorted(
        valid_tickers,
        key=lambda t: t['acc_trade_price_24h'],
        reverse=True
    )
    
    # {마켓: 순위} 딕셔너리 생성
    return {t['market']: rank for rank, t in enumerate(sorted_tickers, 1)}
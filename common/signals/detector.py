

import logging
from typing import Dict, List
import numpy as np
import pandas as pd

from common.models import TickerData
import config

logger = logging.getLogger(config.APP_LOGGER_NAME)

def detect_anomalies(
    enriched_tickers: Dict[str, TickerData],
    SECTORS: Dict,
    REVERSE_SECTOR_MAP: Dict
) -> List[Dict]:
    """신뢰도 스코어를 포함한 이상 현상 탐지"""
    anomalies = []

    current_trade_prices = {}
    for market, ticker in enriched_tickers.items():
        if ticker.candle_history:
            trade_value_krw = ticker.candle_history[-1].volume 
            current_trade_prices[market] = trade_value_krw
    
    trade_price_series = pd.Series(current_trade_prices)

    for market, ticker in enriched_tickers.items():
        if ticker.relative_volume and ticker.relative_volume > config.RVOL_SURGE_THRESHOLD:
            current_price = current_trade_prices.get(market, 0)
            trade_price_percentile = (trade_price_series < current_price).mean() if not trade_price_series.empty else 0.5

            sector_corr = calculate_sector_correlation(market, enriched_tickers, SECTORS, REVERSE_SECTOR_MAP)
            confidence = calculate_confidence_score(ticker, sector_corr, trade_price_percentile)
            
            if confidence >= config.CONFIDENCE_THRESHOLD:
                
                contexts = []
                # 1. 추세 컨텍스트
                if ticker.trend_1h == "UP" and ticker.trend_4h == "UP": contexts.append("단기/중기 모멘텀 일치")
                elif ticker.trend_1h == "UP": contexts.append("단기 상승 모멘텀")
                elif ticker.trend_1h == "DOWN" and ticker.trend_4h == "DOWN": contexts.append("단기/중기 동반 하락")
                elif ticker.trend_4h == "DOWN": contexts.append("중기 하락 모멘텀")

                # 2. 볼린저 밴드 컨텍스트
                if ticker.bb_status == "SQUEEZE": contexts.append("변동성 압축 상태(BB)")
                elif ticker.bb_status == "BREAKOUT_UPPER": contexts.append("BB상단 돌파")
                elif ticker.bb_status == "BREAKOUT_LOWER": contexts.append("BB하단 이탈")
                
                # 3. 희귀도 컨텍스트
                rarity_map = {"HIGH": "★☆☆", "VERY_HIGH": "★★☆", "EXTREME": "★★★"}
                rarity_tag = rarity_map.get(ticker.volatility_tier)
                if rarity_tag: contexts.append(f"희귀도 {rarity_tag}")

                anomalies.append({
                    'market': market,
                    'confidence': confidence,
                    'price_change': ticker.price_change_10m or 0.0,
                    'rvol': ticker.relative_volume,
                    'contexts': contexts
                })
                
    return sorted(anomalies, key=lambda x: x['confidence'], reverse=True)

def calculate_sector_correlation(
    market: str,
    enriched_tickers: Dict[str, TickerData],
    SECTORS: Dict,
    REVERSE_SECTOR_MAP: Dict
) -> float:
    """해당 코인이 속한 섹터의 다른 코인들과 얼마나 동조화되었는지 계산 (0.0 ~ 1.0)"""
    tags = REVERSE_SECTOR_MAP.get(market, [])
    if not tags: return 0.0
    
    primary_sector = tags[0].split('(')[0].strip()
    sector_coins = SECTORS.get(primary_sector, [])
    if len(sector_coins) < 3: return 0.0

    sector_changes = [
        enriched_tickers[coin].price_change_10m
        for coin in sector_coins
        if coin in enriched_tickers and enriched_tickers[coin].price_change_10m is not None
    ]
    if len(sector_changes) < 3: return 0.0

    target_change = enriched_tickers[market].price_change_10m
    if target_change is None: return 0.0

    same_direction = sum(1 for c in sector_changes if (c > 0 and target_change > 0) or (c < 0 and target_change < 0))
    correlation = same_direction / len(sector_changes)
    
    return max(0, (correlation - 0.5) * 2)

def calculate_confidence_score(ticker: TickerData, sector_corr: float, trade_price_percentile: float) -> float:
    """다차원 컨텍스트를 종합하여 0~1 신뢰도 점수 산출"""
    score = 0.0
    price_change = abs(ticker.price_change_10m or 0)
    rvol = ticker.relative_volume or 1.0

    # --- 기본 점수 항목 ---
    # 메인 RVOL 강도 (기여도: 최대 0.30)
    if rvol >= 10.0: score += 0.30
    elif rvol >= 7.0: score += 0.25
    elif rvol >= 5.0: score += 0.20
    else: score += 0.10 

    # 추세 일치도 (기여도: 최대 0.20)
    if ticker.trend_1h == "UP" and ticker.trend_4h == "UP": score += 0.20
    elif ticker.trend_1h == "UP": score += 0.10

    # 가격-거래량 상관성 (기여도: 최대 0.15)
    if price_change > 2.0 and rvol > 4.0: score += 0.15
    elif price_change > 1.0 and rvol > 3.0: score += 0.10

    # --- 보너스 점수 항목 ---
    # RVOL 다각화 지표 (기여도: 최대 +0.20)
    rvol_y = ticker.rvol_vs_yesterday or 1.0
    if rvol_y > 3.0: score += 0.10
    elif rvol_y > 2.0: score += 0.05
    
    vol_accel = ticker.volume_acceleration or 1.0
    if vol_accel > 2.0: score += 0.10
    elif vol_accel > 1.5: score += 0.05

    # 거래량 꾸준함 (기여도: 최대 +0.15)
    score += ticker.rvol_consistency_score * 0.15

    # 섹터 동조화 (기여도: 최대 +0.10)
    score += sector_corr * 0.10

    # 볼린저 밴드 컨텍스트 (기여도: 최대 +0.10)
    if ticker.bb_status == "BREAKOUT_UPPER": score += 0.10
    elif ticker.bb_status == "SQUEEZE": score += 0.05

    # --- 가중치 및 페널티 ---
    # 거래대금 수준 (영향: -0.10 ~ +0.05)
    if trade_price_percentile > 0.7: # 상위 30%
        score += 0.05
    elif trade_price_percentile < 0.2: # 하위 20%
        score -= 0.10

    # 극단적 변동성 페널티
    if ticker.volatility_tier == "EXTREME" and price_change > 10:
        score *= 0.8 # 20% 감점
    
    return min(max(score, 0), 1.0)

def filter_market_wide_events(anomalies: List[Dict], enriched_tickers: Dict[str, TickerData]) -> List[Dict]:
    """시장 전체 이벤트와 진짜 이상 현상을 구분"""
    total = len(enriched_tickers)
    strong_gainers = sum(1 for t in enriched_tickers.values() if t.price_change_10m and t.price_change_10m > 2.0)
    strong_losers = sum(1 for t in enriched_tickers.values() if t.price_change_10m and t.price_change_10m < -2.0)
    
    is_market_event = (strong_gainers / total) > 0.7 or (strong_losers / total) > 0.7
    if not is_market_event: return anomalies

    avg_change = np.mean([t.price_change_10m for t in enriched_tickers.values() if t.price_change_10m])
    extreme_anomalies = [a for a in anomalies if abs(a['price_change']) > abs(avg_change) * 2]
    
    if extreme_anomalies:
        logger.info(f"시장 전체 이벤트 감지, 그러나 {len(extreme_anomalies)}개 극단 이상치 유지")
    return extreme_anomalies
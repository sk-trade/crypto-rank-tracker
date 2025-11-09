#common/signals/detector

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

import config
from common.models import SignalCandidate, TickerData

logger = logging.getLogger(config.APP_LOGGER_NAME)


def detect_anomalies(
    enriched_tickers: Dict[str, TickerData],
    SECTORS: Dict[str, List[str]],
    REVERSE_SECTOR_MAP: Dict[str, List[str]],
) -> List[SignalCandidate]:
    """기술적 지표가 계산된 TickerData를 분석하여 잠재적 시그널 후보 목록을 생성합니다."""
    # 1. 모든 티커의 RVOL에 대한 강건한 Z-score 계산 (이상치 탐지용)
    all_rvols = [
        t.relative_volume
        for t in enriched_tickers.values()
        if t.relative_volume is not None and t.relative_volume > 0
    ]
    if len(all_rvols) > 20:
        median_rvol = np.median(all_rvols)
        mad = np.median(np.abs(all_rvols - median_rvol))  # Median Absolute Deviation
        if mad > 0:
            for ticker in enriched_tickers.values():
                if ticker.relative_volume is not None:
                    # MAD를 사용한 강건한 Z-score 계산
                    ticker.rvol_z_score = (
                        ticker.relative_volume - median_rvol
                    ) / (1.4826 * mad)

    # 2. 현재 거래량의 백분위수 계산 (거래량 수준 평가용)
    current_volumes = {
        market: ticker.candle_history[-1].volume
        for market, ticker in enriched_tickers.items()
        if ticker.candle_history
    }
    volume_series = pd.Series(current_volumes)

    # 3. 각 티커를 순회하며 시그널 후보 생성
    candidates = []
    for market, ticker in enriched_tickers.items():
        if not (
            ticker.rvol_z_score and ticker.rvol_z_score > config.ROBUST_Z_SCORE_THRESHOLD
        ):
            continue

        volume_percentile = (
            (volume_series < current_volumes.get(market, 0)).mean()
            if not volume_series.empty
            else 0.5
        )
        sector_corr = calculate_sector_correlation(
            market, enriched_tickers, SECTORS, REVERSE_SECTOR_MAP
        )
        confidence = calculate_confidence_score(ticker, sector_corr, volume_percentile)

        if confidence >= config.CONFIDENCE_THRESHOLD:
            contexts = _build_contexts(ticker)
            candidate = SignalCandidate(
                market=market,
                confidence=confidence,
                price_change=ticker.price_change_10m or 0.0,
                rvol=ticker.relative_volume or 0.0,
                rvol_z_score=ticker.rvol_z_score,
                contexts=contexts,
                current_price=ticker.candle_history[-1].close_price,
            )
            candidates.append(candidate)

    return sorted(candidates, key=lambda x: x.confidence, reverse=True)


def _build_contexts(ticker: TickerData) -> List[str]:
    """TickerData를 기반으로 시그널에 대한 컨텍스트 문자열 리스트를 생성합니다."""
    contexts = []
    if ticker.trend_1h == "UP" and ticker.trend_4h == "UP":
        contexts.append("단기/중기 모멘텀 일치")
    elif ticker.trend_1h == "UP":
        contexts.append("단기 상승 모멘텀")

    if ticker.bb_status == "BREAKOUT_UPPER":
        contexts.append("BB상단 돌파")
    elif ticker.bb_status == "SQUEEZE":
        contexts.append("변동성 압축 상태(BB)")

    rarity_map = {"HIGH": "★☆☆", "VERY_HIGH": "★★☆", "EXTREME": "★★★"}
    if rarity_tag := rarity_map.get(ticker.volatility_tier):
        contexts.append(f"희귀도 {rarity_tag}")
    return contexts


def calculate_sector_correlation(
    market: str,
    enriched_tickers: Dict[str, TickerData],
    SECTORS: Dict[str, List[str]],
    REVERSE_SECTOR_MAP: Dict[str, List[str]],
) -> float:
    """해당 코인이 속한 섹터 내 다른 코인들과의 가격 변동 동조성을 계산합니다."""
    tags = REVERSE_SECTOR_MAP.get(market, [])
    if not tags:
        return 0.0

    primary_sector = tags[0].split("(")[0].strip()
    sector_coins = SECTORS.get(primary_sector, [])
    if len(sector_coins) < 3:
        return 0.0

    sector_changes = [
        t.price_change_10m
        for coin in sector_coins
        if (t := enriched_tickers.get(coin)) and t.price_change_10m is not None
    ]
    if len(sector_changes) < 3:
        return 0.0

    target_change = enriched_tickers[market].price_change_10m
    if target_change is None:
        return 0.0

    same_direction = sum(
        1
        for c in sector_changes
        if (c > 0 and target_change > 0) or (c < 0 and target_change < 0)
    )
    correlation = same_direction / len(sector_changes)

    # 0.5(무작위)를 0으로, 1.0(완전동조)을 1.0으로 스케일링
    return max(0, (correlation - 0.5) * 2)


def calculate_confidence_score(
    ticker: TickerData, sector_corr: float, volume_percentile: float
) -> float:
    """다양한 지표를 가중 합산하여 시그널의 신뢰도 점수를 0과 1 사이로 계산합니다."""
    score = 0.0
    weights = {
        "z_score_base": 0.35,
        "trend_alignment": 0.20,
        "price_volume_corr": 0.15,
        "decoupling_bonus": 0.15,
        "candle_shape_bonus": 0.10,
        "sector_corr_bonus": 0.10,
        "bb_bonus": 0.10,
        "consistency_bonus": 0.10,
    }

    price_change_abs = abs(ticker.price_change_10m or 0)
    z_score = ticker.rvol_z_score or 0.0

    # --- 1. 핵심 점수 항목 ---
    score += min((z_score / 10.0), 1.0) * weights["z_score_base"]
    if ticker.trend_1h == "UP" and ticker.trend_4h == "UP":
        score += weights["trend_alignment"]
    elif ticker.trend_1h == "UP":
        score += weights["trend_alignment"] * 0.5
    if price_change_abs > 1.0 and z_score > 3.5:
        score += min(price_change_abs / 5.0, 1.0) * weights["price_volume_corr"]

    # --- 2. 보너스 점수 항목 ---
    if ticker.decoupling_status == "STRONG_DECOUPLE":
        score += min(abs(ticker.decoupling_score or 0) / 5.0, 1.0) * weights["decoupling_bonus"]
    elif "AMPLIFIED" in ticker.decoupling_status:
        score += min(abs(ticker.decoupling_score or 0) / 5.0, 1.0) * weights["decoupling_bonus"] * 0.5
    if isinstance(ticker.candle_shape, dict) and ticker.candle_shape.get("reliability") in ["HIGH", "MEDIUM"]:
        shape_type = ticker.candle_shape.get("type")
        if shape_type == "STRONG_MOMENTUM":
            score += weights["candle_shape_bonus"]
        elif shape_type == "STRONG_SUPPORT_DOWN":
            score += weights["candle_shape_bonus"] * 0.7
    score += sector_corr * weights["sector_corr_bonus"]
    if ticker.bb_status == "BREAKOUT_UPPER":
        score += weights["bb_bonus"]
    elif ticker.bb_status == "SQUEEZE":
        score += weights["bb_bonus"] * 0.5
    score += ticker.rvol_consistency_score * weights["consistency_bonus"]

    # --- 3. 조정 및 페널티 ---
    if volume_percentile > 0.8:
        score += 0.05
    elif volume_percentile < 0.2:
        score -= 0.10
    if ticker.volatility_tier == "EXTREME" and price_change_abs > 10:
        score *= 0.8

    return min(max(score, 0), 1.0)


def filter_market_wide_events(
    candidates: List[SignalCandidate], enriched_tickers: Dict[str, TickerData]
) -> List[SignalCandidate]:
    """시장 전체가 급등/급락하는 이벤트와 개별 종목의 이상 현상을 구분합니다."""
    total = len(enriched_tickers)
    strong_gainers = sum(
        1
        for t in enriched_tickers.values()
        if t.price_change_10m and t.price_change_10m > 2.0
    )
    strong_losers = sum(
        1
        for t in enriched_tickers.values()
        if t.price_change_10m and t.price_change_10m < -2.0
    )

    is_market_event = (strong_gainers / total) > 0.7 or (strong_losers / total) > 0.7
    if not is_market_event:
        return candidates 

    avg_change = np.mean(
        [t.price_change_10m for t in enriched_tickers.values() if t.price_change_10m]
    )
    
    extreme_candidates = [
        c for c in candidates if abs(c.price_change) > abs(avg_change) * 2
    ]

    if extreme_candidates:
        logger.info(
            f"시장 전체 이벤트 감지, 그러나 {len(extreme_candidates)}개 극단 이상치 유지"
        )
    return extreme_candidates
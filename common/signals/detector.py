#common/signals/detector

import logging
from typing import Dict, Iterable, List

import numpy as np

import config
from common.models import (
    SignalCandidate,
    TickerData,
    TrendState,
)

logger = logging.getLogger(config.APP_LOGGER_NAME)


def detect_anomalies(
    candidate_markets: Iterable[str],
    enriched_tickers: Dict[str, TickerData],
    SECTORS: Dict[str, List[str]],
    REVERSE_SECTOR_MAP: Dict[str, List[str]],
) -> List[SignalCandidate]:
    """Score selected candidates while retaining whole-market context."""
    # Z-score는 scanner에서 이미 계산됨. detector는 읽기만 한다.

    # 각 티커를 순회하며 시그널 후보 생성
    candidates = []
    for market in candidate_markets:
        ticker = enriched_tickers.get(market)
        if ticker is None:
            continue
        # Candidate selection already requires this metric; retain that contract here.
        conditional_z_score = ticker.conditional_log_rvol_z_score
        if conditional_z_score is None:
            continue
        z_score = conditional_z_score
        price_change = abs(ticker.price_change_10m or 0)
        price_surprise = ticker.price_surprise

        if z_score > 5.0 and price_change < config.WASH_TRADING_MIN_PRICE_CHANGE:
            continue
        if price_surprise is None:
            continue
        if z_score <= config.ROBUST_Z_SCORE_THRESHOLD and price_surprise < config.price_surprise_minimum(ticker.liquidity_tier):
            continue

        sector_corr = calculate_sector_correlation(market, enriched_tickers, SECTORS, REVERSE_SECTOR_MAP)
        signal_score = calculate_signal_score(ticker, sector_corr)

        if signal_score >= config.SIGNAL_SCORE_CANDIDATE_MINIMUM:
            candidate = SignalCandidate(
                market=market,
                signal_score=signal_score,
                price_change=ticker.price_change_10m or 0.0,
                rvol=ticker.relative_volume or 0.0,
                rvol_z_score=conditional_z_score,
                current_price=ticker.candle_history[-1].close_price,
            )
            candidates.append(candidate)

    return sorted(candidates, key=lambda x: x.signal_score, reverse=True)


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

    primary_sector = tags[0]
    sector_coins = SECTORS.get(primary_sector, [])
    if len(sector_coins) < 3:
        return 0.0

    sector_changes = [
        t.price_change_10m
        for coin in sector_coins
        if coin != market and (t := enriched_tickers.get(coin)) and t.price_change_10m is not None
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


def calculate_signal_score(ticker: TickerData, sector_corr: float) -> float:
    """
    사전 시점의 변동성 정규화 가격 surprise와 유동성 구간으로 점수를 매깁니다.
    """
    score = 0.0
    
    z_score = ticker.conditional_log_rvol_z_score or 0
    price_change_abs = abs(ticker.price_change_10m or 0)
    price_surprise = ticker.price_surprise or 0.0
    target_price_surprise = config.price_surprise_minimum(ticker.liquidity_tier)

    # 가격 모멘텀 점수 (최대 0.4)
    # 목표치를 초과 달성할수록 점수가 높아짐 (비율로 계산)
    
    momentum_ratio = price_surprise / target_price_surprise
    if momentum_ratio >= 1.0:
        score += 0.2 # 목표 달성 시 기본 0.2 확보
        # 목표의 2배 달성 시 추가 0.2 (최대 0.4)
        score += min((momentum_ratio - 1.0) * 0.2, 0.2)
    else:
        # 목표 미달 시 비율만큼 점수 (0 ~ 0.2)
        score += momentum_ratio * 0.2
    
    # 거래량 폭발 (최대 0.2)
    # Z-Score는 통계적 수치라 순위 상관없이 절대평가 가능 (Cap 10.0 기준)
    score += min(z_score / 20.0, 0.2) 

    # 추세 정렬 (최대 0.2)
    if (ticker.price_change_10m or 0) > 0:
        if ticker.trend_1h_stable is TrendState.UP:
            score += 0.1
        if ticker.is_above_ma50_daily is True:
            score += 0.1
    elif (ticker.price_change_10m or 0) < 0:
        if ticker.trend_1h_stable is TrendState.DOWN:
            score += 0.1
        if ticker.is_above_ma50_daily is False:
            score += 0.1

    # 보너스 (섹터, 디커플링)
    score += sector_corr * 0.1
    
    if ticker.decoupling_score and abs(ticker.decoupling_score) > config.DECOUPLING_MIN_DEVIATION_PCT:
        score += 0.1
        
    # 페널티 (변동성 없는 거래량 - Wash Trading)
    if z_score > 3.0 and price_change_abs < config.WASH_TRADING_MIN_PRICE_CHANGE:
        score -= 0.3 

    return max(score, 0.0)

def filter_market_wide_events(
    candidates: List[SignalCandidate], enriched_tickers: Dict[str, TickerData]
) -> List[SignalCandidate]:
    """시장 전체가 급등/급락하는 이벤트와 개별 종목의 이상 현상을 구분합니다."""
    total = len(enriched_tickers)
    if total == 0:
        return candidates

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

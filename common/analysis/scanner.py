# common/analysis/scanner.py

import logging
from dataclasses import dataclass
from typing import Dict, List
import numpy as np

import config
from common.models import TickerData # 기존 모델 재사용

logger = logging.getLogger(config.APP_LOGGER_NAME)


@dataclass(frozen=True)
class CandidateDecision:
    eligible: bool
    rejection_reasons: List[str]

def process_lightweight_indicators(
    candles_10m: Dict[str, List], 
    raw_tickers_map: Dict[str, Dict]
) -> Dict[str, TickerData]:
    """10분봉 데이터만으로 최소한의 필수 지표를 계산합니다."""
    lightweight_tickers = {}
    
    # 전체 시장의 RVOL 분포를 파악하기 위한 리스트
    all_rvols = []
    for market, candle_list in candles_10m.items():
        if len(candle_list) >= 154:
            baseline_volumes = [c.volume for c in candle_list[-154:-10]]
            median_volume_24h = np.median(baseline_volumes)
            if median_volume_24h > 0:
                relative_volume = candle_list[-1].volume / median_volume_24h
                all_rvols.append(relative_volume)

    # Robust Z-Score 계산 준비
    median_rvol = np.median(all_rvols) if all_rvols else 0
    mad = np.median(np.abs(np.array(all_rvols) - median_rvol)) if all_rvols else 0
    effective_mad = max(mad, config.MIN_MAD_FLOOR)

    for market, candle_list in candles_10m.items():
        if len(candle_list) < 2: continue
        
        ticker = TickerData(market=market, candle_history=candle_list)
        
        # 가격 변동률 계산
        ticker.price_change_10m = (
            candle_list[-1].close_price / candle_list[-2].close_price - 1
        ) * 100
        
        if len(candle_list) >= 7:
            ticker.price_change_1h = (
                candle_list[-1].close_price / candle_list[-7].close_price - 1
            ) * 100

        if len(candle_list) >= 25:
            ticker.price_change_4h = (
                candle_list[-1].close_price / candle_list[-25].close_price - 1
            ) * 100

        # RVOL 및 Z-Score 계산
        if len(candle_list) >= 154:
            baseline_volumes = [c.volume for c in candle_list[-154:-10]]
            median_volume_24h = np.median(baseline_volumes)
            if median_volume_24h > 0:
                ticker.relative_volume = candle_list[-1].volume / median_volume_24h
                
                raw_z = (ticker.relative_volume - median_rvol) / (1.4826 * effective_mad)
                ticker.rvol_z_score = min(raw_z, config.MAX_Z_SCORE_CAP)

        lightweight_tickers[market] = ticker
        
    return lightweight_tickers


def select_candidates_for_deep_dive(
    lightweight_tickers: Dict[str, TickerData],
    current_rankings: Dict[str, int],
    raw_tickers_map: Dict[str, Dict]
) -> List[str]:
    """
    config에 설정된 동적 임계값을 적용하여 후보군을 선정합니다.
    """
    decisions = evaluate_candidate_eligibility(lightweight_tickers, current_rankings)
    return [market for market, decision in decisions.items() if decision.eligible]


def evaluate_candidate_eligibility(
    lightweight_tickers: Dict[str, TickerData], current_rankings: Dict[str, int]
) -> Dict[str, CandidateDecision]:
    """Evaluate every ticker and retain explicit reasons for rejected candidates."""
    decisions = {}
    for market, ticker in lightweight_tickers.items():
        
        z_score = ticker.rvol_z_score or 0
        price_change = abs(ticker.price_change_10m or 0)
        rank = current_rankings.get(market, 999)

        # 순위에 따른 동적 임계값 적용
        if rank <= config.RANK_THRESHOLD_MAJOR:
            # 메이저 (Top 50)
            min_price_change = config.MAJOR_MIN_PRICE_CHANGE
            min_z_score = config.MAJOR_MIN_Z_SCORE
            is_strict_mode = False 
        elif rank <= config.RANK_THRESHOLD_MID:
            # 중위권 (Top 100)
            min_price_change = config.MID_MIN_PRICE_CHANGE
            min_z_score = config.MID_MIN_Z_SCORE
            is_strict_mode = True 
        else:
            # 하위권 (Top 100 밖)
            min_price_change = config.MINOR_MIN_PRICE_CHANGE
            min_z_score = config.MINOR_MIN_Z_SCORE
            is_strict_mode = True 

        # 필터 조건 확인
        has_volume_spike = z_score > min_z_score
        has_price_spike = price_change > min_price_change

        # Wash Trading 방지
        if z_score > 5.0 and price_change < config.WASH_TRADING_MIN_PRICE_CHANGE:
            decisions[market] = CandidateDecision(False, ["suspected_wash_trading"])
            continue

        # 메이저는 유연하게, 잡코인은 엄격하게
        if is_strict_mode:
            # 잡코인/중위권: 거래량과 가격이 동시에 터져야 함
            if has_volume_spike and has_price_spike:
                decisions[market] = CandidateDecision(True, [])
            else:
                reasons = []
                if not has_volume_spike:
                    reasons.append("volume_anomaly_below_threshold")
                if not has_price_spike:
                    reasons.append("price_move_below_threshold")
                decisions[market] = CandidateDecision(False, reasons)
        else:
            # 메이저: 둘 중 하나만 터져도 감지 (선취매 혹은 뉴스 반응)
            if has_volume_spike or has_price_spike:
                decisions[market] = CandidateDecision(True, [])
            else:
                decisions[market] = CandidateDecision(
                    False, ["volume_and_price_move_below_threshold"]
                )
    return decisions

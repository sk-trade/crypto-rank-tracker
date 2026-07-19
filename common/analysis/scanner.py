# common/analysis/scanner.py

import logging
from typing import Dict, List
import numpy as np

import config
from common.models import CandidateDecision, LiquidityTier, RejectionCode, TickerData

logger = logging.getLogger(config.APP_LOGGER_NAME)

def _historical_price_surprise(candle_list: List) -> float | None:
    """Standardize the latest return using only earlier completed-bar returns."""
    required = config.PRICE_SURPRISE_MIN_RETURN_OBSERVATIONS + 2
    if len(candle_list) < required:
        return None

    historical = candle_list[-(config.PRICE_SURPRISE_LOOKBACK_BARS + 1):-1]
    returns = [
        (current.close_price / previous.close_price - 1) * 100
        for previous, current in zip(historical, historical[1:], strict=False)
        if previous.close_price > 0
    ]
    if len(returns) < config.PRICE_SURPRISE_MIN_RETURN_OBSERVATIONS:
        return None
    volatility = float(np.std(returns, ddof=1))
    if volatility <= 0:
        return None
    latest_return = (candle_list[-1].close_price / candle_list[-2].close_price - 1) * 100
    return abs(latest_return) / volatility


def _rolling_turnover(candle_list: List) -> float | None:
    """Use prior-bar notional turnover so tiering cannot see the current event."""
    prior_candles = candle_list[-(config.ROLLING_TURNOVER_LOOKBACK_BARS + 1):-1]
    if not prior_candles:
        return None
    turnovers = [
        candle.trade_value
        if candle.trade_value is not None
        else candle.close_price * candle.volume
        for candle in prior_candles
    ]
    return float(np.median(turnovers)) if turnovers else None


def _assign_liquidity_tiers(tickers: Dict[str, TickerData]) -> None:
    turnovers = [ticker.rolling_turnover for ticker in tickers.values() if ticker.rolling_turnover]
    if not turnovers:
        return
    low_cutoff, high_cutoff = np.quantile(turnovers, config.LIQUIDITY_TIER_QUANTILES)
    for ticker in tickers.values():
        if ticker.rolling_turnover is None:
            continue
        if ticker.rolling_turnover >= high_cutoff:
            ticker.liquidity_tier = LiquidityTier.HIGH
        elif ticker.rolling_turnover >= low_cutoff:
            ticker.liquidity_tier = LiquidityTier.MEDIUM
        else:
            ticker.liquidity_tier = LiquidityTier.LOW


def _robust_z_score(value: float, baseline: List[float]) -> float | None:
    if len(baseline) < config.CONDITIONAL_VOLUME_MIN_SAMPLES:
        return None
    median = float(np.median(baseline))
    mad = float(np.median(np.abs(np.array(baseline) - median)))
    return min((value - median) / (1.4826 * max(mad, config.MIN_MAD_FLOOR)), config.MAX_Z_SCORE_CAP)


def _conditional_log_rvol_z_score(candle_list: List) -> float | None:
    """Compare a bar only with the same weekday and clock slot in earlier weeks."""
    latest = candle_list[-1]
    if latest.volume <= 0:
        return None
    slot = (latest.timestamp.weekday(), latest.timestamp.hour, latest.timestamp.minute)
    prior = [
        np.log(candle.volume)
        for candle in candle_list[:-1]
        if candle.volume > 0
        and (candle.timestamp.weekday(), candle.timestamp.hour, candle.timestamp.minute) == slot
    ]
    return _robust_z_score(float(np.log(latest.volume)), prior)


def _assign_cross_sectional_log_rvol_z_scores(tickers: Dict[str, TickerData]) -> None:
    values = {
        market: float(np.log(ticker.relative_volume))
        for market, ticker in tickers.items()
        if ticker.relative_volume and ticker.relative_volume > 0
    }
    baseline = list(values.values())
    for market, value in values.items():
        tickers[market].cross_sectional_log_rvol_z_score = _robust_z_score(value, baseline)

def process_lightweight_indicators(
    candles_10m: Dict[str, List],
) -> Dict[str, TickerData]:
    """10분봉 데이터만으로 최소한의 필수 지표를 계산합니다."""
    lightweight_tickers = {}
    
    # 전체 시장의 RVOL 분포를 파악하기 위한 리스트
    all_rvols = []
    for _market, candle_list in candles_10m.items():
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
        if len(candle_list) < 2:
            continue
        
        ticker = TickerData(market=market, candle_history=candle_list)
        
        # 가격 변동률 계산
        ticker.price_change_10m = (
            candle_list[-1].close_price / candle_list[-2].close_price - 1
        ) * 100
        ticker.price_surprise = _historical_price_surprise(candle_list)
        ticker.rolling_turnover = _rolling_turnover(candle_list)
        
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
        ticker.conditional_log_rvol_z_score = _conditional_log_rvol_z_score(candle_list)

        lightweight_tickers[market] = ticker

    _assign_liquidity_tiers(lightweight_tickers)
    _assign_cross_sectional_log_rvol_z_scores(lightweight_tickers)
    return lightweight_tickers


def evaluate_candidate_eligibility(
    lightweight_tickers: Dict[str, TickerData],
) -> Dict[str, CandidateDecision]:
    """Evaluate every ticker and retain explicit reasons for rejected candidates."""
    decisions = {}
    for market, ticker in lightweight_tickers.items():
        
        conditional_z_score = ticker.conditional_log_rvol_z_score
        z_score = conditional_z_score or 0
        price_change = abs(ticker.price_change_10m or 0)
        price_surprise = ticker.price_surprise
        liquidity_tier = ticker.liquidity_tier
        min_z_score = config.rvol_z_score_minimum(liquidity_tier)
        min_price_surprise = config.price_surprise_minimum(liquidity_tier)
        is_strict_mode = liquidity_tier is not LiquidityTier.HIGH

        if price_surprise is None or liquidity_tier is LiquidityTier.UNKNOWN:
            decisions[market] = CandidateDecision(
                eligible=False,
                rejection_reasons=[RejectionCode.PRICE_SURPRISE_UNAVAILABLE],
            )
            continue
        if conditional_z_score is None:
            decisions[market] = CandidateDecision(
                eligible=False,
                rejection_reasons=[RejectionCode.CONDITIONAL_VOLUME_HISTORY_UNAVAILABLE],
            )
            continue

        # 필터 조건 확인
        has_volume_spike = z_score > min_z_score
        has_price_spike = price_surprise > min_price_surprise

        # Wash Trading 방지
        if z_score > 5.0 and price_change < config.WASH_TRADING_MIN_PRICE_CHANGE:
            decisions[market] = CandidateDecision(
                eligible=False,
                rejection_reasons=[RejectionCode.SUSPECTED_WASH_TRADING],
            )
            continue

        # 메이저는 유연하게, 잡코인은 엄격하게
        if is_strict_mode:
            # 잡코인/중위권: 거래량과 가격이 동시에 터져야 함
            if has_volume_spike and has_price_spike:
                decisions[market] = CandidateDecision(eligible=True)
            else:
                reasons = []
                if not has_volume_spike:
                    reasons.append(RejectionCode.VOLUME_ANOMALY_BELOW_THRESHOLD)
                if not has_price_spike:
                    reasons.append(RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD)
                decisions[market] = CandidateDecision(
                    eligible=False, rejection_reasons=reasons
                )
        else:
            # 메이저: 둘 중 하나만 터져도 감지 (선취매 혹은 뉴스 반응)
            if has_volume_spike or has_price_spike:
                decisions[market] = CandidateDecision(eligible=True)
            else:
                decisions[market] = CandidateDecision(
                    eligible=False,
                    rejection_reasons=[
                        RejectionCode.VOLUME_AND_PRICE_SURPRISE_BELOW_THRESHOLD
                    ],
                )
    return decisions

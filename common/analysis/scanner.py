"""Point-in-time lightweight features and broad candidate eligibility."""

from itertools import pairwise

import numpy as np

import config
from common.models import (
    CandidateDecision,
    CandleData,
    LiquidityTier,
    RejectionCode,
    TickerData,
)


def _historical_price_surprise(candles: list[CandleData]) -> float | None:
    """Standardize the latest return using only earlier completed-bar returns."""
    required_candles = config.PRICE_SURPRISE_MIN_RETURN_OBSERVATIONS + 2
    if len(candles) < required_candles:
        return None

    historical_candles = candles[-(config.PRICE_SURPRISE_LOOKBACK_BARS + 1) : -1]
    historical_returns = [
        (current.close_price / previous.close_price - 1) * 100
        for previous, current in pairwise(historical_candles)
        if previous.close_price > 0
    ]
    if len(historical_returns) < config.PRICE_SURPRISE_MIN_RETURN_OBSERVATIONS:
        return None
    historical_volatility = float(np.std(historical_returns, ddof=1))
    if historical_volatility <= 0:
        return None
    latest_return = (candles[-1].close_price / candles[-2].close_price - 1) * 100
    return abs(latest_return) / historical_volatility


def _rolling_turnover(candles: list[CandleData]) -> float | None:
    """Use prior-bar notional turnover so tiering cannot see the current event."""
    prior_candles = candles[-(config.ROLLING_TURNOVER_LOOKBACK_BARS + 1) : -1]
    if not prior_candles:
        return None
    prior_turnovers = [
        candle.trade_value
        if candle.trade_value is not None
        else candle.close_price * candle.volume
        for candle in prior_candles
    ]
    return float(np.median(prior_turnovers)) if prior_turnovers else None


def _assign_liquidity_tiers(tickers: dict[str, TickerData]) -> None:
    rolling_turnovers = [
        ticker.rolling_turnover
        for ticker in tickers.values()
        if ticker.rolling_turnover
    ]
    if not rolling_turnovers:
        return
    low_cutoff, high_cutoff = np.quantile(
        rolling_turnovers,
        config.LIQUIDITY_TIER_QUANTILES,
    )
    for ticker in tickers.values():
        if ticker.rolling_turnover is None:
            continue
        if ticker.rolling_turnover >= high_cutoff:
            ticker.liquidity_tier = LiquidityTier.HIGH
        elif ticker.rolling_turnover >= low_cutoff:
            ticker.liquidity_tier = LiquidityTier.MEDIUM
        else:
            ticker.liquidity_tier = LiquidityTier.LOW


def _robust_z_score(value: float, baseline: list[float]) -> float | None:
    if len(baseline) < config.CONDITIONAL_VOLUME_MIN_SAMPLES:
        return None
    median = float(np.median(baseline))
    median_absolute_deviation = float(np.median(np.abs(np.array(baseline) - median)))
    denominator = 1.4826 * max(
        median_absolute_deviation,
        config.MIN_MAD_FLOOR,
    )
    return min(
        (value - median) / denominator,
        config.MAX_Z_SCORE_CAP,
    )


def _conditional_log_rvol_z_score(
    candles: list[CandleData],
) -> float | None:
    """Compare a bar only with the same weekday and clock slot in earlier weeks."""
    latest_candle = candles[-1]
    if latest_candle.volume <= 0:
        return None
    latest_slot = (
        latest_candle.timestamp.weekday(),
        latest_candle.timestamp.hour,
        latest_candle.timestamp.minute,
    )
    prior_log_volumes = [
        float(np.log(candle.volume))
        for candle in candles[:-1]
        if candle.volume > 0
        and (
            candle.timestamp.weekday(),
            candle.timestamp.hour,
            candle.timestamp.minute,
        )
        == latest_slot
    ]
    return _robust_z_score(
        float(np.log(latest_candle.volume)),
        prior_log_volumes,
    )


def _recent_relative_volume(candles: list[CandleData]) -> float | None:
    if len(candles) < config.RECENT_SCAN_HISTORY_BARS:
        return None
    baseline_start = -config.RECENT_SCAN_HISTORY_BARS
    baseline_end = -config.RVOL_RECENT_EXCLUSION_BARS
    baseline_volumes = [
        candle.volume for candle in candles[baseline_start:baseline_end]
    ]
    median_baseline_volume = float(np.median(baseline_volumes))
    if median_baseline_volume <= 0:
        return None
    return candles[-1].volume / median_baseline_volume


def _assign_cross_sectional_log_rvol_z_scores(
    tickers: dict[str, TickerData],
) -> None:
    log_relative_volumes = {
        market: float(np.log(ticker.relative_volume))
        for market, ticker in tickers.items()
        if ticker.relative_volume and ticker.relative_volume > 0
    }
    cross_section = list(log_relative_volumes.values())
    for market, log_relative_volume in log_relative_volumes.items():
        tickers[market].cross_sectional_log_rvol_z_score = _robust_z_score(
            log_relative_volume,
            cross_section,
        )


def build_lightweight_tickers(
    candles_by_market: dict[str, list[CandleData]],
) -> dict[str, TickerData]:
    """Build broad-scan features from completed 10-minute candles only."""
    relative_volumes = [
        relative_volume
        for candles in candles_by_market.values()
        if (relative_volume := _recent_relative_volume(candles)) is not None
    ]
    median_relative_volume = (
        float(np.median(relative_volumes)) if relative_volumes else 0.0
    )
    relative_volume_mad = (
        float(np.median(np.abs(np.array(relative_volumes) - median_relative_volume)))
        if relative_volumes
        else 0.0
    )
    cross_section_denominator = 1.4826 * max(
        relative_volume_mad,
        config.MIN_MAD_FLOOR,
    )

    lightweight_tickers: dict[str, TickerData] = {}
    for market, candles in candles_by_market.items():
        if len(candles) < 2:
            continue

        ticker = TickerData(market=market, candle_history=candles)
        ticker.price_change_10m = (
            candles[-1].close_price / candles[-2].close_price - 1
        ) * 100
        ticker.price_surprise = _historical_price_surprise(candles)
        ticker.rolling_turnover = _rolling_turnover(candles)

        if len(candles) >= 7:
            ticker.price_change_1h = (
                candles[-1].close_price / candles[-7].close_price - 1
            ) * 100
        if len(candles) >= 25:
            ticker.price_change_4h = (
                candles[-1].close_price / candles[-25].close_price - 1
            ) * 100

        ticker.relative_volume = _recent_relative_volume(candles)
        if ticker.relative_volume is not None:
            raw_z_score = (
                ticker.relative_volume - median_relative_volume
            ) / cross_section_denominator
            ticker.rvol_z_score = min(
                raw_z_score,
                config.MAX_Z_SCORE_CAP,
            )
        ticker.conditional_log_rvol_z_score = _conditional_log_rvol_z_score(candles)
        lightweight_tickers[market] = ticker

    _assign_liquidity_tiers(lightweight_tickers)
    _assign_cross_sectional_log_rvol_z_scores(lightweight_tickers)
    return lightweight_tickers


def evaluate_candidate_eligibility(
    lightweight_tickers: dict[str, TickerData],
) -> dict[str, CandidateDecision]:
    """Evaluate every ticker and retain explicit rejection reasons."""
    decisions: dict[str, CandidateDecision] = {}
    for market, ticker in lightweight_tickers.items():
        conditional_z_score = ticker.conditional_log_rvol_z_score
        price_change = abs(ticker.price_change_10m or 0)
        price_surprise = ticker.price_surprise
        liquidity_tier = ticker.liquidity_tier
        minimum_z_score = config.rvol_z_score_minimum(liquidity_tier)
        minimum_price_surprise = config.price_surprise_minimum(liquidity_tier)

        if price_surprise is None or liquidity_tier is LiquidityTier.UNKNOWN:
            decisions[market] = CandidateDecision(
                eligible=False,
                rejection_reasons=[RejectionCode.PRICE_SURPRISE_UNAVAILABLE],
            )
            continue
        if conditional_z_score is None:
            decisions[market] = CandidateDecision(
                eligible=False,
                rejection_reasons=[
                    RejectionCode.CONDITIONAL_VOLUME_HISTORY_UNAVAILABLE
                ],
            )
            continue

        has_volume_spike = conditional_z_score > minimum_z_score
        has_price_spike = price_surprise > minimum_price_surprise
        if (
            conditional_z_score > 5.0
            and price_change < config.WASH_TRADING_MIN_PRICE_CHANGE
        ):
            decisions[market] = CandidateDecision(
                eligible=False,
                rejection_reasons=[RejectionCode.SUSPECTED_WASH_TRADING],
            )
            continue

        if liquidity_tier is not LiquidityTier.HIGH:
            if has_volume_spike and has_price_spike:
                decisions[market] = CandidateDecision(eligible=True)
                continue
            rejection_reasons = []
            if not has_volume_spike:
                rejection_reasons.append(RejectionCode.VOLUME_ANOMALY_BELOW_THRESHOLD)
            if not has_price_spike:
                rejection_reasons.append(RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD)
            decisions[market] = CandidateDecision(
                eligible=False,
                rejection_reasons=rejection_reasons,
            )
            continue

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

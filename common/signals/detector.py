"""Candidate scoring and market-wide outlier filtering."""

import logging
from collections.abc import Iterable

import numpy as np

import config
from common.models import SignalCandidate, TickerData, TrendState

logger = logging.getLogger(config.APP_LOGGER_NAME)


def detect_anomalies(
    candidate_markets: Iterable[str],
    enriched_tickers: dict[str, TickerData],
    markets_by_sector: dict[str, list[str]],
    sectors_by_market: dict[str, list[str]],
) -> list[SignalCandidate]:
    """Score broad-scan candidates with higher-timeframe and sector context."""
    candidates: list[SignalCandidate] = []
    for market in candidate_markets:
        ticker = enriched_tickers.get(market)
        if ticker is None:
            continue
        conditional_z_score = ticker.conditional_log_rvol_z_score
        price_surprise = ticker.price_surprise
        if conditional_z_score is None or price_surprise is None:
            continue

        absolute_price_change = abs(ticker.price_change_10m or 0)
        if (
            conditional_z_score > 5.0
            and absolute_price_change < config.WASH_TRADING_MIN_PRICE_CHANGE
        ):
            continue
        if (
            conditional_z_score <= config.ROBUST_Z_SCORE_THRESHOLD
            and price_surprise < config.price_surprise_minimum(ticker.liquidity_tier)
        ):
            continue

        sector_alignment = calculate_sector_directional_alignment(
            market,
            enriched_tickers,
            markets_by_sector,
            sectors_by_market,
        )
        signal_score = calculate_candidate_signal_score(
            ticker,
            sector_alignment,
        )
        if signal_score < config.SIGNAL_SCORE_CANDIDATE_MINIMUM:
            continue
        candidates.append(
            SignalCandidate(
                market=market,
                signal_score=signal_score,
                price_change=ticker.price_change_10m or 0.0,
                rvol=ticker.relative_volume or 0.0,
                rvol_z_score=conditional_z_score,
                current_price=ticker.candle_history[-1].close_price,
            )
        )

    return sorted(
        candidates,
        key=lambda candidate: candidate.signal_score,
        reverse=True,
    )


def calculate_sector_directional_alignment(
    market: str,
    enriched_tickers: dict[str, TickerData],
    markets_by_sector: dict[str, list[str]],
    sectors_by_market: dict[str, list[str]],
) -> float:
    """Measure same-direction participation among peers in the primary sector."""
    market_sectors = sectors_by_market.get(market, [])
    if not market_sectors:
        return 0.0

    primary_sector = market_sectors[0]
    peer_markets = markets_by_sector.get(primary_sector, [])
    if len(peer_markets) < 3:
        return 0.0

    peer_changes = [
        peer_ticker.price_change_10m
        for peer_market in peer_markets
        if peer_market != market
        and (peer_ticker := enriched_tickers.get(peer_market))
        and peer_ticker.price_change_10m is not None
    ]
    if len(peer_changes) < 3:
        return 0.0

    target_change = enriched_tickers[market].price_change_10m
    if target_change is None:
        return 0.0
    same_direction_count = sum(
        (peer_change > 0 and target_change > 0)
        or (peer_change < 0 and target_change < 0)
        for peer_change in peer_changes
    )
    agreement_ratio = same_direction_count / len(peer_changes)
    return max(0.0, (agreement_ratio - 0.5) * 2)


def calculate_candidate_signal_score(
    ticker: TickerData,
    sector_alignment: float,
) -> float:
    """Score pre-decision price, activity, trend, and sector evidence."""
    score = 0.0
    volume_z_score = ticker.conditional_log_rvol_z_score or 0.0
    absolute_price_change = abs(ticker.price_change_10m or 0.0)
    price_surprise = ticker.price_surprise or 0.0
    target_price_surprise = config.price_surprise_minimum(ticker.liquidity_tier)

    momentum_ratio = price_surprise / target_price_surprise
    if momentum_ratio >= 1.0:
        score += 0.2
        score += min((momentum_ratio - 1.0) * 0.2, 0.2)
    else:
        score += momentum_ratio * 0.2

    score += min(volume_z_score / 20.0, 0.2)
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

    score += sector_alignment * 0.1
    if (
        ticker.decoupling_score
        and abs(ticker.decoupling_score) > config.DECOUPLING_MIN_DEVIATION_PCT
    ):
        score += 0.1
    if (
        volume_z_score > 3.0
        and absolute_price_change < config.WASH_TRADING_MIN_PRICE_CHANGE
    ):
        score -= 0.3
    return max(score, 0.0)


def filter_market_wide_candidates(
    candidates: list[SignalCandidate],
    enriched_tickers: dict[str, TickerData],
) -> list[SignalCandidate]:
    """During broad market moves, retain only candidates that are true outliers."""
    market_count = len(enriched_tickers)
    if market_count == 0:
        return candidates

    strong_gainer_count = sum(
        ticker.price_change_10m is not None and ticker.price_change_10m > 2.0
        for ticker in enriched_tickers.values()
    )
    strong_loser_count = sum(
        ticker.price_change_10m is not None and ticker.price_change_10m < -2.0
        for ticker in enriched_tickers.values()
    )
    is_market_wide_move = (
        strong_gainer_count / market_count > 0.7
        or strong_loser_count / market_count > 0.7
    )
    if not is_market_wide_move:
        return candidates

    nonzero_changes = [
        ticker.price_change_10m
        for ticker in enriched_tickers.values()
        if ticker.price_change_10m
    ]
    if not nonzero_changes:
        return []
    average_market_change = float(np.mean(nonzero_changes))
    outliers = [
        candidate
        for candidate in candidates
        if abs(candidate.price_change) > abs(average_market_change) * 2
    ]
    if outliers:
        logger.info(
            "Market-wide move detected; retained %d extreme candidates",
            len(outliers),
        )
    return outliers

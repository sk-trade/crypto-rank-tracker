"""Build immutable scan events and resolve their later execution-aware outcomes."""

import datetime
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import config
from common.models import (
    Alert,
    AttentionCandidate,
    CandidateDecision,
    CandleData,
    DataQualityIssue,
    Direction,
    MarketTicker,
    RejectionCode,
    ScanDecision,
    ScanEvent,
    ScanOutcome,
    SignalCandidate,
    TickerData,
)
from common.outcomes import (
    PRIMARY_PERFORMANCE_TARGET,
    directional_net_return,
    favorable_and_adverse_excursions,
)

logger = logging.getLogger(config.APP_LOGGER_NAME)


_EXECUTION_REJECTION_REASONS = frozenset(
    {
        RejectionCode.MARKET_WARNING,
        RejectionCode.DAILY_TURNOVER_BELOW_MINIMUM,
        RejectionCode.ORDERBOOK_UNAVAILABLE,
        RejectionCode.ORDERBOOK_INVALID,
        RejectionCode.ORDERBOOK_DEPTH_BELOW_NOTIONAL,
        RejectionCode.SPREAD_ABOVE_MAXIMUM,
        RejectionCode.SLIPPAGE_ABOVE_MAXIMUM,
        RejectionCode.MOVE_DOES_NOT_COVER_ESTIMATED_COSTS,
    }
)


def _final_decision_for_rejected_candidate(
    reasons: list[RejectionCode],
) -> ScanDecision:
    """Keep the gate that rejected a candidate visible to later model analysis."""
    if RejectionCode.MARKET_REGIME_UNKNOWN in reasons:
        return ScanDecision.MARKET_REGIME_BLOCKED
    if _EXECUTION_REJECTION_REASONS.intersection(reasons):
        return ScanDecision.EXECUTION_BLOCKED
    return ScanDecision.REJECTED_LIGHTWEIGHT


@dataclass(frozen=True)
class _ScanEventContext:
    observed_at: datetime.datetime
    tickers: dict[str, TickerData]
    candidate_decisions: dict[str, CandidateDecision]
    deep_dive_markets: frozenset[str]
    alerts_by_market: dict[str, Alert]
    candidates_by_market: dict[str, SignalCandidate]
    attention_by_market: dict[str, AttentionCandidate]
    attention_coverage: dict[str, float | int]
    execution_rejections_by_market: dict[str, list[RejectionCode]]
    data_quality_issues: tuple[DataQualityIssue, ...]
    raw_tickers_by_market: dict[str, MarketTicker]


def _attention_feature_snapshot(
    candidate: AttentionCandidate,
) -> dict[str, Any]:
    return {
        "lane": candidate.lane.value,
        "lane_rank": candidate.lane_rank,
        "primary_selected": candidate.primary_selected,
        "displayed": candidate.displayed,
        "display_rank": candidate.display_rank,
        "quality_score": candidate.quality_score,
        "ranking_score": candidate.ranking_score,
        "v3_shadow_rank": candidate.v3_shadow_rank,
        "v4_shadow_rank": candidate.v4_shadow_rank,
        "ridge_rank": candidate.ridge_rank,
        "ridge_score": candidate.ridge_score,
        "ridge_base_rank": candidate.ridge_base_rank,
        "ridge_base_ranking_score": candidate.ridge_base_ranking_score,
        "ridge_base_quality_score": candidate.ridge_base_quality_score,
        "ridge_base_exposures_60m": candidate.ridge_base_exposures_60m,
        "primary_exposures_60m": candidate.primary_exposures_60m,
        "score_version": candidate.score_version,
        "context_available": candidate.context_available,
    }


def _build_feature_snapshot(
    market: str,
    ticker: TickerData | None,
    context: _ScanEventContext,
) -> dict[str, Any]:
    snapshot = (
        ticker.model_dump(
            mode="json",
            exclude={"candle_history", "hourly_candles", "daily_candles"},
        )
        if ticker
        else {}
    )
    if raw_ticker := context.raw_tickers_by_market.get(market):
        snapshot["raw_ticker"] = raw_ticker.model_dump(mode="json")
    if execution_rejections := context.execution_rejections_by_market.get(market):
        snapshot["execution_rejection_reasons"] = [
            reason.value for reason in execution_rejections
        ]
    if context.data_quality_issues:
        snapshot["data_quality_issues"] = [
            issue.model_dump(mode="json") for issue in context.data_quality_issues
        ]
    if context.attention_coverage:
        snapshot["attention_coverage"] = dict(context.attention_coverage)
    if attention := context.attention_by_market.get(market):
        snapshot["attention"] = _attention_feature_snapshot(attention)
    return snapshot


def _candidate_rejection_reasons(
    market: str,
    ticker: TickerData | None,
    decision: CandidateDecision | None,
    context: _ScanEventContext,
) -> tuple[bool, list[RejectionCode]]:
    eligible = decision.eligible if decision else False
    reasons = (
        list(decision.rejection_reasons)
        if decision
        else [RejectionCode.COMPLETE_CANDLE_HISTORY_UNAVAILABLE]
    )
    needs_higher_timeframe = (
        ticker is not None
        and market not in context.deep_dive_markets
        and (eligible or market in context.attention_by_market)
    )
    if needs_higher_timeframe:
        reasons.append(RejectionCode.HIGHER_TIMEFRAME_CANDLE_HISTORY_UNAVAILABLE)
    if context.data_quality_issues:
        reasons.extend(issue.code for issue in context.data_quality_issues)
    return eligible, reasons


def _final_scan_decision(
    market: str,
    ticker: TickerData | None,
    eligible: bool,
    reasons: list[RejectionCode],
    context: _ScanEventContext,
) -> ScanDecision:
    if context.data_quality_issues or ticker is None:
        return ScanDecision.DATA_QUALITY_BLOCKED
    if market in context.alerts_by_market:
        return ScanDecision.ALERT_SELECTED
    if market in context.attention_by_market:
        return ScanDecision.ATTENTION_QUEUED
    if not eligible:
        return _final_decision_for_rejected_candidate(reasons)
    if market not in context.deep_dive_markets:
        return ScanDecision.DEEP_DIVE_DATA_BLOCKED
    return ScanDecision.CANDIDATE_NOT_ALERTED


def _signal_direction(
    ticker: TickerData | None,
) -> tuple[Direction | None, datetime.datetime | None]:
    if not ticker or not ticker.candle_history or not ticker.price_change_10m:
        return None, None
    direction = Direction.LONG if ticker.price_change_10m > 0 else Direction.SHORT
    return direction, ticker.candle_history[-1].timestamp


def _attention_event_fields(
    candidate: AttentionCandidate | None,
) -> dict[str, Any]:
    if candidate is None:
        return {
            "attention_stage": None,
            "attention_lane": None,
            "attention_rank": None,
            "attention_lane_rank": None,
            "attention_primary_selected": None,
            "attention_displayed": None,
            "attention_display_rank": None,
            "attention_quality_score": None,
            "attention_ranking_score": None,
            "attention_v3_shadow_rank": None,
            "attention_v4_shadow_rank": None,
            "attention_ridge_rank": None,
            "attention_ridge_score": None,
            "attention_ridge_base_rank": None,
            "attention_ridge_base_ranking_score": None,
            "attention_ridge_base_quality_score": None,
            "attention_ridge_base_exposures_60m": None,
            "attention_primary_exposures_60m": None,
            "attention_score_version": None,
            "attention_first_seen_at": None,
            "attention_episode_id": None,
        }
    return {
        "attention_stage": candidate.stage,
        "attention_lane": candidate.lane,
        "attention_rank": candidate.attention_rank,
        "attention_lane_rank": candidate.lane_rank,
        "attention_primary_selected": candidate.primary_selected,
        "attention_displayed": candidate.displayed,
        "attention_display_rank": candidate.display_rank,
        "attention_quality_score": candidate.quality_score,
        "attention_ranking_score": candidate.ranking_score,
        "attention_v3_shadow_rank": candidate.v3_shadow_rank,
        "attention_v4_shadow_rank": candidate.v4_shadow_rank,
        "attention_ridge_rank": candidate.ridge_rank,
        "attention_ridge_score": candidate.ridge_score,
        "attention_ridge_base_rank": candidate.ridge_base_rank,
        "attention_ridge_base_ranking_score": candidate.ridge_base_ranking_score,
        "attention_ridge_base_quality_score": candidate.ridge_base_quality_score,
        "attention_ridge_base_exposures_60m": candidate.ridge_base_exposures_60m,
        "attention_primary_exposures_60m": candidate.primary_exposures_60m,
        "attention_score_version": candidate.score_version,
        "attention_first_seen_at": candidate.first_seen_at,
        "attention_episode_id": candidate.episode_id,
    }


def _build_market_scan_event(
    market: str,
    context: _ScanEventContext,
) -> ScanEvent:
    ticker = context.tickers.get(market)
    decision = context.candidate_decisions.get(market)
    eligible, reasons = _candidate_rejection_reasons(
        market,
        ticker,
        decision,
        context,
    )
    direction, signal_candle_start = _signal_direction(ticker)
    signal_candidate = context.candidates_by_market.get(market)
    attention_candidate = context.attention_by_market.get(market)
    return ScanEvent(
        event_id=f"{context.observed_at.isoformat()}:{market}",
        observed_at=context.observed_at,
        market=market,
        feature_snapshot=_build_feature_snapshot(market, ticker, context),
        candidate_eligible=eligible,
        rejection_reasons=sorted(set(reasons), key=lambda reason: reason.value),
        final_decision=_final_scan_decision(
            market,
            ticker,
            eligible,
            reasons,
            context,
        ),
        model_version=config.SIGNAL_MODEL_VERSION,
        direction=direction,
        signal_score=(signal_candidate.signal_score if signal_candidate else None),
        signal_candle_start=signal_candle_start,
        **_attention_event_fields(attention_candidate),
    )


def build_scan_events(
    observed_at: datetime.datetime,
    markets: Iterable[str],
    tickers: dict[str, TickerData],
    candidate_decisions: dict[str, CandidateDecision],
    deep_dive_candidates: Iterable[str],
    alerts: Iterable[Alert],
    candidates: Iterable[SignalCandidate] = (),
    attention_candidates: Iterable[AttentionCandidate] = (),
    attention_coverage: dict[str, float | int] | None = None,
    execution_rejections_by_market: dict[str, list[RejectionCode]] | None = None,
    data_quality_issues: Iterable[DataQualityIssue] = (),
    raw_tickers_by_market: dict[str, MarketTicker] | None = None,
) -> list[ScanEvent]:
    """Capture every market's pre-decision state, including rejection paths."""
    context = _ScanEventContext(
        observed_at=observed_at,
        tickers=tickers,
        candidate_decisions=candidate_decisions,
        deep_dive_markets=frozenset(deep_dive_candidates),
        alerts_by_market={alert.candidate.market: alert for alert in alerts},
        candidates_by_market={candidate.market: candidate for candidate in candidates},
        attention_by_market={
            candidate.market: candidate for candidate in attention_candidates
        },
        attention_coverage=attention_coverage or {},
        execution_rejections_by_market=execution_rejections_by_market or {},
        data_quality_issues=tuple(data_quality_issues),
        raw_tickers_by_market=raw_tickers_by_market or {},
    )
    return [_build_market_scan_event(market, context) for market in sorted(markets)]


def resolve_scan_outcomes(
    events: Iterable[ScanEvent], candles_by_market: dict[str, list[CandleData]]
) -> tuple[list[ScanOutcome], list[ScanEvent]]:
    """Resolve available outcomes and retire events outside the recovery window."""
    outcomes = []
    pending = []
    interval = datetime.timedelta(
        minutes=PRIMARY_PERFORMANCE_TARGET.execution_timeframe_minutes
    )
    holding = datetime.timedelta(
        minutes=PRIMARY_PERFORMANCE_TARGET.holding_period_minutes
    )
    candles_by_market_and_time = {
        market: {candle.timestamp: candle for candle in candles}
        for market, candles in candles_by_market.items()
    }
    latest_completed = max(
        (
            timestamp
            for candles in candles_by_market_and_time.values()
            for timestamp in candles
        ),
        default=None,
    )
    recovery_start = (
        latest_completed - interval * (config.RECENT_SCAN_HISTORY_BARS - 1)
        if latest_completed is not None
        else None
    )
    expired_event_ids = []

    for event in events:
        if not event.direction or not event.signal_candle_start:
            continue
        entry_start = event.signal_candle_start + interval
        exit_start = entry_start + holding
        candles = candles_by_market_and_time.get(event.market, {})
        entry = candles.get(entry_start)
        exit_candle = candles.get(exit_start)
        path = [
            candles.get(entry_start + interval * offset)
            for offset in range(PRIMARY_PERFORMANCE_TARGET.holding_period_bars)
        ]
        if not entry or not exit_candle or any(candle is None for candle in path):
            if recovery_start is not None and exit_start < recovery_start:
                expired_event_ids.append(event.event_id)
                continue
            pending.append(event)
            continue
        completed_path = [candle for candle in path if candle is not None]
        mfe, mae = favorable_and_adverse_excursions(
            entry.open_price,
            [candle.high_price for candle in completed_path],
            [candle.low_price for candle in completed_path],
            event.direction,
        )
        outcomes.append(
            ScanOutcome(
                event_id=event.event_id,
                market=event.market,
                entry_candle_start=entry_start,
                exit_candle_start=exit_start,
                entry_price=entry.open_price,
                exit_price=exit_candle.open_price,
                directional_net_return=directional_net_return(
                    entry.open_price, exit_candle.open_price, event.direction
                ),
                mfe=mfe,
                mae=mae,
            )
        )
    if expired_event_ids:
        logger.warning(
            "Retired %d unresolved outcome event(s) outside the candle recovery window: %s",
            len(expired_event_ids),
            ", ".join(expired_event_ids[:10]),
        )
    return outcomes, pending

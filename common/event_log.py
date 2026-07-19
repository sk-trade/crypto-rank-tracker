"""Build immutable scan events and resolve their later execution-aware outcomes."""

import datetime
import logging
from typing import Dict, Iterable, List, Tuple

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
    reasons: List[RejectionCode],
) -> ScanDecision:
    """Keep the gate that rejected a candidate visible to later model analysis."""
    if RejectionCode.MARKET_REGIME_UNKNOWN in reasons:
        return ScanDecision.MARKET_REGIME_BLOCKED
    if _EXECUTION_REJECTION_REASONS.intersection(reasons):
        return ScanDecision.EXECUTION_BLOCKED
    return ScanDecision.REJECTED_LIGHTWEIGHT


def build_scan_events(
    observed_at: datetime.datetime,
    markets: Iterable[str],
    tickers: Dict[str, TickerData],
    candidate_decisions: Dict[str, CandidateDecision],
    deep_dive_candidates: Iterable[str],
    alerts: Iterable[Alert],
    candidates: Iterable[SignalCandidate] = (),
    attention_candidates: Iterable[AttentionCandidate] = (),
    execution_rejections_by_market: Dict[str, List[RejectionCode]] | None = None,
    data_quality_issues: Iterable[DataQualityIssue] = (),
    raw_tickers_by_market: Dict[str, MarketTicker] | None = None,
) -> List[ScanEvent]:
    """Capture every market's pre-decision state, including rejection paths."""
    deep_dive_markets = set(deep_dive_candidates)
    alerts_by_market = {alert.candidate.market: alert for alert in alerts}
    candidates_by_market = {candidate.market: candidate for candidate in candidates}
    attention_by_market = {
        candidate.market: candidate for candidate in attention_candidates
    }
    quality_issues = list(data_quality_issues)
    execution_rejections_by_market = execution_rejections_by_market or {}
    raw_tickers_by_market = raw_tickers_by_market or {}
    events = []

    for market in sorted(markets):
        ticker = tickers.get(market)
        decision = candidate_decisions.get(market)
        feature_snapshot = (
            ticker.model_dump(
                mode="json", exclude={"candle_history", "hourly_candles", "daily_candles"}
            )
            if ticker
            else {}
        )
        if raw_ticker := raw_tickers_by_market.get(market):
            feature_snapshot["raw_ticker"] = raw_ticker.model_dump(mode="json")
        if execution_rejections := execution_rejections_by_market.get(market):
            feature_snapshot["execution_rejection_reasons"] = [
                reason.value for reason in execution_rejections
            ]
        if quality_issues:
            feature_snapshot["data_quality_issues"] = [
                issue.model_dump(mode="json") for issue in quality_issues
            ]
        eligible = decision.eligible if decision else False
        reasons = (
            list(decision.rejection_reasons)
            if decision
            else [RejectionCode.COMPLETE_CANDLE_HISTORY_UNAVAILABLE]
        )
        if (
            ticker
            and market not in deep_dive_markets
            and (eligible or market in attention_by_market)
        ):
            reasons.append(RejectionCode.HIGHER_TIMEFRAME_CANDLE_HISTORY_UNAVAILABLE)
        if quality_issues:
            final_decision = ScanDecision.DATA_QUALITY_BLOCKED
            reasons.extend(issue.code for issue in quality_issues)
        elif not ticker:
            final_decision = ScanDecision.DATA_QUALITY_BLOCKED
        elif market in alerts_by_market:
            final_decision = ScanDecision.ALERT_SELECTED
        elif market in attention_by_market:
            final_decision = ScanDecision.ATTENTION_QUEUED
        elif not eligible:
            final_decision = _final_decision_for_rejected_candidate(reasons)
        elif market not in deep_dive_markets:
            final_decision = ScanDecision.DEEP_DIVE_DATA_BLOCKED
        else:
            final_decision = ScanDecision.CANDIDATE_NOT_ALERTED

        direction = None
        signal_candle_start = None
        if ticker and ticker.candle_history and ticker.price_change_10m:
            direction = (
                Direction.LONG if ticker.price_change_10m > 0 else Direction.SHORT
            )
            signal_candle_start = ticker.candle_history[-1].timestamp
        candidate = candidates_by_market.get(market)
        attention = attention_by_market.get(market)
        events.append(
            ScanEvent(
                event_id=f"{observed_at.isoformat()}:{market}",
                observed_at=observed_at,
                market=market,
                feature_snapshot=feature_snapshot,
                candidate_eligible=eligible,
                rejection_reasons=sorted(set(reasons), key=lambda reason: reason.value),
                final_decision=final_decision,
                model_version=config.SIGNAL_MODEL_VERSION,
                direction=direction,
                signal_score=candidate.signal_score if candidate else None,
                signal_candle_start=signal_candle_start,
                attention_stage=attention.stage if attention else None,
                attention_rank=attention.attention_rank if attention else None,
                attention_episode_id=attention.episode_id if attention else None,
            )
        )
    return events


def resolve_scan_outcomes(
    events: Iterable[ScanEvent], candles_by_market: Dict[str, List[CandleData]]
) -> Tuple[List[ScanOutcome], List[ScanEvent]]:
    """Resolve available outcomes and retire events outside the recovery window."""
    outcomes = []
    pending = []
    interval = datetime.timedelta(minutes=PRIMARY_PERFORMANCE_TARGET.execution_timeframe_minutes)
    holding = datetime.timedelta(minutes=PRIMARY_PERFORMANCE_TARGET.holding_period_minutes)
    latest_completed = max(
        (
            candle.timestamp
            for candles in candles_by_market.values()
            for candle in candles
        ),
        default=None,
    )
    recovery_start = (
        latest_completed
        - interval * (config.RECENT_SCAN_HISTORY_BARS - 1)
        if latest_completed is not None
        else None
    )
    expired_event_ids = []

    for event in events:
        if not event.direction or not event.signal_candle_start:
            continue
        entry_start = event.signal_candle_start + interval
        exit_start = entry_start + holding
        candles = {candle.timestamp: candle for candle in candles_by_market.get(event.market, [])}
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

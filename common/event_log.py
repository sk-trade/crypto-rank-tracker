"""Build immutable scan events and resolve their later execution-aware outcomes."""

import datetime
from typing import Dict, Iterable, List, Tuple

import config
from common.analysis.scanner import CandidateDecision
from common.models import Alert, CandleData, ScanEvent, ScanOutcome, SignalCandidate, TickerData
from common.outcomes import (
    PRIMARY_PERFORMANCE_TARGET,
    directional_net_return,
    favorable_and_adverse_excursions,
)


_EXECUTION_REJECTION_REASONS = frozenset(
    {
        "market_warning",
        "daily_turnover_below_minimum",
        "orderbook_unavailable",
        "orderbook_invalid",
        "orderbook_depth_below_notional",
        "spread_above_maximum",
        "slippage_above_maximum",
        "move_does_not_cover_estimated_costs",
    }
)


def _final_decision_for_rejected_candidate(reasons: List[str]) -> str:
    """Keep the gate that rejected a candidate visible to later model analysis."""
    if "market_regime_unknown" in reasons:
        return "market_regime_blocked"
    if _EXECUTION_REJECTION_REASONS.intersection(reasons):
        return "execution_blocked"
    return "rejected_lightweight"


def build_scan_events(
    observed_at: datetime.datetime,
    markets: Iterable[str],
    tickers: Dict[str, TickerData],
    candidate_decisions: Dict[str, CandidateDecision],
    deep_dive_candidates: Iterable[str],
    alerts: Iterable[Alert],
    candidates: Iterable[SignalCandidate] = (),
    data_quality_issues: Iterable[str] = (),
    raw_tickers_by_market: Dict[str, dict] | None = None,
) -> List[ScanEvent]:
    """Capture every market's pre-decision state, including rejection paths."""
    deep_dive_markets = set(deep_dive_candidates)
    alerts_by_market = {alert.candidate.market: alert for alert in alerts}
    candidates_by_market = {candidate.market: candidate for candidate in candidates}
    quality_issues = list(data_quality_issues)
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
            feature_snapshot["raw_ticker"] = raw_ticker
        eligible = decision.eligible if decision else False
        reasons = list(decision.rejection_reasons) if decision else ["complete_candle_history_unavailable"]
        if quality_issues:
            final_decision = "data_quality_blocked"
            reasons.extend(quality_issues)
        elif not ticker:
            final_decision = "data_quality_blocked"
        elif not eligible:
            final_decision = _final_decision_for_rejected_candidate(reasons)
        elif market not in deep_dive_markets:
            final_decision = "deep_dive_data_blocked"
            reasons.append("higher_timeframe_candle_history_unavailable")
        elif market in alerts_by_market:
            final_decision = "alert_selected"
        else:
            final_decision = "candidate_not_alerted"

        direction = None
        signal_candle_start = None
        if ticker and ticker.candle_history and ticker.price_change_10m:
            direction = "long" if ticker.price_change_10m > 0 else "short"
            signal_candle_start = ticker.candle_history[-1].timestamp
        alert = alerts_by_market.get(market)
        candidate = candidates_by_market.get(market)
        events.append(
            ScanEvent(
                event_id=f"{observed_at.isoformat()}:{market}",
                observed_at=observed_at,
                market=market,
                feature_snapshot=feature_snapshot,
                candidate_eligible=eligible,
                rejection_reasons=sorted(set(reasons)),
                final_decision=final_decision,
                model_version=config.SIGNAL_MODEL_VERSION,
                direction=direction,
                signal_score=candidate.signal_score if candidate else None,
                signal_candle_start=signal_candle_start,
            )
        )
    return events


def resolve_scan_outcomes(
    events: Iterable[ScanEvent], candles_by_market: Dict[str, List[CandleData]]
) -> Tuple[List[ScanOutcome], List[ScanEvent]]:
    """Resolve only events whose entry, exit, and complete holding path are available."""
    outcomes = []
    pending = []
    interval = datetime.timedelta(minutes=PRIMARY_PERFORMANCE_TARGET.execution_timeframe_minutes)
    holding = datetime.timedelta(minutes=PRIMARY_PERFORMANCE_TARGET.holding_period_minutes)

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
    return outcomes, pending

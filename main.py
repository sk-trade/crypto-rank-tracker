"""Google Cloud Function orchestration for completed-candle market scans."""

import asyncio
import datetime
import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import aiohttp
import functions_framework

import config
from common.analysis.deep_dive import (
    classify_market_regime,
    enrich_tickers_with_higher_timeframes,
)
from common.analysis.scanner import (
    build_lightweight_tickers,
    evaluate_candidate_eligibility,
)
from common.analysis.utils import rank_markets_by_24h_turnover
from common.attention import (
    attention_briefing_due,
    build_attention_result,
)
from common.event_log import build_scan_events, resolve_scan_outcomes
from common.execution import ExecutionDecision, assess_execution
from common.models import (
    Alert,
    AlertHistory,
    AttentionCandidate,
    AttentionState,
    CandidateDecision,
    DataQualityIssue,
    MarketRegime,
    MarketRegimeSnapshot,
    MarketTicker,
    RankState,
    RejectionCode,
    ScanEvent,
    ScanHandoffState,
    ScanOutcome,
    SignalCandidate,
    TickerData,
)
from common.notification.engine import AlertEngine
from common.notification.main import (
    NotificationDeliveryError,
    dispatch_data_quality_alert,
    format_and_dispatch_notification,
    recover_pending_notification,
)
from common.residuals import assign_residual_momentum
from common.sector_loader import load_sector_maps
from common.signals.detector import detect_anomalies, filter_market_wide_candidates
from common.state_manager import (
    append_scan_events,
    append_scan_outcomes,
    claim_scan_key,
    complete_scan_key,
    load_alert_history,
    load_attention_state,
    load_pending_scan_events,
    load_rank_state_history,
    release_scan_key,
    save_attention_state,
    save_pending_scan_events,
    save_rank_state_history,
)
from common.storage_client import (
    StateBackendUnavailableError,
    StateErrorCode,
    create_gcs_client,
)
from common.upbit_client import (
    CandleTimeUnit,
    get_all_krw_tickers,
    get_candles,
    get_orderbooks,
)

logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(config.APP_LOGGER_NAME)


class PipelineErrorCode(StrEnum):
    INVALID_SCHEDULE_TIME = "invalid_schedule_time"
    SCHEDULE_TIMEZONE_REQUIRED = "schedule_timezone_required"
    EXECUTION_FAILED = "pipeline_execution_failed"


class PipelineError(RuntimeError):
    def __init__(self, code: PipelineErrorCode, *, field: str | None = None):
        super().__init__(code.value)
        self.code = code
        self.field = field


def filter_markets_with_complete_deep_dive_data(
    candidate_markets: list[str],
    candles_60m: dict,
    candles_daily: dict,
) -> list[str]:
    """Return candidates with complete higher-timeframe supporting evidence."""
    valid_markets = [
        market
        for market in candidate_markets
        if len(candles_60m.get(market, [])) >= config.ATTENTION_CONTEXT_MIN_HOURLY_BARS
        and len(candles_daily.get(market, []))
        >= config.ATTENTION_CONTEXT_MIN_DAILY_BARS
    ]
    blocked_markets = sorted(set(candidate_markets) - set(valid_markets))
    if blocked_markets:
        logger.warning(
            "%d candidate(s) lack 60-minute or daily supporting evidence: %s",
            len(blocked_markets),
            ", ".join(blocked_markets[:10]),
        )
    return valid_markets


def required_deep_dive_markets(candidate_markets: list[str]) -> list[str]:
    """Return selected candidates plus the BTC benchmark used for market regime."""
    return sorted(set(candidate_markets) | {"KRW-BTC"})


def assess_scan_data_quality(
    all_markets: list[str], candles_10m: dict, minimum_success_rate: float
) -> list[DataQualityIssue]:
    """Return operationally actionable reasons why a scan cannot produce signals."""
    if not all_markets:
        return [
            DataQualityIssue(
                code=RejectionCode.MARKET_UNIVERSE_EMPTY,
                message="KRW market universe is empty.",
            )
        ]

    successful_markets = len(candles_10m)
    success_rate = successful_markets / len(all_markets)
    issues = []
    if success_rate < minimum_success_rate:
        issues.append(
            DataQualityIssue(
                code=RejectionCode.CANDLE_COVERAGE_BELOW_MINIMUM,
                message=(
                    f"10-minute candle coverage {successful_markets}/{len(all_markets)} "
                    f"({success_rate:.1%}) is below the {minimum_success_rate:.0%} minimum."
                ),
                details={
                    "successful_markets": successful_markets,
                    "total_markets": len(all_markets),
                    "success_rate": success_rate,
                    "minimum_success_rate": minimum_success_rate,
                },
            )
        )
    if "KRW-BTC" not in candles_10m:
        issues.append(
            DataQualityIssue(
                code=RejectionCode.BTC_CANDLE_HISTORY_UNAVAILABLE,
                message="KRW-BTC completed 10-minute candles are missing or stale.",
            )
        )
    return issues


def _scheduled_scan_time(schedule_time: str | None) -> datetime.datetime:
    if schedule_time is None:
        return datetime.datetime.now(datetime.UTC)
    normalized = (
        f"{schedule_time[:-1]}+00:00" if schedule_time.endswith("Z") else schedule_time
    )
    try:
        parsed = datetime.datetime.fromisoformat(normalized)
    except ValueError as error:
        raise PipelineError(
            PipelineErrorCode.INVALID_SCHEDULE_TIME,
            field="X-CloudScheduler-ScheduleTime",
        ) from error
    if parsed.tzinfo is None:
        raise PipelineError(
            PipelineErrorCode.SCHEDULE_TIMEZONE_REQUIRED,
            field="X-CloudScheduler-ScheduleTime",
        )
    return parsed.astimezone(datetime.UTC)


async def _settle_notification_scan_claim(
    error: NotificationDeliveryError, scan_key: str, gcs_client=None
) -> None:
    if error.scan_handoff_state is ScanHandoffState.DURABLE:
        await complete_scan_key(scan_key, gcs_client)
    elif error.scan_handoff_state is ScanHandoffState.UNCERTAIN:
        logger.error(
            "Notification handoff is uncertain; retaining scan claim %s for retry reconciliation",
            scan_key,
        )
    else:
        await release_scan_key(scan_key, gcs_client=gcs_client)


@dataclass(frozen=True)
class _ScanWindow:
    started_at: datetime.datetime
    closed_at: datetime.datetime
    scan_key: str


@dataclass
class _ScanLifecycle:
    scan_key: str
    execution_id: str | None
    gcs_client: Any = None
    claimed: bool = False
    persisted: bool = False

    async def acquire(self) -> bool:
        self.claimed = await claim_scan_key(
            self.scan_key,
            execution_id=self.execution_id,
            gcs_client=self.gcs_client,
        )
        return self.claimed

    def mark_persisted(self) -> None:
        self.persisted = True

    async def complete(self) -> None:
        await complete_scan_key(self.scan_key, self.gcs_client)
        self.claimed = False
        self.persisted = True

    async def release(self) -> None:
        if not self.claimed:
            return
        await release_scan_key(self.scan_key, gcs_client=self.gcs_client)
        self.claimed = False

    async def settle_notification_error(self, error: NotificationDeliveryError) -> None:
        await _settle_notification_scan_claim(
            error,
            self.scan_key,
            self.gcs_client,
        )
        self.claimed = False

    async def release_if_unpersisted(self) -> None:
        if not self.claimed or self.persisted:
            return
        try:
            await self.release()
        except Exception:
            logger.exception("Failed to release scan claim %s", self.scan_key)


@dataclass(frozen=True)
class _MarketSnapshot:
    raw_tickers: list[MarketTicker]
    raw_tickers_by_market: dict[str, MarketTicker]
    markets: list[str]
    candles_10m: dict[str, list]


@dataclass(frozen=True)
class _BroadScanAnalysis:
    resolved_outcomes: list[ScanOutcome]
    pending_events: list[ScanEvent]
    current_rankings: dict[str, int]
    lightweight_tickers: dict[str, TickerData]
    candidate_decisions: dict[str, CandidateDecision]
    candidate_markets: list[str]
    execution_decisions: dict[str, ExecutionDecision]


@dataclass(frozen=True)
class _ContextAnalysis:
    enriched_tickers: dict[str, TickerData]
    market_regime: MarketRegimeSnapshot
    evidence_markets: list[str]


@dataclass(frozen=True)
class _AttentionAnalysis:
    signal_candidates: list[SignalCandidate]
    alerts: list[Alert]
    alert_history: dict[str, AlertHistory]
    all_candidates: list[AttentionCandidate]
    state: AttentionState
    coverage: dict[str, int | float]
    notification_queue: list[AttentionCandidate]


def _create_state_client():
    storage_method = config.validate_storage_config()
    if storage_method is not config.StorageMethod.GCS:
        logger.info("Using local-file state storage.")
        return None

    try:
        client = create_gcs_client()
    except ImportError as error:
        raise StateBackendUnavailableError(
            StateErrorCode.BACKEND_UNAVAILABLE,
            config.GCS_BUCKET_NAME or "GCS",
        ) from error
    logger.info("Using GCS state storage.")
    return client


def _build_scan_window(schedule_time: str | None) -> _ScanWindow:
    started_at = _scheduled_scan_time(schedule_time)
    closed_at = started_at.replace(second=0, microsecond=0) - datetime.timedelta(
        minutes=started_at.minute % config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
    )
    return _ScanWindow(
        started_at=started_at,
        closed_at=closed_at,
        scan_key=f"completed-candle:{closed_at.isoformat()}",
    )


async def _recover_pending_delivery_error(
    gcs_client=None,
) -> NotificationDeliveryError | None:
    try:
        recovered_delivery = await recover_pending_notification(gcs_client)
    except NotificationDeliveryError as error:
        logger.error(
            "Pending webhook recovery failed; market-state collection will continue: %s",
            error,
        )
        return error

    if recovered_delivery is not None:
        logger.info("Recovered pending webhook delivery; continuing the market scan.")
    return None


def _raise_pending_delivery_error(
    pending_delivery_error: NotificationDeliveryError | None,
) -> None:
    if pending_delivery_error is not None:
        raise pending_delivery_error


def _previous_rankings(
    rank_states: list[RankState],
    scan_closed_at: datetime.datetime,
) -> dict[str, int]:
    return next(
        (
            state.rankings
            for state in reversed(rank_states)
            if state.last_updated < scan_closed_at
        ),
        {},
    )


async def _complete_historical_retry_if_needed(
    previous_attention_state: AttentionState,
    window: _ScanWindow,
    lifecycle: _ScanLifecycle,
    pending_delivery_error: NotificationDeliveryError | None,
) -> bool:
    last_attention_update = previous_attention_state.updated_at
    if last_attention_update is None or last_attention_update <= window.closed_at:
        return False

    logger.warning(
        "Skipping historical retry %s because newer attention state exists at %s; "
        "current-only ticker, orderbook, and alert evidence cannot be backfilled safely.",
        window.closed_at.isoformat(),
        last_attention_update.isoformat(),
    )
    if not await lifecycle.acquire():
        logger.warning("Skipping duplicate scheduled scan for %s", window.scan_key)
        _raise_pending_delivery_error(pending_delivery_error)
        return True

    await lifecycle.complete()
    _raise_pending_delivery_error(pending_delivery_error)
    return True


async def _collect_market_snapshot(
    session: aiohttp.ClientSession,
    scan_started_at: datetime.datetime,
) -> _MarketSnapshot:
    raw_tickers = await get_all_krw_tickers(session)
    markets = [ticker.market for ticker in raw_tickers]
    raw_tickers_by_market = {ticker.market: ticker for ticker in raw_tickers}
    candles_10m = await get_candles(
        session,
        markets,
        time_unit=CandleTimeUnit.MINUTES,
        minutes_unit=10,
        count=config.RECENT_SCAN_HISTORY_BARS,
        as_of=scan_started_at,
        synthesize_no_trade_intervals=True,
        same_slot_lookback_weeks=config.CONDITIONAL_VOLUME_LOOKBACK_WEEKS,
    )
    return _MarketSnapshot(
        raw_tickers=raw_tickers,
        raw_tickers_by_market=raw_tickers_by_market,
        markets=markets,
        candles_10m=candles_10m,
    )


def _immutable_event_conflict_issue(
    conflicting_event_ids: list[str],
) -> DataQualityIssue:
    return DataQualityIssue(
        code=RejectionCode.IMMUTABLE_SCAN_EVENT_CONFLICT,
        message=(
            "A retry produced evidence that conflicts with the first persisted scan; "
            "the original scan remains authoritative."
        ),
        details={
            "conflicting_event_count": len(conflicting_event_ids),
            "conflicting_event_ids": conflicting_event_ids[:10],
        },
    )


async def _dispatch_data_quality_with_claim(
    lifecycle: _ScanLifecycle,
    issues: list[DataQualityIssue],
    observed_at: datetime.datetime,
) -> None:
    try:
        await dispatch_data_quality_alert(
            issues,
            observed_at=observed_at,
            gcs_client=lifecycle.gcs_client,
            scan_key=lifecycle.scan_key,
        )
    except NotificationDeliveryError as error:
        await lifecycle.settle_notification_error(error)
        raise
    except Exception:
        await lifecycle.release()
        raise


async def _record_data_quality_failure(
    snapshot: _MarketSnapshot,
    issues: list[DataQualityIssue],
    window: _ScanWindow,
    lifecycle: _ScanLifecycle,
    pending_delivery_error: NotificationDeliveryError | None,
) -> None:
    logger.error(
        "Skipping scan due to data quality: %s",
        "; ".join(issue.message for issue in issues),
    )
    if not await lifecycle.acquire():
        logger.warning("Skipping duplicate scheduled scan for %s", window.scan_key)
        _raise_pending_delivery_error(pending_delivery_error)
        return

    scan_events = build_scan_events(
        window.closed_at,
        snapshot.markets,
        {},
        {},
        [],
        [],
        data_quality_issues=issues,
        raw_tickers_by_market=snapshot.raw_tickers_by_market,
    )
    conflicting_event_ids = await append_scan_events(
        scan_events,
        lifecycle.gcs_client,
    )
    lifecycle.mark_persisted()
    notification_issues = (
        [_immutable_event_conflict_issue(conflicting_event_ids)]
        if conflicting_event_ids
        else issues
    )
    await _dispatch_data_quality_with_claim(
        lifecycle,
        notification_issues,
        window.closed_at,
    )
    await lifecycle.complete()
    _raise_pending_delivery_error(pending_delivery_error)


async def _assess_candidate_execution(
    session: aiohttp.ClientSession,
    candidate_markets: list[str],
    tickers: dict[str, TickerData],
    raw_tickers_by_market: dict[str, MarketTicker],
) -> dict[str, ExecutionDecision]:
    if not candidate_markets:
        return {}

    orderbooks = await get_orderbooks(session, candidate_markets)
    decisions: dict[str, ExecutionDecision] = {}
    for market in candidate_markets:
        decision = assess_execution(
            tickers[market],
            raw_tickers_by_market.get(market),
            orderbooks.get(market),
        )
        decisions[market] = decision
        tickers[market].execution_spread_bps = decision.spread_bps
        tickers[market].expected_slippage_bps = decision.expected_slippage_bps
        if not decision.executable:
            logger.info(
                "%s remains in the attention filter with execution risk: %s",
                market,
                ", ".join(reason.value for reason in decision.rejection_reasons),
            )
    return decisions


async def _analyze_broad_scan(
    session: aiohttp.ClientSession,
    snapshot: _MarketSnapshot,
    markets_by_sector: dict[str, list[str]],
    sectors_by_market: dict[str, list[str]],
    gcs_client=None,
) -> _BroadScanAnalysis:
    pending_events = await load_pending_scan_events(gcs_client)
    resolved_outcomes, unresolved_events = resolve_scan_outcomes(
        pending_events,
        snapshot.candles_10m,
    )
    current_rankings = rank_markets_by_24h_turnover(snapshot.raw_tickers)
    lightweight_tickers = build_lightweight_tickers(snapshot.candles_10m)
    assign_residual_momentum(lightweight_tickers, markets_by_sector, sectors_by_market)
    candidate_decisions = evaluate_candidate_eligibility(lightweight_tickers)
    candidate_markets = [
        market for market, decision in candidate_decisions.items() if decision.eligible
    ]
    execution_decisions = await _assess_candidate_execution(
        session,
        candidate_markets,
        lightweight_tickers,
        snapshot.raw_tickers_by_market,
    )

    if candidate_markets:
        logger.info(
            "Selected %d broad-scan candidates: %s",
            len(candidate_markets),
            candidate_markets,
        )
    else:
        logger.info(
            "No candidates passed the broad scan; skipping deep-dive collection."
        )

    return _BroadScanAnalysis(
        resolved_outcomes=resolved_outcomes,
        pending_events=unresolved_events,
        current_rankings=current_rankings,
        lightweight_tickers=lightweight_tickers,
        candidate_decisions=candidate_decisions,
        candidate_markets=candidate_markets,
        execution_decisions=execution_decisions,
    )


def _retained_context_markets(
    previous_attention_state: AttentionState,
    tickers: dict[str, TickerData],
    scan_closed_at: datetime.datetime,
) -> list[str]:
    retention_window = datetime.timedelta(
        minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES * 1.5
    )
    return [
        market
        for market, entry in previous_attention_state.entries.items()
        if market in tickers
        and entry.last_seen_at <= scan_closed_at
        and scan_closed_at - entry.last_seen_at <= retention_window
    ]


async def _collect_context_analysis(
    session: aiohttp.ClientSession,
    broad_scan: _BroadScanAnalysis,
    previous_attention_state: AttentionState,
    window: _ScanWindow,
) -> _ContextAnalysis:
    enriched_tickers = broad_scan.lightweight_tickers.copy()
    unknown_regime = MarketRegimeSnapshot(regime=MarketRegime.UNKNOWN)
    retained_markets = _retained_context_markets(
        previous_attention_state,
        broad_scan.lightweight_tickers,
        window.closed_at,
    )
    if not broad_scan.candidate_markets and not retained_markets:
        return _ContextAnalysis(
            enriched_tickers=enriched_tickers,
            market_regime=unknown_regime,
            evidence_markets=[],
        )

    markets_to_fetch = required_deep_dive_markets(
        [*broad_scan.candidate_markets, *retained_markets]
    )
    candles_60m, candles_daily = await asyncio.gather(
        get_candles(
            session,
            markets_to_fetch,
            time_unit=CandleTimeUnit.MINUTES,
            minutes_unit=60,
            count=200,
            as_of=window.started_at,
        ),
        get_candles(
            session,
            markets_to_fetch,
            time_unit=CandleTimeUnit.DAYS,
            count=200,
            as_of=window.started_at,
        ),
    )
    evidence_markets = sorted(
        market
        for market in markets_to_fetch
        if len(candles_60m.get(market, [])) >= config.ATTENTION_CONTEXT_MIN_HOURLY_BARS
        and len(candles_daily.get(market, []))
        >= config.ATTENTION_CONTEXT_MIN_DAILY_BARS
    )
    complete_candidates = filter_markets_with_complete_deep_dive_data(
        broad_scan.candidate_markets,
        candles_60m,
        candles_daily,
    )
    if broad_scan.candidate_markets and not complete_candidates:
        logger.warning(
            "No candidates have complete higher-timeframe evidence; broad-filter "
            "candidates remain recorded as data-limited."
        )

    deep_dive_tickers = enrich_tickers_with_higher_timeframes(
        {
            market: ticker
            for market, ticker in broad_scan.lightweight_tickers.items()
            if market in markets_to_fetch
        },
        candles_60m,
        candles_daily,
    )
    enriched_tickers.update(deep_dive_tickers)
    market_regime = classify_market_regime(enriched_tickers)
    logger.info("Current market regime: %s", market_regime.regime.value)
    if market_regime.regime is MarketRegime.UNKNOWN:
        logger.warning(
            "BTC market regime is unknown; candidates retain unavailable context evidence."
        )
    return _ContextAnalysis(
        enriched_tickers=enriched_tickers,
        market_regime=market_regime,
        evidence_markets=evidence_markets,
    )


def _attention_coverage(
    snapshot: _MarketSnapshot,
    broad_scan: _BroadScanAnalysis,
    candidates: list[AttentionCandidate],
) -> dict[str, int | float]:
    raw_coverage_ratio = (
        len(snapshot.candles_10m) / len(snapshot.markets) if snapshot.markets else 0.0
    )
    active_markets = set(broad_scan.candidate_markets)
    context_count = sum(
        candidate.context_available
        for candidate in candidates
        if candidate.market in active_markets
    )
    context_coverage_ratio = (
        context_count / len(broad_scan.candidate_markets)
        if broad_scan.candidate_markets
        else 1.0
    )
    return {
        "raw_market_count": len(snapshot.candles_10m),
        "raw_market_total": len(snapshot.markets),
        "raw_market_coverage_ratio": raw_coverage_ratio,
        "eligible_candidate_count": len(broad_scan.candidate_markets),
        "eligible_context_count": context_count,
        "eligible_context_coverage_ratio": context_coverage_ratio,
    }


async def _build_attention_analysis(
    snapshot: _MarketSnapshot,
    broad_scan: _BroadScanAnalysis,
    context: _ContextAnalysis,
    previous_rankings: dict[str, int],
    previous_attention_state: AttentionState,
    markets_by_sector: dict[str, list[str]],
    sectors_by_market: dict[str, list[str]],
    window: _ScanWindow,
    gcs_client=None,
) -> _AttentionAnalysis:
    alert_history = await load_alert_history(gcs_client)
    signal_candidates: list[SignalCandidate] = []
    alerts: list[Alert] = []
    if broad_scan.candidate_markets:
        signal_candidates = detect_anomalies(
            broad_scan.candidate_markets,
            context.enriched_tickers,
            markets_by_sector,
            sectors_by_market,
        )
        signal_candidates = filter_market_wide_candidates(
            signal_candidates,
            context.enriched_tickers,
        )
        if signal_candidates:
            alerts = AlertEngine().select_alerts(
                signal_candidates,
                context.enriched_tickers,
                alert_history,
            )
            logger.info("Generated %d final alerts.", len(alerts))

    attention_result = build_attention_result(
        window.closed_at,
        broad_scan.candidate_markets,
        context.enriched_tickers,
        broad_scan.current_rankings,
        previous_rankings,
        signal_candidates,
        alerts,
        previous_state=previous_attention_state,
        execution_decisions=broad_scan.execution_decisions,
        market_regime=context.market_regime,
    )
    coverage = _attention_coverage(
        snapshot,
        broad_scan,
        attention_result.all_candidates,
    )
    logger.info(
        "Attention queue: %d visible / %d survivors / %d material changes.",
        len(attention_result.visible),
        len(attention_result.all_candidates),
        sum(candidate.material_change for candidate in attention_result.all_candidates),
    )
    logger.info(
        "Attention coverage: raw %d/%d (%.1f%%), context %d/%d (%.1f%%).",
        coverage["raw_market_count"],
        coverage["raw_market_total"],
        coverage["raw_market_coverage_ratio"] * 100,
        coverage["eligible_context_count"],
        coverage["eligible_candidate_count"],
        coverage["eligible_context_coverage_ratio"] * 100,
    )

    digest_due = attention_briefing_due(window.closed_at)
    notification_queue = attention_result.all_candidates if digest_due else []
    if attention_result.all_candidates and not digest_due:
        logger.info(
            "Persisting attention state until the next %d-minute briefing window.",
            config.ATTENTION_BRIEFING_INTERVAL_MINUTES,
        )
    return _AttentionAnalysis(
        signal_candidates=signal_candidates,
        alerts=alerts,
        alert_history=alert_history,
        all_candidates=attention_result.all_candidates,
        state=attention_result.state,
        coverage=coverage,
        notification_queue=notification_queue,
    )


async def _handle_scan_event_conflict(
    lifecycle: _ScanLifecycle,
    conflicting_event_ids: list[str],
    observed_at: datetime.datetime,
    pending_delivery_error: NotificationDeliveryError | None,
) -> None:
    lifecycle.mark_persisted()
    await _dispatch_data_quality_with_claim(
        lifecycle,
        [_immutable_event_conflict_issue(conflicting_event_ids)],
        observed_at,
    )
    await lifecycle.complete()
    _raise_pending_delivery_error(pending_delivery_error)


async def _dispatch_market_notification(
    lifecycle: _ScanLifecycle,
    snapshot: _MarketSnapshot,
    broad_scan: _BroadScanAnalysis,
    context: _ContextAnalysis,
    attention: _AttentionAnalysis,
    previous_rankings: dict[str, int],
    markets_by_sector: dict[str, list[str]],
    sectors_by_market: dict[str, list[str]],
    observed_at: datetime.datetime,
) -> None:
    try:
        await format_and_dispatch_notification(
            raw_tickers=snapshot.raw_tickers,
            enriched_tickers=context.enriched_tickers,
            current_rankings=broad_scan.current_rankings,
            previous_rankings=previous_rankings,
            markets_by_sector=markets_by_sector,
            sectors_by_market=sectors_by_market,
            final_alerts=attention.alerts,
            alert_history=attention.alert_history,
            market_regime=context.market_regime,
            observed_at=observed_at,
            attention_queue=attention.notification_queue,
            suppress_unchanged_briefing=True,
            gcs_client=lifecycle.gcs_client,
            scan_key=lifecycle.scan_key,
        )
    except NotificationDeliveryError as error:
        await lifecycle.settle_notification_error(error)
        raise
    except Exception:
        await lifecycle.release()
        raise


async def _persist_successful_scan(
    lifecycle: _ScanLifecycle,
    snapshot: _MarketSnapshot,
    broad_scan: _BroadScanAnalysis,
    context: _ContextAnalysis,
    attention: _AttentionAnalysis,
    previous_rankings: dict[str, int],
    previous_rank_states: list[RankState],
    markets_by_sector: dict[str, list[str]],
    sectors_by_market: dict[str, list[str]],
    window: _ScanWindow,
    pending_delivery_error: NotificationDeliveryError | None,
) -> None:
    if not await lifecycle.acquire():
        logger.warning("Skipping duplicate scheduled scan for %s", window.scan_key)
        _raise_pending_delivery_error(pending_delivery_error)
        return

    scan_events = build_scan_events(
        window.closed_at,
        snapshot.markets,
        broad_scan.lightweight_tickers,
        broad_scan.candidate_decisions,
        context.evidence_markets,
        attention.alerts,
        attention.signal_candidates,
        attention_candidates=attention.all_candidates,
        attention_coverage=attention.coverage,
        execution_rejections_by_market={
            market: decision.rejection_reasons
            for market, decision in broad_scan.execution_decisions.items()
            if decision.rejection_reasons
        },
        raw_tickers_by_market=snapshot.raw_tickers_by_market,
    )
    conflicting_event_ids = await append_scan_events(
        scan_events,
        lifecycle.gcs_client,
    )
    if conflicting_event_ids:
        await _handle_scan_event_conflict(
            lifecycle,
            conflicting_event_ids,
            window.closed_at,
            pending_delivery_error,
        )
        return

    if broad_scan.resolved_outcomes:
        await append_scan_outcomes(
            broad_scan.resolved_outcomes,
            lifecycle.gcs_client,
        )
    await save_pending_scan_events(
        broad_scan.pending_events + [event for event in scan_events if event.direction],
        lifecycle.gcs_client,
    )
    await save_rank_state_history(
        RankState(
            last_updated=window.closed_at,
            rankings=broad_scan.current_rankings,
        ),
        previous_rank_states,
        gcs_client=lifecycle.gcs_client,
    )
    await save_attention_state(
        attention.state,
        gcs_client=lifecycle.gcs_client,
    )
    lifecycle.mark_persisted()

    await _dispatch_market_notification(
        lifecycle,
        snapshot,
        broad_scan,
        context,
        attention,
        previous_rankings,
        markets_by_sector,
        sectors_by_market,
        window.closed_at,
    )
    await lifecycle.complete()
    _raise_pending_delivery_error(pending_delivery_error)


async def run_check(
    execution_id: str | None = None,
    schedule_time: str | None = None,
) -> None:
    """Run one completed-candle scan and durably hand off its notification."""
    gcs_client = _create_state_client()
    lifecycle: _ScanLifecycle | None = None
    try:
        pending_delivery_error = await _recover_pending_delivery_error(gcs_client)
        window = _build_scan_window(schedule_time)
        lifecycle = _ScanLifecycle(
            scan_key=window.scan_key,
            execution_id=execution_id,
            gcs_client=gcs_client,
        )
        previous_rank_states = await load_rank_state_history(gcs_client)
        previous_attention_state = await load_attention_state(gcs_client)
        if await _complete_historical_retry_if_needed(
            previous_attention_state,
            window,
            lifecycle,
            pending_delivery_error,
        ):
            return

        previous_rankings = _previous_rankings(
            previous_rank_states,
            window.closed_at,
        )
        markets_by_sector, sectors_by_market = await load_sector_maps(gcs_client)
        async with aiohttp.ClientSession() as session:
            snapshot = await _collect_market_snapshot(session, window.started_at)
            data_quality_issues = assess_scan_data_quality(
                snapshot.markets,
                snapshot.candles_10m,
                config.CANDLE_SUCCESS_RATE_MINIMUM,
            )
            if data_quality_issues:
                await _record_data_quality_failure(
                    snapshot,
                    data_quality_issues,
                    window,
                    lifecycle,
                    pending_delivery_error,
                )
                return

            broad_scan = await _analyze_broad_scan(
                session,
                snapshot,
                markets_by_sector,
                sectors_by_market,
                gcs_client,
            )
            context = await _collect_context_analysis(
                session,
                broad_scan,
                previous_attention_state,
                window,
            )
            attention = await _build_attention_analysis(
                snapshot,
                broad_scan,
                context,
                previous_rankings,
                previous_attention_state,
                markets_by_sector,
                sectors_by_market,
                window,
                gcs_client,
            )
            await _persist_successful_scan(
                lifecycle,
                snapshot,
                broad_scan,
                context,
                attention,
                previous_rankings,
                previous_rank_states,
                markets_by_sector,
                sectors_by_market,
                window,
                pending_delivery_error,
            )
    except Exception as error:
        if lifecycle is not None:
            await lifecycle.release_if_unpersisted()
        logger.critical(
            "Core scan pipeline failed: %s",
            error,
            exc_info=True,
        )
        raise PipelineError(PipelineErrorCode.EXECUTION_FAILED) from error


@functions_framework.http
def main(request):
    """Run one scheduled scan from a Google Cloud Functions HTTP request."""
    logger.info("Starting scheduled Upbit market scan.")
    try:
        headers = getattr(request, "headers", {}) if request is not None else {}
        schedule_time = headers.get("X-CloudScheduler-ScheduleTime")
        execution_id = schedule_time or headers.get("X-CloudScheduler-Execution-ID")
        run_kwargs = {"execution_id": execution_id}
        if schedule_time:
            run_kwargs["schedule_time"] = schedule_time
        asyncio.run(run_check(**run_kwargs))
    except Exception as error:
        logger.critical("Cloud Function invocation failed: %s", error, exc_info=True)
        return ("Internal Server Error", 500)

    logger.info("Scheduled Upbit market scan completed.")
    return ("OK", 200)


if __name__ == "__main__":
    logger.info("Starting local Upbit market scan.")
    asyncio.run(run_check())
    logger.info("Local Upbit market scan completed.")

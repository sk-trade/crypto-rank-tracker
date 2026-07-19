#main

import asyncio
import datetime
import logging
from enum import StrEnum

import aiohttp
from common.attention import attention_briefing_due, build_attention_queue
from common.notification.engine import AlertEngine
from common.signals.detector import detect_anomalies, filter_market_wide_events
import functions_framework

import config
from common.analysis.utils import calculate_rankings
from common.analysis.scanner import (
    evaluate_candidate_eligibility,
    process_lightweight_indicators,
)
from common.execution import assess_execution
from common.residuals import assign_residual_momentum
from common.analysis.deep_dive import ( 
    enrich_deep_dive_tickers,
    get_market_regime,
)
from common.models import (
    DataQualityIssue,
    MarketRegime,
    MarketRegimeSnapshot,
    RankState,
    RejectionCode,
    ScanHandoffState,
)
from common.event_log import build_scan_events, resolve_scan_outcomes
from common.notification.main import (
    NotificationDeliveryError,
    create_and_dispatch_notification,
    dispatch_data_quality_alert,
    recover_pending_notification,
)
from common.sector_loader import load_and_process_sectors
from common.storage_client import (
    StateBackendUnavailable,
    StateErrorCode,
    create_gcs_client,
)
from common.state_manager import (
    append_scan_events,
    append_scan_outcomes,
    claim_scan_key,
    complete_scan_key,
    release_scan_key,
    load_alert_history,
    load_attention_state,
    load_pending_scan_events,
    load_rank_state_history,
    save_pending_scan_events,
    save_attention_state,
    save_rank_state_history,
)
from common.upbit_client import (
    CandleTimeUnit,
    get_all_krw_tickers,
    get_candles,
    get_orderbooks,
)

# --- 로거 설정 ---
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
        if market in candles_60m and market in candles_daily
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
        return datetime.datetime.now(datetime.timezone.utc)
    normalized = (
        f"{schedule_time[:-1]}+00:00"
        if schedule_time.endswith("Z")
        else schedule_time
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
    return parsed.astimezone(datetime.timezone.utc)


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


async def run_check(
    execution_id: str | None = None, schedule_time: str | None = None
):
    """데이터 수집, 분석, 알림 전송의 핵심 파이프라인을 실행합니다."""
    storage_method = config.validate_storage_config()

    gcs_client = None
    if storage_method is config.StorageMethod.GCS:
        try:
            gcs_client = create_gcs_client()
            logger.info("GCS 저장 모드로 실행됩니다.")
        except ImportError as e:
            raise StateBackendUnavailable(
                StateErrorCode.BACKEND_UNAVAILABLE,
                config.GCS_BUCKET_NAME or "GCS",
            ) from e
    else:
        logger.info("로컬 파일 저장 모드로 실행됩니다.")

    claimed_scan_key = None
    scan_persisted = False
    try:
        pending_delivery_error = None
        try:
            recovered_delivery = await recover_pending_notification(gcs_client)
            if recovered_delivery is not None:
                logger.info("Recovered pending webhook delivery; continuing the market scan.")
        except NotificationDeliveryError as error:
            pending_delivery_error = error
            logger.error(
                "Pending webhook recovery failed; market-state collection will continue: %s",
                error,
            )
        async with aiohttp.ClientSession() as session:
            # 공통 준비 단계
            scan_started_at = _scheduled_scan_time(schedule_time)
            scan_close_at = scan_started_at.replace(second=0, microsecond=0) - datetime.timedelta(
                minutes=scan_started_at.minute % config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
            )
            scan_key = f"completed-candle:{scan_close_at.isoformat()}"
            old_rank_states = await load_rank_state_history(gcs_client)
            previous_attention_state = await load_attention_state(gcs_client)
            if (
                previous_attention_state.updated_at is not None
                and previous_attention_state.updated_at > scan_close_at
            ):
                logger.warning(
                    "Skipping historical retry %s because newer attention state exists at %s; "
                    "current-only ticker, orderbook, and alert evidence cannot be backfilled safely.",
                    scan_close_at.isoformat(),
                    previous_attention_state.updated_at.isoformat(),
                )
                if not await claim_scan_key(
                    scan_key, execution_id=execution_id, gcs_client=gcs_client
                ):
                    logger.warning("Skipping duplicate scheduled scan for %s", scan_key)
                    if pending_delivery_error:
                        raise pending_delivery_error
                    return
                claimed_scan_key = scan_key
                await complete_scan_key(scan_key, gcs_client)
                scan_persisted = True
                claimed_scan_key = None
                if pending_delivery_error:
                    raise pending_delivery_error
                return
            previous_rankings = next(
                (
                    state.rankings
                    for state in reversed(old_rank_states)
                    if state.last_updated < scan_close_at
                ),
                {},
            )
            sectors, reverse_sector_map = await load_and_process_sectors(gcs_client)

            # PHASE 1: 광역 스캔
            raw_tickers = await get_all_krw_tickers(session)
            all_markets = [ticker.market for ticker in raw_tickers]
            raw_tickers_map = {ticker.market: ticker for ticker in raw_tickers}
            candles_10m = await get_candles(
                session,
                all_markets,
                time_unit=CandleTimeUnit.MINUTES,
                minutes_unit=10,
                count=config.RECENT_SCAN_HISTORY_BARS,
                as_of=scan_started_at,
                synthesize_no_trade_intervals=True,
                same_slot_lookback_weeks=config.CONDITIONAL_VOLUME_LOOKBACK_WEEKS,
            )
            data_quality_issues = assess_scan_data_quality(
                all_markets, candles_10m, config.CANDLE_SUCCESS_RATE_MINIMUM
            )
            if data_quality_issues:
                logger.error(
                    "Skipping scan due to data quality: %s",
                    "; ".join(issue.message for issue in data_quality_issues),
                )
                if not await claim_scan_key(
                    scan_key, execution_id=execution_id, gcs_client=gcs_client
                ):
                    logger.warning("Skipping duplicate scheduled scan for %s", scan_key)
                    if pending_delivery_error:
                        raise pending_delivery_error
                    return
                claimed_scan_key = scan_key
                conflicting_event_ids = await append_scan_events(
                    build_scan_events(
                        scan_close_at,
                        all_markets,
                        {},
                        {},
                        [],
                        [],
                        data_quality_issues=data_quality_issues,
                        raw_tickers_by_market=raw_tickers_map,
                    ),
                    gcs_client,
                )
                scan_persisted = True
                notification_issues = data_quality_issues
                if conflicting_event_ids:
                    notification_issues = [
                        DataQualityIssue(
                            code=RejectionCode.IMMUTABLE_SCAN_EVENT_CONFLICT,
                            message=(
                                "A retry produced evidence that conflicts with the first "
                                "persisted scan; the original scan remains authoritative."
                            ),
                            details={
                                "conflicting_event_count": len(conflicting_event_ids),
                                "conflicting_event_ids": conflicting_event_ids[:10],
                            },
                        )
                    ]
                try:
                    await dispatch_data_quality_alert(
                        notification_issues,
                        gcs_client=gcs_client,
                        scan_key=scan_key,
                    )
                except NotificationDeliveryError as error:
                    await _settle_notification_scan_claim(error, scan_key, gcs_client)
                    claimed_scan_key = None
                    raise
                except Exception:
                    await release_scan_key(scan_key, gcs_client=gcs_client)
                    claimed_scan_key = None
                    raise
                await complete_scan_key(scan_key, gcs_client)
                if pending_delivery_error:
                    raise pending_delivery_error
                return

            pending_events = await load_pending_scan_events(gcs_client)
            resolved_outcomes, pending_events = resolve_scan_outcomes(pending_events, candles_10m)
            current_rankings = calculate_rankings(raw_tickers)
            lightweight_tickers = process_lightweight_indicators(candles_10m)
            assign_residual_momentum(lightweight_tickers, sectors, reverse_sector_map)
            candidate_decisions = evaluate_candidate_eligibility(lightweight_tickers)
            candidate_markets = [
                market for market, decision in candidate_decisions.items() if decision.eligible
            ]
            execution_decisions = {}
            if candidate_markets:
                orderbooks = await get_orderbooks(session, candidate_markets)
                for market in candidate_markets:
                    execution = assess_execution(
                        lightweight_tickers[market], raw_tickers_map.get(market), orderbooks.get(market)
                    )
                    execution_decisions[market] = execution
                    lightweight_tickers[market].execution_spread_bps = execution.spread_bps
                    lightweight_tickers[market].expected_slippage_bps = execution.expected_slippage_bps
                    if not execution.executable:
                        logger.info(
                            "%s remains in the attention filter with execution risk: %s",
                            market,
                            ", ".join(reason.value for reason in execution.rejection_reasons),
                        )
            
            if not candidate_markets:
                logger.info("심층 분석 대상이 없습니다. Phase 2를 건너뜁니다.")
            else:
                logger.info(f"{len(candidate_markets)}개의 후보군 선정: {candidate_markets}")

            # PHASE 2: 심층 분석
            enriched_tickers = lightweight_tickers.copy()
            market_regime = MarketRegimeSnapshot(regime=MarketRegime.UNKNOWN)
            deep_dive_candidate_markets = []
            deep_dive_evidence_markets = []
            attention_interval = datetime.timedelta(
                minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES * 1.5
            )
            retained_context_markets = [
                market
                for market, entry in previous_attention_state.entries.items()
                if market in lightweight_tickers
                and entry.last_seen_at <= scan_close_at
                and scan_close_at - entry.last_seen_at <= attention_interval
            ]

            if candidate_markets or retained_context_markets:
                logger.info("Phase 2: 후보군 심층 분석 시작")
                markets_to_fetch = required_deep_dive_markets(
                    [*candidate_markets, *retained_context_markets]
                )
                
                tasks = [
                    get_candles(
                        session,
                        markets_to_fetch,
                        time_unit=CandleTimeUnit.MINUTES,
                        minutes_unit=60,
                        count=200,
                        as_of=scan_started_at,
                    ),
                    get_candles(
                        session,
                        markets_to_fetch,
                        time_unit=CandleTimeUnit.DAYS,
                        count=200,
                        as_of=scan_started_at,
                    ),
                ]
                results = await asyncio.gather(*tasks)
                candles_60m, candles_daily = results[0], results[1]
                deep_dive_evidence_markets = sorted(
                    set(candles_60m) & set(candles_daily)
                )
                deep_dive_candidate_markets = filter_markets_with_complete_deep_dive_data(
                    candidate_markets, candles_60m, candles_daily
                )
                if candidate_markets and not deep_dive_candidate_markets:
                    logger.warning(
                        "No candidates have complete higher-timeframe evidence; "
                        "broad-filter candidates remain visible in the attention queue."
                    )

                deep_dive_enriched = enrich_deep_dive_tickers(
                    {m: t for m, t in lightweight_tickers.items() if m in markets_to_fetch},
                    candles_60m,
                    candles_daily,
                    lightweight_tickers,
                )
                enriched_tickers.update(deep_dive_enriched)

                market_regime = get_market_regime(enriched_tickers)
                logger.info("현재 시장 체제: %s", market_regime.regime.value)
                if market_regime.regime is MarketRegime.UNKNOWN:
                    logger.warning(
                        "BTC market regime is unknown; retaining candidates with unavailable context evidence."
                    )

            # PHASE 3: 알림 생성
            final_alerts = []
            candidates_list = []
            alert_history = await load_alert_history(gcs_client)
            if candidate_markets: 
                candidates_list = detect_anomalies(
                    candidate_markets,
                    enriched_tickers,
                    sectors, 
                    reverse_sector_map
                )
                
                candidates_list = filter_market_wide_events(candidates_list, enriched_tickers)

                if candidates_list:
                    alert_engine = AlertEngine()
                    final_alerts = alert_engine.process_signals(candidates_list, enriched_tickers, alert_history)
                    
                    logger.info(f"최종 알림 생성: {len(final_alerts)}건")

            attention_queue, attention_state = build_attention_queue(
                scan_close_at,
                candidate_markets,
                enriched_tickers,
                current_rankings,
                previous_rankings,
                candidates_list,
                final_alerts,
                previous_state=previous_attention_state,
                execution_decisions=execution_decisions,
                market_regime=market_regime,
            )
            logger.info(
                "관심종목 큐 생성: %d건 (%d건 material change)",
                len(attention_queue),
                sum(candidate.material_change for candidate in attention_queue),
            )
            attention_digest_due = attention_briefing_due(scan_close_at)
            notification_attention_queue = (
                attention_queue if attention_digest_due else []
            )
            if attention_queue and not attention_digest_due:
                logger.info(
                    "관심종목 상태는 저장하고 다음 %d분 브리핑까지 webhook을 보류합니다.",
                    config.ATTENTION_BRIEFING_INTERVAL_MINUTES,
                )

            # 알림 발송 및 상태 저장
            if not await claim_scan_key(
                scan_key, execution_id=execution_id, gcs_client=gcs_client
            ):
                logger.warning("Skipping duplicate scheduled scan for %s", scan_key)
                if pending_delivery_error:
                    raise pending_delivery_error
                return
            claimed_scan_key = scan_key

            scan_events = build_scan_events(
                scan_close_at,
                all_markets,
                lightweight_tickers,
                candidate_decisions,
                deep_dive_evidence_markets,
                final_alerts,
                candidates_list,
                attention_candidates=attention_queue,
                execution_rejections_by_market={
                    market: decision.rejection_reasons
                    for market, decision in execution_decisions.items()
                    if decision.rejection_reasons
                },
                raw_tickers_by_market=raw_tickers_map,
            )
            conflicting_event_ids = await append_scan_events(scan_events, gcs_client)
            if conflicting_event_ids:
                scan_persisted = True
                conflict_issue = DataQualityIssue(
                    code=RejectionCode.IMMUTABLE_SCAN_EVENT_CONFLICT,
                    message=(
                        "A retry produced evidence that conflicts with the first persisted "
                        "scan; the original scan remains authoritative."
                    ),
                    details={
                        "conflicting_event_count": len(conflicting_event_ids),
                        "conflicting_event_ids": conflicting_event_ids[:10],
                    },
                )
                try:
                    await dispatch_data_quality_alert(
                        [conflict_issue],
                        gcs_client=gcs_client,
                        scan_key=scan_key,
                    )
                except NotificationDeliveryError as error:
                    await _settle_notification_scan_claim(error, scan_key, gcs_client)
                    claimed_scan_key = None
                    raise
                except Exception:
                    await release_scan_key(scan_key, gcs_client=gcs_client)
                    claimed_scan_key = None
                    raise
                await complete_scan_key(scan_key, gcs_client)
                if pending_delivery_error:
                    raise pending_delivery_error
                return
            if resolved_outcomes:
                await append_scan_outcomes(resolved_outcomes, gcs_client)
            await save_pending_scan_events(
                pending_events + [event for event in scan_events if event.direction], gcs_client
            )

            new_rank_state = RankState(last_updated=scan_close_at, rankings=current_rankings)
            await save_rank_state_history(new_rank_state, old_rank_states, gcs_client=gcs_client)
            await save_attention_state(attention_state, gcs_client=gcs_client)
            scan_persisted = True

            try:
                await create_and_dispatch_notification(
                    raw_tickers=raw_tickers, enriched_tickers=enriched_tickers, current_rankings=current_rankings,
                    previous_rankings=previous_rankings, SECTORS=sectors, REVERSE_SECTOR_MAP=reverse_sector_map,
                    final_alerts=final_alerts, alert_history=alert_history, market_regime=market_regime,
                    attention_queue=notification_attention_queue,
                    suppress_unchanged_briefing=True,
                    gcs_client=gcs_client, scan_key=scan_key,
                )
            except NotificationDeliveryError as error:
                await _settle_notification_scan_claim(error, scan_key, gcs_client)
                claimed_scan_key = None
                raise
            except Exception:
                await release_scan_key(scan_key, gcs_client=gcs_client)
                claimed_scan_key = None
                raise
            await complete_scan_key(scan_key, gcs_client)
            if pending_delivery_error:
                raise pending_delivery_error

            # # (주석 처리) 현재 분석 결과 로그로 저장
            # tickers_to_save = {
            #     market: TickerData.model_validate(ticker.model_dump())
            #     for market, ticker in enriched_tickers.items()
            # }
            # new_analysis_state = AnalysisState(
            #     last_updated=datetime.datetime.now(datetime.timezone.utc),
            #     tickers=tickers_to_save,
            #     rankings=current_rankings
            # )
            # await save_analysis_log(new_analysis_state, gcs_client=gcs_client)

    except Exception as e:
        if claimed_scan_key and not scan_persisted:
            try:
                await release_scan_key(claimed_scan_key, gcs_client=gcs_client)
            except Exception:
                logger.exception("Failed to release scan claim %s", claimed_scan_key)
        logger.critical(f"핵심 파이프라인 실행 중 예외 발생: {e}", exc_info=True)
        raise PipelineError(PipelineErrorCode.EXECUTION_FAILED) from e


@functions_framework.http
def main(request):
    """Google Cloud Function의 HTTP 트리거 진입점입니다.

    Args:
        request: Cloud Function에서 전달하는 HTTP 요청 객체 (사용되지 않음).

    Returns:
        A tuple containing a response message and an HTTP status code.
    """
    logger.info("업비트 순위 확인 작업 시작 (Cloud Function).")
    try:
        headers = getattr(request, "headers", {}) if request is not None else {}
        schedule_time = headers.get("X-CloudScheduler-ScheduleTime")
        execution_id = schedule_time or headers.get("X-CloudScheduler-Execution-ID")
        run_kwargs = {"execution_id": execution_id}
        if schedule_time:
            run_kwargs["schedule_time"] = schedule_time
        asyncio.run(run_check(**run_kwargs))
    except Exception as e:
        logger.critical(f"작업 실행 중 심각한 오류 발생: {e}", exc_info=True)
        return ("Internal Server Error", 500)

    logger.info("업비트 순위 확인 작업 완료 (Cloud Function).")
    return ("OK", 200)


if __name__ == "__main__":
    logger.info("로컬 환경에서 테스트 실행을 시작합니다.")
    asyncio.run(run_check())
    logger.info("로컬 테스트 실행을 종료합니다.")

#main

import asyncio
import datetime
import logging

import aiohttp
from common.notification.engine import AlertEngine
from common.signals.detector import detect_anomalies, filter_market_wide_events
import functions_framework

import config
from common.analysis.utils import calculate_rankings
from common.analysis.scanner import (
    CandidateDecision,
    evaluate_candidate_eligibility,
    process_lightweight_indicators,
)
from common.execution import assess_execution
from common.residuals import assign_residual_momentum
from common.analysis.deep_dive import ( 
    enrich_deep_dive_tickers,
    get_market_regime,
)
from common.models import Alert, RankState, SignalCandidate
from common.event_log import build_scan_events, resolve_scan_outcomes
from common.notification.main import create_and_dispatch_notification, dispatch_data_quality_alert
from common.sector_loader import load_and_process_sectors
from common.state_manager import (
    append_scan_events,
    append_scan_outcomes,
    claim_scan_key,
    load_alert_history,
    load_pending_scan_events,
    load_rank_state_history,
    save_pending_scan_events,
    save_rank_state_history,
)
from common.upbit_client import (
    UpbitAPIError,
    get_all_krw_tickers,
    get_candles,
    get_orderbooks,
)

# --- 로거 설정 ---
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(config.APP_LOGGER_NAME)


def filter_markets_with_complete_deep_dive_data(
    candidate_markets: list[str],
    candles_60m: dict,
    candles_daily: dict,
) -> list[str]:
    """Block candidates that lack any required higher-timeframe history."""
    valid_markets = [
        market
        for market in candidate_markets
        if market in candles_60m and market in candles_daily
    ]
    blocked_markets = sorted(set(candidate_markets) - set(valid_markets))
    if blocked_markets:
        logger.warning(
            "Blocking %d candidate(s) because 60-minute or daily candle coverage failed: %s",
            len(blocked_markets),
            ", ".join(blocked_markets[:10]),
        )
    return valid_markets


def assess_scan_data_quality(
    all_markets: list[str], candles_10m: dict, minimum_success_rate: float
) -> list[str]:
    """Return operationally actionable reasons why a scan cannot produce signals."""
    if not all_markets:
        return ["KRW market universe is empty."]

    successful_markets = len(candles_10m)
    success_rate = successful_markets / len(all_markets)
    issues = []
    if success_rate < minimum_success_rate:
        issues.append(
            f"10-minute candle coverage {successful_markets}/{len(all_markets)} ({success_rate:.1%}) is below the {minimum_success_rate:.0%} minimum."
        )
    if "KRW-BTC" not in candles_10m:
        issues.append("KRW-BTC completed 10-minute candles are missing or stale.")
    return issues


def filter_candidates_by_market_regime(candidate_markets: list[str], market_regime: dict) -> list[str]:
    """Fail closed when BTC data cannot establish a valid market regime."""
    if market_regime.get("regime") == "UNKNOWN":
        logger.warning("Blocking candidates because BTC market regime is UNKNOWN.")
        return []
    return candidate_markets


def record_market_regime_block(
    candidate_decisions: dict[str, CandidateDecision], market_regime: dict
) -> list[str]:
    """Persist a fail-closed regime gate as a distinct event-log decision."""
    candidate_markets = [market for market, decision in candidate_decisions.items() if decision.eligible]
    if filter_candidates_by_market_regime(candidate_markets, market_regime):
        return candidate_markets
    for market in candidate_markets:
        decision = candidate_decisions[market]
        candidate_decisions[market] = CandidateDecision(
            False, [*decision.rejection_reasons, "market_regime_unknown"]
        )
    return []


async def run_check(execution_id: str | None = None):
    """데이터 수집, 분석, 알림 전송의 핵심 파이프라인을 실행합니다."""
    config.validate_storage_config()

    gcs_client = None
    if config.STATE_STORAGE_METHOD == "GCS":
        try:
            from google.cloud import storage
            gcs_client = storage.Client()
            logger.info("GCS 저장 모드로 실행됩니다.")
        except ImportError as e:
            raise RuntimeError(
                "GCS 모드로 설정되었으나 'google-cloud-storage' 라이브러리가 설치되지 않았습니다. "
                "pip install google-cloud-storage 명령어로 설치해주세요."
            ) from e
    else:
        logger.info("로컬 파일 저장 모드로 실행됩니다.")

    try:
        async with aiohttp.ClientSession() as session:
            # 공통 준비 단계
            scan_started_at = datetime.datetime.now(datetime.timezone.utc)
            scan_close_at = scan_started_at.replace(second=0, microsecond=0) - datetime.timedelta(
                minutes=scan_started_at.minute % config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
            )
            scan_key = f"completed-candle:{scan_close_at.isoformat()}"
            if not await claim_scan_key(scan_key, execution_id=execution_id, gcs_client=gcs_client):
                logger.warning("Skipping duplicate scheduled scan for %s", scan_key)
                return
            old_rank_states = await load_rank_state_history(gcs_client)
            previous_rankings = old_rank_states[-1].rankings if old_rank_states else {}
            sectors, reverse_sector_map = await load_and_process_sectors(gcs_client)

            # PHASE 1: 광역 스캔
            raw_tickers = await get_all_krw_tickers(session)
            all_markets = [ticker["market"] for ticker in raw_tickers]
            raw_tickers_map = {ticker["market"]: ticker for ticker in raw_tickers}
            candles_10m = await get_candles(
                session,
                all_markets,
                time_unit="minutes",
                minutes_unit=10,
                count=config.CONDITIONAL_VOLUME_HISTORY_BARS,
                as_of=scan_started_at,
            )
            data_quality_issues = assess_scan_data_quality(
                all_markets, candles_10m, config.CANDLE_SUCCESS_RATE_MINIMUM
            )
            if data_quality_issues:
                logger.error("Skipping scan due to data quality: %s", "; ".join(data_quality_issues))
                await append_scan_events(
                    build_scan_events(
                        scan_started_at,
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
                await dispatch_data_quality_alert(data_quality_issues)
                return

            pending_events = await load_pending_scan_events(gcs_client)
            resolved_outcomes, pending_events = resolve_scan_outcomes(pending_events, candles_10m)
            if resolved_outcomes:
                await append_scan_outcomes(resolved_outcomes, gcs_client)
            await save_pending_scan_events(pending_events, gcs_client)
            current_rankings = calculate_rankings(raw_tickers)
            lightweight_tickers = process_lightweight_indicators(candles_10m, raw_tickers_map)
            assign_residual_momentum(lightweight_tickers, sectors, reverse_sector_map)
            candidate_decisions = evaluate_candidate_eligibility(lightweight_tickers, current_rankings)
            candidate_markets = [
                market for market, decision in candidate_decisions.items() if decision.eligible
            ]
            if candidate_markets:
                orderbooks = await get_orderbooks(session, candidate_markets)
                for market in candidate_markets:
                    execution = assess_execution(
                        lightweight_tickers[market], raw_tickers_map.get(market), orderbooks.get(market)
                    )
                    lightweight_tickers[market].execution_spread_bps = execution.spread_bps
                    lightweight_tickers[market].expected_slippage_bps = execution.expected_slippage_bps
                    if not execution.executable:
                        candidate_decisions[market] = CandidateDecision(False, execution.rejection_reasons)
                candidate_markets = [
                    market for market, decision in candidate_decisions.items() if decision.eligible
                ]
            
            if not candidate_markets:
                logger.info("심층 분석 대상이 없습니다. Phase 2를 건너뜁니다.")
            else:
                logger.info(f"{len(candidate_markets)}개의 후보군 선정: {candidate_markets}")

            # PHASE 2: 심층 분석
            final_candidates = {}
            enriched_tickers = lightweight_tickers.copy()
            market_regime = {}

            if candidate_markets:
                logger.info("Phase 2: 후보군 심층 분석 시작")
                markets_to_fetch = list(set(candidate_markets + ["KRW-BTC", "KRW-ETH"]))
                
                tasks = [
                    get_candles(
                        session,
                        markets_to_fetch,
                        time_unit="minutes",
                        minutes_unit=60,
                        count=200,
                        as_of=scan_started_at,
                    ),
                    get_candles(
                        session,
                        markets_to_fetch,
                        time_unit="days",
                        count=200,
                        as_of=scan_started_at,
                    ),
                ]
                results = await asyncio.gather(*tasks)
                candles_60m, candles_daily = results[0], results[1]
                candidate_markets = filter_markets_with_complete_deep_dive_data(
                    candidate_markets, candles_60m, candles_daily
                )
                if not candidate_markets:
                    logger.warning("No candidates passed higher-timeframe candle integrity checks.")

                deep_dive_enriched = enrich_deep_dive_tickers(
                    {m: t for m, t in lightweight_tickers.items() if m in markets_to_fetch},
                    candles_60m,
                    candles_daily,
                    lightweight_tickers,
                )
                enriched_tickers.update(deep_dive_enriched)

                market_regime = get_market_regime(enriched_tickers)
                logger.info(f"현재 시장 체제: {market_regime.get('regime', 'UNKNOWN')}")
                candidate_markets = record_market_regime_block(candidate_decisions, market_regime)

            # PHASE 3: 알림 생성
            final_alerts = []
            candidates_list = []
            if candidate_markets: 
                detection_universe = {
                    market: enriched_tickers[market]
                    for market in candidate_markets
                    if market in enriched_tickers
                }
                candidates_list = detect_anomalies(
                    detection_universe, 
                    current_rankings,  
                    sectors, 
                    reverse_sector_map
                )
                
                candidates_list = filter_market_wide_events(candidates_list, enriched_tickers)

                if candidates_list:
                    alert_engine = AlertEngine()
                    alert_history = await load_alert_history(gcs_client)
                    final_alerts = alert_engine.process_signals(candidates_list, enriched_tickers, alert_history)
                    
                    logger.info(f"최종 알림 생성: {len(final_alerts)}건")

            # 알림 발송 및 상태 저장
            alert_history = await load_alert_history(gcs_client)

            await create_and_dispatch_notification(
                raw_tickers=raw_tickers, enriched_tickers=enriched_tickers, current_rankings=current_rankings,
                previous_rankings=previous_rankings, SECTORS=sectors, REVERSE_SECTOR_MAP=reverse_sector_map,
                final_alerts=final_alerts, alert_history=alert_history, market_regime=market_regime, gcs_client=gcs_client
            )

            scan_events = build_scan_events(
                scan_started_at,
                all_markets,
                lightweight_tickers,
                candidate_decisions,
                candidate_markets,
                final_alerts,
                candidates_list,
                raw_tickers_by_market=raw_tickers_map,
            )
            await append_scan_events(scan_events, gcs_client)
            await save_pending_scan_events(
                pending_events + [event for event in scan_events if event.direction], gcs_client
            )

            new_rank_state = RankState(last_updated=datetime.datetime.now(datetime.timezone.utc), rankings=current_rankings)
            await save_rank_state_history(new_rank_state, old_rank_states, gcs_client=gcs_client)

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
        logger.critical(f"핵심 파이프라인 실행 중 예외 발생: {e}", exc_info=True)
        raise RuntimeError("Failed to execute the main pipeline") from e


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
        execution_id = headers.get("X-CloudScheduler-Execution-ID")
        asyncio.run(run_check(execution_id=execution_id))
    except Exception as e:
        logger.critical(f"작업 실행 중 심각한 오류 발생: {e}", exc_info=True)
        return ("Internal Server Error", 500)

    logger.info("업비트 순위 확인 작업 완료 (Cloud Function).")
    return ("OK", 200)


if __name__ == "__main__":
    logger.info("로컬 환경에서 테스트 실행을 시작합니다.")
    asyncio.run(run_check())
    logger.info("로컬 테스트 실행을 종료합니다.")

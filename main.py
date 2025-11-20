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
from common.analysis.scanner import process_lightweight_indicators, select_candidates_for_deep_dive
from common.analysis.deep_dive import ( 
    enrich_deep_dive_tickers,
    get_market_regime,
    calculate_robust_confidence,
)
from common.models import Alert, RankState, SignalCandidate
from common.notification.main import create_and_dispatch_notification
from common.sector_loader import load_and_process_sectors
from common.state_manager import load_alert_history, load_rank_state_history, save_rank_state_history
from common.upbit_client import (
    UpbitAPIError,
    get_all_krw_tickers,
    get_candles,
)

# --- 로거 설정 ---
logging.basicConfig(
    level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(config.APP_LOGGER_NAME)


async def run_check():
    """데이터 수집, 분석, 알림 전송의 핵심 파이프라인을 실행합니다."""
    gcs_client = None
    if config.STATE_STORAGE_METHOD == "GCS":
        try:
            from google.cloud import storage
            gcs_client = storage.Client()
            logger.info("GCS 저장 모드로 실행됩니다.")
        except ImportError:
            logger.critical("GCS 모드로 설정되었으나 'google-cloud-storage' 라이브러리가 설치되지 않았습니다.")
            logger.critical("pip install google-cloud-storage 명령어로 설치해주세요.")
            return
    else:
        logger.info("로컬 파일 저장 모드로 실행됩니다.")

    try:
        async with aiohttp.ClientSession() as session:
            # 공통 준비 단계
            old_rank_states = await load_rank_state_history(gcs_client)
            previous_rankings = old_rank_states[-1].rankings if old_rank_states else {}
            sectors, reverse_sector_map = await load_and_process_sectors(gcs_client)

            # PHASE 1: 광역 스캔
            raw_tickers = await get_all_krw_tickers(session)
            all_markets = [ticker["market"] for ticker in raw_tickers]
            raw_tickers_map = {ticker["market"]: ticker for ticker in raw_tickers}
            candles_10m = await get_candles(session, all_markets, time_unit="minutes", minutes_unit=10, count=200)
            current_rankings = calculate_rankings(raw_tickers)
            lightweight_tickers = process_lightweight_indicators(candles_10m, raw_tickers_map)
            candidate_markets = select_candidates_for_deep_dive(lightweight_tickers, current_rankings, raw_tickers_map)
            
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
                    get_candles(session, markets_to_fetch, time_unit="minutes", minutes_unit=60, count=200),
                    get_candles(session, markets_to_fetch, time_unit="days", count=200),
                ]
                results = await asyncio.gather(*tasks)
                candles_60m, candles_daily = results[0], results[1]

                deep_dive_enriched = enrich_deep_dive_tickers(
                    {m: t for m, t in lightweight_tickers.items() if m in markets_to_fetch},
                    candles_60m,
                    candles_daily,
                    lightweight_tickers,
                )
                enriched_tickers.update(deep_dive_enriched)

                market_regime = get_market_regime(enriched_tickers)
                logger.info(f"현재 시장 체제: {market_regime.get('regime', 'UNKNOWN')}")

                for market in candidate_markets:
                    ticker = enriched_tickers.get(market)
                    if not ticker: continue
                    ticker.final_confidence = calculate_robust_confidence(ticker, market_regime)

            # PHASE 3: 알림 생성
            final_alerts = []
            if candidate_markets: 
                candidates_list = detect_anomalies(
                    enriched_tickers, 
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
        asyncio.run(run_check())
    except Exception as e:
        logger.critical(f"작업 실행 중 심각한 오류 발생: {e}", exc_info=True)
        return ("Internal Server Error", 500)

    logger.info("업비트 순위 확인 작업 완료 (Cloud Function).")
    return ("OK", 200)


if __name__ == "__main__":
    logger.info("로컬 환경에서 테스트 실행을 시작합니다.")
    asyncio.run(run_check())
    logger.info("로컬 테스트 실행을 종료합니다.")
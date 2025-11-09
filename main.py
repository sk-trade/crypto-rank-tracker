#main

import asyncio
import datetime
import logging

import aiohttp
import functions_framework

import config
from common.analysis import calculate_rankings, process_and_enrich_candles
from common.models import RankState
from common.notification.main import create_and_dispatch_notification
from common.sector_loader import load_and_process_sectors
from common.state_manager import load_rank_state_history, save_rank_state_history
from common.upbit_client import (
    UpbitAPIError,
    get_all_krw_tickers,
    get_minutes_candles,
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
            logger.critical(
                "GCS 모드로 설정되었으나 'google-cloud-storage' 라이브러리가 설치되지 않았습니다."
            )
            logger.critical("pip install google-cloud-storage 명령어로 설치해주세요.")
            return
    else:
        logger.info("로컬 파일 저장 모드로 실행됩니다.")

    try:
        async with aiohttp.ClientSession() as session:
            # 1. 이전 상태 및 설정 로드
            logger.info("이전 상태 및 설정 로드를 시작합니다.")
            old_rank_states = await load_rank_state_history(gcs_client)
            previous_rankings = old_rank_states[-1].rankings if old_rank_states else {}
            sectors, reverse_sector_map = await load_and_process_sectors(gcs_client)

            # 2. 데이터 수집
            logger.info("Upbit API를 통해 최신 시장 데이터를 수집합니다.")
            try:
                raw_tickers = await get_all_krw_tickers(session)
                all_markets = [ticker["market"] for ticker in raw_tickers]
                new_candles_data = await get_minutes_candles(
                    session, all_markets, unit=10, count=200
                )
            except UpbitAPIError as e:
                logger.critical(f"업비트 데이터 수집 실패: {e}. 작업 종료.")
                return

            # 3. 데이터 가공 및 지표 계산
            logger.info("수집된 데이터를 가공하고 분석 지표를 계산합니다.")
            enriched_tickers = process_and_enrich_candles(new_candles_data)
            current_rankings = calculate_rankings(raw_tickers)

            # 4. 분석, 포매팅, 알림 전송
            logger.info("시장 분석 및 알림 생성/전송을 시작합니다.")
            await create_and_dispatch_notification(
                enriched_tickers=enriched_tickers,
                raw_tickers=raw_tickers,
                current_rankings=current_rankings,
                previous_rankings=previous_rankings,
                SECTORS=sectors,
                REVERSE_SECTOR_MAP=reverse_sector_map,
                gcs_client=gcs_client,
            )

            # 5. 현재 상태 저장
            logger.info("최신 순위 상태를 저장합니다.")
            new_rank_state = RankState(
                last_updated=datetime.datetime.now(datetime.timezone.utc),
                rankings=current_rankings,
            )
            await save_rank_state_history(
                new_rank_state, old_rank_states, gcs_client=gcs_client
            )

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
#main
import asyncio
import logging
import datetime
import aiohttp
from typing import List, Dict, Any
import functions_framework

from common.analysis import calculate_rankings, process_and_enrich_candles
import config
from common.models import TickerData, AnalysisState, RankState
from common.upbit_client import get_all_krw_tickers, get_minutes_candles, UpbitAPIError
from common.state_manager import load_rank_state_history, save_rank_state_history
from common.notifier import analyze_and_format_notification, send_notification
from common.sector_loader import load_and_process_sectors
from common.state_manager import save_analysis_log


# 로거 설정
logging.basicConfig(level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(config.APP_LOGGER_NAME)


async def run_check():
    """핵심 로직을 실행하는 비동기 함수"""
    gcs_client = None
    if config.STATE_STORAGE_METHOD == "GCS":
        # --- 2. GCS가 필요할 때만 여기서 import ---
        try:
            from google.cloud import storage
        except ImportError:
            logger.critical("GCS 모드로 설정되었으나 'google-cloud-storage' 라이브러리가 설치되지 않았습니다.")
            logger.critical("pip install google-cloud-storage 명령어로 설치해주세요.")
            return # 또는 raise

        gcs_client = storage.Client()
        logger.info("GCS 저장 모드로 실행됩니다.")
    else:
        logger.info("로컬 파일 저장 모드로 실행됩니다.")

    try:
        async with aiohttp.ClientSession() as session:
            # 이전 순위 상태 로드
            old_rank_states = await load_rank_state_history(gcs_client)
            previous_rankings = old_rank_states[-1].rankings if old_rank_states else {}

            # 섹터 정보 로드
            SECTORS, REVERSE_SECTOR_MAP = await load_and_process_sectors(gcs_client)
            
            # 현재 시장 원본 데이터 가져오기 (API 호출)
            try:
                raw_tickers = await get_all_krw_tickers(session)
                all_markets = [ticker['market'] for ticker in raw_tickers]
                new_candles_data = await get_minutes_candles(session, all_markets, unit=10, count=200)
            except UpbitAPIError as e:
                logger.critical(f"업비트 데이터 수집 실패: {e}. 작업 종료.")
                return

            # 데이터 가공 및 파생 지표 계산
            enriched_tickers = process_and_enrich_candles(new_candles_data)
            
            # 현재 24시간 거래대금 순위 계산
            current_rankings = calculate_rankings(raw_tickers)

            # 분석 및 알림 메시지 생성
            notification_message = await analyze_and_format_notification(
                enriched_tickers,
                raw_tickers,
                current_rankings,
                previous_rankings,
                SECTORS,
                REVERSE_SECTOR_MAP,
                gcs_client=gcs_client
            )
            
            # 알림 전송
            if notification_message:
                await send_notification(session, notification_message)
                logger.info("알림 메시지를 생성하여 전송했습니다.")
            else:
                logger.info("알림을 보낼 유의미한 시그널이 없습니다.")

            # 현재 순위 상태 저장
            new_rank_state = RankState(
                last_updated=datetime.datetime.now(datetime.timezone.utc),
                rankings=current_rankings
            )
            await save_rank_state_history(new_rank_state, old_rank_states, gcs_client=gcs_client)

            # 현재 분석 결과 로그로 저장
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
        logger.critical(f"업비트 데이터 수집 실패: {e}. 작업을 중단합니다.")
        raise RuntimeError("Failed to fetch critical data from Upbit") from e

@functions_framework.http
def main(request): 
    """Google Cloud Function 진입점"""
    
    logger.info("업비트 순위 확인 작업 시작.")
    try:
        asyncio.run(run_check())
    except Exception as e:
        logger.critical(f"작업 실행 중 심각한 오류 발생: {e}", exc_info=True)
        return ("Internal Server Error", 500)

    logger.info("업비트 순위 확인 작업 완료.")
    return ("OK", 200)


if __name__ == "__main__":
    logger.info("로컬 테스트 실행을 시작합니다.")
    asyncio.run(run_check())
    logger.info("로컬 테스트 실행을 종료합니다.")
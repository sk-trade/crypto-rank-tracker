import asyncio
import logging
import datetime
from google.cloud import storage
import aiohttp
from typing import List, Dict, Any

import config
from common.models import TickerData, State
from common.upbit_client import get_all_krw_tickers
from common.state_manager import load_previous_states, save_current_state
from common.notifier import analyze_and_format_notification, send_notification

# 로거 설정
logging.basicConfig(level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(config.APP_LOGGER_NAME)


def process_raw_tickers(raw_tickers: List[Dict[str, Any]]) -> Dict[str, TickerData]:
    """
    API에서 받은 데이터를 TickerData 모델로 가공하고 순위를 매깁니다.
    """
    processed = {}
    for ticker_data in raw_tickers:
        market = ticker_data['market']
        try:
            processed[market] = TickerData.model_validate(ticker_data)
        except Exception as e:
            logger.warning(f"데이터 파싱 오류: {market}, 데이터: {ticker_data}, 오류: {e}")
            continue

    # 거래대금 기준으로 정렬
    sorted_markets = sorted(
        processed.keys(),
        key=lambda m: processed[m].trade_volume_24h_krw,
        reverse=True
    )
    
    # 순위 부여
    for rank, market in enumerate(sorted_markets, 1):
        processed[market].rank = rank
        
    return processed


async def run_check():
    """핵심 로직을 실행하는 비동기 함수"""
    gcs_client = None
    if config.STATE_STORAGE_METHOD == "GCS":
        gcs_client = storage.Client()
        logger.info("GCS 저장 모드로 실행됩니다.")
    else:
        logger.info("로컬 파일 저장 모드로 실행됩니다.")
    
    async with aiohttp.ClientSession() as session:
        # 이전 상태 로드
        old_states = await load_previous_states(gcs_client)
        
        # 현재 시장 데이터 가져오기 
        try:
            raw_tickers = await get_all_krw_tickers(session)
        except UpbitAPIError as e:
            logger.critical(f"업비트에서 데이터를 가져오지 못했습니다: {e}. 작업을 종료합니다.")
            return
            
        # 데이터 가공 및 순위 매기기
        new_tickers_data = process_raw_tickers(raw_tickers)
        
        # (확장 포인트) 보조 지표 계산
        # new_tickers_data = await calculate_indicators(session, new_tickers_data)

        # 현재 상태 객체 생성
        new_state = State(
            last_updated=datetime.datetime.now(datetime.timezone.utc),
            tickers=new_tickers_data
        )

        # 변경점 분석 및 알림 메시지 생성
        notification_message = analyze_and_format_notification(new_state, old_states)
        
        # 알림 전송
        if notification_message:
            await send_notification(session, notification_message)
            logger.info("알림 메시지를 생성하여 전송했습니다.")
        else:
            logger.info("알림을 보낼 변동 사항이 없습니다.")

        # 새로운 상태 저장
        await save_current_state(new_state, old_states, gcs_client=gcs_client)


def main(context):
    """Google Cloud Function 진입점"""
    logger.info("업비트 순위 확인 작업 시작.")
    try:
        asyncio.run(run_check())
    except Exception as e:
        logger.critical(f"작업 실행 중 심각한 오류 발생: {e}", exc_info=True)
    logger.info("업비트 순위 확인 작업 완료.")

if __name__ == "__main__":
    logger.info("로컬 테스트 실행을 시작합니다.")
    main(context=None)
    logger.info("로컬 테스트 실행을 종료합니다.")
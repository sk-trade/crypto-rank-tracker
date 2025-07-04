import asyncio
import logging
import datetime
from google.cloud import storage
import aiohttp

import config
from common.upbit_client import get_all_krw_tickers
from common.state_manager import load_previous_states, save_current_state
from common.notifier import analyze_and_format_notification, send_notification

# 로거 설정
logging.basicConfig(level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(config.APP_LOGGER_NAME)


def process_raw_tickers(raw_tickers):
    """API에서 받은 데이터를 내부 상태 형식으로 가공하고 순위를 매깁니다."""
    
    # 1. 거래대금 계산 및 기본 데이터 구조 생성
    processed = {}
    for ticker in raw_tickers:
        market = ticker['market']
        trade_price = ticker.get('trade_price')
        volume_24h = ticker.get('acc_trade_volume_24h')
        
        if trade_price is None or volume_24h is None:
            logger.warning(f"데이터 누락: {market}, 가격: {trade_price}, 거래량: {volume_24h}. 계산에서 제외합니다.")
            continue
            
        processed[market] = {
            "market": market,
            "price": trade_price,
            "trade_volume_24h_krw": trade_price * volume_24h,
        }

    # 2. 거래대금 기준으로 정렬
    sorted_markets = sorted(
        processed.keys(),
        key=lambda m: processed[m]['trade_volume_24h_krw'],
        reverse=True
    )
    
    # 3. 순위 부여
    for rank, market in enumerate(sorted_markets, 1):
        processed[market]['rank'] = rank
        
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
        # 1. 이전 상태 로드
        old_states = await load_previous_states(gcs_client)
        
        # 2. 현재 시장 데이터 가져오기
        raw_tickers = await get_all_krw_tickers(session)
        if not raw_tickers:
            logger.error("업비트에서 데이터를 가져오지 못했습니다. 작업을 종료합니다.")
            return
            
        # 3. 데이터 가공 및 순위 매기기
        new_tickers_data = process_raw_tickers(raw_tickers)
        
        # (확장 포인트) 4. 보조 지표 계산
        # new_tickers_data = await calculate_indicators(session, new_tickers_data)

        # 5. 현재 상태 객체 생성
        new_state = {
            "last_updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "tickers": new_tickers_data
        }

        # 6. 변경점 분석 및 알림 메시지 생성
        notification_message = analyze_and_format_notification(new_state, old_states)
        
        # 7. 알림 전송
        if notification_message:
            await send_notification(session, notification_message)
            logger.info("알림 메시지를 생성하여 전송했습니다.")
        else:
            # 이 경우는 거의 발생하지 않지만, 만약을 위해 로깅
            logger.warning("알림 메시지가 생성되지 않았습니다.")

        # 8. GCS에 새로운 상태 저장
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
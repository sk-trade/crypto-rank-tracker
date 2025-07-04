import asyncio
import logging
import datetime
from google.cloud import storage
import aiohttp

import config
from common.upbit_client import get_all_krw_tickers
from common.state_manager import load_previous_state, save_current_state
from common.notifier import analyze_changes, format_notification, send_notification

# 로거 설정
logging.basicConfig(level=config.LOG_LEVEL, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(config.APP_LOGGER_NAME)


def process_raw_tickers(raw_tickers):
    """API에서 받은 데이터를 내부 상태 형식으로 가공하고 순위를 매깁니다."""
    
    # 1. 거래대금 계산 및 기본 데이터 구조 생성
    processed = {}
    problematic_tickers = []
    for ticker in raw_tickers:
        market = ticker['market']
        trade_price = ticker.get('trade_price', 0)
        volume_24h = ticker.get('acc_trade_volume_24h', 0)
        
        # --- 범인 색출을 위한 디버깅 코드 ---
        # trade_price 또는 volume_24h가 None인지 확인
        if trade_price is None or volume_24h is None:
            # 문제가 되는 종목의 정보를 리스트에 추가
            problematic_tickers.append(
                f"-> 종목: {market}, 가격: {trade_price}, 거래량: {volume_24h}"
            )
            # 일단 계산은 0으로 처리해서 프로그램이 멈추지 않게 함
            trade_price = trade_price or 0
            volume_24h = volume_24h or 0
        # --- 디버깅 코드 끝 ---
            
        processed[market] = {
            "market": market,
            "price": trade_price,
            "trade_volume_24h_krw": trade_price * volume_24h,
        }
    
    # 만약 문제가 있는 종목이 하나라도 있었다면, 종합해서 로그를 남김
    if problematic_tickers:
        logger.warning(
            "업비트 API 응답 중 일부 데이터에 None 값이 포함되어 있습니다:\n" + 
            "\n".join(problematic_tickers)
        )

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
        old_state = await load_previous_state(gcs_client)
        
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
        change_messages = analyze_changes(new_state, old_state)
        notification_message = format_notification(change_messages, new_state)
        
        # 7. 알림 전송
        if notification_message:
            await send_notification(session, notification_message)
        else:
            logger.info("감지된 순위 변동이 없습니다.")

        # 8. GCS에 새로운 상태 저장
        await save_current_state(state=new_state, gcs_client=gcs_client)


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
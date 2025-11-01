import json
import logging
import os
import aiofiles  
import asyncio
from typing import Dict, Any, List, Optional
import config
from common.models import RankState, AnalysisState, AlertHistory
from common.storage_client import load_json, save_json
import datetime

logger = logging.getLogger(config.APP_LOGGER_NAME)


# -- 순위 상태 관리 (히스토리 방식) --
async def load_rank_state_history(gcs_client = None) -> List[RankState]:
    """'순위' 상태 히스토리 리스트를 로드합니다."""
    filename = config.RANK_STATE_FILE_NAME
    data = None
    
    if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
        data = await load_json(filename, gcs_client)
    else:
        filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
        data = await load_json(filepath)

    if not data:
        logger.info(f"순위 상태 파일({filename})이 없어 초기 상태로 시작합니다.")
        return []
    if isinstance(data, dict):
        data = [data]
        
    return [RankState.model_validate(s) for s in data]

async def save_rank_state_history(new_state: RankState, old_states: List[RankState], gcs_client = None):
    """새로운 '순위' 상태를 히스토리에 추가하여 저장합니다."""
    filename = config.RANK_STATE_FILE_NAME
    
    updated_states = old_states + [new_state]
    if len(updated_states) > config.STATE_HISTORY_COUNT:
        updated_states = updated_states[-config.STATE_HISTORY_COUNT:]
    
    data_to_save = [s.model_dump(mode='json') for s in updated_states]
    
    if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
        await save_json(filename, data_to_save, gcs_client)
        logger.info(f"GCS에 순위 히스토리 저장 완료: {filename}")
    else:
        filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
        await save_json(filepath, data_to_save)
        logger.info(f"로컬에 순위 히스토리 저장 완료: {filepath}")


# -- 분석 로그 관리 --
def get_daily_log_filename() -> str:
    """오늘 날짜 기반의 로그 파일 이름을 반환합니다."""
    today = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
    return f"analysis_log_{today}.json"

async def save_analysis_log(state: AnalysisState, gcs_client = None):
    """현재 분석 결과(State)를 오늘 날짜의 로그 파일에 '추가(append)'합니다."""
    filename = get_daily_log_filename()
    new_log_entry = state.model_dump(mode='json', exclude={'tickers': {'__all__': {'candle_history'}}})
    
    try:
        log_list = None
        if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
            log_list = await load_json(filename, gcs_client)
        else:
            filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
            log_list = await load_json(filepath)

        if log_list is None or not isinstance(log_list, list):
            log_list = []
        
        log_list.append(new_log_entry)
        
        if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
            await save_json(filename, log_list, gcs_client)
        else:
            filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
            await save_json(filepath, log_list)
        
        logger.info(f"분석 로그 추가 완료: {filename}")

    except Exception as e:
        logger.error(f"'{filename}' 로그 파일 저장 프로세스 실패: {e}", exc_info=True)

# -- 알림 히스토리 관리 --
async def load_alert_history(gcs_client=None) -> Dict[str, AlertHistory]:
    """알림 히스토리 파일을 로드합니다."""
    data = await load_json(config.ALERT_HISTORY_FILE_NAME, gcs_client)
    if not data:
        return {}
    return {market: AlertHistory.model_validate(alert_data) for market, alert_data in data.items()}

async def save_alert_history(history: Dict[str, AlertHistory], gcs_client=None):
    """알림 히스토리를 저장합니다."""
    # 24시간 이상 지난 오래된 히스토리 자동 정리
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
    cleaned_history = {
        market: alert for market, alert in history.items() 
        if alert.initial_timestamp > cutoff
    }
    data_to_save = {m: ah.model_dump(mode='json') for m, ah in cleaned_history.items()}
    await save_json(config.ALERT_HISTORY_FILE_NAME, data_to_save, gcs_client)
    logger.info(f"알림 히스토리 저장 완료: {config.ALERT_HISTORY_FILE_NAME}")

async def cleanup_old_logs(days_to_keep: int = 7, gcs_client = None):
    """
    설정된 기간보다 오래된 분석 로그 파일을 삭제합니다.
    """
    cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days_to_keep)
    prefix = "analysis_log_"
    
    try:
        if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
            bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
            blobs_to_delete = []
            
            # 비동기적으로 GCS blob 목록을 가져오기 위해 to_thread 사용
            blobs_iterator = await asyncio.to_thread(bucket.list_blobs, prefix=prefix)
            
            for blob in blobs_iterator:
                try:
                    # 파일 이름에서 날짜 추출 (예: analysis_log_2024-05-20.json)
                    date_str = blob.name.replace(prefix, "").replace(".json", "")
                    file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
                    if file_date < cutoff_date:
                        blobs_to_delete.append(blob)
                except ValueError:
                    continue # 날짜 형식이 아닌 파일은 건너뜀
            
            if blobs_to_delete:
                logger.info(f"{len(blobs_to_delete)}개의 오래된 GCS 로그 파일을 삭제합니다.")
                # GCS는 일괄 삭제 API가 없으므로 개별적으로 삭제
                for blob in blobs_to_delete:
                    await asyncio.to_thread(blob.delete)

        else: # LOCAL
            log_dir = config.LOCAL_STATE_DIR
            if not os.path.isdir(log_dir):
                return
            
            for filename in os.listdir(log_dir):
                if filename.startswith(prefix) and filename.endswith(".json"):
                    try:
                        date_str = filename.replace(prefix, "").replace(".json", "")
                        file_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
                        if file_date < cutoff_date:
                            filepath = os.path.join(log_dir, filename)
                            os.remove(filepath)
                            logger.info(f"오래된 로컬 로그 파일 삭제: {filepath}")
                    except ValueError:
                        continue
                        
    except Exception as e:
        logger.error(f"오래된 로그 파일 정리 실패: {e}", exc_info=True)
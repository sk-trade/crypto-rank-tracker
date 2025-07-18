import json
import logging
import os
import aiofiles  # 비동기 파일 I/O를 위해 추가
import asyncio
from google.cloud import storage
from typing import Dict, Any, List
import config

logger = logging.getLogger(config.APP_LOGGER_NAME)

async def load_previous_states(gcs_client: storage.Client = None) -> List[Dict[str, Any]]:
    """
    설정된 저장 방식에 따라 이전 상태 '히스토리 리스트'를 로드합니다.
    """
    if config.STATE_STORAGE_METHOD == "GCS":
        if not gcs_client:
            logger.error("GCS 모드이지만 GCS 클라이언트가 제공되지 않았습니다.")
            return []
        return await _load_from_gcs(gcs_client)
    else: # LOCAL
        return await _load_from_local()


async def save_current_state(new_state: Dict[str, Any], old_states: List[Dict[str, Any]], gcs_client: storage.Client = None):
    """
    새로운 상태를 히스토리에 추가하고, 가장 오래된 상태를 제거하여 저장합니다.
    """
    # 새로운 히스토리 리스트 생성
    updated_states = old_states + [new_state]
    
    # 설정된 히스토리 개수 유지
    if len(updated_states) > config.STATE_HISTORY_COUNT:
        # 리스트의 앞에서부터(가장 오래된 데이터) 초과분 제거
        updated_states = updated_states[-config.STATE_HISTORY_COUNT:]

    if config.STATE_STORAGE_METHOD == "GCS":
        if not gcs_client:
            logger.error("GCS 모드이지만 GCS 클라이언트가 제공되지 않았습니다.")
            return
        await _save_to_gcs(gcs_client, updated_states)
    else: # LOCAL
        await _save_to_local(updated_states)


# --- GCS 전용 헬퍼 함수 ---
async def _load_from_gcs(gcs_client: storage.Client) -> List[Dict[str, Any]]:
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.GCS_STATE_FILE_NAME)
        
        if not await asyncio.to_thread(blob.exists):
            logger.info("GCS 상태 파일이 없어 초기 상태로 시작합니다.")
            return []
            
        data = await asyncio.to_thread(blob.download_as_text)
        states = json.loads(data)
        
        # 단일 dict가 저장된 경우 list로 변환
        if isinstance(states, dict):
            return [states]
        return states

    except Exception as e:
        logger.error(f"GCS 상태 로드 실패: {e}", exc_info=True)
        return []


async def _save_to_gcs(gcs_client: storage.Client, states: List[Dict[str, Any]]):
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.GCS_STATE_FILE_NAME)
        state_json = json.dumps(states, ensure_ascii=False, indent=2)
        
        await asyncio.to_thread(
            blob.upload_from_string,
            state_json,
            content_type="application/json"
        )
        
        logger.info("GCS에 상태 히스토리 저장 완료.")
    except Exception as e:
        logger.error(f"GCS 상태 저장 실패: {e}", exc_info=True)


# --- 로컬 파일 시스템 전용 헬퍼 함수 ---
async def _load_from_local() -> List[Dict[str, Any]]:
    file_path = os.path.join(config.LOCAL_STATE_DIR, config.LOCAL_STATE_FILE_NAME)
    if not os.path.exists(file_path):
        logger.info("로컬 상태 파일이 없어 초기 상태로 시작합니다.")
        return []
    
    try:
        async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
            content = await f.read()
            states = json.loads(content)
            if isinstance(states, dict):
                return [states]
            return states
    except Exception as e:
        logger.error(f"로컬 상태 파일 로드 실패: {e}", exc_info=True)
        return []


async def _save_to_local(states: Dict[str, Any]):
    # 상태 파일을 저장할 디렉토리가 없으면 생성
    os.makedirs(config.LOCAL_STATE_DIR, exist_ok=True)
    file_path = os.path.join(config.LOCAL_STATE_DIR, config.LOCAL_STATE_FILE_NAME)
    try:
        async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(states, ensure_ascii=False, indent=2))
        logger.info(f"로컬에 상태 히스토리 저장 완료: {file_path}")
    except Exception as e:
        logger.error(f"로컬 상태 파일 저장 실패: {e}", exc_info=True)
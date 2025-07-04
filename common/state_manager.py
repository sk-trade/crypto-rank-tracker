import json
import logging
import os
import aiofiles  # 비동기 파일 I/O를 위해 추가
from google.cloud import storage
from typing import Dict, Any
import config

logger = logging.getLogger(config.APP_LOGGER_NAME)

async def load_previous_state(gcs_client: storage.Client = None) -> Dict[str, Any]:
    """
    설정된 저장 방식(GCS 또는 LOCAL)에 따라 이전 상태를 로드합니다.
    """
    if config.STATE_STORAGE_METHOD == "GCS":
        if not gcs_client:
            logger.error("GCS 모드이지만 GCS 클라이언트가 제공되지 않았습니다.")
            return {"tickers": {}}
        return await _load_from_gcs(gcs_client)
    elif config.STATE_STORAGE_METHOD == "LOCAL":
        return await _load_from_local()
    else:
        raise ValueError(f"알 수 없는 저장 방식입니다: {config.STATE_STORAGE_METHOD}")


async def save_current_state(state: Dict[str, Any], gcs_client: storage.Client = None):
    """
    설정된 저장 방식(GCS 또는 LOCAL)에 따라 현재 상태를 저장합니다.
    """
    if config.STATE_STORAGE_METHOD == "GCS":
        if not gcs_client:
            logger.error("GCS 모드이지만 GCS 클라이언트가 제공되지 않았습니다.")
            return
        await _save_to_gcs(gcs_client, state)
    elif config.STATE_STORAGE_METHOD == "LOCAL":
        await _save_to_local(state)
    else:
        raise ValueError(f"알 수 없는 저장 방식입니다: {config.STATE_STORAGE_METHOD}")


# --- GCS 전용 헬퍼 함수 ---
async def _load_from_gcs(gcs_client: storage.Client) -> Dict[str, Any]:
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.GCS_STATE_FILE)
        if not blob.exists():
            logger.info("GCS 상태 파일이 없어 초기 상태로 시작합니다.")
            return {"tickers": {}}
        data = blob.download_as_string()
        return json.loads(data)
    except Exception as e:
        logger.error(f"GCS 상태 로드 실패: {e}", exc_info=True)
        return {"tickers": {}}


async def _save_to_gcs(gcs_client: storage.Client, state: Dict[str, Any]):
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(config.GCS_STATE_FILE)
        blob.upload_from_string(
            data=json.dumps(state, ensure_ascii=False, indent=2),
            content_type="application/json"
        )
        logger.info("GCS에 현재 상태 저장 완료.")
    except Exception as e:
        logger.error(f"GCS 상태 저장 실패: {e}", exc_info=True)


# --- 로컬 파일 시스템 전용 헬퍼 함수 ---
async def _load_from_local() -> Dict[str, Any]:
    file_path = os.path.join(config.LOCAL_STATE_DIR, config.LOCAL_STATE_FILE)
    if not os.path.exists(file_path):
        logger.info("로컬 상태 파일이 없어 초기 상태로 시작합니다.")
        return {"tickers": {}}
    
    try:
        async with aiofiles.open(file_path, mode='r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content)
    except Exception as e:
        logger.error(f"로컬 상태 파일 로드 실패: {e}", exc_info=True)
        return {"tickers": {}}


async def _save_to_local(state: Dict[str, Any]):
    # 상태 파일을 저장할 디렉토리가 없으면 생성
    if not os.path.exists(config.LOCAL_STATE_DIR):
        os.makedirs(config.LOCAL_STATE_DIR)
        logger.info(f"로컬 상태 디렉토리 생성: {config.LOCAL_STATE_DIR}")

    file_path = os.path.join(config.LOCAL_STATE_DIR, config.LOCAL_STATE_FILE)
    try:
        async with aiofiles.open(file_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(state, ensure_ascii=False, indent=2))
        logger.info(f"로컬에 현재 상태 저장 완료: {file_path}")
    except Exception as e:
        logger.error(f"로컬 상태 파일 저장 실패: {e}", exc_info=True)
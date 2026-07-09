#common/storage_client

import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Union

import aiofiles

import config

logger = logging.getLogger(config.APP_LOGGER_NAME)


class StateNotFound(Exception):
    pass


class StateLoadError(Exception):
    pass


async def load_json(
    filename: str, gcs_client=None
) -> Optional[Union[List, Dict]]:
    """설정에 따라 GCS 또는 로컬에서 JSON 파일을 로드합니다.
    
    파일이 없으면 None을 반환합니다.
    로드 실패(JSON 파싱 오류, 권한 오류, 네트워크 오류 등)는 예외를 던집니다.
    """
    if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
        return await _load_json_from_gcs(gcs_client, filename)
    filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
    return await _load_json_from_local(filepath)


async def save_json(filename: str, data: Union[List, Dict], gcs_client=None):
    """설정에 따라 GCS 또는 로컬에 JSON 파일을 저장합니다."""
    try:
        if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
            await _save_json_to_gcs(gcs_client, filename, data)
        else:
            filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
            await _save_json_to_local(filepath, data)
    except Exception as e:
        logger.error(f"파일 저장 실패 ({filename}): {e}")
        raise

async def _load_json_from_gcs(
    gcs_client, filename: str
) -> Optional[Union[List, Dict]]:
    """GCS에서 JSON 파일을 로드하는 헬퍼 함수입니다."""
    bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
    blob = bucket.blob(filename)

    try:
        exists = await asyncio.to_thread(blob.exists)
    except Exception as e:
        raise StateLoadError(f"GCS 파일 존재 여부 확인 실패 ({filename}): {e}") from e

    if not exists:
        return None

    try:
        data = await asyncio.to_thread(blob.download_as_text)
        return json.loads(data)
    except json.JSONDecodeError as e:
        raise StateLoadError(f"GCS JSON 파싱 실패 ({filename}): {e}") from e
    except Exception as e:
        raise StateLoadError(f"GCS 파일 로드 실패 ({filename}): {e}") from e


async def _save_json_to_gcs(gcs_client, filename: str, data: Union[List, Dict]):
    """GCS에 JSON 파일을 저장하는 헬퍼 함수입니다."""
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(filename)
        json_string = json.dumps(data, ensure_ascii=False, indent=2)
        await asyncio.to_thread(
            blob.upload_from_string,
            json_string,
            content_type="application/json",
        )
    except Exception as e:
        logger.error(f"GCS 파일({filename}) 저장 실패: {e}", exc_info=True)
        raise


async def _load_json_from_local(filepath: str) -> Optional[Union[List, Dict]]:
    """로컬 파일 시스템에서 JSON 파일을 로드하는 헬퍼 함수입니다."""
    if not os.path.exists(filepath):
        return None
    try:
        async with aiofiles.open(filepath, mode="r", encoding="utf-8") as f:
            content = await f.read()
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise StateLoadError(f"로컬 JSON 파싱 실패 ({filepath}): {e}") from e
    except Exception as e:
        raise StateLoadError(f"로컬 파일 로드 실패 ({filepath}): {e}") from e


async def _save_json_to_local(filepath: str, data: Union[List, Dict]):
    """로컬 파일 시스템에 JSON 파일을 저장하는 헬퍼 함수입니다."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        json_string = json.dumps(data, ensure_ascii=False, indent=2)
        async with aiofiles.open(filepath, mode="w", encoding="utf-8") as f:
            await f.write(json_string)
    except Exception as e:
        logger.error(f"로컬 파일({filepath}) 저장 실패: {e}", exc_info=True)
        raise
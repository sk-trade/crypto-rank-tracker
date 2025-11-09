#common/storage_client

import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Union

import aiofiles

import config

logger = logging.getLogger(config.APP_LOGGER_NAME)


async def load_json(
    filename: str, gcs_client=None
) -> Optional[Union[List, Dict]]:
    """설정에 따라 GCS 또는 로컬에서 JSON 파일을 로드합니다."""
    try:
        if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
            return await _load_json_from_gcs(gcs_client, filename)
        else:
            filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
            return await _load_json_from_local(filepath)
    except Exception as e:
        logger.warning(f"파일 로드 실패 ({filename}): {e}")
        return None


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
    try:
        bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
        blob = bucket.blob(filename)
        if not await asyncio.to_thread(blob.exists):
            return None
        data = await asyncio.to_thread(blob.download_as_text)
        return json.loads(data)
    except Exception:
        logger.warning(f"GCS 파일({filename}) 로드 실패 또는 파일 없음.")
        return None


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
    except Exception:
        logger.warning(f"로컬 파일({filepath}) 로드 실패 또는 파일 없음.")
        return None


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
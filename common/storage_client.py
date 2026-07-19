#common/storage_client

import asyncio
import json
import logging
import os
import tempfile
from enum import StrEnum
from typing import Dict, List, Optional, Union

import aiofiles

import config

logger = logging.getLogger(config.APP_LOGGER_NAME)
JsonValue = Union[List, Dict, None]


class StateErrorCode(StrEnum):
    INVALID_ARGUMENT = "invalid_argument"
    BACKEND_UNAVAILABLE = "backend_unavailable"
    INVALID_JSON = "invalid_json"
    NULL_DOCUMENT = "null_document"
    INVALID_SCHEMA = "invalid_schema"
    READ_FAILED = "read_failed"
    WRITE_FAILED = "write_failed"
    DELETE_FAILED = "delete_failed"
    CONCURRENT_UPDATE_EXHAUSTED = "concurrent_update_exhausted"


class StateError(RuntimeError):
    def __init__(
        self, code: StateErrorCode, resource: str, *, detail: str | None = None
    ):
        super().__init__(detail or f"{code.value}: {resource}")
        self.code = code
        self.resource = resource
        self.detail = detail


class StateLoadError(StateError):
    """Persisted state could not be read or validated."""


class StateSaveError(StateError):
    """Persisted state could not be written durably."""


class StateBackendUnavailable(StateError):
    """The configured backend cannot be used by the current caller."""


class StateOperationError(StateError):
    """A state operation request violates its public contract."""


async def load_json(
    filename: str, gcs_client=None, *, reject_null: bool = False
) -> JsonValue:
    """설정에 따라 GCS 또는 로컬에서 JSON 파일을 로드합니다.
    
    파일이 없으면 None을 반환합니다.
    reject_null=True이면 존재하는 파일의 explicit JSON null은 오류로 처리합니다.
    로드 실패(JSON 파싱 오류, 권한 오류, 네트워크 오류 등)는 예외를 던집니다.
    """
    if config.storage_method() is config.StorageMethod.GCS:
        if gcs_client is None:
            raise StateBackendUnavailable(
                StateErrorCode.BACKEND_UNAVAILABLE, filename
            )
        return await _load_json_from_gcs(
            gcs_client, filename, reject_null=reject_null
        )
    filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
    return await _load_json_from_local(filepath, reject_null=reject_null)


async def save_json(filename: str, data: JsonValue, gcs_client=None):
    """설정에 따라 GCS 또는 로컬에 JSON 파일을 저장합니다."""
    try:
        if config.storage_method() is config.StorageMethod.GCS:
            if gcs_client is None:
                raise StateBackendUnavailable(
                    StateErrorCode.BACKEND_UNAVAILABLE, filename
                )
            await _save_json_to_gcs(gcs_client, filename, data)
        else:
            filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
            await _save_json_to_local(filepath, data)
    except StateError:
        raise
    except Exception as e:
        logger.error(f"파일 저장 실패 ({filename}): {e}")
        raise StateSaveError(
            StateErrorCode.WRITE_FAILED, filename, detail=str(e)
        ) from e

async def _load_json_from_gcs(
    gcs_client, filename: str, *, reject_null: bool = False
) -> JsonValue:
    """GCS에서 JSON 파일을 로드하는 헬퍼 함수입니다."""
    bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
    blob = bucket.blob(filename)

    try:
        exists = await asyncio.to_thread(blob.exists)
    except Exception as e:
        raise StateLoadError(
            StateErrorCode.READ_FAILED, filename, detail=str(e)
        ) from e

    if not exists:
        return None

    try:
        data = await asyncio.to_thread(blob.download_as_text)
        value = json.loads(data)
        if reject_null and value is None:
            raise StateLoadError(StateErrorCode.NULL_DOCUMENT, filename)
        return value
    except json.JSONDecodeError as e:
        raise StateLoadError(
            StateErrorCode.INVALID_JSON, filename, detail=str(e)
        ) from e
    except StateLoadError:
        raise
    except Exception as e:
        raise StateLoadError(
            StateErrorCode.READ_FAILED, filename, detail=str(e)
        ) from e


async def _save_json_to_gcs(gcs_client, filename: str, data: JsonValue):
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


async def _load_json_from_local(
    filepath: str, *, reject_null: bool = False
) -> Optional[Union[List, Dict]]:
    """로컬 파일 시스템에서 JSON 파일을 로드하는 헬퍼 함수입니다."""
    if not os.path.exists(filepath):
        return None
    try:
        async with aiofiles.open(filepath, mode="r", encoding="utf-8") as f:
            content = await f.read()
        value = json.loads(content)
        if reject_null and value is None:
            raise StateLoadError(StateErrorCode.NULL_DOCUMENT, filepath)
        return value
    except json.JSONDecodeError as e:
        raise StateLoadError(
            StateErrorCode.INVALID_JSON, filepath, detail=str(e)
        ) from e
    except StateLoadError:
        raise
    except Exception as e:
        raise StateLoadError(
            StateErrorCode.READ_FAILED, filepath, detail=str(e)
        ) from e


async def _save_json_to_local(filepath: str, data: JsonValue):
    """로컬 파일 시스템에 JSON 파일을 저장하는 헬퍼 함수입니다."""
    temporary_path = None
    try:
        directory = os.path.dirname(filepath)
        os.makedirs(directory, exist_ok=True)
        json_string = json.dumps(data, ensure_ascii=False, indent=2)
        descriptor, temporary_path = tempfile.mkstemp(
            dir=directory, prefix=f".{os.path.basename(filepath)}.", suffix=".tmp"
        )
        os.close(descriptor)
        async with aiofiles.open(temporary_path, mode="w", encoding="utf-8") as f:
            await f.write(json_string)
            await f.flush()
            await asyncio.to_thread(os.fsync, f.fileno())
        os.replace(temporary_path, filepath)
        temporary_path = None
    except Exception as e:
        logger.error(f"로컬 파일({filepath}) 저장 실패: {e}", exc_info=True)
        raise
    finally:
        if temporary_path and os.path.exists(temporary_path):
            os.unlink(temporary_path)


def create_gcs_client():
    """Create a storage client with an explicit project when the environment provides one."""
    from google.cloud import storage

    return storage.Client(project=config.GCP_PROJECT_ID or None)

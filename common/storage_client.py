"""Durable JSON document access for local files and Google Cloud Storage."""

import asyncio
import json
import logging
import os
import tempfile
from enum import StrEnum
from typing import Any

import aiofiles

import config

logger = logging.getLogger(config.APP_LOGGER_NAME)
JsonDocument = list[Any] | dict[str, Any] | None


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
        self,
        code: StateErrorCode,
        resource: str,
        *,
        detail: str | None = None,
    ) -> None:
        super().__init__(detail or f"{code.value}: {resource}")
        self.code = code
        self.resource = resource
        self.detail = detail


class StateLoadError(StateError):
    """Persisted state could not be read or validated."""


class StateSaveError(StateError):
    """Persisted state could not be written durably."""


class StateBackendUnavailableError(StateError):
    """The configured backend cannot be used by the current caller."""


class StateOperationError(StateError):
    """A state operation request violates its public contract."""


def _parse_json_document(
    serialized_document: str,
    resource: str,
    *,
    reject_null: bool,
) -> JsonDocument:
    try:
        document = json.loads(serialized_document)
    except json.JSONDecodeError as error:
        raise StateLoadError(
            StateErrorCode.INVALID_JSON,
            resource,
            detail=str(error),
        ) from error
    if reject_null and document is None:
        raise StateLoadError(StateErrorCode.NULL_DOCUMENT, resource)
    return document


async def load_json(
    filename: str,
    gcs_client=None,
    *,
    reject_null: bool = False,
) -> JsonDocument:
    """Load one JSON document from the configured state backend."""
    if config.storage_method() is config.StorageMethod.GCS:
        if gcs_client is None:
            raise StateBackendUnavailableError(
                StateErrorCode.BACKEND_UNAVAILABLE,
                filename,
            )
        return await _load_json_from_gcs(
            gcs_client,
            filename,
            reject_null=reject_null,
        )
    filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
    return await _load_json_from_local(filepath, reject_null=reject_null)


async def save_json(
    filename: str,
    data: JsonDocument,
    gcs_client=None,
) -> None:
    """Durably replace one JSON document in the configured state backend."""
    try:
        if config.storage_method() is config.StorageMethod.GCS:
            if gcs_client is None:
                raise StateBackendUnavailableError(
                    StateErrorCode.BACKEND_UNAVAILABLE,
                    filename,
                )
            await _save_json_to_gcs(gcs_client, filename, data)
        else:
            filepath = os.path.join(config.LOCAL_STATE_DIR, filename)
            await _save_json_to_local(filepath, data)
    except StateError:
        raise
    except Exception as error:
        logger.exception("Failed to save state document: %s", filename)
        raise StateSaveError(
            StateErrorCode.WRITE_FAILED,
            filename,
            detail=str(error),
        ) from error


async def _load_json_from_gcs(
    gcs_client,
    filename: str,
    *,
    reject_null: bool = False,
) -> JsonDocument:
    """Load one JSON document from GCS; return None when it does not exist."""
    blob = gcs_client.bucket(config.GCS_BUCKET_NAME).blob(filename)
    try:
        exists = await asyncio.to_thread(blob.exists)
    except Exception as error:
        raise StateLoadError(
            StateErrorCode.READ_FAILED,
            filename,
            detail=str(error),
        ) from error
    if not exists:
        return None

    try:
        serialized_document = await asyncio.to_thread(blob.download_as_text)
    except Exception as error:
        raise StateLoadError(
            StateErrorCode.READ_FAILED,
            filename,
            detail=str(error),
        ) from error
    return _parse_json_document(
        serialized_document,
        filename,
        reject_null=reject_null,
    )


async def _save_json_to_gcs(
    gcs_client,
    filename: str,
    data: JsonDocument,
) -> None:
    """Replace one GCS JSON object."""
    try:
        blob = gcs_client.bucket(config.GCS_BUCKET_NAME).blob(filename)
        serialized_document = json.dumps(data, ensure_ascii=False, indent=2)
        await asyncio.to_thread(
            blob.upload_from_string,
            serialized_document,
            content_type="application/json",
        )
    except Exception:
        logger.exception("Failed to save GCS state document: %s", filename)
        raise


async def _load_json_from_local(
    filepath: str,
    *,
    reject_null: bool = False,
) -> JsonDocument:
    """Load one local JSON document; return None when it does not exist."""
    if not os.path.exists(filepath):
        return None
    try:
        async with aiofiles.open(filepath, encoding="utf-8") as state_file:
            serialized_document = await state_file.read()
    except Exception as error:
        raise StateLoadError(
            StateErrorCode.READ_FAILED,
            filepath,
            detail=str(error),
        ) from error
    return _parse_json_document(
        serialized_document,
        filepath,
        reject_null=reject_null,
    )


async def _save_json_to_local(
    filepath: str,
    data: JsonDocument,
) -> None:
    """Atomically replace one local JSON document."""
    temporary_path: str | None = None
    try:
        directory = os.path.dirname(filepath)
        os.makedirs(directory, exist_ok=True)
        serialized_document = json.dumps(data, ensure_ascii=False, indent=2)
        descriptor, temporary_path = tempfile.mkstemp(
            dir=directory,
            prefix=f".{os.path.basename(filepath)}.",
            suffix=".tmp",
        )
        os.close(descriptor)
        async with aiofiles.open(
            temporary_path,
            mode="w",
            encoding="utf-8",
        ) as temporary_file:
            await temporary_file.write(serialized_document)
            await temporary_file.flush()
            await asyncio.to_thread(os.fsync, temporary_file.fileno())
        os.replace(temporary_path, filepath)
        temporary_path = None
    except Exception:
        logger.exception("Failed to save local state document: %s", filepath)
        raise
    finally:
        if temporary_path and os.path.exists(temporary_path):
            os.unlink(temporary_path)


def create_gcs_client():
    """Create a GCS client with the configured project when available."""
    from google.cloud import storage

    return storage.Client(project=config.GCP_PROJECT_ID or None)

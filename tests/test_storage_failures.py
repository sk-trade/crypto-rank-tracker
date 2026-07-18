import asyncio
import datetime
from unittest.mock import AsyncMock, Mock

import pytest

import config
from common import state_manager, storage_client
from common.models import AnalysisState
from common.storage_client import (
    StateBackendUnavailable,
    StateErrorCode,
    StateLoadError,
    StateSaveError,
    _load_json_from_gcs,
    _load_json_from_local,
    create_gcs_client,
    load_json,
    save_json,
)


class _Blob:
    def __init__(self, *, exists_error=None, download_error=None, upload_error=None):
        self.exists_error, self.download_error, self.upload_error = exists_error, download_error, upload_error

    def exists(self):
        if self.exists_error:
            raise self.exists_error
        return True

    def download_as_text(self):
        if self.download_error:
            raise self.download_error
        return "{}"

    def upload_from_string(self, *_args, **_kwargs):
        if self.upload_error:
            raise self.upload_error


class _Client:
    def __init__(self, blob):
        self._blob = blob

    def bucket(self, _name):
        return self

    def blob(self, _name):
        return self._blob


def test_gcs_exists_and_download_failures_are_explicit_state_load_errors():
    with pytest.raises(StateLoadError) as exists_error:
        asyncio.run(_load_json_from_gcs(_Client(_Blob(exists_error=RuntimeError("no access"))), "x.json"))
    assert exists_error.value.code is StateErrorCode.READ_FAILED

    with pytest.raises(StateLoadError) as download_error:
        asyncio.run(_load_json_from_gcs(_Client(_Blob(download_error=RuntimeError("read failed"))), "x.json"))
    assert download_error.value.code is StateErrorCode.READ_FAILED


def test_existing_json_null_can_be_rejected_without_treating_missing_as_corrupt(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text("null", encoding="utf-8")

    assert asyncio.run(_load_json_from_local(str(tmp_path / "missing.json"), reject_null=True)) is None
    with pytest.raises(StateLoadError) as error:
        asyncio.run(_load_json_from_local(str(state_path), reject_null=True))
    assert error.value.code is StateErrorCode.NULL_DOCUMENT


def test_gcs_json_null_can_be_rejected_explicitly():
    blob = _Blob()
    blob.download_as_text = lambda: "null"

    with pytest.raises(StateLoadError) as error:
        asyncio.run(_load_json_from_gcs(_Client(blob), "state.json", reject_null=True))
    assert error.value.code is StateErrorCode.NULL_DOCUMENT


def test_gcs_upload_failure_propagates_to_the_caller(monkeypatch):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")

    with pytest.raises(StateSaveError) as error:
        asyncio.run(
            save_json(
                "x.json",
                {},
                _Client(_Blob(upload_error=RuntimeError("write failed"))),
            )
        )
    assert error.value.code is StateErrorCode.WRITE_FAILED


def test_gcs_mode_never_silently_falls_back_to_local_storage(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))

    with pytest.raises(StateBackendUnavailable) as load_error:
        asyncio.run(load_json("state.json"))
    assert load_error.value.code is StateErrorCode.BACKEND_UNAVAILABLE

    with pytest.raises(StateBackendUnavailable) as save_error:
        asyncio.run(save_json("state.json", {"unexpected": "local write"}))
    assert save_error.value.code is StateErrorCode.BACKEND_UNAVAILABLE

    assert not (tmp_path / "state.json").exists()


def test_local_save_keeps_the_previous_complete_state_if_atomic_replace_fails(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    state_file = tmp_path / "state.json"
    state_file.write_text('{"previous": true}', encoding="utf-8")

    def fail_replace(_source, _destination):
        raise OSError("replace failed")

    monkeypatch.setattr(storage_client.os, "replace", fail_replace)

    with pytest.raises(StateSaveError) as error:
        asyncio.run(save_json("state.json", {"new": True}))

    assert error.value.code is StateErrorCode.WRITE_FAILED
    assert state_file.read_text(encoding="utf-8") == '{"previous": true}'
    assert not list(tmp_path.glob(".state.json.*.tmp"))


def test_gcs_client_uses_explicit_project_when_configured(monkeypatch):
    from google.cloud import storage

    client = object()
    monkeypatch.setattr(config, "GCP_PROJECT_ID", "project-id")
    client_factory = Mock(return_value=client)
    monkeypatch.setattr(storage, "Client", client_factory)

    assert create_gcs_client() is client
    client_factory.assert_called_once_with(project="project-id")


def test_analysis_log_rejects_corrupt_existing_shape_without_overwriting(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    filename = state_manager.get_daily_log_filename()
    state_file = tmp_path / filename
    state_file.write_text('{"unexpected": true}', encoding="utf-8")
    state = AnalysisState(
        last_updated=datetime.datetime.now(datetime.timezone.utc), tickers={}
    )

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.save_analysis_log(state))

    assert error.value.code is StateErrorCode.INVALID_SCHEMA
    assert state_file.read_text(encoding="utf-8") == '{"unexpected": true}'


def test_analysis_log_propagates_typed_write_failure(monkeypatch):
    state = AnalysisState(
        last_updated=datetime.datetime.now(datetime.timezone.utc), tickers={}
    )
    write_error = StateSaveError(StateErrorCode.WRITE_FAILED, "analysis.json")
    monkeypatch.setattr(state_manager, "load_json", AsyncMock(return_value=None))
    monkeypatch.setattr(
        state_manager, "save_json", AsyncMock(side_effect=write_error)
    )

    with pytest.raises(StateSaveError) as error:
        asyncio.run(state_manager.save_analysis_log(state))

    assert error.value is write_error


def test_log_cleanup_never_falls_back_to_local_in_gcs_mode(monkeypatch):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    local_cleanup = Mock(side_effect=AssertionError("local cleanup must not run"))
    monkeypatch.setattr(state_manager, "_cleanup_local_logs", local_cleanup)

    with pytest.raises(StateBackendUnavailable) as error:
        asyncio.run(state_manager.cleanup_old_logs())

    assert error.value.code is StateErrorCode.BACKEND_UNAVAILABLE
    local_cleanup.assert_not_called()

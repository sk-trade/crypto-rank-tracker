import asyncio
from unittest.mock import Mock

import pytest

import config
from common import storage_client
from common.storage_client import (
    StateBackendUnavailable,
    StateLoadError,
    _load_json_from_gcs,
    _save_json_to_gcs,
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
    with pytest.raises(StateLoadError):
        asyncio.run(_load_json_from_gcs(_Client(_Blob(exists_error=RuntimeError("no access"))), "x.json"))
    with pytest.raises(StateLoadError):
        asyncio.run(_load_json_from_gcs(_Client(_Blob(download_error=RuntimeError("read failed"))), "x.json"))


def test_gcs_upload_failure_propagates_to_the_caller():
    with pytest.raises(RuntimeError, match="write failed"):
        asyncio.run(_save_json_to_gcs(_Client(_Blob(upload_error=RuntimeError("write failed"))), "x.json", {}))


def test_gcs_mode_never_silently_falls_back_to_local_storage(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))

    with pytest.raises(StateBackendUnavailable, match="initialized GCS client"):
        asyncio.run(load_json("state.json"))
    with pytest.raises(StateBackendUnavailable, match="initialized GCS client"):
        asyncio.run(save_json("state.json", {"unexpected": "local write"}))

    assert not (tmp_path / "state.json").exists()


def test_local_save_keeps_the_previous_complete_state_if_atomic_replace_fails(monkeypatch, tmp_path):
    state_file = tmp_path / "state.json"
    state_file.write_text('{"previous": true}', encoding="utf-8")

    def fail_replace(_source, _destination):
        raise OSError("replace failed")

    monkeypatch.setattr(storage_client.os, "replace", fail_replace)

    with pytest.raises(OSError, match="replace failed"):
        asyncio.run(storage_client._save_json_to_local(str(state_file), {"new": True}))

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

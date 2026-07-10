import asyncio

import pytest

from common.storage_client import StateLoadError, _load_json_from_gcs, _save_json_to_gcs


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

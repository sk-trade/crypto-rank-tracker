import pytest

import config


def test_validate_storage_config_requires_bucket_for_gcs(monkeypatch):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", None)

    with pytest.raises(RuntimeError, match="GCS_BUCKET_NAME is required"):
        config.validate_storage_config()


def test_validate_storage_config_allows_local_without_bucket(monkeypatch):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", None)

    config.validate_storage_config()

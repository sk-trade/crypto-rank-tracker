import pytest

import config


def test_validate_storage_config_requires_bucket_for_gcs(monkeypatch):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", None)

    with pytest.raises(config.StorageConfigError) as error:
        config.validate_storage_config()
    assert error.value.code is config.ConfigErrorCode.GCS_BUCKET_REQUIRED
    assert error.value.field == "GCS_BUCKET_NAME"


def test_validate_storage_config_allows_local_without_bucket(monkeypatch):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", None)

    assert config.validate_storage_config() is config.StorageMethod.LOCAL


@pytest.mark.parametrize("storage_method", ["", "gcs", "LOCAL ", "filesystem"])
def test_validate_storage_config_rejects_invalid_explicit_storage_methods(monkeypatch, storage_method):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", storage_method)

    with pytest.raises(config.StorageConfigError) as error:
        config.validate_storage_config()
    assert error.value.code is config.ConfigErrorCode.INVALID_STORAGE_METHOD
    assert error.value.field == "STATE_STORAGE_METHOD"

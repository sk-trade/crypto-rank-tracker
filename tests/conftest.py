import pytest


@pytest.fixture(autouse=True)
def isolate_runtime_side_effects(monkeypatch, tmp_path):
    """Keep every test on ephemeral local state with outbound delivery disabled."""
    import config

    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "GCP_PROJECT_ID", None)
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", None)
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))

import asyncio
import json

import pytest

import config
from common import state_manager
from common.storage_client import StateBackendUnavailable


def test_local_scan_claim_is_atomic_and_preserves_execution_metadata(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))

    async def claim_twice():
        return await asyncio.gather(
            state_manager.claim_scan_key("completed-candle:2026-06-18T00:10:00+00:00", "run-a"),
            state_manager.claim_scan_key("completed-candle:2026-06-18T00:10:00+00:00", "run-b"),
        )

    claims = asyncio.run(claim_twice())

    assert sorted(claims) == [False, True]
    state = json.loads((tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME).read_text())
    assert len(state["claims"]) == 1
    assert state["claims"][0]["execution_id"] in {"run-a", "run-b"}


def test_gcs_scan_claim_requires_a_gcs_client(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))

    with pytest.raises(StateBackendUnavailable, match="initialized GCS client"):
        asyncio.run(state_manager.claim_scan_key("completed-candle:2026-06-18T00:10:00+00:00"))

    assert not (tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME).exists()


def test_local_scan_claim_can_be_released_for_retry(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "run-a")) is True
    asyncio.run(state_manager.release_scan_key(scan_key))

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "run-b")) is True


def test_gcs_claim_reads_the_same_generation_it_conditionally_updates(monkeypatch):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", "bucket")

    class Blob:
        generation = 7

        def exists(self):
            return True

        def reload(self):
            return None

        def download_as_text(self, *, if_generation_match):
            assert if_generation_match == self.generation
            return '{"claims": []}'

        def upload_from_string(self, _value, *, content_type, if_generation_match):
            assert content_type == "application/json"
            assert if_generation_match == self.generation

    blob = Blob()

    class Bucket:
        def blob(self, _filename):
            return blob

    class Client:
        def bucket(self, _name):
            return Bucket()

    assert asyncio.run(state_manager.claim_scan_key("scan", "run", Client())) is True

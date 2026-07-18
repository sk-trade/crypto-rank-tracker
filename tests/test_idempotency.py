import asyncio
import datetime
import json

import pytest
from pydantic import ValidationError

import config
from common import state_manager
from common.models import ScanClaimState
from common.storage_client import (
    StateBackendUnavailable,
    StateErrorCode,
    StateLoadError,
    StateOperationError,
)


def _duplicate_claims(kind, scan_key, retry_execution_id="retry"):
    now = datetime.datetime.now(datetime.timezone.utc)
    stale_time = now - datetime.timedelta(
        seconds=state_manager.SCAN_CLAIM_LEASE_SECONDS + 1
    )
    stale_claim = {
        "scan_key": scan_key,
        "execution_id": "old-run",
        "claimed_at": stale_time.isoformat(),
        "status": "in_progress",
    }
    fresh_retry_claim = {
        "scan_key": scan_key,
        "execution_id": retry_execution_id,
        "claimed_at": now.isoformat(),
        "status": "in_progress",
    }
    completed_claim = {
        "scan_key": scan_key,
        "execution_id": "completed-run",
        "claimed_at": now.isoformat(),
        "status": "completed",
        "completed_at": now.isoformat(),
    }
    legacy_claim = {"scan_key": scan_key}
    cases = {
        "stale_then_completed": [stale_claim, completed_claim],
        "completed_then_stale": [completed_claim, stale_claim],
        "stale_then_legacy": [stale_claim, legacy_claim],
        "legacy_then_stale": [legacy_claim, stale_claim],
        "same_execution_then_completed": [fresh_retry_claim, completed_claim],
        "completed_twice": [
            completed_claim,
            {**completed_claim, "execution_id": "completed-run-2"},
        ],
        "in_progress_twice": [
            stale_claim,
            {**stale_claim, "execution_id": "old-run-2"},
        ],
    }
    return cases[kind]


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

    with pytest.raises(StateBackendUnavailable) as error:
        asyncio.run(state_manager.claim_scan_key("completed-candle:2026-06-18T00:10:00+00:00"))

    assert error.value.code is StateErrorCode.BACKEND_UNAVAILABLE
    assert error.value.resource == state_manager.IDEMPOTENCY_STATE_FILE_NAME
    assert not (tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME).exists()


@pytest.mark.parametrize(
    ("scan_key", "execution_id", "resource"),
    [
        ("", "run-a", "scan_key"),
        (None, "run-a", "scan_key"),
        (123, "run-a", "scan_key"),
        ("valid-key", "", "execution_id"),
        ("valid-key", 123, "execution_id"),
    ],
)
def test_invalid_claim_inputs_do_not_create_local_state(
    monkeypatch, tmp_path, scan_key, execution_id, resource
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))

    with pytest.raises(StateOperationError) as error:
        asyncio.run(state_manager.claim_scan_key(scan_key, execution_id))

    assert error.value.code is StateErrorCode.INVALID_ARGUMENT
    assert error.value.resource == resource
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("scan_key", "execution_id", "resource"),
    [
        ("", "run-a", "scan_key"),
        (None, "run-a", "scan_key"),
        (123, "run-a", "scan_key"),
        ("valid-key", "", "execution_id"),
        ("valid-key", 123, "execution_id"),
    ],
)
def test_invalid_claim_inputs_do_not_access_gcs(
    monkeypatch, scan_key, execution_id, resource
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", "bucket")

    class Client:
        bucket_called = False

        def bucket(self, _name):
            self.bucket_called = True
            raise AssertionError("invalid input reached GCS")

    client = Client()

    with pytest.raises(StateOperationError) as error:
        asyncio.run(state_manager.claim_scan_key(scan_key, execution_id, client))

    assert error.value.code is StateErrorCode.INVALID_ARGUMENT
    assert error.value.resource == resource
    assert client.bucket_called is False


def test_none_execution_id_remains_a_valid_claim_input(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))

    assert asyncio.run(state_manager.claim_scan_key("valid-key", None)) is True
    state = json.loads(
        (tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME).read_text()
    )
    parsed = ScanClaimState.model_validate(state)
    assert parsed.claims[0].execution_id is None


@pytest.mark.parametrize("operation", ["complete", "release"])
def test_invalid_mutation_key_does_not_create_state(monkeypatch, tmp_path, operation):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))

    with pytest.raises(StateOperationError) as error:
        if operation == "complete":
            asyncio.run(state_manager.complete_scan_key(""))
        else:
            asyncio.run(state_manager.release_scan_key(""))

    assert error.value.code is StateErrorCode.INVALID_ARGUMENT
    assert error.value.resource == "scan_key"
    assert list(tmp_path.iterdir()) == []


def test_local_scan_claim_can_be_released_for_retry(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "run-a")) is True
    asyncio.run(state_manager.release_scan_key(scan_key))

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "run-b")) is True


def test_same_scheduler_execution_reclaims_an_in_progress_scan(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "run-a")) is True
    assert asyncio.run(state_manager.claim_scan_key(scan_key, "run-a")) is True

    state = json.loads((tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME).read_text())
    assert state["claims"][0]["status"] == "in_progress"


def test_completed_scan_key_cannot_be_reclaimed(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "run-a")) is True
    asyncio.run(state_manager.complete_scan_key(scan_key))

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "run-a")) is False
    state = json.loads((tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME).read_text())
    assert state["claims"][0]["status"] == "completed"


def test_malformed_local_claim_state_cannot_reclaim_a_completed_scan(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"
    malformed_state = [{"scan_key": scan_key, "status": "completed"}]
    state_path = tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME
    state_path.write_text(json.dumps(malformed_state), encoding="utf-8")

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.claim_scan_key(scan_key, "retry"))

    assert error.value.code is StateErrorCode.INVALID_SCHEMA
    assert error.value.resource == state_manager.IDEMPOTENCY_STATE_FILE_NAME
    assert json.loads(state_path.read_text(encoding="utf-8")) == malformed_state


@pytest.mark.parametrize(
    "claim",
    [
        {
            "scan_key": "completed-candle:2026-06-18T00:10:00+00:00",
            "status": "in_progress",
            "execution_id": "original-run",
        },
        {
            "scan_key": "completed-candle:2026-06-18T00:10:00+00:00",
            "status": "in_progress",
            "execution_id": "original-run",
            "claimed_at": "not-a-timestamp",
        },
        {
            "scan_key": "completed-candle:2026-06-18T00:10:00+00:00",
            "status": "in_progress",
            "execution_id": 123,
            "claimed_at": "2026-06-18T00:10:00+00:00",
        },
        {
            "scan_key": "completed-candle:2026-06-18T00:10:00+00:00",
            "status": "unknown",
            "execution_id": "original-run",
            "claimed_at": "2026-06-18T00:10:00+00:00",
        },
    ],
)
def test_malformed_active_claim_cannot_be_reassigned(monkeypatch, tmp_path, claim):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    state_path = tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME
    original_state = {"claims": [claim]}
    state_path.write_text(json.dumps(original_state), encoding="utf-8")

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.claim_scan_key(claim["scan_key"], "different-run"))

    assert error.value.code is StateErrorCode.INVALID_SCHEMA
    assert json.loads(state_path.read_text(encoding="utf-8")) == original_state


@pytest.mark.parametrize(
    "duplicate_kind",
    [
        "stale_then_completed",
        "completed_then_stale",
        "stale_then_legacy",
        "legacy_then_stale",
        "same_execution_then_completed",
        "completed_twice",
        "in_progress_twice",
    ],
)
def test_duplicate_local_claim_state_is_rejected_and_preserved(
    monkeypatch, tmp_path, duplicate_kind
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"
    original_text = json.dumps(
        {"claims": _duplicate_claims(duplicate_kind, scan_key)}
    )
    state_path = tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME
    state_path.write_text(original_text, encoding="utf-8")

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.claim_scan_key(scan_key, "retry"))

    assert error.value.code is StateErrorCode.INVALID_SCHEMA
    assert state_path.read_text(encoding="utf-8") == original_text


@pytest.mark.parametrize("operation", ["complete", "release"])
def test_duplicate_local_claim_state_blocks_all_mutations(
    monkeypatch, tmp_path, operation
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"
    original_text = json.dumps(
        {"claims": _duplicate_claims("stale_then_completed", scan_key)}
    )
    state_path = tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME
    state_path.write_text(original_text, encoding="utf-8")

    with pytest.raises(StateLoadError) as error:
        if operation == "complete":
            asyncio.run(state_manager.complete_scan_key(scan_key))
        else:
            asyncio.run(state_manager.release_scan_key(scan_key))

    assert error.value.code is StateErrorCode.INVALID_SCHEMA
    assert state_path.read_text(encoding="utf-8") == original_text


def test_distinct_local_claim_records_remain_valid(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    state_path = tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME
    state_path.write_text(
        json.dumps(
            {
                "claims": [
                    {
                        "scan_key": "completed-candle:2026-06-18T00:00:00+00:00",
                        "status": "completed",
                        "completed_at": now,
                    },
                    {"scan_key": "completed-candle:2026-06-18T00:10:00+00:00"},
                ]
            }
        ),
        encoding="utf-8",
    )

    assert asyncio.run(state_manager.claim_scan_key("new-scan", "run")) is True


def test_legacy_claim_without_status_remains_a_completed_marker(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"
    state_path = tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME
    state_path.write_text(json.dumps({"claims": [{"scan_key": scan_key}]}), encoding="utf-8")

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "retry")) is False


def test_legacy_claim_shape_is_accepted_only_at_the_persistence_boundary():
    with pytest.raises(ValidationError):
        ScanClaimState.model_validate({"claims": [{"scan_key": "legacy-scan"}]})


def test_stale_in_progress_scan_can_be_reclaimed(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    scan_key = "completed-candle:2026-06-18T00:10:00+00:00"
    stale_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        seconds=state_manager.SCAN_CLAIM_LEASE_SECONDS + 1
    )
    (tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME).write_text(
        json.dumps(
            {
                "claims": [
                    {
                        "scan_key": scan_key,
                        "execution_id": "old-run",
                        "claimed_at": stale_time.isoformat(),
                        "status": "in_progress",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    assert asyncio.run(state_manager.claim_scan_key(scan_key, "new-run")) is True
    state = json.loads((tmp_path / state_manager.IDEMPOTENCY_STATE_FILE_NAME).read_text())
    assert state["claims"][0]["execution_id"] == "new-run"
    assert state["claims"][0]["status"] == "in_progress"


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


@pytest.mark.parametrize(
    ("raw_state", "expected_code"),
    [
        ("[]", StateErrorCode.INVALID_SCHEMA),
        (
            json.dumps(
                {
                    "claims": [
                        {
                            "scan_key": "scan",
                            "status": "in_progress",
                            "execution_id": "original-run",
                            "claimed_at": "not-a-timestamp",
                        }
                    ]
                }
            ),
            StateErrorCode.INVALID_SCHEMA,
        ),
        ("{", StateErrorCode.INVALID_JSON),
    ],
)
def test_malformed_gcs_claim_state_is_not_overwritten(
    monkeypatch, raw_state, expected_code
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", "bucket")

    class Blob:
        generation = 7
        uploaded = False

        def exists(self):
            return True

        def reload(self):
            return None

        def download_as_text(self, *, if_generation_match):
            assert if_generation_match == self.generation
            return raw_state

        def upload_from_string(self, _value, *, content_type, if_generation_match):
            self.uploaded = True

    blob = Blob()

    class Bucket:
        def blob(self, _filename):
            return blob

    class Client:
        def bucket(self, _name):
            return Bucket()

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.claim_scan_key("scan", "run", Client()))

    assert error.value.code is expected_code
    assert error.value.resource == state_manager.IDEMPOTENCY_STATE_FILE_NAME
    assert blob.uploaded is False


@pytest.mark.parametrize("operation", ["claim", "complete", "release"])
def test_duplicate_gcs_claim_state_blocks_all_mutations(monkeypatch, operation):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "GCS")
    monkeypatch.setattr(config, "GCS_BUCKET_NAME", "bucket")
    scan_key = "scan"
    raw_state = json.dumps(
        {"claims": _duplicate_claims("stale_then_completed", scan_key)}
    )

    class Blob:
        generation = 7
        uploaded = False

        def exists(self):
            return True

        def reload(self):
            return None

        def download_as_text(self, *, if_generation_match):
            assert if_generation_match == self.generation
            return raw_state

        def upload_from_string(self, _value, *, content_type, if_generation_match):
            self.uploaded = True

    blob = Blob()

    class Bucket:
        def blob(self, _filename):
            return blob

    class Client:
        def bucket(self, _name):
            return Bucket()

    with pytest.raises(StateLoadError) as error:
        if operation == "claim":
            asyncio.run(state_manager.claim_scan_key(scan_key, "retry", Client()))
        elif operation == "complete":
            asyncio.run(state_manager.complete_scan_key(scan_key, Client()))
        else:
            asyncio.run(state_manager.release_scan_key(scan_key, Client()))

    assert error.value.code is StateErrorCode.INVALID_SCHEMA
    assert blob.uploaded is False

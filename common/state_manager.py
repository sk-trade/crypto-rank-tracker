#common/state_manager
import asyncio
import datetime
import fcntl
import json
import logging
import os
from typing import Any, Callable, Dict, List, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field, ValidationError

import config
from common.models import (
    AlertHistory,
    AnalysisState,
    AttentionState,
    NotificationBacklog,
    NotificationOutbox,
    RankState,
    ScanClaim,
    ScanClaimState,
    ScanClaimStatus,
    ScanEvent,
    ScanOutcome,
    SignalType,
    StructureDirection,
)
from common.storage_client import (
    StateBackendUnavailable,
    StateErrorCode,
    StateLoadError,
    StateOperationError,
    StateSaveError,
    load_json,
    save_json,
)

logger = logging.getLogger(config.APP_LOGGER_NAME)
IDEMPOTENCY_STATE_FILE_NAME = "processed_scan_keys.json"
NOTIFICATION_OUTBOX_FILE_NAME = "notification_outbox.json"
NOTIFICATION_BACKLOG_FILE_NAME = "notification_backlog.json"
SCAN_CLAIM_LEASE_SECONDS = 600
PersistedRecord = TypeVar("PersistedRecord", bound=BaseModel)


class _LegacyCompletedScanClaim(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    scan_key: str = Field(min_length=1)


# --- 순위 상태 관리 ---


def _ordered_rank_states(states: List[RankState]) -> List[RankState]:
    states_by_timestamp = {state.last_updated: state for state in states}
    return sorted(states_by_timestamp.values(), key=lambda state: state.last_updated)


async def load_rank_state_history(gcs_client=None) -> List[RankState]:
    """'순위' 상태 히스토리 리스트를 로드합니다."""
    filename = config.RANK_STATE_FILE_NAME
    data = await load_json(filename, gcs_client, reject_null=True)

    if data is None:
        logger.info(f"순위 상태 파일({filename})이 없어 초기 상태로 시작합니다.")
        return []

    # 과거 호환성을 위해 단일 객체로 저장된 경우 리스트로 변환
    if isinstance(data, dict):
        data = [data]
    elif not isinstance(data, list):
        raise StateLoadError(
            StateErrorCode.INVALID_SCHEMA, filename
        )

    try:
        return _ordered_rank_states([RankState.model_validate(s) for s in data])
    except ValidationError as error:
        raise StateLoadError(StateErrorCode.INVALID_SCHEMA, filename) from error


async def save_rank_state_history(
    new_state: RankState, old_states: List[RankState], gcs_client=None
):
    """새로운 '순위' 상태를 히스토리에 추가하여 저장합니다."""
    filename = config.RANK_STATE_FILE_NAME

    updated_states = _ordered_rank_states([*old_states, new_state])
    if len(updated_states) > config.STATE_HISTORY_COUNT:
        updated_states = updated_states[-config.STATE_HISTORY_COUNT:]

    data_to_save = [s.model_dump(mode="json") for s in updated_states]
    await save_json(filename, data_to_save, gcs_client)
    logger.info(f"순위 히스토리 저장 완료: {filename}")


async def load_attention_state(gcs_client=None) -> AttentionState:
    """Load candidate progression independently from notification delivery state."""
    filename = config.ATTENTION_STATE_FILE_NAME
    data = await load_json(filename, gcs_client, reject_null=True)
    if data is None:
        return AttentionState()
    try:
        return AttentionState.model_validate(data)
    except ValidationError as error:
        raise StateLoadError(StateErrorCode.INVALID_SCHEMA, filename) from error


async def save_attention_state(state: AttentionState, gcs_client=None) -> None:
    """Persist the latest bounded attention episodes even when no webhook is configured."""
    await save_json(
        config.ATTENTION_STATE_FILE_NAME,
        state.model_dump(mode="json"),
        gcs_client,
    )


# --- 분석 로그 관리 ---


def get_daily_log_filename() -> str:
    """오늘 날짜 기반의 로그 파일 이름을 반환합니다."""
    return _get_daily_filename("analysis_log")


def _get_daily_filename(prefix: str) -> str:
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    return f"{prefix}_{today}.json"


def _get_hourly_filename(prefix: str, timestamp: datetime.datetime) -> str:
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise StateOperationError(
            StateErrorCode.INVALID_ARGUMENT, f"{prefix}_timestamp"
        )
    hour = timestamp.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H")
    return f"{prefix}_{hour}Z.json"


def _validate_record_list(
    filename: str, data: Any, record_type: type[PersistedRecord]
) -> List[PersistedRecord]:
    if data is None:
        return []
    if not isinstance(data, list):
        raise StateLoadError(StateErrorCode.INVALID_SCHEMA, filename)
    try:
        return [record_type.model_validate(item) for item in data]
    except ValidationError as error:
        raise StateLoadError(StateErrorCode.INVALID_SCHEMA, filename) from error


async def _append_records(
    filename: str,
    records: List[PersistedRecord],
    record_type: type[PersistedRecord],
    gcs_client=None,
) -> List[str]:
    existing = await load_json(filename, gcs_client, reject_null=True)
    existing_records = _validate_record_list(filename, existing, record_type)
    records_by_id = {
        item.event_id: item.model_dump(mode="json") for item in existing_records
    }
    incoming_by_id = {
        record.event_id: record.model_dump(mode="json") for record in records
    }
    conflicting_ids = []
    for event_id, payload in incoming_by_id.items():
        existing_payload = records_by_id.get(event_id)
        if existing_payload is None:
            continue
        elif existing_payload != payload:
            conflicting_ids.append(event_id)
    if conflicting_ids:
        logger.error(
            "Preserved %d immutable record(s) in %s after conflicting retry data: %s",
            len(conflicting_ids),
            filename,
            ", ".join(conflicting_ids[:10]),
        )
    else:
        records_by_id.update(incoming_by_id)
    await save_json(filename, list(records_by_id.values()), gcs_client)
    return conflicting_ids


async def _append_partitioned_records(
    prefix: str,
    records: List[PersistedRecord],
    record_type: type[PersistedRecord],
    timestamp_for_record: Callable[[PersistedRecord], datetime.datetime],
    gcs_client=None,
) -> List[str]:
    records_by_file: Dict[str, List[PersistedRecord]] = {}
    for record in records:
        filename = _get_hourly_filename(prefix, timestamp_for_record(record))
        records_by_file.setdefault(filename, []).append(record)

    conflicting_ids = []
    for filename in sorted(records_by_file):
        conflicting_ids.extend(
            await _append_records(
                filename, records_by_file[filename], record_type, gcs_client
            )
        )
    return conflicting_ids


async def append_scan_events(events: List[ScanEvent], gcs_client=None):
    return await _append_partitioned_records(
        "scan_events",
        events,
        ScanEvent,
        lambda event: event.observed_at,
        gcs_client,
    )


async def append_scan_outcomes(outcomes: List[ScanOutcome], gcs_client=None):
    return await _append_partitioned_records(
        "scan_outcomes",
        outcomes,
        ScanOutcome,
        lambda outcome: outcome.exit_candle_start,
        gcs_client,
    )


async def load_pending_scan_events(gcs_client=None) -> List[ScanEvent]:
    filename = "pending_scan_events.json"
    data = await load_json(filename, gcs_client, reject_null=True)
    return _validate_record_list(filename, data, ScanEvent)


async def save_pending_scan_events(events: List[ScanEvent], gcs_client=None):
    events_by_id = {event.event_id: event for event in events}
    await save_json(
        "pending_scan_events.json",
        [event.model_dump(mode="json") for event in events_by_id.values()],
        gcs_client,
    )


def _validate_scan_key(scan_key: Any) -> str:
    if not isinstance(scan_key, str) or not scan_key:
        raise StateOperationError(StateErrorCode.INVALID_ARGUMENT, "scan_key")
    return scan_key


def _validate_execution_id(execution_id: Any) -> str | None:
    if execution_id is not None and (
        not isinstance(execution_id, str) or not execution_id
    ):
        raise StateOperationError(StateErrorCode.INVALID_ARGUMENT, "execution_id")
    return execution_id


async def claim_scan_key(scan_key: str, execution_id: str | None = None, gcs_client=None) -> bool:
    """Atomically acquire or resume an in-progress completed-candle scan."""
    scan_key = _validate_scan_key(scan_key)
    execution_id = _validate_execution_id(execution_id)
    if config.storage_method() is config.StorageMethod.GCS:
        if gcs_client is None:
            raise StateBackendUnavailable(
                StateErrorCode.BACKEND_UNAVAILABLE, IDEMPOTENCY_STATE_FILE_NAME
            )
        return await _claim_scan_key_in_gcs(scan_key, execution_id, gcs_client)
    return await asyncio.to_thread(_claim_scan_key_locally, scan_key, execution_id)


async def complete_scan_key(scan_key: str, gcs_client=None) -> None:
    """Mark a claimed scan complete after its durable state or outbox handoff exists."""
    scan_key = _validate_scan_key(scan_key)
    if config.storage_method() is config.StorageMethod.GCS:
        if gcs_client is None:
            raise StateBackendUnavailable(
                StateErrorCode.BACKEND_UNAVAILABLE, IDEMPOTENCY_STATE_FILE_NAME
            )
        await _complete_scan_key_in_gcs(scan_key, gcs_client)
        return
    await asyncio.to_thread(_complete_scan_key_locally, scan_key)


async def release_scan_key(scan_key: str, gcs_client=None) -> None:
    """Release a claim when a scan fails before external notification begins."""
    scan_key = _validate_scan_key(scan_key)
    if config.storage_method() is config.StorageMethod.GCS:
        if gcs_client is None:
            raise StateBackendUnavailable(
                StateErrorCode.BACKEND_UNAVAILABLE, IDEMPOTENCY_STATE_FILE_NAME
            )
        await _release_scan_key_in_gcs(scan_key, gcs_client)
        return
    await asyncio.to_thread(_release_scan_key_locally, scan_key)


def _parse_scan_claim_state(state: Any) -> ScanClaimState:
    try:
        if isinstance(state, dict) and isinstance(state.get("claims"), list):
            claims = []
            for claim in state["claims"]:
                if isinstance(claim, dict) and "status" not in claim:
                    legacy = _LegacyCompletedScanClaim.model_validate(claim)
                    claims.append(
                        ScanClaim(
                            scan_key=legacy.scan_key,
                            status=ScanClaimStatus.COMPLETED,
                        )
                    )
                else:
                    claims.append(claim)
            state = {**state, "claims": claims}
        return ScanClaimState.model_validate(state)
    except ValidationError as error:
        raise StateLoadError(
            StateErrorCode.INVALID_SCHEMA, IDEMPOTENCY_STATE_FILE_NAME
        ) from error


def _decode_scan_claim_state(raw_state: str) -> ScanClaimState:
    try:
        state = json.loads(raw_state)
    except json.JSONDecodeError as error:
        raise StateLoadError(
            StateErrorCode.INVALID_JSON, IDEMPOTENCY_STATE_FILE_NAME
        ) from error
    return _parse_scan_claim_state(state)


def _claim_can_resume(
    claim: ScanClaim, execution_id: str | None, now: datetime.datetime
) -> bool:
    if claim.status is not ScanClaimStatus.IN_PROGRESS:
        return False
    if execution_id and claim.execution_id == execution_id:
        return True
    return (now - claim.claimed_at).total_seconds() > SCAN_CLAIM_LEASE_SECONDS


def _acquire_claim(
    state: ScanClaimState, scan_key: str, execution_id: str | None
) -> tuple[bool, ScanClaimState]:
    now = datetime.datetime.now(datetime.timezone.utc)
    for claim in state.claims:
        if claim.scan_key != scan_key:
            continue
        if not _claim_can_resume(claim, execution_id, now):
            return False, state
        claim.execution_id = execution_id
        claim.claimed_at = now
        claim.status = ScanClaimStatus.IN_PROGRESS
        claim.completed_at = None
        return True, state
    state.claims.append(
        ScanClaim(
            scan_key=scan_key,
            execution_id=execution_id,
            claimed_at=now,
            status=ScanClaimStatus.IN_PROGRESS,
        )
    )
    return True, state


def _claim_scan_key_locally(scan_key: str, execution_id: str | None) -> bool:
    os.makedirs(config.LOCAL_STATE_DIR, exist_ok=True)
    lock_path = os.path.join(config.LOCAL_STATE_DIR, f"{IDEMPOTENCY_STATE_FILE_NAME}.lock")
    state_path = os.path.join(config.LOCAL_STATE_DIR, IDEMPOTENCY_STATE_FILE_NAME)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            try:
                with open(state_path, encoding="utf-8") as state_file:
                    state = _decode_scan_claim_state(state_file.read())
            except FileNotFoundError:
                state = ScanClaimState()
            acquired, state = _acquire_claim(state, scan_key, execution_id)
            if not acquired:
                return False
            state.claims = state.claims[-config.IDEMPOTENCY_KEY_HISTORY_LIMIT :]
            temporary_path = f"{state_path}.tmp"
            with open(temporary_path, "w", encoding="utf-8") as state_file:
                json.dump(state.model_dump(mode="json"), state_file, ensure_ascii=False)
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(temporary_path, state_path)
            return True
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _complete_scan_key_locally(scan_key: str) -> None:
    os.makedirs(config.LOCAL_STATE_DIR, exist_ok=True)
    lock_path = os.path.join(config.LOCAL_STATE_DIR, f"{IDEMPOTENCY_STATE_FILE_NAME}.lock")
    state_path = os.path.join(config.LOCAL_STATE_DIR, IDEMPOTENCY_STATE_FILE_NAME)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            try:
                with open(state_path, encoding="utf-8") as state_file:
                    state = _decode_scan_claim_state(state_file.read())
            except FileNotFoundError:
                return
            claim = next(
                (claim for claim in state.claims if claim.scan_key == scan_key), None
            )
            if claim is None:
                return
            claim.status = ScanClaimStatus.COMPLETED
            claim.completed_at = datetime.datetime.now(datetime.timezone.utc)
            temporary_path = f"{state_path}.tmp"
            with open(temporary_path, "w", encoding="utf-8") as state_file:
                json.dump(state.model_dump(mode="json"), state_file, ensure_ascii=False)
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(temporary_path, state_path)
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _release_scan_key_locally(scan_key: str) -> None:
    os.makedirs(config.LOCAL_STATE_DIR, exist_ok=True)
    lock_path = os.path.join(config.LOCAL_STATE_DIR, f"{IDEMPOTENCY_STATE_FILE_NAME}.lock")
    state_path = os.path.join(config.LOCAL_STATE_DIR, IDEMPOTENCY_STATE_FILE_NAME)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            try:
                with open(state_path, encoding="utf-8") as state_file:
                    state = _decode_scan_claim_state(state_file.read())
            except FileNotFoundError:
                return
            state.claims = [
                claim for claim in state.claims if claim.scan_key != scan_key
            ]
            temporary_path = f"{state_path}.tmp"
            with open(temporary_path, "w", encoding="utf-8") as state_file:
                json.dump(state.model_dump(mode="json"), state_file, ensure_ascii=False)
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(temporary_path, state_path)
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


async def _claim_scan_key_in_gcs(scan_key: str, execution_id: str | None, gcs_client) -> bool:
    """Use object generations so competing Cloud Function instances cannot both claim a key."""
    try:
        from google.api_core.exceptions import PreconditionFailed
    except ImportError as error:
        raise StateBackendUnavailable(
            StateErrorCode.BACKEND_UNAVAILABLE, IDEMPOTENCY_STATE_FILE_NAME
        ) from error

    blob = gcs_client.bucket(config.GCS_BUCKET_NAME).blob(IDEMPOTENCY_STATE_FILE_NAME)
    for _ in range(5):
        exists = await asyncio.to_thread(blob.exists)
        if exists:
            await asyncio.to_thread(blob.reload)
            generation = int(blob.generation)
            try:
                raw_state = await asyncio.to_thread(
                    blob.download_as_text, if_generation_match=generation
                )
            except PreconditionFailed:
                continue
            state = _decode_scan_claim_state(raw_state)
        else:
            state = ScanClaimState()
            generation = 0
        acquired, state = _acquire_claim(state, scan_key, execution_id)
        if not acquired:
            return False
        state.claims = state.claims[-config.IDEMPOTENCY_KEY_HISTORY_LIMIT :]
        try:
            await asyncio.to_thread(
                blob.upload_from_string,
                json.dumps(state.model_dump(mode="json"), ensure_ascii=False),
                content_type="application/json",
                if_generation_match=generation,
            )
            return True
        except PreconditionFailed:
            continue
    raise StateSaveError(
        StateErrorCode.CONCURRENT_UPDATE_EXHAUSTED, IDEMPOTENCY_STATE_FILE_NAME
    )


async def _complete_scan_key_in_gcs(scan_key: str, gcs_client) -> None:
    try:
        from google.api_core.exceptions import PreconditionFailed
    except ImportError as error:
        raise StateBackendUnavailable(
            StateErrorCode.BACKEND_UNAVAILABLE, IDEMPOTENCY_STATE_FILE_NAME
        ) from error

    blob = gcs_client.bucket(config.GCS_BUCKET_NAME).blob(IDEMPOTENCY_STATE_FILE_NAME)
    for _ in range(5):
        if not await asyncio.to_thread(blob.exists):
            return
        await asyncio.to_thread(blob.reload)
        generation = int(blob.generation)
        try:
            raw_state = await asyncio.to_thread(
                blob.download_as_text, if_generation_match=generation
            )
        except PreconditionFailed:
            continue
        state = _decode_scan_claim_state(raw_state)
        claim = next(
            (claim for claim in state.claims if claim.scan_key == scan_key), None
        )
        if claim is None:
            return
        claim.status = ScanClaimStatus.COMPLETED
        claim.completed_at = datetime.datetime.now(datetime.timezone.utc)
        try:
            await asyncio.to_thread(
                blob.upload_from_string,
                json.dumps(state.model_dump(mode="json"), ensure_ascii=False),
                content_type="application/json",
                if_generation_match=generation,
            )
            return
        except PreconditionFailed:
            continue
    raise StateSaveError(
        StateErrorCode.CONCURRENT_UPDATE_EXHAUSTED, IDEMPOTENCY_STATE_FILE_NAME
    )


async def _release_scan_key_in_gcs(scan_key: str, gcs_client) -> None:
    try:
        from google.api_core.exceptions import PreconditionFailed
    except ImportError as error:
        raise StateBackendUnavailable(
            StateErrorCode.BACKEND_UNAVAILABLE, IDEMPOTENCY_STATE_FILE_NAME
        ) from error

    blob = gcs_client.bucket(config.GCS_BUCKET_NAME).blob(IDEMPOTENCY_STATE_FILE_NAME)
    for _ in range(5):
        if not await asyncio.to_thread(blob.exists):
            return
        await asyncio.to_thread(blob.reload)
        generation = int(blob.generation)
        try:
            raw_state = await asyncio.to_thread(
                blob.download_as_text, if_generation_match=generation
            )
        except PreconditionFailed:
            continue
        state = _decode_scan_claim_state(raw_state)
        state.claims = [
            claim for claim in state.claims if claim.scan_key != scan_key
        ]
        try:
            await asyncio.to_thread(
                blob.upload_from_string,
                json.dumps(state.model_dump(mode="json"), ensure_ascii=False),
                content_type="application/json",
                if_generation_match=generation,
            )
            return
        except PreconditionFailed:
            continue
    raise StateSaveError(
        StateErrorCode.CONCURRENT_UPDATE_EXHAUSTED, IDEMPOTENCY_STATE_FILE_NAME
    )


async def save_analysis_log(state: AnalysisState, gcs_client=None):
    """현재 분석 결과를 오늘 날짜의 로그 파일에 추가(append)합니다."""
    filename = get_daily_log_filename()
    # candle_history는 용량이 크므로 로그에서 제외
    new_log_entry = state.model_dump(
        mode="json", exclude={"tickers": {"__all__": {"candle_history"}}}
    )

    log_list = await load_json(filename, gcs_client, reject_null=True)
    if log_list is None:
        log_list = []
    elif not isinstance(log_list, list):
        raise StateLoadError(StateErrorCode.INVALID_SCHEMA, filename)

    log_list.append(new_log_entry)
    await save_json(filename, log_list, gcs_client)
    logger.info(f"분석 로그 추가 완료: {filename}")


# --- 알림 히스토리 관리 ---


_LEGACY_ALERT_SIGNAL_TYPES = {
    "BULL_MOMENTUM_SUSTAINED": SignalType.MOMENTUM_ACCELERATION,
    "BEAR_MOMENTUM_SUSTAINED": SignalType.DOWNTREND_ACCELERATION,
}
_STRUCTURED_ALERT_DIRECTIONS = {
    SignalType.BREAKOUT_START: StructureDirection.BULLISH,
    SignalType.MOMENTUM_ACCELERATION: StructureDirection.BULLISH,
    SignalType.BREAKDOWN_START: StructureDirection.BEARISH,
    SignalType.DOWNTREND_ACCELERATION: StructureDirection.BEARISH,
}


def _migrate_alert_history_payload(
    data: Dict[str, Any],
) -> tuple[Dict[str, Any], bool]:
    migrated_data: Dict[str, Any] = {}
    migrated = False

    for market, raw_alert_data in data.items():
        if not isinstance(raw_alert_data, dict):
            migrated_data[market] = raw_alert_data
            continue

        alert_data = dict(raw_alert_data)
        raw_signal_type = alert_data.get("last_signal_type")
        legacy_replacement = (
            _LEGACY_ALERT_SIGNAL_TYPES.get(raw_signal_type)
            if isinstance(raw_signal_type, str)
            else None
        )
        if legacy_replacement is not None:
            alert_data["last_signal_type"] = legacy_replacement.value
            migrated = True

        try:
            signal_type = SignalType(alert_data.get("last_signal_type"))
        except (TypeError, ValueError):
            signal_type = None

        missing_structure = (
            "structure_level" not in alert_data
            and "structure_direction" not in alert_data
        )
        if missing_structure:
            direction = _STRUCTURED_ALERT_DIRECTIONS.get(signal_type)
            if direction is not None:
                alert_data["structure_level"] = alert_data.get("initial_price")
                alert_data["structure_direction"] = direction.value
                migrated = True
            elif signal_type is not None and signal_type.is_failure:
                alert_data["structure_level"] = None
                alert_data["structure_direction"] = None
                migrated = True

        migrated_data[market] = alert_data

    return migrated_data, migrated


def _active_alert_history(
    history: Dict[str, AlertHistory], now: Optional[datetime.datetime] = None
) -> Dict[str, AlertHistory]:
    cutoff = (now or datetime.datetime.now(datetime.timezone.utc)) - datetime.timedelta(hours=24)
    return {
        market: alert
        for market, alert in history.items()
        if alert.last_alert_timestamp > cutoff
    }


def _alert_history_file_name() -> str:
    return (
        config.SHADOW_ALERT_HISTORY_FILE_NAME
        if config.SHADOW_MODE
        else config.ALERT_HISTORY_FILE_NAME
    )


async def load_alert_history(gcs_client=None) -> Dict[str, AlertHistory]:
    """알림 히스토리 파일을 로드합니다."""
    filename = _alert_history_file_name()
    data = await load_json(filename, gcs_client, reject_null=True)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise StateLoadError(StateErrorCode.INVALID_SCHEMA, filename)
    migrated_data, migrated = _migrate_alert_history_payload(data)
    try:
        history = {
            market: AlertHistory.model_validate(alert_data)
            for market, alert_data in migrated_data.items()
        }
    except ValidationError as error:
        raise StateLoadError(StateErrorCode.INVALID_SCHEMA, filename) from error
    active_history = _active_alert_history(history)
    if migrated:
        await save_alert_history(active_history, gcs_client)
    return active_history


async def save_alert_history(history: Dict[str, AlertHistory], gcs_client=None):
    """알림 히스토리를 저장하며, 24시간 이상된 기록은 자동으로 정리합니다."""
    filename = _alert_history_file_name()
    cleaned_history = _active_alert_history(history)
    data_to_save = {m: ah.model_dump(mode="json") for m, ah in cleaned_history.items()}
    await save_json(filename, data_to_save, gcs_client)
    logger.info(f"알림 히스토리 저장 완료: {filename}")


def _parse_notification_outbox(data: Any) -> NotificationOutbox:
    try:
        return NotificationOutbox.model_validate(data)
    except ValidationError as error:
        raise StateLoadError(
            StateErrorCode.INVALID_SCHEMA, NOTIFICATION_OUTBOX_FILE_NAME
        ) from error


async def load_notification_outbox(gcs_client=None) -> Optional[NotificationOutbox]:
    """Load the single pending webhook delivery, failing closed on corrupt state."""
    data = await load_json(NOTIFICATION_OUTBOX_FILE_NAME, gcs_client)
    if data is None:
        return None
    return _parse_notification_outbox(data)


async def save_notification_outbox(
    outbox: Optional[NotificationOutbox], gcs_client=None
) -> None:
    """Persist or clear the single pending webhook delivery."""
    payload = outbox.model_dump(mode="json") if outbox is not None else None
    await save_json(NOTIFICATION_OUTBOX_FILE_NAME, payload, gcs_client)


async def load_notification_backlog(gcs_client=None) -> List[NotificationOutbox]:
    """Load deferred prepared notifications in FIFO order."""
    data = await load_json(
        NOTIFICATION_BACKLOG_FILE_NAME, gcs_client, reject_null=True
    )
    if data is None:
        return []
    try:
        return NotificationBacklog.model_validate(data).root
    except ValidationError as error:
        raise StateLoadError(
            StateErrorCode.INVALID_SCHEMA, NOTIFICATION_BACKLOG_FILE_NAME
        ) from error


async def save_notification_backlog(
    backlog: List[NotificationOutbox], gcs_client=None
) -> None:
    """Persist the deferred notification FIFO after validating every record."""
    try:
        validated = NotificationBacklog.model_validate(backlog).root
    except ValidationError as error:
        raise StateSaveError(
            StateErrorCode.INVALID_SCHEMA, NOTIFICATION_BACKLOG_FILE_NAME
        ) from error
    await save_json(
        NOTIFICATION_BACKLOG_FILE_NAME,
        [item.model_dump(mode="json") for item in validated],
        gcs_client,
    )


# --- 로그 정리 ---


async def cleanup_old_logs(days_to_keep: int = 7, gcs_client=None):
    """설정된 기간보다 오래된 분석 로그 파일을 삭제합니다."""
    cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        days=days_to_keep
    )
    prefix = "analysis_log_"

    try:
        if config.storage_method() is config.StorageMethod.GCS:
            if gcs_client is None:
                raise StateBackendUnavailable(
                    StateErrorCode.BACKEND_UNAVAILABLE, prefix
                )
            await _cleanup_gcs_logs(gcs_client, prefix, cutoff_date)
        else:
            _cleanup_local_logs(prefix, cutoff_date)
    except StateBackendUnavailable:
        raise
    except Exception as error:
        raise StateSaveError(
            StateErrorCode.DELETE_FAILED, prefix, detail=str(error)
        ) from error


async def _cleanup_gcs_logs(gcs_client, prefix: str, cutoff_date: datetime.datetime):
    """GCS에서 오래된 로그 파일을 삭제합니다."""
    bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)
    blobs_iterator = await asyncio.to_thread(bucket.list_blobs, prefix=prefix)

    for blob in blobs_iterator:
        try:
            date_str = blob.name.replace(prefix, "").replace(".json", "")
            file_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=datetime.timezone.utc
            )
            if file_date < cutoff_date:
                logger.info(f"오래된 GCS 로그 파일 삭제: {blob.name}")
                await asyncio.to_thread(blob.delete)
        except ValueError:
            continue  # 날짜 형식이 아닌 파일은 건너뜀


def _cleanup_local_logs(prefix: str, cutoff_date: datetime.datetime):
    """로컬에서 오래된 로그 파일을 삭제합니다."""
    log_dir = config.LOCAL_STATE_DIR
    if not os.path.isdir(log_dir):
        return

    for filename in os.listdir(log_dir):
        if filename.startswith(prefix) and filename.endswith(".json"):
            try:
                date_str = filename.replace(prefix, "").replace(".json", "")
                file_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").replace(
                    tzinfo=datetime.timezone.utc
                )
                if file_date < cutoff_date:
                    filepath = os.path.join(log_dir, filename)
                    os.remove(filepath)
                    logger.info(f"오래된 로컬 로그 파일 삭제: {filepath}")
            except ValueError:
                continue

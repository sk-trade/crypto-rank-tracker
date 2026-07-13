#common/state_manager
import asyncio
import datetime
import fcntl
import json
import logging
import os
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

import config
from common.models import AlertHistory, AnalysisState, RankState, ScanEvent, ScanOutcome
from common.storage_client import StateBackendUnavailable, StateLoadError, load_json, save_json

logger = logging.getLogger(config.APP_LOGGER_NAME)
IDEMPOTENCY_STATE_FILE_NAME = "processed_scan_keys.json"
NOTIFICATION_OUTBOX_FILE_NAME = "notification_outbox.json"
NOTIFICATION_BACKLOG_FILE_NAME = "notification_backlog.json"
SCAN_CLAIM_LEASE_SECONDS = 600


# --- 순위 상태 관리 ---


async def load_rank_state_history(gcs_client=None) -> List[RankState]:
    """'순위' 상태 히스토리 리스트를 로드합니다."""
    filename = config.RANK_STATE_FILE_NAME
    data = await load_json(filename, gcs_client)

    if data is None:
        logger.info(f"순위 상태 파일({filename})이 없어 초기 상태로 시작합니다.")
        return []

    # 과거 호환성을 위해 단일 객체로 저장된 경우 리스트로 변환
    if isinstance(data, dict):
        data = [data]
    elif not isinstance(data, list):
        raise StateLoadError(
            f"순위 상태 형식 오류 ({filename}): JSON array 또는 legacy object가 필요합니다."
        )

    try:
        return [RankState.model_validate(s) for s in data]
    except ValidationError as error:
        raise StateLoadError(f"순위 상태 내용 오류 ({filename})") from error


async def save_rank_state_history(
    new_state: RankState, old_states: List[RankState], gcs_client=None
):
    """새로운 '순위' 상태를 히스토리에 추가하여 저장합니다."""
    filename = config.RANK_STATE_FILE_NAME

    updated_states = [state for state in old_states if state.last_updated != new_state.last_updated]
    updated_states.append(new_state)
    if len(updated_states) > config.STATE_HISTORY_COUNT:
        updated_states = updated_states[-config.STATE_HISTORY_COUNT:]

    data_to_save = [s.model_dump(mode="json") for s in updated_states]
    await save_json(filename, data_to_save, gcs_client)
    logger.info(f"순위 히스토리 저장 완료: {filename}")


# --- 분석 로그 관리 ---


def get_daily_log_filename() -> str:
    """오늘 날짜 기반의 로그 파일 이름을 반환합니다."""
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    return f"analysis_log_{today}.json"


def _get_daily_filename(prefix: str) -> str:
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    return f"{prefix}_{today}.json"


async def _append_records(filename: str, records: List[ScanEvent] | List[ScanOutcome], gcs_client=None):
    existing = await load_json(filename, gcs_client)
    if not isinstance(existing, list):
        existing = []
    records_by_id = {
        item.get("event_id"): item
        for item in existing
        if isinstance(item, dict) and item.get("event_id")
    }
    for record in records:
        records_by_id[record.event_id] = record.model_dump(mode="json")
    await save_json(filename, list(records_by_id.values()), gcs_client)


async def append_scan_events(events: List[ScanEvent], gcs_client=None):
    await _append_records(_get_daily_filename("scan_events"), events, gcs_client)


async def append_scan_outcomes(outcomes: List[ScanOutcome], gcs_client=None):
    await _append_records(_get_daily_filename("scan_outcomes"), outcomes, gcs_client)


async def load_pending_scan_events(gcs_client=None) -> List[ScanEvent]:
    data = await load_json("pending_scan_events.json", gcs_client)
    return [ScanEvent.model_validate(item) for item in data] if isinstance(data, list) else []


async def save_pending_scan_events(events: List[ScanEvent], gcs_client=None):
    events_by_id = {event.event_id: event for event in events}
    await save_json(
        "pending_scan_events.json",
        [event.model_dump(mode="json") for event in events_by_id.values()],
        gcs_client,
    )


async def claim_scan_key(scan_key: str, execution_id: str | None = None, gcs_client=None) -> bool:
    """Atomically acquire or resume an in-progress completed-candle scan."""
    if config.STATE_STORAGE_METHOD == "GCS":
        if gcs_client is None:
            raise StateBackendUnavailable("GCS state storage requires an initialized GCS client")
        return await _claim_scan_key_in_gcs(scan_key, execution_id, gcs_client)
    return await asyncio.to_thread(_claim_scan_key_locally, scan_key, execution_id)


async def complete_scan_key(scan_key: str, gcs_client=None) -> None:
    """Mark a claimed scan complete after its durable state or outbox handoff exists."""
    if config.STATE_STORAGE_METHOD == "GCS":
        if gcs_client is None:
            raise StateBackendUnavailable("GCS state storage requires an initialized GCS client")
        await _complete_scan_key_in_gcs(scan_key, gcs_client)
        return
    await asyncio.to_thread(_complete_scan_key_locally, scan_key)


async def release_scan_key(scan_key: str, gcs_client=None) -> None:
    """Release a claim when a scan fails before external notification begins."""
    if config.STATE_STORAGE_METHOD == "GCS":
        if gcs_client is None:
            raise StateBackendUnavailable("GCS state storage requires an initialized GCS client")
        await _release_scan_key_in_gcs(scan_key, gcs_client)
        return
    await asyncio.to_thread(_release_scan_key_locally, scan_key)


def _claim_status(claim: Dict[str, Any]) -> str:
    # Legacy claim records were permanent completion markers.
    return claim.get("status", "completed")


def _claim_can_resume(
    claim: Dict[str, Any], execution_id: str | None, now: datetime.datetime
) -> bool:
    if _claim_status(claim) != "in_progress":
        return False
    if execution_id and claim.get("execution_id") == execution_id:
        return True
    try:
        claimed_at = datetime.datetime.fromisoformat(claim["claimed_at"])
        if claimed_at.tzinfo is None:
            claimed_at = claimed_at.replace(tzinfo=datetime.timezone.utc)
    except (KeyError, TypeError, ValueError):
        return True
    return (now - claimed_at.astimezone(datetime.timezone.utc)).total_seconds() > SCAN_CLAIM_LEASE_SECONDS


def _acquire_claim(
    claims: List[Dict[str, Any]], scan_key: str, execution_id: str | None
) -> tuple[bool, List[Dict[str, Any]]]:
    now = datetime.datetime.now(datetime.timezone.utc)
    for claim in claims:
        if claim.get("scan_key") != scan_key:
            continue
        if not _claim_can_resume(claim, execution_id, now):
            return False, claims
        claim.update(
            {
                "execution_id": execution_id,
                "claimed_at": now.isoformat(),
                "status": "in_progress",
            }
        )
        claim.pop("completed_at", None)
        return True, claims
    claims.append(
        {
            "scan_key": scan_key,
            "execution_id": execution_id,
            "claimed_at": now.isoformat(),
            "status": "in_progress",
        }
    )
    return True, claims


def _claim_scan_key_locally(scan_key: str, execution_id: str | None) -> bool:
    os.makedirs(config.LOCAL_STATE_DIR, exist_ok=True)
    lock_path = os.path.join(config.LOCAL_STATE_DIR, f"{IDEMPOTENCY_STATE_FILE_NAME}.lock")
    state_path = os.path.join(config.LOCAL_STATE_DIR, IDEMPOTENCY_STATE_FILE_NAME)
    with open(lock_path, "a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            try:
                with open(state_path, encoding="utf-8") as state_file:
                    state = json.load(state_file)
            except FileNotFoundError:
                state = {"claims": []}
            claims = state.get("claims", []) if isinstance(state, dict) else []
            acquired, claims = _acquire_claim(claims, scan_key, execution_id)
            if not acquired:
                return False
            state = {"claims": claims[-config.IDEMPOTENCY_KEY_HISTORY_LIMIT :]}
            temporary_path = f"{state_path}.tmp"
            with open(temporary_path, "w", encoding="utf-8") as state_file:
                json.dump(state, state_file, ensure_ascii=False)
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
                    state = json.load(state_file)
            except FileNotFoundError:
                return
            claims = state.get("claims", []) if isinstance(state, dict) else []
            changed = False
            for claim in claims:
                if claim.get("scan_key") == scan_key:
                    claim["status"] = "completed"
                    claim["completed_at"] = datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat()
                    changed = True
                    break
            if not changed:
                return
            temporary_path = f"{state_path}.tmp"
            with open(temporary_path, "w", encoding="utf-8") as state_file:
                json.dump({"claims": claims}, state_file, ensure_ascii=False)
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
                    state = json.load(state_file)
            except FileNotFoundError:
                return
            claims = state.get("claims", []) if isinstance(state, dict) else []
            remaining = [claim for claim in claims if claim.get("scan_key") != scan_key]
            temporary_path = f"{state_path}.tmp"
            with open(temporary_path, "w", encoding="utf-8") as state_file:
                json.dump({"claims": remaining}, state_file, ensure_ascii=False)
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
        raise RuntimeError("google-cloud-storage is required for GCS idempotency") from error

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
            state = json.loads(raw_state)
        else:
            state = {"claims": []}
            generation = 0
        claims = state.get("claims", []) if isinstance(state, dict) else []
        acquired, claims = _acquire_claim(claims, scan_key, execution_id)
        if not acquired:
            return False
        try:
            await asyncio.to_thread(
                blob.upload_from_string,
                json.dumps({"claims": claims[-config.IDEMPOTENCY_KEY_HISTORY_LIMIT :]}, ensure_ascii=False),
                content_type="application/json",
                if_generation_match=generation,
            )
            return True
        except PreconditionFailed:
            continue
    raise RuntimeError("Could not atomically claim scan key after concurrent updates")


async def _complete_scan_key_in_gcs(scan_key: str, gcs_client) -> None:
    try:
        from google.api_core.exceptions import PreconditionFailed
    except ImportError as error:
        raise RuntimeError("google-cloud-storage is required for GCS idempotency") from error

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
        state = json.loads(raw_state)
        claims = state.get("claims", []) if isinstance(state, dict) else []
        changed = False
        for claim in claims:
            if claim.get("scan_key") == scan_key:
                claim["status"] = "completed"
                claim["completed_at"] = datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat()
                changed = True
                break
        if not changed:
            return
        try:
            await asyncio.to_thread(
                blob.upload_from_string,
                json.dumps({"claims": claims}, ensure_ascii=False),
                content_type="application/json",
                if_generation_match=generation,
            )
            return
        except PreconditionFailed:
            continue
    raise RuntimeError("Could not atomically complete scan key after concurrent updates")


async def _release_scan_key_in_gcs(scan_key: str, gcs_client) -> None:
    try:
        from google.api_core.exceptions import PreconditionFailed
    except ImportError as error:
        raise RuntimeError("google-cloud-storage is required for GCS idempotency") from error

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
        state = json.loads(raw_state)
        claims = state.get("claims", []) if isinstance(state, dict) else []
        remaining = [claim for claim in claims if claim.get("scan_key") != scan_key]
        try:
            await asyncio.to_thread(
                blob.upload_from_string,
                json.dumps({"claims": remaining}, ensure_ascii=False),
                content_type="application/json",
                if_generation_match=generation,
            )
            return
        except PreconditionFailed:
            continue
    raise RuntimeError("Could not atomically release scan key after concurrent updates")


async def save_analysis_log(state: AnalysisState, gcs_client=None):
    """현재 분석 결과를 오늘 날짜의 로그 파일에 추가(append)합니다."""
    filename = get_daily_log_filename()
    # candle_history는 용량이 크므로 로그에서 제외
    new_log_entry = state.model_dump(
        mode="json", exclude={"tickers": {"__all__": {"candle_history"}}}
    )

    try:
        log_list = await load_json(filename, gcs_client)
        if not isinstance(log_list, list):
            log_list = []

        log_list.append(new_log_entry)
        await save_json(filename, log_list, gcs_client)
        logger.info(f"분석 로그 추가 완료: {filename}")

    except Exception as e:
        logger.error(f"'{filename}' 로그 파일 저장 실패: {e}", exc_info=True)


# --- 알림 히스토리 관리 ---


def _active_alert_history(
    history: Dict[str, AlertHistory], now: Optional[datetime.datetime] = None
) -> Dict[str, AlertHistory]:
    cutoff = (now or datetime.datetime.now(datetime.timezone.utc)) - datetime.timedelta(hours=24)
    return {
        market: alert
        for market, alert in history.items()
        if alert.last_alert_timestamp > cutoff
    }


async def load_alert_history(gcs_client=None) -> Dict[str, AlertHistory]:
    """알림 히스토리 파일을 로드합니다."""
    data = await load_json(config.ALERT_HISTORY_FILE_NAME, gcs_client)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise StateLoadError(
            f"알림 히스토리 형식 오류 ({config.ALERT_HISTORY_FILE_NAME}): JSON object가 필요합니다."
        )

    history = {
        market: AlertHistory.model_validate(alert_data)
        for market, alert_data in data.items()
    }
    return _active_alert_history(history)


async def save_alert_history(history: Dict[str, AlertHistory], gcs_client=None):
    """알림 히스토리를 저장하며, 24시간 이상된 기록은 자동으로 정리합니다."""
    cleaned_history = _active_alert_history(history)
    data_to_save = {m: ah.model_dump(mode="json") for m, ah in cleaned_history.items()}
    await save_json(config.ALERT_HISTORY_FILE_NAME, data_to_save, gcs_client)
    logger.info(f"알림 히스토리 저장 완료: {config.ALERT_HISTORY_FILE_NAME}")


def _validate_notification_outbox(data: Any) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise StateLoadError(
            f"알림 outbox 형식 오류 ({NOTIFICATION_OUTBOX_FILE_NAME}): JSON object가 필요합니다."
        )
    if data.get("status") not in {"prepared", "attempting", "delivered"}:
        raise StateLoadError(
            "알림 outbox status는 prepared, attempting 또는 delivered여야 합니다."
        )
    if not isinstance(data.get("delivery_id"), str) or not isinstance(data.get("message"), str):
        raise StateLoadError("알림 outbox delivery_id와 message는 문자열이어야 합니다.")
    history = data.get("alert_history")
    if history is not None and (
        not isinstance(history, dict)
        or any(not isinstance(value, dict) for value in history.values())
    ):
        raise StateLoadError("알림 outbox alert_history는 JSON object 또는 null이어야 합니다.")
    previous_history = data.get("previous_alert_history")
    if previous_history is not None and (
        not isinstance(previous_history, dict)
        or any(not isinstance(value, dict) for value in previous_history.values())
    ):
        raise StateLoadError(
            "알림 outbox previous_alert_history는 JSON object 또는 null이어야 합니다."
        )
    alert_markets = data.get("alert_markets", [])
    if not isinstance(alert_markets, list) or any(
        not isinstance(market, str) for market in alert_markets
    ):
        raise StateLoadError("알림 outbox alert_markets는 문자열 배열이어야 합니다.")
    scan_key = data.get("scan_key")
    if scan_key is not None and not isinstance(scan_key, str):
        raise StateLoadError("알림 outbox scan_key는 문자열 또는 null이어야 합니다.")
    kind = data.get("kind", "briefing")
    if kind not in {"briefing", "alert", "data_quality"}:
        raise StateLoadError(
            "알림 outbox kind는 briefing, alert 또는 data_quality여야 합니다."
        )
    return data


async def load_notification_outbox(gcs_client=None) -> Optional[Dict[str, Any]]:
    """Load the single pending webhook delivery, failing closed on corrupt state."""
    data = await load_json(NOTIFICATION_OUTBOX_FILE_NAME, gcs_client)
    if data is None:
        return None
    return _validate_notification_outbox(data)


async def save_notification_outbox(outbox: Optional[Dict[str, Any]], gcs_client=None) -> None:
    """Persist or clear the single pending webhook delivery."""
    if outbox is not None:
        _validate_notification_outbox(outbox)
    await save_json(NOTIFICATION_OUTBOX_FILE_NAME, outbox, gcs_client)


async def load_notification_backlog(gcs_client=None) -> List[Dict[str, Any]]:
    """Load deferred prepared notifications in FIFO order."""
    data = await load_json(NOTIFICATION_BACKLOG_FILE_NAME, gcs_client)
    if data is None:
        return []
    if not isinstance(data, list):
        raise StateLoadError(
            f"알림 backlog 형식 오류 ({NOTIFICATION_BACKLOG_FILE_NAME}): JSON array가 필요합니다."
        )
    backlog = []
    for item in data:
        validated = _validate_notification_outbox(item)
        if validated["status"] != "prepared":
            raise StateLoadError("알림 backlog에는 prepared 상태만 저장할 수 있습니다.")
        backlog.append(validated)
    return backlog


async def save_notification_backlog(
    backlog: List[Dict[str, Any]], gcs_client=None
) -> None:
    """Persist the deferred notification FIFO after validating every record."""
    for item in backlog:
        validated = _validate_notification_outbox(item)
        if validated["status"] != "prepared":
            raise StateLoadError("알림 backlog에는 prepared 상태만 저장할 수 있습니다.")
    await save_json(NOTIFICATION_BACKLOG_FILE_NAME, backlog, gcs_client)


# --- 로그 정리 ---


async def cleanup_old_logs(days_to_keep: int = 7, gcs_client=None):
    """설정된 기간보다 오래된 분석 로그 파일을 삭제합니다."""
    cutoff_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
        days=days_to_keep
    )
    prefix = "analysis_log_"

    try:
        if config.STATE_STORAGE_METHOD == "GCS" and gcs_client:
            await _cleanup_gcs_logs(gcs_client, prefix, cutoff_date)
        else:
            _cleanup_local_logs(prefix, cutoff_date)

    except Exception as e:
        logger.error(f"오래된 로그 파일 정리 실패: {e}", exc_info=True)


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

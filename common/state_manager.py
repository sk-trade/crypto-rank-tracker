#common/state_manager
import asyncio
import datetime
import json
import logging
import os
from typing import Dict, List

import config
from common.models import AlertHistory, AnalysisState, RankState
from common.storage_client import load_json, save_json

logger = logging.getLogger(config.APP_LOGGER_NAME)


# --- 순위 상태 관리 ---


async def load_rank_state_history(gcs_client=None) -> List[RankState]:
    """'순위' 상태 히스토리 리스트를 로드합니다."""
    filename = config.RANK_STATE_FILE_NAME
    data = await load_json(filename, gcs_client)

    if not data:
        logger.info(f"순위 상태 파일({filename})이 없어 초기 상태로 시작합니다.")
        return []

    # 과거 호환성을 위해 단일 객체로 저장된 경우 리스트로 변환
    if isinstance(data, dict):
        data = [data]

    return [RankState.model_validate(s) for s in data]


async def save_rank_state_history(
    new_state: RankState, old_states: List[RankState], gcs_client=None
):
    """새로운 '순위' 상태를 히스토리에 추가하여 저장합니다."""
    filename = config.RANK_STATE_FILE_NAME

    updated_states = old_states + [new_state]
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


async def load_alert_history(gcs_client=None) -> Dict[str, AlertHistory]:
    """알림 히스토리 파일을 로드합니다."""
    data = await load_json(config.ALERT_HISTORY_FILE_NAME, gcs_client)
    if not isinstance(data, dict):
        if data is not None:
            logger.warning(
                f"'{config.ALERT_HISTORY_FILE_NAME}' 파일의 형식이 올바르지 않습니다 (Dict가 아님). 빈 히스토리로 시작합니다."
            )
        return {}

    return {
        market: AlertHistory.model_validate(alert_data)
        for market, alert_data in data.items()
    }


async def save_alert_history(history: Dict[str, AlertHistory], gcs_client=None):
    """알림 히스토리를 저장하며, 24시간 이상된 기록은 자동으로 정리합니다."""
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
    cleaned_history = {
        market: alert
        for market, alert in history.items()
        if alert.last_alert_timestamp > cutoff
    }
    data_to_save = {m: ah.model_dump(mode="json") for m, ah in cleaned_history.items()}
    await save_json(config.ALERT_HISTORY_FILE_NAME, data_to_save, gcs_client)
    logger.info(f"알림 히스토리 저장 완료: {config.ALERT_HISTORY_FILE_NAME}")


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
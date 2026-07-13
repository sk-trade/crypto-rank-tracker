# common/notification/main

import asyncio
import dataclasses
import datetime
import hashlib
import logging
from typing import Any, Dict, List, Optional

import aiohttp


@dataclasses.dataclass
class DispatchResult:
    sent: bool
    reason: Optional[str] = None
    status_code: Optional[int] = None
    skipped: bool = False


class NotificationDeliveryError(RuntimeError):
    """A configured delivery failed or could not be finalized durably."""

    def __init__(self, message: str, *, delivery_confirmed: bool = False):
        super().__init__(message)
        self.delivery_confirmed = delivery_confirmed

import config
from common.models import Alert, AlertHistory, TickerData
from common.notification.engine import AlertEngine
from common.notification.formatter import NotificationFormatter
from common.signals.detector import detect_anomalies, filter_market_wide_events
from common.state_manager import (
    load_notification_outbox,
    save_alert_history,
    save_notification_outbox,
)

logger = logging.getLogger(config.APP_LOGGER_NAME)


async def dispatch_data_quality_alert(issues: List[str], gcs_client=None) -> DispatchResult:
    """Notify operators that market data is unusable without emitting a market briefing."""
    message = NotificationFormatter().format_data_quality_alert(issues)
    result = await _queue_and_dispatch_notification(message, gcs_client=gcs_client)
    if result.sent:
        logger.warning("Data-quality incident notification sent: %s", "; ".join(issues))
    elif not result.skipped:
        logger.error("Data-quality incident notification was not sent (%s): %s", result.reason, "; ".join(issues))
    return result


async def create_and_dispatch_notification(
    raw_tickers: List[Dict[str, Any]],
    enriched_tickers: Dict[str, TickerData],
    current_rankings: Dict[str, int],
    previous_rankings: Dict[str, int],
    SECTORS: Dict[str, List[str]],
    REVERSE_SECTOR_MAP: Dict[str, List[str]],
    alert_history: Dict[str, AlertHistory], 
    market_regime: Dict[str, Any],
    final_alerts: Optional[List[Alert]] = None,
    gcs_client=None,
) -> DispatchResult:
    """시장 브리핑을 생성하고, 최종 알림이 있을 경우 함께 전송합니다."""

    # 메시지 포매팅
    formatter = NotificationFormatter()
    message = formatter.format_daily_briefing(
        alerts=final_alerts or [], # final_alerts가 None이면 빈 리스트 전달
        raw_tickers=raw_tickers,
        enriched_tickers=enriched_tickers,
        current_rankings=current_rankings,
        previous_rankings=previous_rankings,
        SECTORS=SECTORS,
        REVERSE_SECTOR_MAP=REVERSE_SECTOR_MAP,
        alert_history=alert_history,
        market_regime=market_regime,
    )
    
    # 알림 전송
    if not message:
        logger.info("알림을 보낼 메시지가 없습니다.")
        return DispatchResult(sent=False, reason="empty message", skipped=True)

    # 우선순위 높은 알림이 있을 때만 @channel 멘션
    use_channel_mention = False
    if final_alerts:
        top_alert = final_alerts[0]
        if top_alert.priority >= 2: 
            use_channel_mention = True

    final_message = f"@channel\n{message}" if use_channel_mention else message
    updated_history = None
    if final_alerts:
        copied_history = {
            market: entry.model_copy(deep=True) for market, entry in alert_history.items()
        }
        updated_history = _update_alert_history(copied_history, final_alerts)
    dispatch_result = await _queue_and_dispatch_notification(
        final_message, updated_history, gcs_client
    )
    if dispatch_result.sent:
        logger.info("알림 메시지를 생성하여 전송했습니다.")
    return dispatch_result


def _serialize_alert_history(
    history: Optional[Dict[str, AlertHistory]],
) -> Optional[Dict[str, Dict[str, Any]]]:
    if history is None:
        return None
    return {market: entry.model_dump(mode="json") for market, entry in history.items()}


def _deserialize_alert_history(payload: Dict[str, Dict[str, Any]]) -> Dict[str, AlertHistory]:
    return {
        market: AlertHistory.model_validate(entry)
        for market, entry in payload.items()
    }


async def _queue_and_dispatch_notification(
    message: str,
    updated_history: Optional[Dict[str, AlertHistory]] = None,
    gcs_client=None,
) -> DispatchResult:
    """Durably prepare a configured webhook before attempting the external side effect."""
    if not config.WEBHOOK_URL:
        return await send_notification(message)
    if await load_notification_outbox(gcs_client) is not None:
        raise NotificationDeliveryError("a prior webhook delivery is still pending")

    delivery_id = hashlib.sha256(message.encode("utf-8")).hexdigest()[:16]
    outbox = {
        "delivery_id": delivery_id,
        "status": "prepared",
        "message": message,
        "alert_history": _serialize_alert_history(updated_history),
    }
    await save_notification_outbox(outbox, gcs_client)
    return await _deliver_prepared_notification(outbox, gcs_client)


async def _deliver_prepared_notification(outbox: Dict[str, Any], gcs_client=None) -> DispatchResult:
    history_payload = outbox.get("alert_history")
    if history_payload is not None:
        await save_alert_history(_deserialize_alert_history(history_payload), gcs_client)

    attempting = {**outbox, "status": "attempting"}
    await save_notification_outbox(attempting, gcs_client)
    result = await send_notification(outbox["message"])
    if result.sent:
        try:
            await save_notification_outbox(None, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                "webhook delivered but outbox finalization failed",
                delivery_confirmed=True,
            ) from error
        return result

    try:
        await save_notification_outbox({**outbox, "status": "prepared"}, gcs_client)
    except Exception as error:
        raise NotificationDeliveryError(
            f"webhook failed and retry state could not be restored: {result.reason}"
        ) from error
    raise NotificationDeliveryError(f"configured webhook delivery failed: {result.reason}")


async def recover_pending_notification(gcs_client=None) -> Optional[DispatchResult]:
    """Resolve one pending delivery before starting a new market scan."""
    outbox = await load_notification_outbox(gcs_client)
    if outbox is None:
        return None
    if not config.WEBHOOK_URL:
        raise NotificationDeliveryError("pending webhook delivery cannot run without WEBHOOK_URL")
    if outbox["status"] == "attempting":
        # A lost process cannot know whether the webhook accepted the request. Treat the
        # attempt as delivered to preserve the product's no-duplicate alert contract.
        try:
            await save_notification_outbox(None, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                "uncertain webhook attempt could not be finalized",
                delivery_confirmed=True,
            ) from error
        logger.error(
            "Cleared uncertain webhook attempt %s without resending to avoid duplicate alerts.",
            outbox["delivery_id"],
        )
        return DispatchResult(sent=True, reason="uncertain prior attempt treated as delivered")
    return await _deliver_prepared_notification(outbox, gcs_client)


def _update_alert_history(
    history: Dict[str, AlertHistory], alerts: List[Alert]
) -> Dict[str, AlertHistory]:
    """알림 발송 내역을 기반으로 히스토리를 업데이트합니다."""
    now = datetime.datetime.now(datetime.timezone.utc)
    for alert in alerts:
        candidate = alert.candidate
        market = candidate.market
        signal_type = alert.signal_type

        if signal_type in ["BREAKOUT_START", "BREAKDOWN_START"]:
            history[market] = AlertHistory(
                market=market,
                last_alert_timestamp=now,
                last_signal_type=signal_type,
                last_price=candidate.current_price,
                last_rvol=candidate.rvol,
                initial_timestamp=now,
                initial_price=candidate.current_price,
                structure_level=alert.structure_level,
                structure_direction=("bullish" if signal_type == "BREAKOUT_START" else "bearish"),
            )
        elif "ACCELERATION" in signal_type or "FAILED" in signal_type:
            if market in history:
                history[market].last_alert_timestamp = now
                history[market].last_signal_type = signal_type
                history[market].last_price = candidate.current_price
                history[market].last_rvol = candidate.rvol
                if "FAILED" in signal_type:
                    history[market].structure_level = None
                    history[market].structure_direction = None
    return history


async def send_notification(message: str) -> DispatchResult:
    """웹훅을 통해 메시지를 보냅니다. 전송 결과를 반환합니다."""
    if not config.WEBHOOK_URL:
        logger.warning("웹훅 URL이 설정되지 않았습니다. 알림을 보내지 않습니다.")
        return DispatchResult(
            sent=False, reason="WEBHOOK_URL not configured", skipped=True
        )

    payload: Dict[str, Any] = {"text": message}
    if message.strip().startswith("@channel"):
        payload["link_names"] = True

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.WEBHOOK_URL, json=payload, timeout=10
            ) as response:
                if response.ok:
                    logger.info("웹훅 알림 전송 성공.")
                    return DispatchResult(sent=True)
                else:
                    error_text = await response.text()
                    logger.error(
                        f"웹훅 전송 실패 ({response.status}): {error_text}"
                    )
                    return DispatchResult(sent=False, reason=f"HTTP {response.status}", status_code=response.status)
    except Exception as e:
        logger.error(f"웹훅 전송 중 예외 발생: {e}", exc_info=True)
        return DispatchResult(sent=False, reason=str(e))

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
    delivery_uncertain: bool = False
    delivery_id: Optional[str] = None
    scan_key: Optional[str] = None
    queued: bool = False


class NotificationDeliveryError(RuntimeError):
    """A configured delivery failed or could not be finalized durably."""

    def __init__(
        self,
        message: str,
        *,
        delivery_confirmed: bool = False,
        scan_handoff_durable: bool = False,
    ):
        super().__init__(message)
        self.delivery_confirmed = delivery_confirmed
        self.scan_handoff_durable = scan_handoff_durable or delivery_confirmed

import config
from common.models import Alert, AlertHistory, TickerData
from common.notification.engine import AlertEngine
from common.notification.formatter import NotificationFormatter
from common.signals.detector import detect_anomalies, filter_market_wide_events
from common.state_manager import (
    complete_scan_key,
    load_alert_history,
    load_notification_backlog,
    load_notification_outbox,
    save_alert_history,
    save_notification_backlog,
    save_notification_outbox,
)

logger = logging.getLogger(config.APP_LOGGER_NAME)
MAX_NOTIFICATION_BACKLOG = 144


async def dispatch_data_quality_alert(
    issues: List[str], gcs_client=None, scan_key: str | None = None
) -> DispatchResult:
    """Notify operators that market data is unusable without emitting a market briefing."""
    message = NotificationFormatter().format_data_quality_alert(issues)
    result = await _queue_and_dispatch_notification(
        message,
        gcs_client=gcs_client,
        scan_key=scan_key,
        notification_kind="data_quality",
    )
    if result.sent:
        logger.warning("Data-quality incident notification sent: %s", "; ".join(issues))
    elif result.queued:
        logger.warning("Data-quality incident notification queued behind a pending delivery.")
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
    scan_key: str | None = None,
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
        final_message,
        updated_history,
        gcs_client,
        scan_key=scan_key,
        alert_markets=[alert.candidate.market for alert in final_alerts or []],
        previous_history=alert_history if final_alerts else None,
        notification_kind="alert" if final_alerts else "briefing",
    )
    if dispatch_result.sent:
        logger.info("알림 메시지를 생성하여 전송했습니다.")
    elif dispatch_result.queued:
        logger.info("알림 메시지를 기존 pending delivery 뒤에 저장했습니다.")
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


def _refresh_alert_history_for_delivery(
    history: Dict[str, AlertHistory], alert_markets: List[str]
) -> Dict[str, AlertHistory]:
    now = datetime.datetime.now(datetime.timezone.utc)
    for market in alert_markets:
        entry = history.get(market)
        if entry is None:
            continue
        is_initial_transition = (
            entry.last_signal_type in {"BREAKOUT_START", "BREAKDOWN_START"}
            and entry.initial_timestamp == entry.last_alert_timestamp
        )
        entry.last_alert_timestamp = now
        if is_initial_transition:
            entry.initial_timestamp = now
    return history


def _merge_outbox_alert_history(
    current: Dict[str, AlertHistory], outbox: Dict[str, Any]
) -> Dict[str, AlertHistory]:
    history_payload = outbox.get("alert_history")
    if history_payload is None:
        return current
    updates = _deserialize_alert_history(history_payload)
    alert_markets = outbox.get("alert_markets", [])
    if not alert_markets:
        return updates
    for market in alert_markets:
        if market in updates:
            current[market] = updates[market]
    return current


def _rebase_deferred_alert_entry(
    old_previous: Optional[AlertHistory],
    deferred: AlertHistory,
    new_previous: AlertHistory,
) -> AlertHistory:
    if old_previous is None or deferred.last_signal_type in {
        "BREAKOUT_START",
        "BREAKDOWN_START",
    }:
        rebased = deferred.model_copy(deep=True)
    else:
        old_previous_data = old_previous.model_dump(mode="python")
        deferred_data = deferred.model_dump(mode="python")
        rebased_data = new_previous.model_dump(mode="python")
        for field, value in deferred_data.items():
            if old_previous_data.get(field) != value:
                rebased_data[field] = value
        rebased = AlertHistory.model_validate(rebased_data)

    if rebased.last_alert_timestamp < new_previous.last_alert_timestamp:
        is_initial_transition = (
            rebased.last_signal_type in {"BREAKOUT_START", "BREAKDOWN_START"}
            and rebased.initial_timestamp == rebased.last_alert_timestamp
        )
        rebased.last_alert_timestamp = new_previous.last_alert_timestamp
        if is_initial_transition:
            rebased.initial_timestamp = new_previous.last_alert_timestamp
    return rebased


def _rebase_deferred_notification(
    outbox: Dict[str, Any], market: str, new_previous: AlertHistory
) -> tuple[Dict[str, Any], AlertHistory]:
    alert_history = {
        key: dict(value) for key, value in (outbox.get("alert_history") or {}).items()
    }
    if market not in alert_history:
        raise ValueError(f"deferred alert history is missing {market}")
    previous_history = {
        key: dict(value)
        for key, value in (outbox.get("previous_alert_history") or {}).items()
    }
    old_previous = (
        AlertHistory.model_validate(previous_history[market])
        if market in previous_history
        else None
    )
    deferred = AlertHistory.model_validate(alert_history[market])
    rebased = _rebase_deferred_alert_entry(
        old_previous, deferred, new_previous
    )
    previous_history[market] = new_previous.model_dump(mode="json")
    alert_history[market] = rebased.model_dump(mode="json")
    return (
        {
            **outbox,
            "previous_alert_history": previous_history,
            "alert_history": alert_history,
        },
        rebased,
    )


def _notification_delivery_id(
    message: str, scan_key: str | None, notification_kind: str
) -> str:
    identity = (
        f"{scan_key}\0{notification_kind}"
        if scan_key
        else f"unscoped\0{notification_kind}\0{message}"
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


async def _persist_outbox_alert_history(
    outbox: Dict[str, Any], gcs_client=None
) -> None:
    history_payload = outbox.get("alert_history")
    if history_payload is None:
        return
    updates = _deserialize_alert_history(history_payload)
    alert_markets = outbox.get("alert_markets", [])
    if not alert_markets:
        await save_alert_history(updates, gcs_client)
        return
    current = await load_alert_history(gcs_client)
    current = _merge_outbox_alert_history(
        current,
        {**outbox, "alert_history": _serialize_alert_history(updates)},
    )
    await save_alert_history(current, gcs_client)


async def _prepare_alert_history_for_delivery(
    outbox: Dict[str, Any], gcs_client=None
) -> Dict[str, Any]:
    history_payload = outbox.get("alert_history")
    backlog = await load_notification_backlog(gcs_client)
    rebased_backlog = [
        item for item in backlog if item["delivery_id"] != outbox["delivery_id"]
    ]
    prepared = outbox

    if history_payload is not None:
        alert_markets = outbox.get("alert_markets", [])
        refreshed = _refresh_alert_history_for_delivery(
            _deserialize_alert_history(history_payload), alert_markets
        )
        prepared = {
            **outbox,
            "alert_history": _serialize_alert_history(refreshed),
        }
        for market in alert_markets:
            predecessor = refreshed.get(market)
            if predecessor is None:
                continue
            for index, deferred in enumerate(rebased_backlog):
                if market not in deferred.get("alert_markets", []):
                    continue
                rebased_backlog[index], predecessor = _rebase_deferred_notification(
                    deferred, market, predecessor
                )

    if rebased_backlog != backlog:
        await save_notification_backlog(rebased_backlog, gcs_client)
    if any(
        pending.get("alert_history") is not None
        for pending in [prepared, *rebased_backlog]
    ):
        current = await load_alert_history(gcs_client)
        for pending in [prepared, *rebased_backlog]:
            current = _merge_outbox_alert_history(current, pending)
        await save_alert_history(current, gcs_client)
    return prepared


async def _reconcile_pending_alert_history(
    outbox: Dict[str, Any], backlog: List[Dict[str, Any]], gcs_client=None
) -> None:
    if not any(
        pending.get("alert_history") is not None
        for pending in [outbox, *backlog]
    ):
        return
    current = await load_alert_history(gcs_client)
    for pending in [outbox, *backlog]:
        current = _merge_outbox_alert_history(current, pending)
    await save_alert_history(current, gcs_client)


async def _rollback_outbox_alert_history(
    outbox: Dict[str, Any], gcs_client=None
) -> None:
    previous_payload = outbox.get("previous_alert_history")
    alert_markets = outbox.get("alert_markets", [])
    if previous_payload is None or not alert_markets:
        return
    previous = _deserialize_alert_history(previous_payload)
    current = await load_alert_history(gcs_client)
    for market in alert_markets:
        if market in previous:
            current[market] = previous[market]
        else:
            current.pop(market, None)
    await save_alert_history(current, gcs_client)


async def _enqueue_deferred_notification(
    outbox: Dict[str, Any], backlog: List[Dict[str, Any]], gcs_client=None
) -> DispatchResult:
    if any(item["delivery_id"] == outbox["delivery_id"] for item in backlog):
        return DispatchResult(
            sent=False,
            reason="notification already queued",
            delivery_id=outbox["delivery_id"],
            scan_key=outbox.get("scan_key"),
            queued=True,
        )
    if outbox["kind"] == "briefing":
        backlog = [item for item in backlog if item.get("kind") != "briefing"]
    if len(backlog) >= MAX_NOTIFICATION_BACKLOG:
        raise NotificationDeliveryError(
            f"notification backlog reached {MAX_NOTIFICATION_BACKLOG} retained records"
        )
    backlog.append(outbox)
    await save_notification_backlog(backlog, gcs_client)
    try:
        await _persist_outbox_alert_history(outbox, gcs_client)
    except Exception as error:
        raise NotificationDeliveryError(
            "notification was queued but alert history could not be persisted",
            scan_handoff_durable=True,
        ) from error
    return DispatchResult(
        sent=False,
        reason="queued behind pending webhook delivery",
        delivery_id=outbox["delivery_id"],
        scan_key=outbox.get("scan_key"),
        queued=True,
    )


async def _queue_and_dispatch_notification(
    message: str,
    updated_history: Optional[Dict[str, AlertHistory]] = None,
    gcs_client=None,
    *,
    scan_key: str | None = None,
    alert_markets: Optional[List[str]] = None,
    previous_history: Optional[Dict[str, AlertHistory]] = None,
    notification_kind: str = "briefing",
) -> DispatchResult:
    """Durably prepare a configured webhook before attempting the external side effect."""
    if not config.WEBHOOK_URL:
        return await send_notification(message)
    delivery_id = _notification_delivery_id(message, scan_key, notification_kind)
    outbox = {
        "delivery_id": delivery_id,
        "status": "prepared",
        "message": message,
        "alert_history": _serialize_alert_history(updated_history),
        "previous_alert_history": _serialize_alert_history(previous_history),
        "alert_markets": sorted(set(alert_markets or [])),
        "scan_key": scan_key,
        "kind": notification_kind,
    }
    active = await load_notification_outbox(gcs_client)
    backlog = await load_notification_backlog(gcs_client)
    if active is not None:
        if active["delivery_id"] == delivery_id:
            return DispatchResult(
                sent=False,
                reason="notification already owns the active outbox",
                delivery_id=delivery_id,
                scan_key=scan_key,
                queued=True,
            )
        return await _enqueue_deferred_notification(outbox, backlog, gcs_client)
    if backlog:
        return await _enqueue_deferred_notification(outbox, backlog, gcs_client)
    await save_notification_outbox(outbox, gcs_client)
    try:
        return await _deliver_prepared_notification(outbox, gcs_client)
    except NotificationDeliveryError:
        raise
    except Exception as error:
        raise NotificationDeliveryError(
            "prepared notification could not be advanced durably",
            scan_handoff_durable=True,
        ) from error


async def _deliver_prepared_notification(outbox: Dict[str, Any], gcs_client=None) -> DispatchResult:
    outbox = await _prepare_alert_history_for_delivery(outbox, gcs_client)

    attempting = {**outbox, "status": "attempting"}
    await save_notification_outbox(attempting, gcs_client)
    result = await send_notification(
        outbox["message"], delivery_id=outbox["delivery_id"]
    )
    result.delivery_id = outbox["delivery_id"]
    result.scan_key = outbox.get("scan_key")
    if result.sent:
        delivered = {**outbox, "status": "delivered"}
        try:
            await save_notification_outbox(delivered, gcs_client)
            if scan_key := outbox.get("scan_key"):
                await complete_scan_key(scan_key, gcs_client)
            await save_notification_outbox(None, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                "webhook delivered but outbox finalization failed",
                delivery_confirmed=True,
                scan_handoff_durable=True,
            ) from error
        return result

    if result.delivery_uncertain:
        try:
            if scan_key := outbox.get("scan_key"):
                await complete_scan_key(scan_key, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                "webhook outcome is uncertain and scan completion failed",
                scan_handoff_durable=True,
            ) from error
        raise NotificationDeliveryError(
            f"webhook outcome is uncertain: {result.reason}",
            scan_handoff_durable=True,
        )

    try:
        await save_notification_outbox({**outbox, "status": "prepared"}, gcs_client)
        if scan_key := outbox.get("scan_key"):
            await complete_scan_key(scan_key, gcs_client)
    except Exception as error:
        raise NotificationDeliveryError(
            f"webhook failed and retry state could not be restored: {result.reason}",
            scan_handoff_durable=True,
        ) from error
    raise NotificationDeliveryError(
        f"configured webhook delivery failed: {result.reason}",
        scan_handoff_durable=True,
    )


async def _promote_next_notification(
    gcs_client=None, *, exclude_delivery_id: str | None = None
) -> Optional[Dict[str, Any]]:
    backlog = await load_notification_backlog(gcs_client)
    filtered = [
        item
        for item in backlog
        if item["delivery_id"] != exclude_delivery_id
    ]
    if not filtered:
        if backlog:
            await save_notification_backlog([], gcs_client)
        return None
    next_outbox = filtered[0]
    await save_notification_outbox(next_outbox, gcs_client)
    await save_notification_backlog(filtered[1:], gcs_client)
    return next_outbox


async def _cancel_pending_notifications(
    outbox: Optional[Dict[str, Any]],
    backlog: List[Dict[str, Any]],
    gcs_client=None,
) -> None:
    for deferred in reversed(backlog):
        await _rollback_outbox_alert_history(deferred, gcs_client)
    if outbox is not None and outbox["status"] == "prepared":
        await _rollback_outbox_alert_history(outbox, gcs_client)
    scan_keys = {
        item.get("scan_key")
        for item in [*(backlog or []), *([outbox] if outbox else [])]
        if item.get("scan_key")
    }
    for scan_key in scan_keys:
        await complete_scan_key(scan_key, gcs_client)
    await save_notification_backlog([], gcs_client)
    await save_notification_outbox(None, gcs_client)


async def recover_pending_notification(gcs_client=None) -> Optional[DispatchResult]:
    """Resolve one pending delivery before starting a new market scan."""
    outbox = await load_notification_outbox(gcs_client)
    backlog = await load_notification_backlog(gcs_client)
    if outbox is None and not backlog:
        return None
    if not config.WEBHOOK_URL:
        try:
            await _cancel_pending_notifications(outbox, backlog, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                "pending webhook cancellation could not be persisted",
                scan_handoff_durable=True,
            ) from error
        representative = outbox or backlog[0]
        return DispatchResult(
            sent=False,
            reason="pending webhook delivery canceled because WEBHOOK_URL is not configured",
            skipped=True,
            delivery_id=representative["delivery_id"],
            scan_key=representative.get("scan_key"),
        )
    if outbox is None:
        outbox = await _promote_next_notification(gcs_client)
        if outbox is None:
            return None
        backlog = await load_notification_backlog(gcs_client)
    if outbox["status"] in {"attempting", "delivered"}:
        await _reconcile_pending_alert_history(outbox, backlog, gcs_client)
    if outbox["status"] == "attempting":
        try:
            if scan_key := outbox.get("scan_key"):
                await complete_scan_key(scan_key, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                "uncertain webhook attempt could not complete its scan claim",
                scan_handoff_durable=True,
            ) from error
        raise NotificationDeliveryError(
            "webhook attempt outcome is uncertain; inspect the receiver using delivery_id "
            f"{outbox['delivery_id']} before resetting or clearing the outbox",
            scan_handoff_durable=True,
        )
    if outbox["status"] == "delivered":
        try:
            if scan_key := outbox.get("scan_key"):
                await complete_scan_key(scan_key, gcs_client)
            await save_notification_outbox(None, gcs_client)
            await _promote_next_notification(
                gcs_client, exclude_delivery_id=outbox["delivery_id"]
            )
        except Exception as error:
            raise NotificationDeliveryError(
                "confirmed webhook delivery could not be finalized",
                delivery_confirmed=True,
                scan_handoff_durable=True,
            ) from error
        return DispatchResult(
            sent=True,
            reason="confirmed prior delivery finalized",
            delivery_id=outbox["delivery_id"],
            scan_key=outbox.get("scan_key"),
        )
    try:
        result = await _deliver_prepared_notification(outbox, gcs_client)
    except NotificationDeliveryError:
        raise
    except Exception as error:
        raise NotificationDeliveryError(
            "pending prepared notification could not be advanced durably",
            scan_handoff_durable=True,
        ) from error
    await _promote_next_notification(
        gcs_client, exclude_delivery_id=outbox["delivery_id"]
    )
    return result


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


async def send_notification(
    message: str, delivery_id: str | None = None
) -> DispatchResult:
    """웹훅을 통해 메시지를 보냅니다. 전송 결과를 반환합니다."""
    if not config.WEBHOOK_URL:
        logger.warning("웹훅 URL이 설정되지 않았습니다. 알림을 보내지 않습니다.")
        return DispatchResult(
            sent=False, reason="WEBHOOK_URL not configured", skipped=True
        )

    payload: Dict[str, Any] = {"text": message}
    headers = {"X-Webhook-Delivery-ID": delivery_id} if delivery_id else None
    if message.strip().startswith("@channel"):
        payload["link_names"] = True

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.WEBHOOK_URL, json=payload, headers=headers, timeout=10
            ) as response:
                if response.ok:
                    logger.info("웹훅 알림 전송 성공.")
                    return DispatchResult(sent=True, delivery_id=delivery_id)
                else:
                    error_text = await response.text()
                    logger.error(
                        f"웹훅 전송 실패 ({response.status}): {error_text}"
                    )
                    return DispatchResult(
                        sent=False,
                        reason=f"HTTP {response.status}",
                        status_code=response.status,
                        delivery_id=delivery_id,
                    )
    except (aiohttp.ClientConnectorError, aiohttp.InvalidURL) as e:
        logger.error(f"웹훅 연결 실패: {e}", exc_info=True)
        return DispatchResult(
            sent=False, reason=str(e), delivery_id=delivery_id
        )
    except Exception as e:
        logger.error(f"웹훅 전송 중 예외 발생: {e}", exc_info=True)
        return DispatchResult(
            sent=False,
            reason=str(e),
            delivery_uncertain=True,
            delivery_id=delivery_id,
        )

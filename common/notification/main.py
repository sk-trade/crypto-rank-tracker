# common/notification/main

import dataclasses
import datetime
import hashlib
import logging
from typing import Any, Dict, List, Optional

import aiohttp

import config
from common.models import (
    Alert,
    AlertHistory,
    AttentionCandidate,
    DataQualityIssue,
    DeliveryState,
    DispatchCode,
    DispatchOutcome,
    MarketTicker,
    MarketRegimeSnapshot,
    NotificationErrorCode,
    NotificationKind,
    NotificationOutbox,
    NotificationStatus,
    ScanHandoffState,
    TickerData,
)
from common.notification.formatter import NotificationFormatter
from common.state_manager import (
    complete_scan_key,
    load_alert_history,
    load_notification_backlog,
    load_notification_outbox,
    save_alert_history,
    save_notification_backlog,
    save_notification_outbox,
)


@dataclasses.dataclass(frozen=True)
class DispatchResult:
    outcome: DispatchOutcome
    code: Optional[DispatchCode] = None
    detail: Optional[str] = None
    status_code: Optional[int] = None
    delivery_id: Optional[str] = None
    scan_key: Optional[str] = None


class NotificationDeliveryError(RuntimeError):
    """A configured delivery failed or could not be finalized durably."""

    def __init__(
        self,
        code: NotificationErrorCode,
        *,
        delivery_state: DeliveryState = DeliveryState.NOT_CONFIRMED,
        scan_handoff_state: ScanHandoffState = ScanHandoffState.NOT_DURABLE,
        detail: Optional[str] = None,
    ):
        super().__init__(detail or code.value)
        self.code = code
        self.delivery_state = delivery_state
        self.scan_handoff_state = scan_handoff_state
        self.detail = detail

logger = logging.getLogger(config.APP_LOGGER_NAME)
MAX_NOTIFICATION_BACKLOG = 144


async def dispatch_data_quality_alert(
    issues: List[DataQualityIssue], gcs_client=None, scan_key: str | None = None
) -> DispatchResult:
    """Notify operators that market data is unusable without emitting a market briefing."""
    message = NotificationFormatter().format_data_quality_alert(issues)
    result = await _queue_and_dispatch_notification(
        message,
        gcs_client=gcs_client,
        scan_key=scan_key,
        notification_kind=NotificationKind.DATA_QUALITY,
    )
    if result.outcome is DispatchOutcome.SENT:
        logger.warning(
            "Data-quality incident notification sent: %s",
            "; ".join(issue.message for issue in issues),
        )
    elif result.outcome is DispatchOutcome.QUEUED:
        logger.warning("Data-quality incident notification queued behind a pending delivery.")
    elif result.outcome is not DispatchOutcome.SKIPPED:
        logger.error(
            "Data-quality incident notification was not sent (%s): %s",
            result.code,
            "; ".join(issue.message for issue in issues),
        )
    return result


async def create_and_dispatch_notification(
    raw_tickers: List[MarketTicker],
    enriched_tickers: Dict[str, TickerData],
    current_rankings: Dict[str, int],
    previous_rankings: Dict[str, int],
    SECTORS: Dict[str, List[str]],
    REVERSE_SECTOR_MAP: Dict[str, List[str]],
    alert_history: Dict[str, AlertHistory], 
    market_regime: MarketRegimeSnapshot,
    final_alerts: Optional[List[Alert]] = None,
    attention_queue: Optional[List[AttentionCandidate]] = None,
    suppress_unchanged_briefing: bool = False,
    gcs_client=None,
    scan_key: str | None = None,
) -> DispatchResult:
    """시장 브리핑을 생성하고, 최종 알림이 있을 경우 함께 전송합니다."""

    if suppress_unchanged_briefing and not final_alerts and not attention_queue:
        logger.info("새 관심종목이나 중요 상태 변경이 없어 webhook 전송을 건너뜁니다.")
        return DispatchResult(
            outcome=DispatchOutcome.SKIPPED,
            code=DispatchCode.EMPTY_MESSAGE,
            detail="no_material_attention_change",
        )

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
        attention_queue=attention_queue or [],
    )
    
    # 알림 전송
    if not message:
        logger.info("알림을 보낼 메시지가 없습니다.")
        return DispatchResult(
            outcome=DispatchOutcome.SKIPPED, code=DispatchCode.EMPTY_MESSAGE
        )

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
        mention_channel=use_channel_mention,
        notification_kind=(
            NotificationKind.ALERT if final_alerts else NotificationKind.BRIEFING
        ),
    )
    if dispatch_result.outcome is DispatchOutcome.SENT:
        logger.info("알림 메시지를 생성하여 전송했습니다.")
    elif dispatch_result.outcome is DispatchOutcome.QUEUED:
        logger.info("알림 메시지를 기존 pending delivery 뒤에 저장했습니다.")
    return dispatch_result


def _refresh_alert_history_for_delivery(
    history: Dict[str, AlertHistory], alert_markets: List[str]
) -> Dict[str, AlertHistory]:
    now = datetime.datetime.now(datetime.timezone.utc)
    for market in alert_markets:
        entry = history.get(market)
        if entry is None:
            continue
        is_initial_transition = (
            entry.last_signal_type.starts_structure
            and entry.initial_timestamp == entry.last_alert_timestamp
        )
        entry.last_alert_timestamp = now
        if is_initial_transition:
            entry.initial_timestamp = now
    return history


def _merge_outbox_alert_history(
    current: Dict[str, AlertHistory], outbox: NotificationOutbox
) -> Dict[str, AlertHistory]:
    if outbox.alert_history is None:
        return current
    if not outbox.alert_markets:
        return {
            market: entry.model_copy(deep=True)
            for market, entry in outbox.alert_history.items()
        }
    for market in outbox.alert_markets:
        if market in outbox.alert_history:
            current[market] = outbox.alert_history[market].model_copy(deep=True)
    return current


def _rebase_deferred_alert_entry(
    old_previous: Optional[AlertHistory],
    deferred: AlertHistory,
    new_previous: AlertHistory,
) -> AlertHistory:
    if old_previous is None or deferred.last_signal_type.starts_structure:
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
            rebased.last_signal_type.starts_structure
            and rebased.initial_timestamp == rebased.last_alert_timestamp
        )
        rebased.last_alert_timestamp = new_previous.last_alert_timestamp
        if is_initial_transition:
            rebased.initial_timestamp = new_previous.last_alert_timestamp
    return rebased


def _rebase_deferred_notification(
    outbox: NotificationOutbox, market: str, new_previous: AlertHistory
) -> tuple[NotificationOutbox, AlertHistory]:
    alert_history = {
        key: value.model_copy(deep=True)
        for key, value in (outbox.alert_history or {}).items()
    }
    if market not in alert_history:
        raise ValueError(f"deferred alert history is missing {market}")
    previous_history = {
        key: value.model_copy(deep=True)
        for key, value in (outbox.previous_alert_history or {}).items()
    }
    old_previous = previous_history.get(market)
    deferred = alert_history[market]
    rebased = _rebase_deferred_alert_entry(
        old_previous, deferred, new_previous
    )
    previous_history[market] = new_previous.model_copy(deep=True)
    alert_history[market] = rebased
    return (
        outbox.model_copy(
            update={
                "previous_alert_history": previous_history,
                "alert_history": alert_history,
            }
        ),
        rebased,
    )


def _notification_delivery_id(
    message: str, scan_key: str | None, notification_kind: NotificationKind
) -> str:
    identity = (
        f"{scan_key}\0{notification_kind}"
        if scan_key
        else f"unscoped\0{notification_kind}\0{message}"
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]


async def _persist_outbox_alert_history(
    outbox: NotificationOutbox, gcs_client=None
) -> None:
    if outbox.alert_history is None:
        return
    if not outbox.alert_markets:
        await save_alert_history(outbox.alert_history, gcs_client)
        return
    current = await load_alert_history(gcs_client)
    current = _merge_outbox_alert_history(current, outbox)
    await save_alert_history(current, gcs_client)


async def _prepare_alert_history_for_delivery(
    outbox: NotificationOutbox, gcs_client=None
) -> NotificationOutbox:
    backlog = await load_notification_backlog(gcs_client)
    rebased_backlog = [
        item for item in backlog if item.delivery_id != outbox.delivery_id
    ]
    prepared = outbox

    if outbox.alert_history is not None:
        alert_markets = outbox.alert_markets
        refreshed = _refresh_alert_history_for_delivery(
            {
                market: entry.model_copy(deep=True)
                for market, entry in outbox.alert_history.items()
            },
            alert_markets,
        )
        prepared = outbox.model_copy(update={"alert_history": refreshed})
        for market in alert_markets:
            predecessor = refreshed.get(market)
            if predecessor is None:
                continue
            for index, deferred in enumerate(rebased_backlog):
                if market not in deferred.alert_markets:
                    continue
                rebased_backlog[index], predecessor = _rebase_deferred_notification(
                    deferred, market, predecessor
                )

    if rebased_backlog != backlog:
        await save_notification_backlog(rebased_backlog, gcs_client)
    if any(
        pending.alert_history is not None
        for pending in [prepared, *rebased_backlog]
    ):
        current = await load_alert_history(gcs_client)
        for pending in [prepared, *rebased_backlog]:
            current = _merge_outbox_alert_history(current, pending)
        await save_alert_history(current, gcs_client)
    return prepared


async def _reconcile_pending_alert_history(
    outbox: NotificationOutbox,
    backlog: List[NotificationOutbox],
    gcs_client=None,
) -> None:
    if not any(
        pending.alert_history is not None
        for pending in [outbox, *backlog]
    ):
        return
    current = await load_alert_history(gcs_client)
    for pending in [outbox, *backlog]:
        current = _merge_outbox_alert_history(current, pending)
    await save_alert_history(current, gcs_client)


async def _rollback_outbox_alert_history(
    outbox: NotificationOutbox, gcs_client=None
) -> None:
    if outbox.previous_alert_history is None or not outbox.alert_markets:
        return
    current = await load_alert_history(gcs_client)
    for market in outbox.alert_markets:
        if market in outbox.previous_alert_history:
            current[market] = outbox.previous_alert_history[market]
        else:
            current.pop(market, None)
    await save_alert_history(current, gcs_client)


async def _enqueue_deferred_notification(
    outbox: NotificationOutbox,
    backlog: List[NotificationOutbox],
    gcs_client=None,
) -> DispatchResult:
    if any(item.delivery_id == outbox.delivery_id for item in backlog):
        return DispatchResult(
            outcome=DispatchOutcome.QUEUED,
            code=DispatchCode.ALREADY_QUEUED,
            delivery_id=outbox.delivery_id,
            scan_key=outbox.scan_key,
        )
    if outbox.kind is NotificationKind.BRIEFING:
        removed_briefings = [
            item for item in backlog if item.kind is NotificationKind.BRIEFING
        ]
        for scan_key in {item.scan_key for item in removed_briefings if item.scan_key}:
            await complete_scan_key(scan_key, gcs_client)
        backlog = [
            item for item in backlog if item.kind is not NotificationKind.BRIEFING
        ]
    if len(backlog) >= MAX_NOTIFICATION_BACKLOG:
        raise NotificationDeliveryError(
            NotificationErrorCode.BACKLOG_CAPACITY_EXCEEDED,
            detail=f"backlog capacity is {MAX_NOTIFICATION_BACKLOG}",
        )
    backlog.append(outbox)
    try:
        await save_notification_backlog(backlog, gcs_client)
    except Exception as error:
        try:
            persisted_backlog = await load_notification_backlog(gcs_client)
        except Exception:
            raise NotificationDeliveryError(
                NotificationErrorCode.BACKLOG_WRITE_UNVERIFIED,
                scan_handoff_state=ScanHandoffState.UNCERTAIN,
            ) from error
        if any(
            item.delivery_id == outbox.delivery_id
            for item in persisted_backlog
        ):
            raise NotificationDeliveryError(
                NotificationErrorCode.BACKLOG_WRITE_COMMITTED_WITHOUT_ACK,
                scan_handoff_state=ScanHandoffState.DURABLE,
            ) from error
        raise NotificationDeliveryError(
            NotificationErrorCode.BACKLOG_WRITE_NOT_PERSISTED
        ) from error
    try:
        await _persist_outbox_alert_history(outbox, gcs_client)
        if scan_key := outbox.scan_key:
            await complete_scan_key(scan_key, gcs_client)
    except Exception as error:
        raise NotificationDeliveryError(
            NotificationErrorCode.QUEUED_HANDOFF_FINALIZATION_FAILED,
            scan_handoff_state=ScanHandoffState.DURABLE,
        ) from error
    return DispatchResult(
        outcome=DispatchOutcome.QUEUED,
        code=DispatchCode.QUEUED_BEHIND_PENDING,
        delivery_id=outbox.delivery_id,
        scan_key=outbox.scan_key,
    )


async def _queue_and_dispatch_notification(
    message: str,
    updated_history: Optional[Dict[str, AlertHistory]] = None,
    gcs_client=None,
    *,
    scan_key: str | None = None,
    alert_markets: Optional[List[str]] = None,
    previous_history: Optional[Dict[str, AlertHistory]] = None,
    mention_channel: bool = False,
    notification_kind: NotificationKind = NotificationKind.BRIEFING,
) -> DispatchResult:
    """Durably prepare a configured webhook before attempting the external side effect."""
    if not config.WEBHOOK_URL:
        if config.SHADOW_MODE and updated_history is not None:
            await save_alert_history(updated_history, gcs_client)
        return await send_notification(message, mention_channel=mention_channel)
    delivery_id = _notification_delivery_id(message, scan_key, notification_kind)
    outbox = NotificationOutbox(
        delivery_id=delivery_id,
        status=NotificationStatus.PREPARED,
        message=message,
        alert_history=updated_history,
        previous_alert_history=previous_history,
        alert_markets=sorted(set(alert_markets or [])),
        scan_key=scan_key,
        kind=notification_kind,
        mention_channel=mention_channel,
    )
    active = await load_notification_outbox(gcs_client)
    backlog = await load_notification_backlog(gcs_client)
    if scan_key:
        existing = next(
            (
                pending
                for pending in [*([active] if active else []), *backlog]
                if pending.scan_key == scan_key
            ),
            None,
        )
        if existing is not None:
            await complete_scan_key(scan_key, gcs_client)
            return DispatchResult(
                outcome=DispatchOutcome.QUEUED,
                code=DispatchCode.SCAN_ALREADY_OWNED,
                delivery_id=existing.delivery_id,
                scan_key=scan_key,
            )
    if active is not None:
        if active.delivery_id == delivery_id:
            return DispatchResult(
                outcome=DispatchOutcome.QUEUED,
                code=DispatchCode.ACTIVE_OUTBOX_ALREADY_OWNED,
                delivery_id=delivery_id,
                scan_key=scan_key,
            )
        return await _enqueue_deferred_notification(outbox, backlog, gcs_client)
    if backlog:
        return await _enqueue_deferred_notification(outbox, backlog, gcs_client)
    try:
        await save_notification_outbox(outbox, gcs_client)
    except Exception as error:
        try:
            persisted_outbox = await load_notification_outbox(gcs_client)
        except Exception:
            raise NotificationDeliveryError(
                NotificationErrorCode.OUTBOX_WRITE_UNVERIFIED,
                scan_handoff_state=ScanHandoffState.UNCERTAIN,
            ) from error
        if (
            persisted_outbox is not None
            and persisted_outbox.delivery_id == outbox.delivery_id
        ):
            raise NotificationDeliveryError(
                NotificationErrorCode.OUTBOX_WRITE_COMMITTED_WITHOUT_ACK,
                scan_handoff_state=ScanHandoffState.DURABLE,
            ) from error
        raise NotificationDeliveryError(
            NotificationErrorCode.OUTBOX_WRITE_NOT_PERSISTED
        ) from error
    try:
        return await _deliver_prepared_notification(outbox, gcs_client)
    except NotificationDeliveryError:
        raise
    except Exception as error:
        raise NotificationDeliveryError(
            NotificationErrorCode.PREPARED_ADVANCE_FAILED,
            scan_handoff_state=ScanHandoffState.DURABLE,
        ) from error


async def _deliver_prepared_notification(
    outbox: NotificationOutbox, gcs_client=None
) -> DispatchResult:
    outbox = await _prepare_alert_history_for_delivery(outbox, gcs_client)

    attempting = outbox.model_copy(update={"status": NotificationStatus.ATTEMPTING})
    await save_notification_outbox(attempting, gcs_client)
    result = await send_notification(
        outbox.message,
        delivery_id=outbox.delivery_id,
        mention_channel=outbox.mention_channel,
    )
    result = dataclasses.replace(
        result, delivery_id=outbox.delivery_id, scan_key=outbox.scan_key
    )
    if result.outcome is DispatchOutcome.SENT:
        delivered = outbox.model_copy(update={"status": NotificationStatus.DELIVERED})
        try:
            await save_notification_outbox(delivered, gcs_client)
            if scan_key := outbox.scan_key:
                await complete_scan_key(scan_key, gcs_client)
            await save_notification_outbox(None, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                NotificationErrorCode.DELIVERY_FINALIZATION_FAILED,
                delivery_state=DeliveryState.CONFIRMED,
                scan_handoff_state=ScanHandoffState.DURABLE,
            ) from error
        return result

    if result.outcome is DispatchOutcome.UNCERTAIN:
        try:
            if scan_key := outbox.scan_key:
                await complete_scan_key(scan_key, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                NotificationErrorCode.UNCERTAIN_DELIVERY_SCAN_COMPLETION_FAILED,
                delivery_state=DeliveryState.UNCERTAIN,
                scan_handoff_state=ScanHandoffState.DURABLE,
            ) from error
        raise NotificationDeliveryError(
            NotificationErrorCode.DELIVERY_OUTCOME_UNCERTAIN,
            delivery_state=DeliveryState.UNCERTAIN,
            scan_handoff_state=ScanHandoffState.DURABLE,
            detail=result.detail,
        )

    try:
        await save_notification_outbox(
            outbox.model_copy(update={"status": NotificationStatus.PREPARED}),
            gcs_client,
        )
        if scan_key := outbox.scan_key:
            await complete_scan_key(scan_key, gcs_client)
    except Exception as error:
        raise NotificationDeliveryError(
            NotificationErrorCode.RETRY_STATE_RESTORE_FAILED,
            scan_handoff_state=ScanHandoffState.DURABLE,
            detail=result.detail,
        ) from error
    raise NotificationDeliveryError(
        NotificationErrorCode.DELIVERY_FAILED,
        scan_handoff_state=ScanHandoffState.DURABLE,
        detail=result.detail,
    )


async def _promote_next_notification(
    gcs_client=None, *, exclude_delivery_id: str | None = None
) -> Optional[NotificationOutbox]:
    backlog = await load_notification_backlog(gcs_client)
    filtered = [
        item
        for item in backlog
        if item.delivery_id != exclude_delivery_id
    ]
    if not filtered:
        if backlog:
            await save_notification_backlog([], gcs_client)
        return None
    next_outbox = filtered[0]
    await save_notification_outbox(next_outbox, gcs_client)
    await save_notification_backlog(filtered[1:], gcs_client)
    return next_outbox


async def _complete_pending_notification_scans(
    outbox: Optional[NotificationOutbox],
    backlog: List[NotificationOutbox],
    gcs_client=None,
) -> None:
    scan_keys = {
        item.scan_key
        for item in [*backlog, *([outbox] if outbox else [])]
        if item.scan_key
    }
    for scan_key in scan_keys:
        await complete_scan_key(scan_key, gcs_client)


async def _cancel_pending_notifications(
    outbox: Optional[NotificationOutbox],
    backlog: List[NotificationOutbox],
    gcs_client=None,
) -> bool:
    preserve_ambiguous_attempt = (
        outbox is not None and outbox.status is NotificationStatus.ATTEMPTING
    )
    for deferred in reversed(backlog):
        await _rollback_outbox_alert_history(deferred, gcs_client)
    if outbox is not None and outbox.status is NotificationStatus.PREPARED:
        await _rollback_outbox_alert_history(outbox, gcs_client)
    await _complete_pending_notification_scans(outbox, backlog, gcs_client)
    await save_notification_backlog([], gcs_client)
    if not preserve_ambiguous_attempt:
        await save_notification_outbox(None, gcs_client)
    return preserve_ambiguous_attempt


async def recover_pending_notification(gcs_client=None) -> Optional[DispatchResult]:
    """Resolve one pending delivery before starting a new market scan."""
    outbox = await load_notification_outbox(gcs_client)
    backlog = await load_notification_backlog(gcs_client)
    if outbox is None and not backlog:
        return None
    try:
        await _complete_pending_notification_scans(outbox, backlog, gcs_client)
    except Exception as error:
        raise NotificationDeliveryError(
            NotificationErrorCode.PENDING_SCAN_HANDOFF_FAILED,
            scan_handoff_state=ScanHandoffState.DURABLE,
        ) from error
    if not config.WEBHOOK_URL:
        try:
            preserved_ambiguous_attempt = await _cancel_pending_notifications(
                outbox, backlog, gcs_client
            )
        except Exception as error:
            raise NotificationDeliveryError(
                NotificationErrorCode.PENDING_CANCELLATION_FAILED,
                scan_handoff_state=ScanHandoffState.DURABLE,
            ) from error
        if preserved_ambiguous_attempt:
            raise NotificationDeliveryError(
                NotificationErrorCode.AMBIGUOUS_ATTEMPT_REQUIRES_RECONCILIATION,
                delivery_state=DeliveryState.UNCERTAIN,
                scan_handoff_state=ScanHandoffState.DURABLE,
                detail=outbox.delivery_id,
            )
        representative = outbox or backlog[0]
        return DispatchResult(
            outcome=DispatchOutcome.SKIPPED,
            code=DispatchCode.PENDING_CANCELED,
            delivery_id=representative.delivery_id,
            scan_key=representative.scan_key,
        )
    if outbox is None:
        outbox = await _promote_next_notification(gcs_client)
        if outbox is None:
            return None
        backlog = await load_notification_backlog(gcs_client)
    if outbox.status in {
        NotificationStatus.ATTEMPTING,
        NotificationStatus.DELIVERED,
    }:
        await _reconcile_pending_alert_history(outbox, backlog, gcs_client)
    if outbox.status is NotificationStatus.ATTEMPTING:
        try:
            if scan_key := outbox.scan_key:
                await complete_scan_key(scan_key, gcs_client)
        except Exception as error:
            raise NotificationDeliveryError(
                NotificationErrorCode.UNCERTAIN_DELIVERY_SCAN_COMPLETION_FAILED,
                delivery_state=DeliveryState.UNCERTAIN,
                scan_handoff_state=ScanHandoffState.DURABLE,
            ) from error
        raise NotificationDeliveryError(
            NotificationErrorCode.AMBIGUOUS_ATTEMPT_REQUIRES_RECONCILIATION,
            delivery_state=DeliveryState.UNCERTAIN,
            scan_handoff_state=ScanHandoffState.DURABLE,
            detail=outbox.delivery_id,
        )
    if outbox.status is NotificationStatus.DELIVERED:
        try:
            if scan_key := outbox.scan_key:
                await complete_scan_key(scan_key, gcs_client)
            await save_notification_outbox(None, gcs_client)
            await _promote_next_notification(
                gcs_client, exclude_delivery_id=outbox.delivery_id
            )
        except Exception as error:
            raise NotificationDeliveryError(
                NotificationErrorCode.CONFIRMED_DELIVERY_FINALIZATION_FAILED,
                delivery_state=DeliveryState.CONFIRMED,
                scan_handoff_state=ScanHandoffState.DURABLE,
            ) from error
        return DispatchResult(
            outcome=DispatchOutcome.SENT,
            code=DispatchCode.PRIOR_DELIVERY_FINALIZED,
            delivery_id=outbox.delivery_id,
            scan_key=outbox.scan_key,
        )
    try:
        result = await _deliver_prepared_notification(outbox, gcs_client)
    except NotificationDeliveryError:
        raise
    except Exception as error:
        raise NotificationDeliveryError(
            NotificationErrorCode.PENDING_ADVANCE_FAILED,
            scan_handoff_state=ScanHandoffState.DURABLE,
        ) from error
    await _promote_next_notification(
        gcs_client, exclude_delivery_id=outbox.delivery_id
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

        if signal_type.starts_structure:
            history[market] = AlertHistory(
                market=market,
                last_alert_timestamp=now,
                last_signal_type=signal_type,
                last_price=candidate.current_price,
                last_rvol=candidate.rvol,
                initial_timestamp=now,
                initial_price=candidate.current_price,
                structure_level=alert.structure_level,
                structure_direction=signal_type.structure_direction,
            )
        elif signal_type.updates_existing_structure:
            if market in history:
                history[market].last_alert_timestamp = now
                history[market].last_signal_type = signal_type
                history[market].last_price = candidate.current_price
                history[market].last_rvol = candidate.rvol
                if signal_type.is_failure:
                    history[market].structure_level = None
                    history[market].structure_direction = None
    return history


async def send_notification(
    message: str,
    delivery_id: str | None = None,
    *,
    mention_channel: bool = False,
) -> DispatchResult:
    """웹훅을 통해 메시지를 보냅니다. 전송 결과를 반환합니다."""
    if not config.WEBHOOK_URL:
        logger.warning("웹훅 URL이 설정되지 않았습니다. 알림을 보내지 않습니다.")
        return DispatchResult(
            outcome=DispatchOutcome.SKIPPED,
            code=DispatchCode.WEBHOOK_NOT_CONFIGURED,
        )

    payload: Dict[str, Any] = {"text": message}
    headers = {"X-Webhook-Delivery-ID": delivery_id} if delivery_id else None
    if mention_channel:
        payload["link_names"] = True

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.WEBHOOK_URL, json=payload, headers=headers, timeout=10
            ) as response:
                if response.ok:
                    logger.info("웹훅 알림 전송 성공.")
                    return DispatchResult(
                        outcome=DispatchOutcome.SENT, delivery_id=delivery_id
                    )
                else:
                    error_text = await response.text()
                    logger.error(
                        f"웹훅 전송 실패 ({response.status}): {error_text}"
                    )
                    return DispatchResult(
                        outcome=DispatchOutcome.FAILED,
                        code=DispatchCode.HTTP_ERROR,
                        detail=error_text,
                        status_code=response.status,
                        delivery_id=delivery_id,
                    )
    except (aiohttp.ClientConnectorError, aiohttp.InvalidURL) as e:
        logger.error(f"웹훅 연결 실패: {e}", exc_info=True)
        return DispatchResult(
            outcome=DispatchOutcome.FAILED,
            code=DispatchCode.CONNECTION_ERROR,
            detail=str(e),
            delivery_id=delivery_id,
        )
    except Exception as e:
        logger.error(f"웹훅 전송 중 예외 발생: {e}", exc_info=True)
        return DispatchResult(
            outcome=DispatchOutcome.UNCERTAIN,
            code=DispatchCode.UNEXPECTED_ERROR,
            detail=str(e),
            delivery_id=delivery_id,
        )

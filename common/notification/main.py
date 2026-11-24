"""Durable notification formatting, queuing, delivery, and recovery."""

import dataclasses
import datetime
import hashlib
import logging
from typing import Any

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
    MarketRegimeSnapshot,
    MarketTicker,
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
    code: DispatchCode | None = None
    detail: str | None = None
    status_code: int | None = None
    delivery_id: str | None = None
    scan_key: str | None = None


class NotificationDeliveryError(RuntimeError):
    """A configured delivery failed or could not be finalized durably."""

    def __init__(
        self,
        code: NotificationErrorCode,
        *,
        delivery_state: DeliveryState = DeliveryState.NOT_CONFIRMED,
        scan_handoff_state: ScanHandoffState = ScanHandoffState.NOT_DURABLE,
        detail: str | None = None,
    ):
        super().__init__(detail or code.value)
        self.code = code
        self.delivery_state = delivery_state
        self.scan_handoff_state = scan_handoff_state
        self.detail = detail


logger = logging.getLogger(config.APP_LOGGER_NAME)
MAX_NOTIFICATION_BACKLOG = 144


async def dispatch_data_quality_alert(
    issues: list[DataQualityIssue],
    *,
    observed_at: datetime.datetime,
    gcs_client=None,
    scan_key: str | None = None,
) -> DispatchResult:
    """Notify operators that market data is unusable without emitting a market briefing."""
    message = NotificationFormatter().format_data_quality_alert(issues, observed_at)
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
        logger.warning(
            "Data-quality incident notification queued behind a pending delivery."
        )
    elif result.outcome is not DispatchOutcome.SKIPPED:
        logger.error(
            "Data-quality incident notification was not sent (%s): %s",
            result.code,
            "; ".join(issue.message for issue in issues),
        )
    return result


async def format_and_dispatch_notification(
    raw_tickers: list[MarketTicker],
    enriched_tickers: dict[str, TickerData],
    current_rankings: dict[str, int],
    previous_rankings: dict[str, int],
    markets_by_sector: dict[str, list[str]],
    sectors_by_market: dict[str, list[str]],
    alert_history: dict[str, AlertHistory],
    market_regime: MarketRegimeSnapshot,
    observed_at: datetime.datetime | None = None,
    final_alerts: list[Alert] | None = None,
    attention_queue: list[AttentionCandidate] | None = None,
    suppress_unchanged_briefing: bool = False,
    gcs_client=None,
    scan_key: str | None = None,
) -> DispatchResult:
    """Format one briefing and hand it to the durable delivery queue."""

    if suppress_unchanged_briefing and not final_alerts and not attention_queue:
        logger.info("Skipping webhook because attention state has no material change")
        return DispatchResult(
            outcome=DispatchOutcome.SKIPPED,
            code=DispatchCode.EMPTY_MESSAGE,
            detail="no_material_attention_change",
        )

    formatter = NotificationFormatter()
    message = formatter.format_daily_briefing(
        alerts=final_alerts or [],
        raw_tickers=raw_tickers,
        enriched_tickers=enriched_tickers,
        current_rankings=current_rankings,
        previous_rankings=previous_rankings,
        markets_by_sector=markets_by_sector,
        sectors_by_market=sectors_by_market,
        market_regime=market_regime,
        observed_at=observed_at or datetime.datetime.now(datetime.UTC),
        attention_queue=attention_queue or [],
    )

    if not message:
        logger.info(
            "Skipping notification because formatting produced an empty message"
        )
        return DispatchResult(
            outcome=DispatchOutcome.SKIPPED, code=DispatchCode.EMPTY_MESSAGE
        )

    use_channel_mention = bool(final_alerts and final_alerts[0].priority >= 2)

    final_message = f"@channel\n{message}" if use_channel_mention else message
    updated_history = None
    if final_alerts:
        copied_history = {
            market: entry.model_copy(deep=True)
            for market, entry in alert_history.items()
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
        logger.info("Notification delivered")
    elif dispatch_result.outcome is DispatchOutcome.QUEUED:
        logger.info("Notification queued behind an active delivery")
    return dispatch_result


def _refresh_alert_history_for_delivery(
    history: dict[str, AlertHistory], alert_markets: list[str]
) -> dict[str, AlertHistory]:
    now = datetime.datetime.now(datetime.UTC)
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
    current: dict[str, AlertHistory], outbox: NotificationOutbox
) -> dict[str, AlertHistory]:
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
    old_previous: AlertHistory | None,
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
    rebased = _rebase_deferred_alert_entry(old_previous, deferred, new_previous)
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
        pending.alert_history is not None for pending in [prepared, *rebased_backlog]
    ):
        current = await load_alert_history(gcs_client)
        for pending in [prepared, *rebased_backlog]:
            current = _merge_outbox_alert_history(current, pending)
        await save_alert_history(current, gcs_client)
    return prepared


async def _reconcile_pending_alert_history(
    outbox: NotificationOutbox,
    backlog: list[NotificationOutbox],
    gcs_client=None,
) -> None:
    if not any(pending.alert_history is not None for pending in [outbox, *backlog]):
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
    backlog: list[NotificationOutbox],
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
        if any(item.delivery_id == outbox.delivery_id for item in persisted_backlog):
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


def _build_notification_outbox(
    message: str,
    updated_history: dict[str, AlertHistory] | None,
    *,
    scan_key: str | None,
    alert_markets: list[str] | None,
    previous_history: dict[str, AlertHistory] | None,
    mention_channel: bool,
    notification_kind: NotificationKind,
) -> NotificationOutbox:
    return NotificationOutbox(
        delivery_id=_notification_delivery_id(message, scan_key, notification_kind),
        status=NotificationStatus.PREPARED,
        message=message,
        alert_history=updated_history,
        previous_alert_history=previous_history,
        alert_markets=sorted(set(alert_markets or [])),
        scan_key=scan_key,
        kind=notification_kind,
        mention_channel=mention_channel,
    )


def _notification_for_scan(
    active: NotificationOutbox | None,
    backlog: list[NotificationOutbox],
    scan_key: str | None,
) -> NotificationOutbox | None:
    if scan_key is None:
        return None
    pending = [*([active] if active else []), *backlog]
    return next((item for item in pending if item.scan_key == scan_key), None)


async def _persist_prepared_outbox(
    outbox: NotificationOutbox,
    gcs_client=None,
) -> None:
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


async def _advance_new_outbox(
    outbox: NotificationOutbox,
    gcs_client=None,
) -> DispatchResult:
    try:
        return await _deliver_prepared_notification(outbox, gcs_client)
    except NotificationDeliveryError:
        raise
    except Exception as error:
        raise NotificationDeliveryError(
            NotificationErrorCode.PREPARED_ADVANCE_FAILED,
            scan_handoff_state=ScanHandoffState.DURABLE,
        ) from error


async def _queue_and_dispatch_notification(
    message: str,
    updated_history: dict[str, AlertHistory] | None = None,
    gcs_client=None,
    *,
    scan_key: str | None = None,
    alert_markets: list[str] | None = None,
    previous_history: dict[str, AlertHistory] | None = None,
    mention_channel: bool = False,
    notification_kind: NotificationKind = NotificationKind.BRIEFING,
) -> DispatchResult:
    """Durably prepare a configured webhook before attempting the external side effect."""
    if not config.WEBHOOK_URL:
        if config.SHADOW_MODE and updated_history is not None:
            await save_alert_history(updated_history, gcs_client)
        return await send_webhook_notification(message, mention_channel=mention_channel)

    outbox = _build_notification_outbox(
        message,
        updated_history,
        scan_key=scan_key,
        alert_markets=alert_markets,
        previous_history=previous_history,
        mention_channel=mention_channel,
        notification_kind=notification_kind,
    )
    active = await load_notification_outbox(gcs_client)
    backlog = await load_notification_backlog(gcs_client)

    if existing := _notification_for_scan(active, backlog, scan_key):
        await complete_scan_key(scan_key, gcs_client)
        return DispatchResult(
            outcome=DispatchOutcome.QUEUED,
            code=DispatchCode.SCAN_ALREADY_OWNED,
            delivery_id=existing.delivery_id,
            scan_key=scan_key,
        )
    if active is not None and active.delivery_id == outbox.delivery_id:
        return DispatchResult(
            outcome=DispatchOutcome.QUEUED,
            code=DispatchCode.ACTIVE_OUTBOX_ALREADY_OWNED,
            delivery_id=outbox.delivery_id,
            scan_key=scan_key,
        )
    if active is not None or backlog:
        return await _enqueue_deferred_notification(outbox, backlog, gcs_client)

    await _persist_prepared_outbox(outbox, gcs_client)
    return await _advance_new_outbox(outbox, gcs_client)


async def _deliver_prepared_notification(
    outbox: NotificationOutbox, gcs_client=None
) -> DispatchResult:
    outbox = await _prepare_alert_history_for_delivery(outbox, gcs_client)

    attempting = outbox.model_copy(update={"status": NotificationStatus.ATTEMPTING})
    await save_notification_outbox(attempting, gcs_client)
    result = await send_webhook_notification(
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
) -> NotificationOutbox | None:
    backlog = await load_notification_backlog(gcs_client)
    filtered = [item for item in backlog if item.delivery_id != exclude_delivery_id]
    if not filtered:
        if backlog:
            await save_notification_backlog([], gcs_client)
        return None
    next_outbox = filtered[0]
    await save_notification_outbox(next_outbox, gcs_client)
    await save_notification_backlog(filtered[1:], gcs_client)
    return next_outbox


async def _complete_pending_notification_scans(
    outbox: NotificationOutbox | None,
    backlog: list[NotificationOutbox],
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
    outbox: NotificationOutbox | None,
    backlog: list[NotificationOutbox],
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


async def _complete_pending_scan_handoffs(
    outbox: NotificationOutbox | None,
    backlog: list[NotificationOutbox],
    gcs_client=None,
) -> None:
    try:
        await _complete_pending_notification_scans(outbox, backlog, gcs_client)
    except Exception as error:
        raise NotificationDeliveryError(
            NotificationErrorCode.PENDING_SCAN_HANDOFF_FAILED,
            scan_handoff_state=ScanHandoffState.DURABLE,
        ) from error


async def _recover_without_webhook(
    outbox: NotificationOutbox | None,
    backlog: list[NotificationOutbox],
    gcs_client=None,
) -> DispatchResult:
    try:
        preserved_ambiguous_attempt = await _cancel_pending_notifications(
            outbox,
            backlog,
            gcs_client,
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


async def _raise_ambiguous_attempt(
    outbox: NotificationOutbox,
    gcs_client=None,
) -> None:
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


async def _finalize_delivered_notification(
    outbox: NotificationOutbox,
    gcs_client=None,
) -> DispatchResult:
    try:
        if scan_key := outbox.scan_key:
            await complete_scan_key(scan_key, gcs_client)
        await save_notification_outbox(None, gcs_client)
        await _promote_next_notification(
            gcs_client,
            exclude_delivery_id=outbox.delivery_id,
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


async def _recover_active_notification(
    outbox: NotificationOutbox,
    gcs_client=None,
) -> DispatchResult:
    if outbox.status is NotificationStatus.ATTEMPTING:
        await _raise_ambiguous_attempt(outbox, gcs_client)
    if outbox.status is NotificationStatus.DELIVERED:
        return await _finalize_delivered_notification(outbox, gcs_client)

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
        gcs_client,
        exclude_delivery_id=outbox.delivery_id,
    )
    return result


async def recover_pending_notification(gcs_client=None) -> DispatchResult | None:
    """Resolve one pending delivery before starting a new market scan."""
    outbox = await load_notification_outbox(gcs_client)
    backlog = await load_notification_backlog(gcs_client)
    if outbox is None and not backlog:
        return None

    await _complete_pending_scan_handoffs(outbox, backlog, gcs_client)
    if not config.WEBHOOK_URL:
        return await _recover_without_webhook(outbox, backlog, gcs_client)

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
    return await _recover_active_notification(outbox, gcs_client)


def _update_alert_history(
    history: dict[str, AlertHistory], alerts: list[Alert]
) -> dict[str, AlertHistory]:
    """Apply confirmed alert transitions to a copied history mapping."""
    now = datetime.datetime.now(datetime.UTC)
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
        elif signal_type.updates_existing_structure and market in history:
            history[market].last_alert_timestamp = now
            history[market].last_signal_type = signal_type
            history[market].last_price = candidate.current_price
            history[market].last_rvol = candidate.rvol
            if signal_type.is_failure:
                history[market].structure_level = None
                history[market].structure_direction = None
    return history


async def send_webhook_notification(
    message: str,
    delivery_id: str | None = None,
    *,
    mention_channel: bool = False,
) -> DispatchResult:
    """Attempt one webhook side effect and return its delivery state."""
    if not config.WEBHOOK_URL:
        logger.warning("Webhook URL is not configured; notification skipped")
        return DispatchResult(
            outcome=DispatchOutcome.SKIPPED,
            code=DispatchCode.WEBHOOK_NOT_CONFIGURED,
        )

    payload: dict[str, Any] = {"text": message}
    headers = {"X-Webhook-Delivery-ID": delivery_id} if delivery_id else None
    if mention_channel:
        payload["link_names"] = True

    try:
        async with (
            aiohttp.ClientSession() as session,
            session.post(
                config.WEBHOOK_URL, json=payload, headers=headers, timeout=10
            ) as response,
        ):
            if response.ok:
                logger.info("Webhook delivery confirmed")
                return DispatchResult(
                    outcome=DispatchOutcome.SENT, delivery_id=delivery_id
                )
            error_text = await response.text()
            logger.error(
                "Webhook returned HTTP %d: %s",
                response.status,
                error_text,
            )
            return DispatchResult(
                outcome=DispatchOutcome.FAILED,
                code=DispatchCode.HTTP_ERROR,
                detail=error_text,
                status_code=response.status,
                delivery_id=delivery_id,
            )
    except (aiohttp.ClientConnectorError, aiohttp.InvalidURL) as error:
        logger.exception("Webhook connection failed")
        return DispatchResult(
            outcome=DispatchOutcome.FAILED,
            code=DispatchCode.CONNECTION_ERROR,
            detail=str(error),
            delivery_id=delivery_id,
        )
    except Exception as error:
        logger.exception("Webhook delivery ended in an uncertain state")
        return DispatchResult(
            outcome=DispatchOutcome.UNCERTAIN,
            code=DispatchCode.UNEXPECTED_ERROR,
            detail=str(error),
            delivery_id=delivery_id,
        )

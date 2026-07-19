import asyncio
import datetime
import json
from unittest.mock import AsyncMock

import pytest

import config
import common.notification.main as notification
from common import state_manager
from common.models import (
    Alert,
    AlertHistory,
    DeliveryState,
    DispatchCode,
    DispatchOutcome,
    MarketRegime,
    MarketRegimeSnapshot,
    NotificationErrorCode,
    NotificationKind,
    NotificationOutbox,
    NotificationStatus,
    ScanHandoffState,
    SignalCandidate,
    SignalType,
    StructureDirection,
    TickerData,
)
from common.notification.engine import AlertEngine
from common.state_manager import load_alert_history
from common.storage_client import StateErrorCode, StateLoadError, StateSaveError


def _history(
    signal_type: SignalType,
    last_price: float,
    now: datetime.datetime,
    minutes_ago: int = 30,
) -> AlertHistory:
    return AlertHistory(
        market="KRW-BTC",
        last_alert_timestamp=now - datetime.timedelta(minutes=minutes_ago),
        last_signal_type=signal_type,
        last_price=last_price,
        last_rvol=1.0,
        initial_timestamp=now - datetime.timedelta(hours=2),
        initial_price=last_price,
        structure_level=100.0,
        structure_direction=signal_type.structure_direction,
    )


def _candidate(current_price: float, price_change: float = 1.2) -> SignalCandidate:
    return SignalCandidate(
        market="KRW-BTC",
        signal_score=0.75,
        price_change=price_change,
        rvol=1.0,
        rvol_z_score=1.0,
        current_price=current_price,
    )


def _ticker() -> TickerData:
    return TickerData(
        market="KRW-BTC",
        candle_history=[],
        price_change_10m=0.0,
        price_change_1h=0.0,
    )


def _legacy_alert_history_payload(
    signal_type: str, *, last_price: float = 102.0
) -> dict:
    now = datetime.datetime.now(datetime.timezone.utc)
    return {
        "KRW-BTC": {
            "market": "KRW-BTC",
            "last_alert_timestamp": now.isoformat(),
            "last_signal_type": signal_type,
            "last_price": last_price,
            "last_rvol": 2.0,
            "initial_timestamp": (now - datetime.timedelta(minutes=30)).isoformat(),
            "initial_price": 100.0,
        }
    }


def _process_signal_type(
    previous_signal_type: SignalType, current_price: float
) -> SignalType:
    now = datetime.datetime.now(datetime.timezone.utc)
    alerts = AlertEngine().process_signals(
        candidates=[_candidate(current_price)],
        enriched_tickers={"KRW-BTC": _ticker()},
        history={
            "KRW-BTC": _history(previous_signal_type, 100.0, now),
        },
    )

    assert len(alerts) == 1
    return alerts[0].signal_type


def test_alert_history_wrong_shape_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    (tmp_path / config.ALERT_HISTORY_FILE_NAME).write_text("[]", encoding="utf-8")

    with pytest.raises(StateLoadError) as error:
        asyncio.run(load_alert_history())
    assert error.value.code is StateErrorCode.INVALID_SCHEMA


def test_alert_history_invalid_signal_shape_still_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    payload = _legacy_alert_history_payload("BREAKOUT_START")
    payload["KRW-BTC"]["last_signal_type"] = ["BREAKOUT_START"]
    (tmp_path / config.ALERT_HISTORY_FILE_NAME).write_text(
        json.dumps(payload), encoding="utf-8"
    )

    with pytest.raises(StateLoadError) as error:
        asyncio.run(load_alert_history())

    assert error.value.code is StateErrorCode.INVALID_SCHEMA


@pytest.mark.parametrize(
    ("persisted_signal", "expected_signal", "expected_direction"),
    [
        (
            "BREAKOUT_START",
            SignalType.BREAKOUT_START,
            StructureDirection.BULLISH,
        ),
        (
            "MOMENTUM_ACCELERATION",
            SignalType.MOMENTUM_ACCELERATION,
            StructureDirection.BULLISH,
        ),
        (
            "BULL_MOMENTUM_SUSTAINED",
            SignalType.MOMENTUM_ACCELERATION,
            StructureDirection.BULLISH,
        ),
        (
            "BREAKDOWN_START",
            SignalType.BREAKDOWN_START,
            StructureDirection.BEARISH,
        ),
        (
            "DOWNTREND_ACCELERATION",
            SignalType.DOWNTREND_ACCELERATION,
            StructureDirection.BEARISH,
        ),
        (
            "BEAR_MOMENTUM_SUSTAINED",
            SignalType.DOWNTREND_ACCELERATION,
            StructureDirection.BEARISH,
        ),
        ("BULL_MOMENTUM_FAILED", SignalType.BULL_MOMENTUM_FAILED, None),
        ("BEAR_MOMENTUM_FAILED", SignalType.BEAR_MOMENTUM_FAILED, None),
    ],
)
def test_production_alert_history_is_migrated_to_canonical_state(
    monkeypatch,
    tmp_path,
    persisted_signal,
    expected_signal,
    expected_direction,
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    history_path = tmp_path / config.ALERT_HISTORY_FILE_NAME
    history_path.write_text(
        json.dumps(_legacy_alert_history_payload(persisted_signal)), encoding="utf-8"
    )

    history = asyncio.run(load_alert_history())

    migrated = history["KRW-BTC"]
    assert migrated.last_signal_type is expected_signal
    assert migrated.structure_direction is expected_direction
    expected_level = None if expected_direction is None else 100.0
    assert migrated.structure_level == expected_level

    canonical_payload = json.loads(history_path.read_text(encoding="utf-8"))
    canonical = canonical_payload["KRW-BTC"]
    assert canonical["last_signal_type"] == expected_signal.value
    assert canonical["structure_level"] == expected_level
    expected_direction_value = (
        None if expected_direction is None else expected_direction.value
    )
    assert canonical["structure_direction"] == expected_direction_value


@pytest.mark.parametrize(
    ("persisted_signal", "last_price", "current_price", "expected_signal"),
    [
        (
            "BULL_MOMENTUM_SUSTAINED",
            102.0,
            104.0,
            SignalType.MOMENTUM_ACCELERATION,
        ),
        (
            "BEAR_MOMENTUM_SUSTAINED",
            98.0,
            96.0,
            SignalType.DOWNTREND_ACCELERATION,
        ),
    ],
)
def test_migrated_sustained_history_remains_usable_by_typed_cooldown(
    monkeypatch,
    tmp_path,
    persisted_signal,
    last_price,
    current_price,
    expected_signal,
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    history_path = tmp_path / config.ALERT_HISTORY_FILE_NAME
    history_path.write_text(
        json.dumps(
            _legacy_alert_history_payload(
                persisted_signal, last_price=last_price
            )
        ),
        encoding="utf-8",
    )

    history = asyncio.run(load_alert_history())
    alerts = AlertEngine().process_signals(
        candidates=[_candidate(current_price)],
        enriched_tickers={"KRW-BTC": _ticker()},
        history=history,
    )

    assert len(alerts) == 1
    assert alerts[0].signal_type is expected_signal

    rewrite = AsyncMock()
    monkeypatch.setattr(state_manager, "save_json", rewrite)
    reloaded = asyncio.run(load_alert_history())
    assert reloaded["KRW-BTC"].last_signal_type is expected_signal
    rewrite.assert_not_awaited()


def test_alert_history_migration_rewrite_failure_is_explicit(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    (tmp_path / config.ALERT_HISTORY_FILE_NAME).write_text(
        json.dumps(_legacy_alert_history_payload("BULL_MOMENTUM_SUSTAINED")),
        encoding="utf-8",
    )
    rewrite_error = StateSaveError(
        StateErrorCode.WRITE_FAILED, config.ALERT_HISTORY_FILE_NAME
    )
    monkeypatch.setattr(
        state_manager,
        "save_json",
        AsyncMock(side_effect=rewrite_error),
    )

    with pytest.raises(StateSaveError) as error:
        asyncio.run(load_alert_history())

    assert error.value is rewrite_error


def test_alert_history_expires_before_transition_classification(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    stale = _history(
        SignalType.BREAKOUT_START,
        100.0,
        datetime.datetime.now(datetime.timezone.utc),
        minutes_ago=25 * 60,
    )
    (tmp_path / config.ALERT_HISTORY_FILE_NAME).write_text(
        json.dumps({"KRW-BTC": stale.model_dump(mode="json")}), encoding="utf-8"
    )

    assert asyncio.run(load_alert_history()) == {}


def _breakout_alert() -> Alert:
    return Alert(
        candidate=_candidate(101.0),
        ticker_data=_ticker(),
        signal_type=SignalType.BREAKOUT_START,
        priority=3,
        structure_level=100.0,
    )


def _briefing_args():
    return {
        "raw_tickers": [],
        "enriched_tickers": {},
        "current_rankings": {},
        "previous_rankings": {},
        "SECTORS": {},
        "REVERSE_SECTOR_MAP": {},
        "alert_history": {},
        "market_regime": MarketRegimeSnapshot(regime=MarketRegime.UNKNOWN),
        "final_alerts": [_breakout_alert()],
    }


def _notification_record(
    delivery_id: str,
    scan_key: str,
    *,
    kind: NotificationKind = NotificationKind.BRIEFING,
    status: NotificationStatus = NotificationStatus.PREPARED,
) -> NotificationOutbox:
    return NotificationOutbox(
        delivery_id=delivery_id,
        status=status,
        message=delivery_id,
        scan_key=scan_key,
        kind=kind,
    )


def test_configured_webhook_failure_is_queued_and_retried(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setattr(
        notification.NotificationFormatter,
        "format_daily_briefing",
        lambda self, **kwargs: "briefing",
    )
    send = AsyncMock(
        side_effect=[
            notification.DispatchResult(outcome=DispatchOutcome.FAILED, code=DispatchCode.HTTP_ERROR, status_code=500),
            notification.DispatchResult(outcome=DispatchOutcome.SENT),
        ]
    )
    monkeypatch.setattr(notification, "send_notification", send)

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))
    assert error.value.code is NotificationErrorCode.DELIVERY_FAILED

    pending = asyncio.run(state_manager.load_notification_outbox())
    history = asyncio.run(load_alert_history())
    assert pending.status is NotificationStatus.PREPARED
    assert history["KRW-BTC"].last_signal_type is SignalType.BREAKOUT_START

    result = asyncio.run(notification.recover_pending_notification())

    assert result.outcome is DispatchOutcome.SENT
    assert asyncio.run(state_manager.load_notification_outbox()) is None
    assert send.await_count == 2
    assert all(
        call.kwargs["delivery_id"] == pending.delivery_id
        for call in send.await_args_list
    )


def test_prepared_outbox_keeps_the_scan_handoff_durable_when_history_save_fails(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setattr(
        notification.NotificationFormatter,
        "format_daily_briefing",
        lambda self, **kwargs: "briefing",
    )
    monkeypatch.setattr(
        notification,
        "save_alert_history",
        AsyncMock(side_effect=RuntimeError("state write failed")),
    )
    send = AsyncMock()
    monkeypatch.setattr(notification, "send_notification", send)

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(
            notification.create_and_dispatch_notification(
                **_briefing_args(), scan_key="scan-a"
            )
        )

    assert error.value.scan_handoff_state is ScanHandoffState.DURABLE
    pending = asyncio.run(state_manager.load_notification_outbox())
    assert pending.status is NotificationStatus.PREPARED
    assert pending.scan_key == "scan-a"
    send.assert_not_awaited()


def test_committed_initial_outbox_write_error_is_a_durable_handoff(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    outbox = None

    async def load_outbox(_gcs_client=None):
        return outbox

    async def save_outbox(value, _gcs_client=None):
        nonlocal outbox
        outbox = value
        raise TimeoutError("response lost after commit")

    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(
        notification, "load_notification_backlog", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)
    send = AsyncMock()
    monkeypatch.setattr(notification, "send_notification", send)

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(
            notification._queue_and_dispatch_notification(
                "briefing", scan_key="scan-a"
            )
        )

    assert error.value.scan_handoff_state is ScanHandoffState.DURABLE
    assert error.value.scan_handoff_state is not ScanHandoffState.UNCERTAIN
    assert outbox.status is NotificationStatus.PREPARED
    assert outbox.scan_key == "scan-a"
    send.assert_not_awaited()


def test_unreadable_initial_outbox_write_error_is_an_uncertain_handoff(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    load_outbox = AsyncMock(side_effect=[None, TimeoutError("read unavailable")])
    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(
        notification, "load_notification_backlog", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        notification,
        "save_notification_outbox",
        AsyncMock(side_effect=TimeoutError("write outcome unknown")),
    )

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(
            notification._queue_and_dispatch_notification(
                "briefing", scan_key="scan-a"
            )
        )

    assert error.value.scan_handoff_state is not ScanHandoffState.DURABLE
    assert error.value.scan_handoff_state is ScanHandoffState.UNCERTAIN


def test_absent_initial_outbox_after_write_error_is_not_a_durable_handoff(
    monkeypatch,
):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setattr(
        notification, "load_notification_outbox", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        notification, "load_notification_backlog", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        notification,
        "save_notification_outbox",
        AsyncMock(side_effect=TimeoutError("write failed before commit")),
    )

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(
            notification._queue_and_dispatch_notification(
                "briefing", scan_key="scan-a"
            )
        )

    assert error.value.scan_handoff_state is not ScanHandoffState.DURABLE
    assert error.value.scan_handoff_state is not ScanHandoffState.UNCERTAIN


def test_committed_deferred_backlog_write_error_is_a_durable_handoff(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    backlog = [_notification_record("old-delivery", "scan-old")]

    async def load_backlog(_gcs_client=None):
        return list(backlog)

    async def save_backlog(value, _gcs_client=None):
        nonlocal backlog
        backlog = list(value)
        raise TimeoutError("response lost after commit")

    monkeypatch.setattr(
        notification, "load_notification_outbox", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(notification, "load_notification_backlog", load_backlog)
    monkeypatch.setattr(notification, "save_notification_backlog", save_backlog)
    monkeypatch.setattr(notification, "complete_scan_key", AsyncMock())

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(
            notification._queue_and_dispatch_notification(
                "new briefing", scan_key="scan-new"
            )
        )

    assert error.value.scan_handoff_state is ScanHandoffState.DURABLE
    assert error.value.scan_handoff_state is not ScanHandoffState.UNCERTAIN
    assert [item.scan_key for item in backlog] == ["scan-new"]


def test_webhook_request_uses_explicit_delivery_and_mention_metadata(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    request = {}

    class Response:
        ok = True

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        def post(self, url, **kwargs):
            request.update({"url": url, **kwargs})
            return Response()

    monkeypatch.setattr(notification.aiohttp, "ClientSession", Session)

    result = asyncio.run(
        notification.send_notification(
            "@channel\nbriefing",
            delivery_id="delivery-1",
            mention_channel=True,
        )
    )

    assert result.outcome is DispatchOutcome.SENT
    assert request["headers"] == {"X-Webhook-Delivery-ID": "delivery-1"}
    assert request["json"] == {"text": "@channel\nbriefing", "link_names": True}

    request.clear()
    asyncio.run(
        notification.send_notification(
            "@channel is literal text",
            delivery_id="delivery-2",
        )
    )
    assert request["json"] == {"text": "@channel is literal text"}


def test_delivery_id_is_stable_within_a_scan_and_distinct_across_scans():
    first = notification._notification_delivery_id("same message", "scan-a", "alert")

    assert first == notification._notification_delivery_id(
        "changed retry text", "scan-a", "alert"
    )
    assert first != notification._notification_delivery_id(
        "same message", "scan-b", "alert"
    )


def test_confirmed_send_with_failed_outbox_clear_is_not_repeated(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setattr(
        notification.NotificationFormatter,
        "format_daily_briefing",
        lambda self, **kwargs: "briefing",
    )
    outbox = None
    fail_clear = True
    operation_order = []

    async def load_outbox(_gcs_client=None):
        return outbox

    async def save_outbox(value, _gcs_client=None):
        nonlocal outbox
        if value is None and fail_clear:
            raise RuntimeError("state write failed after delivery")
        outbox = value

    async def save_history(_value, _gcs_client=None):
        operation_order.append("history")

    send = AsyncMock(return_value=notification.DispatchResult(outcome=DispatchOutcome.SENT))

    async def ordered_send(_message, **_kwargs):
        operation_order.append("send")
        return await send(_message)

    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)
    monkeypatch.setattr(notification, "save_alert_history", save_history)
    monkeypatch.setattr(notification, "send_notification", ordered_send)

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))

    assert error.value.delivery_state is DeliveryState.CONFIRMED
    assert outbox.status is NotificationStatus.DELIVERED
    assert operation_order == ["history", "send"]

    fail_clear = False
    resend = AsyncMock()
    monkeypatch.setattr(notification, "send_notification", resend)
    result = asyncio.run(notification.recover_pending_notification())

    assert result.outcome is DispatchOutcome.SENT
    assert outbox is None
    resend.assert_not_awaited()


def test_uncertain_attempt_is_preserved_for_operator_resolution(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    outbox = NotificationOutbox(
        delivery_id="delivery-1",
        status=NotificationStatus.ATTEMPTING,
        message="briefing",
    )

    async def load_outbox(_gcs_client=None):
        return outbox

    save_outbox = AsyncMock()
    send = AsyncMock()
    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)
    monkeypatch.setattr(notification, "send_notification", send)

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(notification.recover_pending_notification())

    assert (
        error.value.code
        is NotificationErrorCode.AMBIGUOUS_ATTEMPT_REQUIRES_RECONCILIATION
    )
    assert error.value.delivery_state is DeliveryState.UNCERTAIN
    assert error.value.scan_handoff_state is ScanHandoffState.DURABLE
    save_outbox.assert_not_awaited()
    send.assert_not_awaited()


def test_ambiguous_network_failure_keeps_attempting_outbox(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setattr(
        notification.NotificationFormatter,
        "format_daily_briefing",
        lambda self, **kwargs: "briefing",
    )
    outbox = None

    async def load_outbox(_gcs_client=None):
        return outbox

    async def save_outbox(value, _gcs_client=None):
        nonlocal outbox
        outbox = value

    send = AsyncMock(
        return_value=notification.DispatchResult(outcome=DispatchOutcome.UNCERTAIN, code=DispatchCode.UNEXPECTED_ERROR, detail="response lost")
    )
    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)
    monkeypatch.setattr(notification, "save_alert_history", AsyncMock())
    monkeypatch.setattr(notification, "send_notification", send)

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))

    assert error.value.code is NotificationErrorCode.DELIVERY_OUTCOME_UNCERTAIN
    assert error.value.delivery_state is DeliveryState.UNCERTAIN
    assert outbox.status is NotificationStatus.ATTEMPTING
    assert send.await_count == 1

    with pytest.raises(notification.NotificationDeliveryError) as recovery_error:
        asyncio.run(notification.recover_pending_notification())
    assert (
        recovery_error.value.code
        is NotificationErrorCode.AMBIGUOUS_ATTEMPT_REQUIRES_RECONCILIATION
    )
    assert send.await_count == 1


def test_missing_webhook_cancels_a_pending_delivery(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    outbox = NotificationOutbox(
        delivery_id="delivery-1",
        status=NotificationStatus.PREPARED,
        message="briefing",
    )

    async def load_outbox(_gcs_client=None):
        return outbox

    save_outbox = AsyncMock()
    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)

    result = asyncio.run(notification.recover_pending_notification())

    assert result.outcome is DispatchOutcome.SKIPPED
    save_outbox.assert_awaited_once_with(None, None)


def test_missing_webhook_preserves_an_ambiguous_attempt_and_cancels_deferred_work(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    now = datetime.datetime.now(datetime.timezone.utc)
    attempted = _history(SignalType.BREAKOUT_START, 101.0, now, minutes_ago=0)
    deferred_history = attempted.model_copy(
        update={
            "last_signal_type": SignalType.MOMENTUM_ACCELERATION,
            "last_price": 110.0,
        }
    )
    active = _notification_record(
            "active-delivery", "scan-active", kind=NotificationKind.ALERT, status=NotificationStatus.ATTEMPTING
        ).model_copy(
            update={
                "alert_history": {"KRW-BTC": attempted},
                "previous_alert_history": {},
                "alert_markets": ["KRW-BTC"],
            }
        )
    deferred = _notification_record(
        "deferred-delivery", "scan-deferred", kind=NotificationKind.ALERT
    ).model_copy(
        update={
            "alert_history": {"KRW-BTC": deferred_history},
            "previous_alert_history": {"KRW-BTC": attempted},
            "alert_markets": ["KRW-BTC"],
        }
    )
    asyncio.run(state_manager.save_alert_history({"KRW-BTC": deferred_history}))
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(notification.recover_pending_notification())

    assert (
        error.value.code
        is NotificationErrorCode.AMBIGUOUS_ATTEMPT_REQUIRES_RECONCILIATION
    )
    assert asyncio.run(state_manager.load_notification_outbox()) == active
    assert asyncio.run(state_manager.load_notification_backlog()) == []
    assert asyncio.run(load_alert_history())["KRW-BTC"] == attempted


def test_missing_webhook_rolls_back_prepared_alert_history(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setattr(
        notification.NotificationFormatter,
        "format_daily_briefing",
        lambda self, **kwargs: "briefing",
    )
    monkeypatch.setattr(
        notification,
        "send_notification",
        AsyncMock(return_value=notification.DispatchResult(outcome=DispatchOutcome.FAILED, code=DispatchCode.HTTP_ERROR, status_code=500)),
    )

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))
    assert error.value.code is NotificationErrorCode.DELIVERY_FAILED
    assert "KRW-BTC" in asyncio.run(load_alert_history())

    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    result = asyncio.run(notification.recover_pending_notification())

    assert result.outcome is DispatchOutcome.SKIPPED
    assert asyncio.run(load_alert_history()) == {}


def test_new_alert_is_queued_behind_an_unresolved_outbox(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setattr(
        notification.NotificationFormatter,
        "format_daily_briefing",
        lambda self, **kwargs: "new briefing",
    )
    active = NotificationOutbox(
        delivery_id="active-delivery",
        status=NotificationStatus.ATTEMPTING,
        message="old briefing",
        scan_key="scan-old",
        kind=NotificationKind.BRIEFING,
    )
    asyncio.run(state_manager.save_notification_outbox(active))
    send = AsyncMock()
    monkeypatch.setattr(notification, "send_notification", send)

    result = asyncio.run(
        notification.create_and_dispatch_notification(
            **_briefing_args(), scan_key="scan-new"
        )
    )

    backlog = asyncio.run(state_manager.load_notification_backlog())
    assert result.outcome is DispatchOutcome.QUEUED
    assert len(backlog) == 1
    assert backlog[0].scan_key == "scan-new"
    assert backlog[0].kind is NotificationKind.ALERT
    assert "KRW-BTC" in asyncio.run(load_alert_history())
    assert asyncio.run(state_manager.load_notification_outbox()) == active
    send.assert_not_awaited()


def test_retry_reuses_a_durable_notification_for_the_same_scan_across_kinds(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = _notification_record(
        "active-delivery", "scan-active", status=NotificationStatus.ATTEMPTING
    )
    existing = _notification_record(
        "existing-alert", "scan-shared", kind=NotificationKind.ALERT
    )
    assert asyncio.run(
        state_manager.claim_scan_key("scan-shared", execution_id="run-a")
    )
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([existing]))

    result = asyncio.run(
        notification._queue_and_dispatch_notification(
            "recomputed briefing",
            scan_key="scan-shared",
            notification_kind=NotificationKind.BRIEFING,
        )
    )

    assert result.outcome is DispatchOutcome.QUEUED
    assert result.delivery_id == "existing-alert"
    assert asyncio.run(state_manager.load_notification_backlog()) == [existing]
    assert not asyncio.run(
        state_manager.claim_scan_key("scan-shared", execution_id="run-a")
    )


def test_recovery_finalizes_deferred_scan_claims_before_the_scan_can_recompute(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = _notification_record(
        "active-delivery", "scan-active", status=NotificationStatus.ATTEMPTING
    )
    deferred = _notification_record(
        "deferred-delivery", "scan-deferred", kind=NotificationKind.ALERT
    )
    assert asyncio.run(
        state_manager.claim_scan_key("scan-deferred", execution_id="run-a")
    )
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(notification.recover_pending_notification())

    assert (
        error.value.code
        is NotificationErrorCode.AMBIGUOUS_ATTEMPT_REQUIRES_RECONCILIATION
    )
    assert not asyncio.run(
        state_manager.claim_scan_key("scan-deferred", execution_id="run-a")
    )


def test_repeated_briefings_coalesce_to_the_latest_deferred_scan(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = _notification_record(
        "active-delivery", "scan-active", status=NotificationStatus.ATTEMPTING
    )
    asyncio.run(state_manager.save_notification_outbox(active))

    asyncio.run(
        notification._queue_and_dispatch_notification(
            "first briefing", scan_key="scan-first"
        )
    )
    asyncio.run(
        notification._queue_and_dispatch_notification(
            "second briefing", scan_key="scan-second"
        )
    )

    backlog = asyncio.run(state_manager.load_notification_backlog())
    assert [item.scan_key for item in backlog] == ["scan-second"]
    assert backlog[0].message == "second briefing"


def test_notification_backlog_capacity_failure_is_explicit(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = _notification_record(
        "active-delivery", "scan-active", status=NotificationStatus.ATTEMPTING
    )
    backlog = [
        _notification_record(
            f"deferred-{index}", f"scan-{index}", kind=NotificationKind.DATA_QUALITY
        )
        for index in range(notification.MAX_NOTIFICATION_BACKLOG)
    ]
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog(backlog))

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(
            notification._queue_and_dispatch_notification(
                "overflow", scan_key="scan-overflow", notification_kind=NotificationKind.DATA_QUALITY
            )
        )

    assert error.value.code is NotificationErrorCode.BACKLOG_CAPACITY_EXCEEDED
    assert asyncio.run(state_manager.load_notification_backlog()) == backlog


def test_malformed_notification_backlog_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    (tmp_path / state_manager.NOTIFICATION_BACKLOG_FILE_NAME).write_text(
        "{}", encoding="utf-8"
    )

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.load_notification_backlog())
    assert error.value.code is StateErrorCode.INVALID_SCHEMA


def test_recovery_promotes_deferred_notification_after_active_success(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = NotificationOutbox(
        delivery_id="active-delivery",
        status=NotificationStatus.PREPARED,
        message="old briefing",
        scan_key="scan-old",
        kind=NotificationKind.BRIEFING,
    )
    deferred = NotificationOutbox(
        delivery_id="deferred-delivery",
        status=NotificationStatus.PREPARED,
        message="new alert",
        scan_key="scan-new",
        kind=NotificationKind.ALERT,
    )
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))
    send = AsyncMock(return_value=notification.DispatchResult(outcome=DispatchOutcome.SENT))
    monkeypatch.setattr(notification, "send_notification", send)

    first = asyncio.run(notification.recover_pending_notification())

    assert first.outcome is DispatchOutcome.SENT
    assert asyncio.run(state_manager.load_notification_outbox()) == deferred
    assert asyncio.run(state_manager.load_notification_backlog()) == []
    assert send.await_count == 1

    second = asyncio.run(notification.recover_pending_notification())

    assert second.outcome is DispatchOutcome.SENT
    assert asyncio.run(state_manager.load_notification_outbox()) is None
    assert send.await_count == 2


def test_briefing_recovery_reconciles_deferred_alert_history_after_queue_crash(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    now = datetime.datetime.now(datetime.timezone.utc)
    alert_history = _history(SignalType.BREAKOUT_START, 101.0, now, minutes_ago=0)
    active = _notification_record("active-delivery", "scan-active")
    deferred = _notification_record(
        "deferred-delivery", "scan-deferred", kind=NotificationKind.ALERT
    ).model_copy(
        update={
            "alert_history": {"KRW-BTC": alert_history},
            "previous_alert_history": {},
            "alert_markets": ["KRW-BTC"],
        }
    )
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))
    monkeypatch.setattr(
        notification,
        "send_notification",
        AsyncMock(return_value=notification.DispatchResult(outcome=DispatchOutcome.SENT)),
    )

    result = asyncio.run(notification.recover_pending_notification())

    assert result.outcome is DispatchOutcome.SENT
    assert asyncio.run(load_alert_history())["KRW-BTC"] == alert_history
    assert asyncio.run(state_manager.load_notification_outbox()) == deferred


def test_recovery_deduplicates_a_promoted_record_left_in_the_backlog(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = _notification_record("active-delivery", "scan-active")
    deferred = _notification_record("deferred-delivery", "scan-deferred")
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([active, deferred]))
    send = AsyncMock(return_value=notification.DispatchResult(outcome=DispatchOutcome.SENT))
    monkeypatch.setattr(notification, "send_notification", send)

    result = asyncio.run(notification.recover_pending_notification())

    assert result.outcome is DispatchOutcome.SENT
    assert asyncio.run(state_manager.load_notification_outbox()) == deferred
    assert asyncio.run(state_manager.load_notification_backlog()) == []
    send.assert_awaited_once()


def test_earlier_delivery_preserves_and_rebases_a_later_alert_for_the_same_market(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    old_timestamp = datetime.datetime.now(
        datetime.timezone.utc
    ) - datetime.timedelta(hours=25)
    breakout = _history(
        SignalType.BREAKOUT_START, 101.0, old_timestamp, minutes_ago=0
    )
    breakout.last_alert_timestamp = old_timestamp
    breakout.initial_timestamp = old_timestamp
    acceleration = breakout.model_copy(
        update={
            "last_alert_timestamp": old_timestamp + datetime.timedelta(minutes=10),
            "last_signal_type": SignalType.MOMENTUM_ACCELERATION,
            "last_price": 110.0,
            "last_rvol": 2.0,
        }
    )
    active = _notification_record(
        "active-delivery", "scan-active", kind=NotificationKind.ALERT
    ).model_copy(
        update={
            "alert_history": {"KRW-BTC": breakout},
            "previous_alert_history": {},
            "alert_markets": ["KRW-BTC"],
        }
    )
    deferred = _notification_record(
        "deferred-delivery", "scan-deferred", kind=NotificationKind.ALERT
    ).model_copy(
        update={
            "alert_history": {"KRW-BTC": acceleration},
            "previous_alert_history": {"KRW-BTC": breakout},
            "alert_markets": ["KRW-BTC"],
        }
    )
    asyncio.run(state_manager.save_alert_history({"KRW-BTC": acceleration}))
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))
    monkeypatch.setattr(
        notification,
        "send_notification",
        AsyncMock(return_value=notification.DispatchResult(outcome=DispatchOutcome.SENT)),
    )

    result = asyncio.run(notification.recover_pending_notification())

    assert result.outcome is DispatchOutcome.SENT
    current = asyncio.run(load_alert_history())["KRW-BTC"]
    promoted = asyncio.run(state_manager.load_notification_outbox())
    refreshed_predecessor = promoted.previous_alert_history["KRW-BTC"]
    assert current.last_signal_type is SignalType.MOMENTUM_ACCELERATION
    assert current.last_price == 110.0
    assert current.initial_timestamp == refreshed_predecessor.initial_timestamp
    assert current.last_alert_timestamp >= refreshed_predecessor.last_alert_timestamp
    assert refreshed_predecessor.last_alert_timestamp > old_timestamp

    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    canceled = asyncio.run(notification.recover_pending_notification())

    assert canceled.outcome is DispatchOutcome.SKIPPED
    rolled_back = asyncio.run(load_alert_history())["KRW-BTC"]
    assert rolled_back.last_signal_type is SignalType.BREAKOUT_START
    assert rolled_back.last_alert_timestamp == refreshed_predecessor.last_alert_timestamp


def test_missing_webhook_rolls_back_active_and_deferred_alert_history(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    now = datetime.datetime.now(datetime.timezone.utc)
    btc = _history(SignalType.BREAKOUT_START, 101.0, now, minutes_ago=0)
    eth = btc.model_copy(
        update={"market": "KRW-ETH", "last_price": 202.0, "initial_price": 202.0}
    )
    active = NotificationOutbox(
        delivery_id="active-delivery",
        status=NotificationStatus.PREPARED,
        message="old alert",
        alert_history={"KRW-BTC": btc},
        previous_alert_history={},
        alert_markets=["KRW-BTC"],
        scan_key="scan-old",
        kind=NotificationKind.ALERT,
    )
    deferred = NotificationOutbox(
        delivery_id="deferred-delivery",
        status=NotificationStatus.PREPARED,
        message="new alert",
        alert_history={"KRW-BTC": btc, "KRW-ETH": eth},
        previous_alert_history={"KRW-BTC": btc},
        alert_markets=["KRW-ETH"],
        scan_key="scan-new",
        kind=NotificationKind.ALERT,
    )
    asyncio.run(state_manager.save_alert_history({"KRW-BTC": btc, "KRW-ETH": eth}))
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))

    result = asyncio.run(notification.recover_pending_notification())

    assert result.outcome is DispatchOutcome.SKIPPED
    assert asyncio.run(load_alert_history()) == {}
    assert asyncio.run(state_manager.load_notification_outbox()) is None
    assert asyncio.run(state_manager.load_notification_backlog()) == []


def test_delayed_delivery_refreshes_the_alert_cooldown_timestamp(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    old_timestamp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=25)
    history = _history(
        SignalType.BREAKOUT_START, 101.0, old_timestamp, minutes_ago=0
    )
    history.last_alert_timestamp = old_timestamp
    history.initial_timestamp = old_timestamp
    outbox = NotificationOutbox(
        delivery_id="delivery-1",
        status=NotificationStatus.PREPARED,
        message="briefing",
        alert_history={"KRW-BTC": history},
        alert_markets=["KRW-BTC"],
    )
    saved_history = None

    async def load_outbox(_gcs_client=None):
        return outbox

    async def save_history(value, _gcs_client=None):
        nonlocal saved_history
        saved_history = value

    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", AsyncMock())
    monkeypatch.setattr(notification, "save_alert_history", save_history)
    monkeypatch.setattr(
        notification,
        "send_notification",
        AsyncMock(return_value=notification.DispatchResult(outcome=DispatchOutcome.SENT)),
    )

    asyncio.run(notification.recover_pending_notification())

    refreshed = saved_history["KRW-BTC"]
    assert refreshed.last_alert_timestamp > old_timestamp + datetime.timedelta(hours=24)
    assert refreshed.initial_timestamp == refreshed.last_alert_timestamp


def test_missing_webhook_skips_without_outbox_or_history_mutation(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    monkeypatch.setattr(config, "SHADOW_MODE", False, raising=False)
    monkeypatch.setattr(
        notification.NotificationFormatter,
        "format_daily_briefing",
        lambda self, **kwargs: "briefing",
    )
    save_outbox = AsyncMock()
    save_history = AsyncMock()
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)
    monkeypatch.setattr(notification, "save_alert_history", save_history)

    result = asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))

    assert result.outcome is DispatchOutcome.SKIPPED
    save_outbox.assert_not_awaited()
    save_history.assert_not_awaited()


def test_missing_webhook_shadow_mode_persists_isolated_cooldown_history(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    monkeypatch.setattr(config, "SHADOW_MODE", True, raising=False)
    monkeypatch.setattr(
        notification.NotificationFormatter,
        "format_daily_briefing",
        lambda self, **kwargs: "briefing",
    )
    save_outbox = AsyncMock()
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)

    result = asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))

    assert result.outcome is DispatchOutcome.SKIPPED
    save_outbox.assert_not_awaited()
    assert not (tmp_path / config.ALERT_HISTORY_FILE_NAME).exists()
    shadow_path = tmp_path / config.SHADOW_ALERT_HISTORY_FILE_NAME
    payload = json.loads(shadow_path.read_text(encoding="utf-8"))
    assert payload["KRW-BTC"]["last_signal_type"] == SignalType.BREAKOUT_START.value

    history = asyncio.run(load_alert_history())
    assert history["KRW-BTC"].last_signal_type is SignalType.BREAKOUT_START
    assert AlertEngine().process_signals(
        [_candidate(101.0)], {"KRW-BTC": _ticker()}, history
    ) == []


def test_process_signals_classifies_downtrend_follow_up_as_acceleration():
    assert (
        _process_signal_type(SignalType.DOWNTREND_ACCELERATION, 98.0)
        is SignalType.DOWNTREND_ACCELERATION
    )


def test_process_signals_classifies_momentum_reversal_as_bull_failed():
    assert (
        _process_signal_type(SignalType.MOMENTUM_ACCELERATION, 97.0)
        is SignalType.BULL_MOMENTUM_FAILED
    )


def test_process_signals_classifies_followup_bull_failed_as_bull_failed():
    assert (
        _process_signal_type(SignalType.BULL_MOMENTUM_FAILED, 97.0)
        is SignalType.BULL_MOMENTUM_FAILED
    )


def test_process_signals_classifies_followup_bear_failed_as_bear_failed():
    assert (
        _process_signal_type(SignalType.BEAR_MOMENTUM_FAILED, 102.0)
        is SignalType.BEAR_MOMENTUM_FAILED
    )


def test_prior_downtrend_acceleration_with_continued_lower_price_is_acceleration():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(98.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history(
                SignalType.DOWNTREND_ACCELERATION, 100.0, now
            ),
        },
    )

    assert signal_type is SignalType.DOWNTREND_ACCELERATION


def test_prior_downtrend_acceleration_with_rebound_is_bear_failed():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(102.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history(
                SignalType.DOWNTREND_ACCELERATION, 100.0, now
            ),
        },
    )

    assert signal_type is SignalType.BEAR_MOMENTUM_FAILED


def test_prior_momentum_acceleration_with_continued_upside_is_acceleration():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(102.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history(SignalType.MOMENTUM_ACCELERATION, 100.0, now),
        },
    )

    assert signal_type is SignalType.MOMENTUM_ACCELERATION


def test_prior_momentum_acceleration_with_reversal_down_is_bull_failed():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(97.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history(SignalType.MOMENTUM_ACCELERATION, 100.0, now),
        },
    )

    assert signal_type is SignalType.BULL_MOMENTUM_FAILED


def test_bullish_continuation_below_threshold_is_suppressed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(100.99),
        ticker=_ticker(),
        history={"KRW-BTC": _history(SignalType.BREAKOUT_START, 100.0, now)},
    )

    assert signal_type is None


def test_bearish_continuation_at_threshold_is_allowed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(99.0),
        ticker=_ticker(),
        history={"KRW-BTC": _history(SignalType.BREAKDOWN_START, 100.0, now)},
    )

    assert signal_type is SignalType.DOWNTREND_ACCELERATION


def test_small_continuation_is_allowed_after_cooldown_expires():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(100.01),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history(
                SignalType.BREAKOUT_START,
                100.0,
                now,
                minutes_ago=61,
            )
        },
    )

    assert signal_type is SignalType.MOMENTUM_ACCELERATION


def test_structure_failure_is_allowed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(99.0),
        ticker=_ticker(),
        history={"KRW-BTC": _history(SignalType.BREAKOUT_START, 100.0, now)},
    )

    assert signal_type is SignalType.BULL_MOMENTUM_FAILED

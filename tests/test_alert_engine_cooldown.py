import asyncio
import datetime
import json
from unittest.mock import AsyncMock

import pytest

import config
import common.notification.main as notification
from common import state_manager
from common.models import Alert, AlertHistory, SignalCandidate, TickerData
from common.notification.engine import AlertEngine
from common.state_manager import load_alert_history
from common.storage_client import StateLoadError


def _history(
    signal_type: str,
    last_price: float,
    now: datetime.datetime,
    minutes_ago: int = 30,
) -> AlertHistory:
    bearish = "BEAR" in signal_type or "DOWNTREND" in signal_type or "BREAKDOWN" in signal_type
    return AlertHistory(
        market="KRW-BTC",
        last_alert_timestamp=now - datetime.timedelta(minutes=minutes_ago),
        last_signal_type=signal_type,
        last_price=last_price,
        last_rvol=1.0,
        initial_timestamp=now - datetime.timedelta(hours=2),
        initial_price=last_price,
        structure_level=100.0,
        structure_direction="bearish" if bearish else "bullish",
    )


def _candidate(current_price: float, price_change: float = 1.2) -> SignalCandidate:
    return SignalCandidate(
        market="KRW-BTC",
        signal_score=0.75,
        price_change=price_change,
        rvol=1.0,
        rvol_z_score=1.0,
        contexts=[],
        current_price=current_price,
    )


def _ticker() -> TickerData:
    return TickerData(
        market="KRW-BTC",
        candle_history=[],
        price_change_10m=0.0,
        price_change_1h=0.0,
    )


def _process_signal_type(previous_signal_type: str, current_price: float) -> str:
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

    with pytest.raises(StateLoadError, match="JSON object"):
        asyncio.run(load_alert_history())


def test_alert_history_expires_before_transition_classification(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    stale = _history(
        "BREAKOUT_START",
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
        signal_type="BREAKOUT_START",
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
        "market_regime": {},
        "final_alerts": [_breakout_alert()],
    }


def _notification_record(
    delivery_id: str,
    scan_key: str,
    *,
    kind: str = "briefing",
    status: str = "prepared",
):
    return {
        "delivery_id": delivery_id,
        "status": status,
        "message": delivery_id,
        "alert_history": None,
        "previous_alert_history": None,
        "alert_markets": [],
        "scan_key": scan_key,
        "kind": kind,
    }


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
            notification.DispatchResult(False, "HTTP 500", 500),
            notification.DispatchResult(True),
        ]
    )
    monkeypatch.setattr(notification, "send_notification", send)

    with pytest.raises(notification.NotificationDeliveryError, match="HTTP 500"):
        asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))

    pending = asyncio.run(state_manager.load_notification_outbox())
    history = asyncio.run(load_alert_history())
    assert pending["status"] == "prepared"
    assert history["KRW-BTC"].last_signal_type == "BREAKOUT_START"

    result = asyncio.run(notification.recover_pending_notification())

    assert result.sent is True
    assert asyncio.run(state_manager.load_notification_outbox()) is None
    assert send.await_count == 2
    assert all(
        call.kwargs["delivery_id"] == pending["delivery_id"]
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

    assert error.value.scan_handoff_durable is True
    pending = asyncio.run(state_manager.load_notification_outbox())
    assert pending["status"] == "prepared"
    assert pending["scan_key"] == "scan-a"
    send.assert_not_awaited()


def test_webhook_request_exposes_the_delivery_id_header(monkeypatch):
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
        notification.send_notification("briefing", delivery_id="delivery-1")
    )

    assert result.sent is True
    assert request["headers"] == {"X-Webhook-Delivery-ID": "delivery-1"}


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

    send = AsyncMock(return_value=notification.DispatchResult(True))

    async def ordered_send(_message, **_kwargs):
        operation_order.append("send")
        return await send(_message)

    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)
    monkeypatch.setattr(notification, "save_alert_history", save_history)
    monkeypatch.setattr(notification, "send_notification", ordered_send)

    with pytest.raises(notification.NotificationDeliveryError) as error:
        asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))

    assert error.value.delivery_confirmed is True
    assert outbox["status"] == "delivered"
    assert operation_order == ["history", "send"]

    fail_clear = False
    resend = AsyncMock()
    monkeypatch.setattr(notification, "send_notification", resend)
    result = asyncio.run(notification.recover_pending_notification())

    assert result.sent is True
    assert outbox is None
    resend.assert_not_awaited()


def test_uncertain_attempt_is_preserved_for_operator_resolution(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    outbox = {
        "delivery_id": "delivery-1",
        "status": "attempting",
        "message": "briefing",
        "alert_history": None,
    }

    async def load_outbox(_gcs_client=None):
        return outbox

    save_outbox = AsyncMock()
    send = AsyncMock()
    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)
    monkeypatch.setattr(notification, "send_notification", send)

    with pytest.raises(notification.NotificationDeliveryError, match="outcome is uncertain"):
        asyncio.run(notification.recover_pending_notification())

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
        return_value=notification.DispatchResult(
            False, "response lost", delivery_uncertain=True
        )
    )
    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)
    monkeypatch.setattr(notification, "save_alert_history", AsyncMock())
    monkeypatch.setattr(notification, "send_notification", send)

    with pytest.raises(notification.NotificationDeliveryError, match="outcome is uncertain"):
        asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))

    assert outbox["status"] == "attempting"
    assert send.await_count == 1

    with pytest.raises(notification.NotificationDeliveryError, match="outcome is uncertain"):
        asyncio.run(notification.recover_pending_notification())
    assert send.await_count == 1


def test_missing_webhook_cancels_a_pending_delivery(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    outbox = {
        "delivery_id": "delivery-1",
        "status": "prepared",
        "message": "briefing",
        "alert_history": None,
    }

    async def load_outbox(_gcs_client=None):
        return outbox

    save_outbox = AsyncMock()
    monkeypatch.setattr(notification, "load_notification_outbox", load_outbox)
    monkeypatch.setattr(notification, "save_notification_outbox", save_outbox)

    result = asyncio.run(notification.recover_pending_notification())

    assert result.skipped is True
    save_outbox.assert_awaited_once_with(None, None)


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
        AsyncMock(return_value=notification.DispatchResult(False, "HTTP 500", 500)),
    )

    with pytest.raises(notification.NotificationDeliveryError, match="HTTP 500"):
        asyncio.run(notification.create_and_dispatch_notification(**_briefing_args()))
    assert "KRW-BTC" in asyncio.run(load_alert_history())

    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    result = asyncio.run(notification.recover_pending_notification())

    assert result.skipped is True
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
    active = {
        "delivery_id": "active-delivery",
        "status": "attempting",
        "message": "old briefing",
        "alert_history": None,
        "previous_alert_history": None,
        "alert_markets": [],
        "scan_key": "scan-old",
        "kind": "briefing",
    }
    asyncio.run(state_manager.save_notification_outbox(active))
    send = AsyncMock()
    monkeypatch.setattr(notification, "send_notification", send)

    result = asyncio.run(
        notification.create_and_dispatch_notification(
            **_briefing_args(), scan_key="scan-new"
        )
    )

    backlog = asyncio.run(state_manager.load_notification_backlog())
    assert result.queued is True
    assert len(backlog) == 1
    assert backlog[0]["scan_key"] == "scan-new"
    assert backlog[0]["kind"] == "alert"
    assert "KRW-BTC" in asyncio.run(load_alert_history())
    assert asyncio.run(state_manager.load_notification_outbox()) == active
    send.assert_not_awaited()


def test_repeated_briefings_coalesce_to_the_latest_deferred_scan(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = _notification_record(
        "active-delivery", "scan-active", status="attempting"
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
    assert [item["scan_key"] for item in backlog] == ["scan-second"]
    assert backlog[0]["message"] == "second briefing"


def test_notification_backlog_capacity_failure_is_explicit(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = _notification_record(
        "active-delivery", "scan-active", status="attempting"
    )
    backlog = [
        _notification_record(
            f"deferred-{index}", f"scan-{index}", kind="data_quality"
        )
        for index in range(notification.MAX_NOTIFICATION_BACKLOG)
    ]
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog(backlog))

    with pytest.raises(
        notification.NotificationDeliveryError,
        match=f"reached {notification.MAX_NOTIFICATION_BACKLOG}",
    ):
        asyncio.run(
            notification._queue_and_dispatch_notification(
                "overflow", scan_key="scan-overflow", notification_kind="data_quality"
            )
        )

    assert asyncio.run(state_manager.load_notification_backlog()) == backlog


def test_malformed_notification_backlog_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    (tmp_path / state_manager.NOTIFICATION_BACKLOG_FILE_NAME).write_text(
        "{}", encoding="utf-8"
    )

    with pytest.raises(StateLoadError, match="JSON array"):
        asyncio.run(state_manager.load_notification_backlog())


def test_recovery_promotes_deferred_notification_after_active_success(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    active = {
        "delivery_id": "active-delivery",
        "status": "prepared",
        "message": "old briefing",
        "alert_history": None,
        "previous_alert_history": None,
        "alert_markets": [],
        "scan_key": "scan-old",
        "kind": "briefing",
    }
    deferred = {
        "delivery_id": "deferred-delivery",
        "status": "prepared",
        "message": "new alert",
        "alert_history": None,
        "previous_alert_history": None,
        "alert_markets": [],
        "scan_key": "scan-new",
        "kind": "alert",
    }
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))
    send = AsyncMock(return_value=notification.DispatchResult(True))
    monkeypatch.setattr(notification, "send_notification", send)

    first = asyncio.run(notification.recover_pending_notification())

    assert first.sent is True
    assert asyncio.run(state_manager.load_notification_outbox()) == deferred
    assert asyncio.run(state_manager.load_notification_backlog()) == []
    assert send.await_count == 1

    second = asyncio.run(notification.recover_pending_notification())

    assert second.sent is True
    assert asyncio.run(state_manager.load_notification_outbox()) is None
    assert send.await_count == 2


def test_briefing_recovery_reconciles_deferred_alert_history_after_queue_crash(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    now = datetime.datetime.now(datetime.timezone.utc)
    alert_history = _history("BREAKOUT_START", 101.0, now, minutes_ago=0)
    active = _notification_record("active-delivery", "scan-active")
    deferred = {
        **_notification_record("deferred-delivery", "scan-deferred", kind="alert"),
        "alert_history": {"KRW-BTC": alert_history.model_dump(mode="json")},
        "previous_alert_history": {},
        "alert_markets": ["KRW-BTC"],
    }
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))
    monkeypatch.setattr(
        notification,
        "send_notification",
        AsyncMock(return_value=notification.DispatchResult(True)),
    )

    result = asyncio.run(notification.recover_pending_notification())

    assert result.sent is True
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
    send = AsyncMock(return_value=notification.DispatchResult(True))
    monkeypatch.setattr(notification, "send_notification", send)

    result = asyncio.run(notification.recover_pending_notification())

    assert result.sent is True
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
    breakout = _history("BREAKOUT_START", 101.0, old_timestamp, minutes_ago=0)
    breakout.last_alert_timestamp = old_timestamp
    breakout.initial_timestamp = old_timestamp
    acceleration = breakout.model_copy(
        update={
            "last_alert_timestamp": old_timestamp + datetime.timedelta(minutes=10),
            "last_signal_type": "MOMENTUM_ACCELERATION",
            "last_price": 110.0,
            "last_rvol": 2.0,
        }
    )
    active = {
        **_notification_record("active-delivery", "scan-active", kind="alert"),
        "alert_history": {"KRW-BTC": breakout.model_dump(mode="json")},
        "previous_alert_history": {},
        "alert_markets": ["KRW-BTC"],
    }
    deferred = {
        **_notification_record("deferred-delivery", "scan-deferred", kind="alert"),
        "alert_history": {"KRW-BTC": acceleration.model_dump(mode="json")},
        "previous_alert_history": {
            "KRW-BTC": breakout.model_dump(mode="json")
        },
        "alert_markets": ["KRW-BTC"],
    }
    asyncio.run(state_manager.save_alert_history({"KRW-BTC": acceleration}))
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))
    monkeypatch.setattr(
        notification,
        "send_notification",
        AsyncMock(return_value=notification.DispatchResult(True)),
    )

    result = asyncio.run(notification.recover_pending_notification())

    assert result.sent is True
    current = asyncio.run(load_alert_history())["KRW-BTC"]
    promoted = asyncio.run(state_manager.load_notification_outbox())
    refreshed_predecessor = AlertHistory.model_validate(
        promoted["previous_alert_history"]["KRW-BTC"]
    )
    assert current.last_signal_type == "MOMENTUM_ACCELERATION"
    assert current.last_price == 110.0
    assert current.initial_timestamp == refreshed_predecessor.initial_timestamp
    assert current.last_alert_timestamp >= refreshed_predecessor.last_alert_timestamp
    assert refreshed_predecessor.last_alert_timestamp > old_timestamp

    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    canceled = asyncio.run(notification.recover_pending_notification())

    assert canceled.skipped is True
    rolled_back = asyncio.run(load_alert_history())["KRW-BTC"]
    assert rolled_back.last_signal_type == "BREAKOUT_START"
    assert rolled_back.last_alert_timestamp == refreshed_predecessor.last_alert_timestamp


def test_missing_webhook_rolls_back_active_and_deferred_alert_history(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
    now = datetime.datetime.now(datetime.timezone.utc)
    btc = _history("BREAKOUT_START", 101.0, now, minutes_ago=0)
    eth = btc.model_copy(
        update={"market": "KRW-ETH", "last_price": 202.0, "initial_price": 202.0}
    )
    active = {
        "delivery_id": "active-delivery",
        "status": "prepared",
        "message": "old alert",
        "alert_history": {"KRW-BTC": btc.model_dump(mode="json")},
        "previous_alert_history": {},
        "alert_markets": ["KRW-BTC"],
        "scan_key": "scan-old",
        "kind": "alert",
    }
    deferred = {
        "delivery_id": "deferred-delivery",
        "status": "prepared",
        "message": "new alert",
        "alert_history": {
            "KRW-BTC": btc.model_dump(mode="json"),
            "KRW-ETH": eth.model_dump(mode="json"),
        },
        "previous_alert_history": {"KRW-BTC": btc.model_dump(mode="json")},
        "alert_markets": ["KRW-ETH"],
        "scan_key": "scan-new",
        "kind": "alert",
    }
    asyncio.run(state_manager.save_alert_history({"KRW-BTC": btc, "KRW-ETH": eth}))
    asyncio.run(state_manager.save_notification_outbox(active))
    asyncio.run(state_manager.save_notification_backlog([deferred]))

    result = asyncio.run(notification.recover_pending_notification())

    assert result.skipped is True
    assert asyncio.run(load_alert_history()) == {}
    assert asyncio.run(state_manager.load_notification_outbox()) is None
    assert asyncio.run(state_manager.load_notification_backlog()) == []


def test_delayed_delivery_refreshes_the_alert_cooldown_timestamp(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", "https://example.invalid/webhook")
    old_timestamp = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=25)
    history = _history("BREAKOUT_START", 101.0, old_timestamp, minutes_ago=0)
    history.last_alert_timestamp = old_timestamp
    history.initial_timestamp = old_timestamp
    outbox = {
        "delivery_id": "delivery-1",
        "status": "prepared",
        "message": "briefing",
        "alert_history": {"KRW-BTC": history.model_dump(mode="json")},
        "alert_markets": ["KRW-BTC"],
    }
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
        AsyncMock(return_value=notification.DispatchResult(True)),
    )

    asyncio.run(notification.recover_pending_notification())

    refreshed = saved_history["KRW-BTC"]
    assert refreshed.last_alert_timestamp > old_timestamp + datetime.timedelta(hours=24)
    assert refreshed.initial_timestamp == refreshed.last_alert_timestamp


def test_missing_webhook_skips_without_outbox_or_history_mutation(monkeypatch):
    monkeypatch.setattr(config, "WEBHOOK_URL", None)
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

    assert result.skipped is True
    save_outbox.assert_not_awaited()
    save_history.assert_not_awaited()


def test_process_signals_classifies_downtrend_follow_up_as_acceleration():
    assert (
        _process_signal_type("DOWNTREND_ACCELERATION", 98.0)
        == "DOWNTREND_ACCELERATION"
    )


def test_process_signals_classifies_momentum_reversal_as_bull_failed():
    assert (
        _process_signal_type("MOMENTUM_ACCELERATION", 97.0)
        == "BULL_MOMENTUM_FAILED"
    )


def test_process_signals_classifies_followup_bull_sustained_as_bull_sustained():
    assert (
        _process_signal_type("BULL_MOMENTUM_SUSTAINED", 104.0)
        == "MOMENTUM_ACCELERATION"
    )


def test_process_signals_classifies_followup_bull_failed_as_bull_failed():
    assert (
        _process_signal_type("BULL_MOMENTUM_FAILED", 97.0)
        == "BULL_MOMENTUM_FAILED"
    )


def test_process_signals_classifies_followup_bear_sustained_as_bear_sustained():
    assert (
        _process_signal_type("BEAR_MOMENTUM_SUSTAINED", 96.0)
        == "DOWNTREND_ACCELERATION"
    )


def test_process_signals_classifies_followup_bear_failed_as_bear_failed():
    assert (
        _process_signal_type("BEAR_MOMENTUM_FAILED", 102.0)
        == "BEAR_MOMENTUM_FAILED"
    )


def test_prior_downtrend_acceleration_with_continued_lower_price_is_acceleration():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(98.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("DOWNTREND_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "DOWNTREND_ACCELERATION"


def test_prior_downtrend_acceleration_with_rebound_is_bear_failed():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(102.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("DOWNTREND_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "BEAR_MOMENTUM_FAILED"


def test_prior_momentum_acceleration_with_continued_upside_is_acceleration():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(102.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("MOMENTUM_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "MOMENTUM_ACCELERATION"


def test_prior_momentum_acceleration_with_reversal_down_is_bull_failed():
    now = datetime.datetime.now(datetime.timezone.utc)
    engine = AlertEngine()

    signal_type, _, _ = engine._get_alert_type_and_priority(
        candidate=_candidate(97.0),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history("MOMENTUM_ACCELERATION", 100.0, now),
        },
    )

    assert signal_type == "BULL_MOMENTUM_FAILED"


def test_bullish_continuation_below_threshold_is_suppressed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(100.99),
        ticker=_ticker(),
        history={"KRW-BTC": _history("BREAKOUT_START", 100.0, now)},
    )

    assert signal_type is None


def test_bearish_continuation_at_threshold_is_allowed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(99.0),
        ticker=_ticker(),
        history={"KRW-BTC": _history("BREAKDOWN_START", 100.0, now)},
    )

    assert signal_type == "DOWNTREND_ACCELERATION"


def test_small_continuation_is_allowed_after_cooldown_expires():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(100.01),
        ticker=_ticker(),
        history={
            "KRW-BTC": _history(
                "BREAKOUT_START",
                100.0,
                now,
                minutes_ago=61,
            )
        },
    )

    assert signal_type == "MOMENTUM_ACCELERATION"


def test_structure_failure_is_allowed_during_cooldown():
    now = datetime.datetime.now(datetime.timezone.utc)

    signal_type, _, _ = AlertEngine()._get_alert_type_and_priority(
        candidate=_candidate(99.0),
        ticker=_ticker(),
        history={"KRW-BTC": _history("BREAKOUT_START", 100.0, now)},
    )

    assert signal_type == "BULL_MOMENTUM_FAILED"

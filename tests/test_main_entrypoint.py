import datetime

import pytest
from unittest.mock import AsyncMock

import main as app
from common.models import CandleData


@pytest.fixture(autouse=True)
def no_pending_notification(monkeypatch):
    monkeypatch.setattr(app, "recover_pending_notification", AsyncMock(return_value=None))


def test_cloud_function_entrypoint_returns_ok_when_run_check_succeeds(monkeypatch):
    async def run_check_success(execution_id=None):
        return None

    monkeypatch.setattr(app, "run_check", run_check_success)

    assert app.main(None) == ("OK", 200)


def test_cloud_function_entrypoint_returns_500_when_run_check_fails(monkeypatch):
    async def run_check_failure(execution_id=None):
        raise RuntimeError("boom")

    monkeypatch.setattr(app, "run_check", run_check_failure)

    assert app.main(None) == ("Internal Server Error", 500)


def test_run_check_dispatches_data_quality_incident_and_skips_market_briefing(monkeypatch):
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(return_value=[{"market": "KRW-BTC"}, {"market": "KRW-ETH"}]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-ETH": []}))
    data_quality_alert = AsyncMock()
    monkeypatch.setattr(app, "dispatch_data_quality_alert", data_quality_alert)
    monkeypatch.setattr(app, "append_scan_events", AsyncMock())
    market_briefing = AsyncMock()
    monkeypatch.setattr(app, "create_and_dispatch_notification", market_briefing)
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())

    import asyncio

    asyncio.run(app.run_check())

    data_quality_alert.assert_awaited_once()
    market_briefing.assert_not_awaited()
    app.save_rank_state_history.assert_not_awaited()


def test_run_check_skips_mutations_when_the_completed_candle_is_claimed(monkeypatch):
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=False))
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(return_value=[{"market": "KRW-BTC", "acc_trade_price_24h": 1.0}]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-BTC": []}))
    append_events = AsyncMock()
    monkeypatch.setattr(app, "append_scan_events", append_events)
    data_quality_alert = AsyncMock()
    monkeypatch.setattr(app, "dispatch_data_quality_alert", data_quality_alert)

    import asyncio

    asyncio.run(app.run_check())

    append_events.assert_not_awaited()
    data_quality_alert.assert_not_awaited()


def test_run_check_does_not_consume_scan_key_when_market_collection_fails(monkeypatch):
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(app, "get_all_krw_tickers", AsyncMock(side_effect=RuntimeError("temporary")))
    claim = AsyncMock()
    monkeypatch.setattr(app, "claim_scan_key", claim)

    import asyncio

    with pytest.raises(RuntimeError, match="Failed to execute the main pipeline"):
        asyncio.run(app.run_check())

    claim.assert_not_awaited()


def test_run_check_releases_claim_when_persistence_fails_before_notification(monkeypatch):
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(return_value=[{"market": "KRW-BTC", "acc_trade_price_24h": 1.0}]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-BTC": []}))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "append_scan_events", AsyncMock(side_effect=RuntimeError("disk")))
    release = AsyncMock()
    monkeypatch.setattr(app, "release_scan_key", release)
    notify = AsyncMock()
    monkeypatch.setattr(app, "dispatch_data_quality_alert", notify)

    import asyncio

    with pytest.raises(RuntimeError, match="Failed to execute the main pipeline"):
        asyncio.run(app.run_check())

    release.assert_awaited_once()
    notify.assert_not_awaited()


def test_unknown_regime_marks_candidate_decisions_for_event_logging():
    decisions = {
        "KRW-BTC": app.CandidateDecision(True, []),
        "KRW-ETH": app.CandidateDecision(True, []),
    }

    assert app.record_market_regime_block(["KRW-BTC"], decisions, {"regime": "UNKNOWN"}) == []
    assert decisions["KRW-BTC"] == app.CandidateDecision(False, ["market_regime_unknown"])
    assert decisions["KRW-ETH"] == app.CandidateDecision(True, [])


def test_run_check_persists_events_for_every_market_after_a_valid_scan(monkeypatch):
    timestamp = datetime.datetime(2026, 6, 18, tzinfo=datetime.timezone.utc)
    candles = [
        CandleData(
            market="KRW-BTC",
            timestamp=timestamp,
            open_price=100.0,
            high_price=100.0,
            low_price=100.0,
            close_price=100.0,
            volume=1.0,
        ),
        CandleData(
            market="KRW-BTC",
            timestamp=timestamp + datetime.timedelta(minutes=10),
            open_price=100.0,
            high_price=100.0,
            low_price=100.0,
            close_price=100.0,
            volume=1.0,
        ),
    ]
    raw_tickers = [
        {"market": "KRW-BTC", "acc_trade_price_24h": 2.0},
        {"market": "KRW-ETH", "acc_trade_price_24h": 1.0},
    ]
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(app, "get_all_krw_tickers", AsyncMock(return_value=raw_tickers))
    monkeypatch.setattr(
        app,
        "get_candles",
        AsyncMock(return_value={"KRW-BTC": candles, "KRW-ETH": candles}),
    )
    monkeypatch.setattr(app, "load_pending_scan_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_alert_history", AsyncMock(return_value={}))
    append_events = AsyncMock()
    monkeypatch.setattr(app, "append_scan_events", append_events)
    monkeypatch.setattr(app, "append_scan_outcomes", AsyncMock())
    monkeypatch.setattr(app, "save_pending_scan_events", AsyncMock())
    monkeypatch.setattr(app, "create_and_dispatch_notification", AsyncMock())
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())

    import asyncio

    asyncio.run(app.run_check())

    persisted_events = append_events.await_args.args[0]
    assert {event.market for event in persisted_events} == {"KRW-BTC", "KRW-ETH"}
    assert all(event.final_decision == "rejected_lightweight" for event in persisted_events)


def _configure_valid_scan_with_notification_error(monkeypatch, error):
    timestamp = datetime.datetime(2026, 6, 18, tzinfo=datetime.timezone.utc)
    candles = [
        CandleData(
            market="KRW-BTC",
            timestamp=timestamp + datetime.timedelta(minutes=10 * index),
            open_price=100.0,
            high_price=100.0,
            low_price=100.0,
            close_price=100.0,
            volume=1.0,
        )
        for index in range(2)
    ]
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(return_value=[{"market": "KRW-BTC", "acc_trade_price_24h": 1.0}]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-BTC": candles}))
    monkeypatch.setattr(app, "load_pending_scan_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_alert_history", AsyncMock(return_value={}))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "append_scan_events", AsyncMock())
    monkeypatch.setattr(app, "append_scan_outcomes", AsyncMock())
    monkeypatch.setattr(app, "save_pending_scan_events", AsyncMock())
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())
    monkeypatch.setattr(
        app, "create_and_dispatch_notification", AsyncMock(side_effect=error)
    )
    release = AsyncMock()
    monkeypatch.setattr(app, "release_scan_key", release)
    return release


def test_run_check_releases_claim_when_configured_webhook_fails(monkeypatch):
    release = _configure_valid_scan_with_notification_error(
        monkeypatch,
        app.NotificationDeliveryError("HTTP 500"),
    )

    import asyncio

    with pytest.raises(RuntimeError, match="Failed to execute the main pipeline"):
        asyncio.run(app.run_check())

    release.assert_awaited_once()


def test_run_check_retains_claim_after_confirmed_delivery_finalization_failure(monkeypatch):
    release = _configure_valid_scan_with_notification_error(
        monkeypatch,
        app.NotificationDeliveryError("outbox clear failed", delivery_confirmed=True),
    )

    import asyncio

    with pytest.raises(RuntimeError, match="Failed to execute the main pipeline"):
        asyncio.run(app.run_check())

    release.assert_not_awaited()

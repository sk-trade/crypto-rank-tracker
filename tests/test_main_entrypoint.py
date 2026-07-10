import datetime

import pytest
from unittest.mock import AsyncMock

import main as app
from common.models import CandleData


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


def test_run_check_skips_all_side_effects_when_the_completed_candle_is_claimed(monkeypatch):
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=False))
    market_loader = AsyncMock()
    monkeypatch.setattr(app, "get_all_krw_tickers", market_loader)

    import asyncio

    asyncio.run(app.run_check())

    market_loader.assert_not_awaited()


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

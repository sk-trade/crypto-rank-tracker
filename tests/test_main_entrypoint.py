import pytest
from unittest.mock import AsyncMock

import main as app


def test_cloud_function_entrypoint_returns_ok_when_run_check_succeeds(monkeypatch):
    async def run_check_success():
        return None

    monkeypatch.setattr(app, "run_check", run_check_success)

    assert app.main(None) == ("OK", 200)


def test_cloud_function_entrypoint_returns_500_when_run_check_fails(monkeypatch):
    async def run_check_failure():
        raise RuntimeError("boom")

    monkeypatch.setattr(app, "run_check", run_check_failure)

    assert app.main(None) == ("Internal Server Error", 500)


def test_run_check_dispatches_data_quality_incident_and_skips_market_briefing(monkeypatch):
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(return_value=[{"market": "KRW-BTC"}, {"market": "KRW-ETH"}]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-ETH": []}))
    data_quality_alert = AsyncMock()
    monkeypatch.setattr(app, "dispatch_data_quality_alert", data_quality_alert)
    market_briefing = AsyncMock()
    monkeypatch.setattr(app, "create_and_dispatch_notification", market_briefing)
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())

    import asyncio

    asyncio.run(app.run_check())

    data_quality_alert.assert_awaited_once()
    market_briefing.assert_not_awaited()
    app.save_rank_state_history.assert_not_awaited()

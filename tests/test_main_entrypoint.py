import datetime
from types import SimpleNamespace

import pytest
from unittest.mock import AsyncMock, Mock

import main as app
from common.models import (
    AttentionState,
    CandleData,
    CandidateDecision,
    DeliveryState,
    EvidenceFamily,
    EvidenceVerdict,
    MarketEvent,
    MarketTicker,
    MarketRegime,
    MarketRegimeSnapshot,
    NotificationErrorCode,
    RejectionCode,
    ScanDecision,
    ScanHandoffState,
    TickerData,
)


def _market_ticker(market: str, turnover: float = 1.0) -> MarketTicker:
    return MarketTicker(
        market=market,
        acc_trade_price_24h=turnover,
        market_event=MarketEvent(
            warning=False,
            caution={"PRICE_FLUCTUATIONS": False},
        ),
    )


@pytest.fixture(autouse=True)
def no_pending_notification(monkeypatch):
    monkeypatch.setattr(app, "recover_pending_notification", AsyncMock(return_value=None))
    monkeypatch.setattr(app, "complete_scan_key", AsyncMock())
    monkeypatch.setattr(
        app, "load_attention_state", AsyncMock(return_value=AttentionState())
    )


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


def test_attention_briefing_uses_deterministic_half_hour_cadence():
    assert app.attention_briefing_due(
        datetime.datetime(2026, 7, 19, 1, 30, tzinfo=datetime.timezone.utc)
    )
    assert not app.attention_briefing_due(
        datetime.datetime(2026, 7, 19, 1, 40, tzinfo=datetime.timezone.utc)
    )
    with pytest.raises(ValueError):
        app.attention_briefing_due(datetime.datetime(2026, 7, 19, 1, 30))


def test_run_check_dispatches_data_quality_incident_and_skips_market_briefing(monkeypatch):
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(
            return_value=[_market_ticker("KRW-BTC"), _market_ticker("KRW-ETH")]
        ),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-ETH": []}))
    data_quality_alert = AsyncMock()
    monkeypatch.setattr(app, "dispatch_data_quality_alert", data_quality_alert)
    monkeypatch.setattr(app, "append_scan_events", AsyncMock(return_value=[]))
    market_briefing = AsyncMock()
    monkeypatch.setattr(app, "create_and_dispatch_notification", market_briefing)
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())

    import asyncio

    asyncio.run(app.run_check())

    data_quality_alert.assert_awaited_once()
    market_briefing.assert_not_awaited()
    app.save_rank_state_history.assert_not_awaited()


def test_data_quality_scan_retains_claim_when_notification_handoff_is_uncertain(
    monkeypatch,
):
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(
            return_value=[_market_ticker("KRW-BTC"), _market_ticker("KRW-ETH")]
        ),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-ETH": []}))
    monkeypatch.setattr(app, "append_scan_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        app,
        "dispatch_data_quality_alert",
        AsyncMock(
            side_effect=app.NotificationDeliveryError(
                NotificationErrorCode.OUTBOX_WRITE_UNVERIFIED,
                scan_handoff_state=ScanHandoffState.UNCERTAIN,
            )
        ),
    )
    release = AsyncMock()
    monkeypatch.setattr(app, "release_scan_key", release)

    import asyncio

    with pytest.raises(app.PipelineError) as error:
        asyncio.run(app.run_check(execution_id="run-a"))

    assert error.value.code is app.PipelineErrorCode.EXECUTION_FAILED
    app.complete_scan_key.assert_not_awaited()
    release.assert_not_awaited()


def test_run_check_skips_mutations_when_the_completed_candle_is_claimed(monkeypatch):
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=False))
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(return_value=[_market_ticker("KRW-BTC")]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-BTC": []}))
    append_events = AsyncMock(return_value=[])
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

    with pytest.raises(app.PipelineError) as error:
        asyncio.run(app.run_check())

    assert error.value.code is app.PipelineErrorCode.EXECUTION_FAILED
    claim.assert_not_awaited()


def test_run_check_releases_claim_when_persistence_fails_before_notification(monkeypatch):
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(return_value=[_market_ticker("KRW-BTC")]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-BTC": []}))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "append_scan_events", AsyncMock(side_effect=RuntimeError("disk")))
    release = AsyncMock()
    monkeypatch.setattr(app, "release_scan_key", release)
    notify = AsyncMock()
    monkeypatch.setattr(app, "dispatch_data_quality_alert", notify)

    import asyncio

    with pytest.raises(app.PipelineError) as error:
        asyncio.run(app.run_check())

    assert error.value.code is app.PipelineErrorCode.EXECUTION_FAILED
    release.assert_awaited_once()
    notify.assert_not_awaited()


def test_deep_dive_fetches_candidates_and_the_btc_benchmark_only():
    assert app.required_deep_dive_markets(["KRW-XRP", "KRW-BTC"]) == [
        "KRW-BTC",
        "KRW-XRP",
    ]


def test_run_check_keeps_execution_risk_in_the_attention_pipeline(monkeypatch):
    timestamp = datetime.datetime(2026, 7, 19, 1, 20, tzinfo=datetime.timezone.utc)
    lightweight = {
        market: TickerData(
            market=market,
            candle_history=[
                CandleData(
                    market=market,
                    timestamp=timestamp,
                    open_price=100.0,
                    high_price=101.0,
                    low_price=99.0,
                    close_price=100.0,
                    volume=10.0,
                )
            ],
            price_change_10m=1.0,
            relative_volume=3.0,
            rolling_turnover=100_000_000.0,
        )
        for market in ["KRW-BTC", "KRW-XRP", "KRW-ADA"]
    }
    decisions = {
        "KRW-BTC": CandidateDecision(
            eligible=False,
            rejection_reasons=[RejectionCode.PRICE_SURPRISE_UNAVAILABLE],
        ),
        "KRW-XRP": CandidateDecision(eligible=True),
        "KRW-ADA": CandidateDecision(
            eligible=False,
            rejection_reasons=[RejectionCode.PRICE_SURPRISE_UNAVAILABLE],
        ),
    }
    broad_candles = {market: [] for market in lightweight}
    higher_timeframe = {
        market: lightweight[market].candle_history for market in lightweight
    }
    get_candles = AsyncMock(
        side_effect=[broad_candles, higher_timeframe, higher_timeframe]
    )
    detect = Mock(return_value=[])
    _, previous_attention = app.build_attention_queue(
        timestamp,
        ["KRW-ADA"],
        lightweight,
        {"KRW-ADA": 3},
        {},
        [],
        [],
    )
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        app,
        "load_and_process_sectors",
        AsyncMock(
            return_value=(
                {"Layer 1": ["KRW-XRP", "KRW-ADA"]},
                {"KRW-XRP": ["Layer 1"], "KRW-ADA": ["Layer 1"]},
            )
        ),
    )
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(return_value=[_market_ticker(market) for market in lightweight]),
    )
    monkeypatch.setattr(app, "get_candles", get_candles)
    monkeypatch.setattr(app, "process_lightweight_indicators", Mock(return_value=lightweight))
    monkeypatch.setattr(app, "assign_residual_momentum", Mock())
    monkeypatch.setattr(app, "evaluate_candidate_eligibility", Mock(return_value=decisions))
    monkeypatch.setattr(app, "get_orderbooks", AsyncMock(return_value={"KRW-XRP": object()}))
    monkeypatch.setattr(
        app,
        "assess_execution",
        Mock(
            return_value=SimpleNamespace(
                executable=False,
                spread_bps=45.0,
                expected_slippage_bps=2.0,
                rejection_reasons=[RejectionCode.SPREAD_ABOVE_MAXIMUM],
            )
        ),
    )
    def enrich_with_context(tickers, hourly, daily, _all_tickers):
        return {
            market: ticker.model_copy(
                update={
                    "hourly_candles": hourly.get(market, []),
                    "daily_candles": daily.get(market, []),
                }
            )
            for market, ticker in tickers.items()
        }

    monkeypatch.setattr(
        app, "enrich_deep_dive_tickers", Mock(side_effect=enrich_with_context)
    )
    monkeypatch.setattr(
        app,
        "get_market_regime",
        Mock(return_value=MarketRegimeSnapshot(regime=MarketRegime.TRENDING_BULL)),
    )
    monkeypatch.setattr(app, "detect_anomalies", detect)
    monkeypatch.setattr(app, "filter_market_wide_events", Mock(return_value=[]))
    monkeypatch.setattr(app, "load_pending_scan_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_alert_history", AsyncMock(return_value={}))
    monkeypatch.setattr(
        app, "load_attention_state", AsyncMock(return_value=previous_attention)
    )
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    append_events = AsyncMock(return_value=[])
    monkeypatch.setattr(app, "append_scan_events", append_events)
    monkeypatch.setattr(app, "append_scan_outcomes", AsyncMock())
    monkeypatch.setattr(app, "save_pending_scan_events", AsyncMock())
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())
    save_attention = AsyncMock()
    monkeypatch.setattr(app, "save_attention_state", save_attention)
    dispatch = AsyncMock()
    monkeypatch.setattr(app, "create_and_dispatch_notification", dispatch)

    import asyncio

    asyncio.run(app.run_check(schedule_time="2026-07-19T01:30:00Z"))

    assert get_candles.await_args_list[1].args[1] == [
        "KRW-ADA",
        "KRW-BTC",
        "KRW-XRP",
    ]
    assert get_candles.await_args_list[2].args[1] == [
        "KRW-ADA",
        "KRW-BTC",
        "KRW-XRP",
    ]
    assert get_candles.await_args_list[1].kwargs["time_unit"] is app.CandleTimeUnit.MINUTES
    assert get_candles.await_args_list[1].kwargs["minutes_unit"] == 60
    assert get_candles.await_args_list[2].kwargs["time_unit"] is app.CandleTimeUnit.DAYS
    scored_markets, context, _sectors, _reverse = detect.call_args.args
    assert scored_markets == ["KRW-XRP"]
    assert set(context) == {"KRW-BTC", "KRW-XRP", "KRW-ADA"}
    save_attention.assert_awaited_once()
    saved_state = save_attention.await_args.args[0]
    assert set(saved_state.entries) == {"KRW-ADA", "KRW-XRP"}
    events = {event.market: event for event in append_events.await_args.args[0]}
    assert events["KRW-XRP"].final_decision is ScanDecision.ATTENTION_QUEUED
    assert events["KRW-ADA"].final_decision is ScanDecision.ATTENTION_QUEUED
    assert (
        RejectionCode.HIGHER_TIMEFRAME_CANDLE_HISTORY_UNAVAILABLE
        not in events["KRW-ADA"].rejection_reasons
    )
    assert events["KRW-XRP"].feature_snapshot["execution_rejection_reasons"] == [
        RejectionCode.SPREAD_ABOVE_MAXIMUM.value
    ]
    attention_queue = dispatch.await_args.kwargs["attention_queue"]
    assert {candidate.market for candidate in attention_queue} == {
        "KRW-ADA",
        "KRW-XRP",
    }
    ada = next(candidate for candidate in attention_queue if candidate.market == "KRW-ADA")
    ada_context = next(
        item for item in ada.evidence if item.family is EvidenceFamily.CONTEXT
    )
    assert ada_context.verdict is not EvidenceVerdict.UNAVAILABLE
    assert dispatch.await_args.kwargs["suppress_unchanged_briefing"] is True


def test_historical_retry_skips_current_only_evidence_and_notifications(monkeypatch):
    scan_at = datetime.datetime(2026, 7, 19, 1, 30, tzinfo=datetime.timezone.utc)
    future_state = AttentionState(
        updated_at=scan_at + datetime.timedelta(minutes=10)
    )
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        app, "load_attention_state", AsyncMock(return_value=future_state)
    )
    market_fetch = AsyncMock()
    monkeypatch.setattr(app, "get_all_krw_tickers", market_fetch)
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    append_events = AsyncMock()
    monkeypatch.setattr(app, "append_scan_events", append_events)
    save_rank = AsyncMock()
    monkeypatch.setattr(app, "save_rank_state_history", save_rank)
    save_attention = AsyncMock()
    monkeypatch.setattr(app, "save_attention_state", save_attention)
    dispatch = AsyncMock()
    monkeypatch.setattr(app, "create_and_dispatch_notification", dispatch)
    build_queue = Mock()
    monkeypatch.setattr(app, "build_attention_queue", build_queue)

    import asyncio

    asyncio.run(app.run_check(schedule_time=scan_at.isoformat()))

    market_fetch.assert_not_awaited()
    build_queue.assert_not_called()
    append_events.assert_not_awaited()
    save_rank.assert_not_awaited()
    save_attention.assert_not_awaited()
    dispatch.assert_not_awaited()
    app.complete_scan_key.assert_awaited_once()


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
        _market_ticker("KRW-BTC", 2.0),
        _market_ticker("KRW-ETH", 1.0),
    ]
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    market_fetch = AsyncMock(return_value=raw_tickers)
    monkeypatch.setattr(app, "get_all_krw_tickers", market_fetch)
    monkeypatch.setattr(app, "recover_pending_notification", AsyncMock(return_value=object()))
    monkeypatch.setattr(
        app,
        "get_candles",
        AsyncMock(return_value={"KRW-BTC": candles, "KRW-ETH": candles}),
    )
    monkeypatch.setattr(app, "load_pending_scan_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_alert_history", AsyncMock(return_value={}))
    append_events = AsyncMock(return_value=[])
    monkeypatch.setattr(app, "append_scan_events", append_events)
    monkeypatch.setattr(app, "append_scan_outcomes", AsyncMock())
    monkeypatch.setattr(app, "save_pending_scan_events", AsyncMock())
    monkeypatch.setattr(app, "create_and_dispatch_notification", AsyncMock())
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())

    import asyncio

    asyncio.run(app.run_check())

    persisted_events = append_events.await_args.args[0]
    market_fetch.assert_awaited_once()
    assert {event.market for event in persisted_events} == {"KRW-BTC", "KRW-ETH"}
    assert all(
        event.final_decision is ScanDecision.REJECTED_LIGHTWEIGHT
        for event in persisted_events
    )


def test_conflicting_retry_reports_data_quality_instead_of_sending_a_mismatched_briefing(
    monkeypatch,
):
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
        AsyncMock(return_value=[_market_ticker("KRW-BTC")]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-BTC": candles}))
    monkeypatch.setattr(app, "load_pending_scan_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_alert_history", AsyncMock(return_value={}))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(
        app, "append_scan_events", AsyncMock(return_value=["event-1"])
    )
    monkeypatch.setattr(app, "append_scan_outcomes", AsyncMock())
    monkeypatch.setattr(app, "save_pending_scan_events", AsyncMock())
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())
    incident = AsyncMock()
    monkeypatch.setattr(app, "dispatch_data_quality_alert", incident)
    briefing = AsyncMock()
    monkeypatch.setattr(app, "create_and_dispatch_notification", briefing)

    import asyncio

    asyncio.run(app.run_check(execution_id="retry-a"))

    issues = incident.await_args.args[0]
    assert issues[0].code is RejectionCode.IMMUTABLE_SCAN_EVENT_CONFLICT
    app.append_scan_outcomes.assert_not_awaited()
    app.save_pending_scan_events.assert_not_awaited()
    app.save_rank_state_history.assert_not_awaited()
    briefing.assert_not_awaited()


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
        AsyncMock(return_value=[_market_ticker("KRW-BTC")]),
    )
    monkeypatch.setattr(app, "get_candles", AsyncMock(return_value={"KRW-BTC": candles}))
    monkeypatch.setattr(app, "load_pending_scan_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "load_alert_history", AsyncMock(return_value={}))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "append_scan_events", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "append_scan_outcomes", AsyncMock())
    monkeypatch.setattr(app, "save_pending_scan_events", AsyncMock())
    monkeypatch.setattr(app, "save_rank_state_history", AsyncMock())
    monkeypatch.setattr(
        app, "create_and_dispatch_notification", AsyncMock(side_effect=error)
    )
    release = AsyncMock()
    monkeypatch.setattr(app, "release_scan_key", release)
    return release


def test_run_check_completes_claim_when_notification_handoff_is_durable(monkeypatch):
    release = _configure_valid_scan_with_notification_error(
        monkeypatch,
        app.NotificationDeliveryError(
            NotificationErrorCode.DELIVERY_FAILED,
            scan_handoff_state=ScanHandoffState.DURABLE,
        ),
    )

    import asyncio

    with pytest.raises(app.PipelineError) as error:
        asyncio.run(app.run_check())

    assert error.value.code is app.PipelineErrorCode.EXECUTION_FAILED
    app.complete_scan_key.assert_awaited_once()
    release.assert_not_awaited()


def test_run_check_releases_claim_when_notification_handoff_is_not_durable(
    monkeypatch,
):
    release = _configure_valid_scan_with_notification_error(
        monkeypatch,
        app.NotificationDeliveryError(NotificationErrorCode.BACKLOG_CAPACITY_EXCEEDED),
    )

    import asyncio

    with pytest.raises(app.PipelineError) as error:
        asyncio.run(app.run_check(execution_id="run-a"))

    assert error.value.code is app.PipelineErrorCode.EXECUTION_FAILED
    app.complete_scan_key.assert_not_awaited()
    release.assert_awaited_once()


def test_run_check_retains_claim_when_notification_handoff_is_uncertain(monkeypatch):
    release = _configure_valid_scan_with_notification_error(
        monkeypatch,
        app.NotificationDeliveryError(
            NotificationErrorCode.OUTBOX_WRITE_UNVERIFIED,
            scan_handoff_state=ScanHandoffState.UNCERTAIN,
        ),
    )

    import asyncio

    with pytest.raises(app.PipelineError) as error:
        asyncio.run(app.run_check(execution_id="run-a"))

    assert error.value.code is app.PipelineErrorCode.EXECUTION_FAILED
    app.complete_scan_key.assert_not_awaited()
    release.assert_not_awaited()


def test_run_check_retains_claim_after_confirmed_delivery_finalization_failure(monkeypatch):
    release = _configure_valid_scan_with_notification_error(
        monkeypatch,
        app.NotificationDeliveryError(
            NotificationErrorCode.DELIVERY_FINALIZATION_FAILED,
            delivery_state=DeliveryState.CONFIRMED,
            scan_handoff_state=ScanHandoffState.DURABLE,
        ),
    )

    import asyncio

    with pytest.raises(app.PipelineError) as error:
        asyncio.run(app.run_check())

    assert error.value.code is app.PipelineErrorCode.EXECUTION_FAILED
    app.complete_scan_key.assert_awaited_once()
    release.assert_not_awaited()


def test_pending_webhook_failure_does_not_block_market_state_collection(monkeypatch):
    release = _configure_valid_scan_with_notification_error(
        monkeypatch,
        AssertionError("new notification must not replace a pending outbox"),
    )
    pending_error = app.NotificationDeliveryError(
        NotificationErrorCode.DELIVERY_FAILED,
        scan_handoff_state=ScanHandoffState.DURABLE,
    )
    monkeypatch.setattr(
        app, "recover_pending_notification", AsyncMock(side_effect=pending_error)
    )
    notify = AsyncMock()
    monkeypatch.setattr(app, "create_and_dispatch_notification", notify)

    import asyncio

    with pytest.raises(app.PipelineError) as error:
        asyncio.run(app.run_check(execution_id="run-a"))

    assert error.value.code is app.PipelineErrorCode.EXECUTION_FAILED
    app.get_all_krw_tickers.assert_awaited_once()
    app.save_rank_state_history.assert_awaited_once()
    app.complete_scan_key.assert_awaited_once()
    notify.assert_awaited_once()
    assert notify.await_args.kwargs["scan_key"].startswith("completed-candle:")
    release.assert_not_awaited()


def test_cloud_function_uses_scheduler_schedule_time_as_retry_identity(monkeypatch):
    run_check = AsyncMock()
    monkeypatch.setattr(app, "run_check", run_check)

    class Request:
        headers = {
            "X-CloudScheduler-Execution-ID": "attempt-specific-id",
            "X-CloudScheduler-ScheduleTime": "2026-07-13T15:00:00Z",
        }

    assert app.main(Request()) == ("OK", 200)
    run_check.assert_awaited_once_with(
        execution_id="2026-07-13T15:00:00Z",
        schedule_time="2026-07-13T15:00:00Z",
    )


def test_scheduler_retry_derives_the_scan_from_the_original_schedule_time(monkeypatch):
    monkeypatch.setattr(app, "load_rank_state_history", AsyncMock(return_value=[]))
    monkeypatch.setattr(app, "claim_scan_key", AsyncMock(return_value=True))
    monkeypatch.setattr(app, "load_and_process_sectors", AsyncMock(return_value=({}, {})))
    monkeypatch.setattr(
        app,
        "get_all_krw_tickers",
        AsyncMock(
            return_value=[_market_ticker("KRW-BTC"), _market_ticker("KRW-ETH")]
        ),
    )
    candles = AsyncMock(return_value={"KRW-ETH": []})
    monkeypatch.setattr(app, "get_candles", candles)
    monkeypatch.setattr(app, "dispatch_data_quality_alert", AsyncMock())
    monkeypatch.setattr(app, "append_scan_events", AsyncMock(return_value=[]))

    import asyncio

    asyncio.run(
        app.run_check(
            execution_id="2026-07-13T15:04:59Z",
            schedule_time="2026-07-13T15:04:59Z",
        )
    )

    expected_time = datetime.datetime(
        2026, 7, 13, 15, 4, 59, tzinfo=datetime.timezone.utc
    )
    app.claim_scan_key.assert_awaited_once_with(
        "completed-candle:2026-07-13T15:00:00+00:00",
        execution_id="2026-07-13T15:04:59Z",
        gcs_client=None,
    )
    assert candles.await_args.kwargs["as_of"] == expected_time

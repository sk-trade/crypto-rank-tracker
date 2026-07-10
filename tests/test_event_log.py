import datetime
import asyncio

import pytest

from common.analysis.scanner import CandidateDecision
from common.event_log import build_scan_events, resolve_scan_outcomes
from common.models import CandleData, ScanEvent, TickerData
from common import state_manager


UTC = datetime.timezone.utc


def _candle(timestamp: datetime.datetime, open_price: float, high: float, low: float) -> CandleData:
    return CandleData(
        market="KRW-BTC",
        timestamp=timestamp,
        open_price=open_price,
        high_price=high,
        low_price=low,
        close_price=open_price,
        volume=1.0,
    )


def test_scan_events_include_all_markets_and_rejection_decisions():
    observed_at = datetime.datetime(2026, 6, 18, 0, 10, tzinfo=UTC)
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle(observed_at, 100.0, 101.0, 99.0)],
        price_change_10m=2.0,
    )

    events = build_scan_events(
        observed_at,
        ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        {"KRW-BTC": ticker, "KRW-ETH": ticker.model_copy(update={"market": "KRW-ETH"})},
        {
            "KRW-BTC": CandidateDecision(True, []),
            "KRW-ETH": CandidateDecision(False, ["price_move_below_threshold"]),
        },
        ["KRW-BTC"],
        [],
        raw_tickers_by_market={"KRW-XRP": {"market": "KRW-XRP", "trade_price": 1.0}},
    )

    by_market = {event.market: event for event in events}
    assert set(by_market) == {"KRW-BTC", "KRW-ETH", "KRW-XRP"}
    assert by_market["KRW-BTC"].final_decision == "candidate_not_alerted"
    assert by_market["KRW-ETH"].rejection_reasons == ["price_move_below_threshold"]
    assert by_market["KRW-XRP"].final_decision == "data_quality_blocked"
    assert by_market["KRW-BTC"].feature_snapshot["market"] == "KRW-BTC"
    assert by_market["KRW-XRP"].feature_snapshot["raw_ticker"]["market"] == "KRW-XRP"


def test_outcome_resolution_waits_for_complete_entry_path_and_uses_fixed_target():
    signal_start = datetime.datetime(2026, 6, 18, 0, 0, tzinfo=UTC)
    event = ScanEvent(
        event_id="event-1",
        observed_at=signal_start,
        market="KRW-BTC",
        feature_snapshot={},
        candidate_eligible=True,
        rejection_reasons=[],
        final_decision="alert_sent",
        model_version="heuristic-v1",
        direction="long",
        signal_candle_start=signal_start,
    )
    timestamps = [signal_start + datetime.timedelta(minutes=10 * step) for step in range(1, 8)]
    candles = [_candle(timestamp, 100 + index, 102 + index, 98 + index) for index, timestamp in enumerate(timestamps)]

    outcomes, pending = resolve_scan_outcomes([event], {"KRW-BTC": candles})

    assert pending == []
    assert len(outcomes) == 1
    assert outcomes[0].entry_price == 100.0
    assert outcomes[0].exit_price == 106.0
    assert outcomes[0].directional_net_return == pytest.approx(0.059)
    assert outcomes[0].mfe == pytest.approx(0.07)
    assert outcomes[0].mae == pytest.approx(-0.02)


def test_outcome_resolution_keeps_events_pending_until_all_six_holding_bars_are_complete():
    signal_start = datetime.datetime(2026, 6, 18, 0, 0, tzinfo=UTC)
    event = ScanEvent(
        event_id="event-1",
        observed_at=signal_start,
        market="KRW-BTC",
        feature_snapshot={},
        candidate_eligible=True,
        rejection_reasons=[],
        final_decision="alert_sent",
        model_version="heuristic-v1",
        direction="long",
        signal_candle_start=signal_start,
    )

    outcomes, pending = resolve_scan_outcomes([event], {"KRW-BTC": []})

    assert outcomes == []
    assert pending == [event]


def test_event_and_pending_records_are_persisted_without_mutating_prior_records(monkeypatch):
    store = {}

    async def load_json(filename, _gcs_client):
        return store.get(filename)

    async def save_json(filename, value, _gcs_client):
        store[filename] = value

    monkeypatch.setattr(state_manager, "load_json", load_json)
    monkeypatch.setattr(state_manager, "save_json", save_json)
    event = ScanEvent(
        event_id="event-1",
        observed_at=datetime.datetime(2026, 6, 18, tzinfo=UTC),
        market="KRW-BTC",
        feature_snapshot={"price_change_10m": 1.0},
        candidate_eligible=False,
        rejection_reasons=["price_move_below_threshold"],
        final_decision="rejected_lightweight",
        model_version="heuristic-v1",
    )

    async def persist_records():
        await state_manager.append_scan_events([event])
        await state_manager.save_pending_scan_events([event])
        return await state_manager.load_pending_scan_events()

    pending = asyncio.run(persist_records())

    assert len(pending) == 1
    assert pending[0] == event
    event_logs = next(value for name, value in store.items() if name.startswith("scan_events_"))
    assert event_logs[0]["rejection_reasons"] == ["price_move_below_threshold"]

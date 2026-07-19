import datetime
import asyncio

import pytest
from pydantic import ValidationError

import config
from common.analysis.deep_dive import enrich_deep_dive_tickers
from common.event_log import build_scan_events, resolve_scan_outcomes
from common.models import (
    Alert,
    CandidateDecision,
    CandleData,
    Direction,
    MarketEvent,
    MarketTicker,
    RejectionCode,
    ScanDecision,
    ScanEvent,
    ScanOutcome,
    SignalCandidate,
    SignalType,
    TickerData,
)
from common import state_manager
from common.storage_client import (
    StateErrorCode,
    StateLoadError,
    StateOperationError,
)


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


@pytest.mark.parametrize(
    "payload",
    [
        {
            "eligible": True,
            "rejection_reasons": [RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD],
        },
        {"eligible": False, "rejection_reasons": []},
    ],
)
def test_candidate_decision_rejects_contradictory_states(payload):
    with pytest.raises(ValidationError):
        CandidateDecision.model_validate(payload)


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
            "KRW-BTC": CandidateDecision(eligible=True),
            "KRW-ETH": CandidateDecision(
                eligible=False,
                rejection_reasons=[RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD],
            ),
        },
        ["KRW-BTC"],
        [],
        raw_tickers_by_market={
            "KRW-XRP": MarketTicker(
                market="KRW-XRP",
                trade_price=1.0,
                acc_trade_price_24h=1.0,
                market_event=MarketEvent(
                    warning=False,
                    caution={"PRICE_FLUCTUATIONS": False},
                ),
            )
        },
    )

    by_market = {event.market: event for event in events}
    assert set(by_market) == {"KRW-BTC", "KRW-ETH", "KRW-XRP"}
    assert by_market["KRW-BTC"].final_decision is ScanDecision.CANDIDATE_NOT_ALERTED
    assert by_market["KRW-ETH"].rejection_reasons == [
        RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD
    ]
    assert by_market["KRW-XRP"].final_decision is ScanDecision.DATA_QUALITY_BLOCKED
    assert by_market["KRW-BTC"].feature_snapshot["market"] == "KRW-BTC"
    assert by_market["KRW-XRP"].feature_snapshot["raw_ticker"]["market"] == "KRW-XRP"


def test_scan_events_preserve_execution_and_regime_blocking_stages():
    observed_at = datetime.datetime(2026, 6, 18, 0, 10, tzinfo=UTC)
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle(observed_at, 100.0, 101.0, 99.0)],
        price_change_10m=2.0,
    )

    events = build_scan_events(
        observed_at,
        ["KRW-BTC", "KRW-ETH"],
        {"KRW-BTC": ticker, "KRW-ETH": ticker.model_copy(update={"market": "KRW-ETH"})},
        {
            "KRW-BTC": CandidateDecision(
                eligible=False,
                rejection_reasons=[RejectionCode.ORDERBOOK_UNAVAILABLE],
            ),
            "KRW-ETH": CandidateDecision(
                eligible=False,
                rejection_reasons=[RejectionCode.MARKET_REGIME_UNKNOWN],
            ),
        },
        [],
        [],
    )

    by_market = {event.market: event for event in events}
    assert by_market["KRW-BTC"].final_decision is ScanDecision.EXECUTION_BLOCKED
    assert by_market["KRW-ETH"].final_decision is ScanDecision.MARKET_REGIME_BLOCKED


def test_scan_event_records_alert_selection_without_claiming_delivery():
    observed_at = datetime.datetime(2026, 6, 18, 0, 10, tzinfo=UTC)
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle(observed_at, 100.0, 101.0, 99.0)],
        price_change_10m=2.0,
    )
    candidate = SignalCandidate(
        market="KRW-BTC",
        signal_score=0.9,
        price_change=2.0,
        rvol=3.0,
        rvol_z_score=4.0,
        current_price=100.0,
    )
    alert = Alert(
        candidate=candidate,
        ticker_data=ticker,
        signal_type=SignalType.BREAKOUT_START,
        priority=3,
    )

    event = build_scan_events(
        observed_at,
        ["KRW-BTC"],
        {"KRW-BTC": ticker},
        {"KRW-BTC": CandidateDecision(eligible=True)},
        ["KRW-BTC"],
        [alert],
        [candidate],
    )[0]

    assert event.final_decision is ScanDecision.ALERT_SELECTED


def test_deep_dive_ma_flags_are_json_serializable_in_scan_events():
    observed_at = datetime.datetime(2026, 6, 18, 0, 10, tzinfo=UTC)
    daily_candles = [
        _candle(
            observed_at - datetime.timedelta(days=199 - index),
            float(index + 1),
            float(index + 2),
            float(index) + 0.5,
        )
        for index in range(200)
    ]
    ticker = TickerData(
        market="KRW-BTC",
        candle_history=[_candle(observed_at, 200.0, 201.0, 199.0)],
        price_change_10m=2.0,
    )

    enriched = enrich_deep_dive_tickers(
        {"KRW-BTC": ticker},
        {},
        {"KRW-BTC": daily_candles},
        {"KRW-BTC": ticker},
    )
    event = build_scan_events(
        observed_at,
        ["KRW-BTC"],
        enriched,
        {"KRW-BTC": CandidateDecision(eligible=True)},
        ["KRW-BTC"],
        [],
    )[0]

    assert type(enriched["KRW-BTC"].is_above_ma50_daily) is bool
    assert type(enriched["KRW-BTC"].is_above_ma200_daily) is bool
    assert event.feature_snapshot["is_above_ma50_daily"] is True
    assert event.feature_snapshot["is_above_ma200_daily"] is True


def test_outcome_resolution_waits_for_complete_entry_path_and_uses_fixed_target():
    signal_start = datetime.datetime(2026, 6, 18, 0, 0, tzinfo=UTC)
    event = ScanEvent(
        event_id="event-1",
        observed_at=signal_start,
        market="KRW-BTC",
        feature_snapshot={},
        candidate_eligible=True,
        rejection_reasons=[],
        final_decision=ScanDecision.ALERT_SELECTED,
        model_version="heuristic-v1",
        direction=Direction.LONG,
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
        final_decision=ScanDecision.ALERT_SELECTED,
        model_version="heuristic-v1",
        direction=Direction.LONG,
        signal_candle_start=signal_start,
    )

    outcomes, pending = resolve_scan_outcomes([event], {"KRW-BTC": []})

    assert outcomes == []
    assert pending == [event]


def test_outcome_resolution_retires_events_older_than_the_recoverable_candle_window():
    signal_start = datetime.datetime(2026, 6, 18, 0, 0, tzinfo=UTC)
    event = ScanEvent(
        event_id="expired-event",
        observed_at=signal_start,
        market="KRW-XRP",
        feature_snapshot={},
        candidate_eligible=True,
        rejection_reasons=[],
        final_decision=ScanDecision.ALERT_SELECTED,
        model_version="heuristic-v1",
        direction=Direction.LONG,
        signal_candle_start=signal_start,
    )
    latest_benchmark = _candle(
        signal_start + datetime.timedelta(days=3), 100.0, 101.0, 99.0
    )

    outcomes, pending = resolve_scan_outcomes(
        [event], {"KRW-BTC": [latest_benchmark]}
    )

    assert outcomes == []
    assert pending == []


def test_event_and_pending_records_are_persisted_without_mutating_prior_records(monkeypatch):
    store = {}

    async def load_json(filename, _gcs_client, **_kwargs):
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
        rejection_reasons=[RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD],
        final_decision=ScanDecision.REJECTED_LIGHTWEIGHT,
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
    assert event_logs[0]["rejection_reasons"] == ["price_surprise_below_threshold"]


def test_conflicting_scan_event_retry_preserves_the_first_immutable_record(monkeypatch):
    store = {}

    async def load_json(filename, _gcs_client, **_kwargs):
        return store.get(filename)

    async def save_json(filename, value, _gcs_client):
        store[filename] = value

    monkeypatch.setattr(state_manager, "load_json", load_json)
    monkeypatch.setattr(state_manager, "save_json", save_json)
    original = ScanEvent(
        event_id="event-1",
        observed_at=datetime.datetime(2026, 6, 18, tzinfo=UTC),
        market="KRW-BTC",
        feature_snapshot={"spread_bps": 1.0},
        candidate_eligible=False,
        rejection_reasons=[RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD],
        final_decision=ScanDecision.REJECTED_LIGHTWEIGHT,
        model_version="heuristic-v1",
    )
    conflicting = original.model_copy(
        update={"feature_snapshot": {"spread_bps": 99.0}}
    )
    new_market = original.model_copy(
        update={"event_id": "event-2", "market": "KRW-ETH"}
    )

    async def persist_conflict():
        await state_manager.append_scan_events([original])
        return await state_manager.append_scan_events([conflicting, new_market])

    conflicts = asyncio.run(persist_conflict())
    event_logs = next(value for name, value in store.items() if name.startswith("scan_events_"))

    assert conflicts == ["event-1"]
    assert event_logs == [original.model_dump(mode="json")]


def test_scan_evidence_is_partitioned_into_deterministic_utc_hour_shards(monkeypatch):
    store = {}

    async def load_json(filename, _gcs_client, **_kwargs):
        return store.get(filename)

    async def save_json(filename, value, _gcs_client):
        store[filename] = value

    monkeypatch.setattr(state_manager, "load_json", load_json)
    monkeypatch.setattr(state_manager, "save_json", save_json)
    first_hour = datetime.datetime(2026, 6, 18, 0, 10, tzinfo=UTC)
    next_hour = first_hour + datetime.timedelta(hours=1)
    first_event = ScanEvent(
        event_id="event-1",
        observed_at=first_hour,
        market="KRW-BTC",
        feature_snapshot={},
        candidate_eligible=False,
        rejection_reasons=[RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD],
        final_decision=ScanDecision.REJECTED_LIGHTWEIGHT,
        model_version="heuristic-v1",
    )
    second_event = first_event.model_copy(
        update={
            "event_id": "event-2",
            "observed_at": next_hour,
            "market": "KRW-ETH",
        }
    )
    first_outcome = ScanOutcome(
        event_id="event-1",
        market="KRW-BTC",
        entry_candle_start=first_hour,
        exit_candle_start=next_hour,
        entry_price=100.0,
        exit_price=101.0,
        directional_net_return=0.009,
        mfe=0.02,
        mae=-0.01,
    )
    second_outcome = first_outcome.model_copy(
        update={
            "event_id": "event-2",
            "market": "KRW-ETH",
            "exit_candle_start": next_hour + datetime.timedelta(hours=1),
        }
    )

    async def persist_records():
        await state_manager.append_scan_events([first_event, second_event])
        await state_manager.append_scan_events([first_event])
        await state_manager.append_scan_outcomes([first_outcome, second_outcome])

    asyncio.run(persist_records())

    assert list(store) == [
        "scan_events_2026-06-18T00Z.json",
        "scan_events_2026-06-18T01Z.json",
        "scan_outcomes_2026-06-18T01Z.json",
        "scan_outcomes_2026-06-18T02Z.json",
    ]
    assert [item["event_id"] for item in store["scan_events_2026-06-18T00Z.json"]] == [
        "event-1"
    ]


def test_scan_evidence_rejects_naive_partition_timestamps_before_writing(monkeypatch):
    writes = []

    async def save_json(filename, value, _gcs_client):
        writes.append((filename, value))

    monkeypatch.setattr(state_manager, "save_json", save_json)
    aware = datetime.datetime(2026, 6, 18, 0, 10, tzinfo=UTC)
    event = ScanEvent(
        event_id="event-1",
        observed_at=aware,
        market="KRW-BTC",
        feature_snapshot={},
        candidate_eligible=False,
        rejection_reasons=[RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD],
        final_decision=ScanDecision.REJECTED_LIGHTWEIGHT,
        model_version="heuristic-v1",
    )
    invalid_event = event.model_copy(
        update={"event_id": "event-2", "observed_at": aware.replace(tzinfo=None)}
    )

    with pytest.raises(StateOperationError) as error:
        asyncio.run(state_manager.append_scan_events([event, invalid_event]))

    assert error.value.code is StateErrorCode.INVALID_ARGUMENT
    assert writes == []


@pytest.mark.parametrize("persisted", [{}, False, 0, ""])
def test_pending_scan_events_wrong_shape_fails_closed(monkeypatch, persisted):
    async def load_json(_filename, _gcs_client, **_kwargs):
        return persisted

    monkeypatch.setattr(state_manager, "load_json", load_json)

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.load_pending_scan_events())
    assert error.value.code is StateErrorCode.INVALID_SCHEMA


def test_pending_scan_events_invalid_record_is_an_explicit_load_error(monkeypatch):
    async def load_json(_filename, _gcs_client, **_kwargs):
        return [{"event_id": "incomplete"}]

    monkeypatch.setattr(state_manager, "load_json", load_json)

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.load_pending_scan_events())
    assert error.value.code is StateErrorCode.INVALID_SCHEMA


def test_scan_event_append_does_not_overwrite_wrong_shaped_history(monkeypatch):
    writes = []

    async def load_json(_filename, _gcs_client, **_kwargs):
        return {"existing": "evidence"}

    async def save_json(filename, value, _gcs_client):
        writes.append((filename, value))

    monkeypatch.setattr(state_manager, "load_json", load_json)
    monkeypatch.setattr(state_manager, "save_json", save_json)
    event = ScanEvent(
        event_id="event-1",
        observed_at=datetime.datetime(2026, 6, 18, tzinfo=UTC),
        market="KRW-BTC",
        feature_snapshot={},
        candidate_eligible=False,
        rejection_reasons=[RejectionCode.PRICE_SURPRISE_BELOW_THRESHOLD],
        final_decision=ScanDecision.REJECTED_LIGHTWEIGHT,
        model_version="heuristic-v1",
    )

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.append_scan_events([event]))
    assert error.value.code is StateErrorCode.INVALID_SCHEMA

    assert writes == []


def test_pending_scan_events_explicit_null_fails_closed(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    (tmp_path / "pending_scan_events.json").write_text("null", encoding="utf-8")

    with pytest.raises(StateLoadError) as error:
        asyncio.run(state_manager.load_pending_scan_events())
    assert error.value.code is StateErrorCode.NULL_DOCUMENT

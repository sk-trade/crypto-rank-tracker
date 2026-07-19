import asyncio
import datetime
import json
from pathlib import Path

import pytest

import config
from common.models import CandleData, MarketEvent, MarketTicker
from common.replay import (
    REPLAY_VARIANT_ATTENTION,
    REPLAY_VARIANT_BASELINE,
    REPLAY_VARIANT_PROGRESSION,
    aggregate_hourly_candles,
    replay_10m_bar_count,
    replay_daily_bar_count,
    replay_feature_history,
    replay_warmup_10m_bars,
    run_point_in_time_replay,
)
from replay_upbit import _tmp_path, collect_dataset, load_dataset, save_dataset


UTC = datetime.timezone.utc


def _candle(
    market: str, timestamp: datetime.datetime, price: float = 100.0
) -> CandleData:
    return CandleData(
        market=market,
        timestamp=timestamp,
        open_price=price,
        high_price=price * 1.02,
        low_price=price * 0.98,
        close_price=price,
        volume=100.0,
        trade_value=price * 100.0,
    )


def test_replay_window_adds_feature_warmup_and_future_outcomes():
    bars_per_week = 7 * 24 * 6

    assert replay_warmup_10m_bars() == 3 * bars_per_week + 1
    assert replay_10m_bar_count(7) == replay_warmup_10m_bars() + 7 * 144 + 12
    assert replay_daily_bar_count(7) == 209


def test_replay_evaluation_days_are_configurable_but_bounded():
    assert replay_10m_bar_count(1) < replay_10m_bar_count(7)
    assert replay_10m_bar_count(7) < replay_10m_bar_count(30)
    assert replay_10m_bar_count(30) == replay_warmup_10m_bars() + 30 * 144 + 12
    assert replay_daily_bar_count(30) == 232
    with pytest.raises(ValueError):
        replay_10m_bar_count(config.REPLAY_MAX_EVALUATION_DAYS + 1)


def test_replay_feature_history_matches_the_live_compact_layout():
    count = replay_warmup_10m_bars()
    start = datetime.datetime(2026, 6, 1, tzinfo=UTC)
    candles = [
        _candle("KRW-BTC", start + datetime.timedelta(minutes=10 * index))
        for index in range(count)
    ]
    current_index = count - 1

    history = replay_feature_history(candles, current_index)

    bars_per_week = 7 * 24 * 6
    assert len(history) == config.CONDITIONAL_VOLUME_LOOKBACK_WEEKS + 154
    assert [candle.timestamp for candle in history[:3]] == [
        candles[current_index - weeks_ago * bars_per_week].timestamp
        for weeks_ago in range(3, 0, -1)
    ]
    assert history[-154:] == candles[-154:]


def test_hourly_aggregation_preserves_multi_timeframe_ohlcv():
    start = datetime.datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
    candles = [
        _candle("KRW-BTC", start + datetime.timedelta(minutes=10 * index), 100 + index)
        for index in range(6)
    ]

    result = aggregate_hourly_candles({"KRW-BTC": candles})["KRW-BTC"]

    assert len(result) == 1
    assert result[0].timestamp == start
    assert result[0].open_price == 100.0
    assert result[0].close_price == 105.0
    assert result[0].volume == 600.0
    assert result[0].trade_value == 61_500.0


def test_replay_cache_round_trip_is_restricted_to_tmp(tmp_path):
    timestamp = datetime.datetime(2026, 7, 1, tzinfo=UTC)
    candles = {"KRW-BTC": [_candle("KRW-BTC", timestamp)]}
    manifest = {
        "schema_version": 1,
        "as_of": timestamp.isoformat(),
        "evaluation_days": 7,
    }

    save_dataset(tmp_path, candles, candles, manifest)
    loaded = load_dataset(tmp_path, 7)

    assert loaded is not None
    assert load_dataset(tmp_path, 7, timestamp) is not None
    assert load_dataset(
        tmp_path, 7, timestamp + datetime.timedelta(minutes=10)
    ) is None
    assert loaded[0] == candles
    assert loaded[1] == candles
    assert _tmp_path(tmp_path) == tmp_path.resolve()
    with pytest.raises(ValueError):
        _tmp_path(Path("/var/tmp/not-tmp-output"))

    incomplete = {**loaded[2], "complete": False}
    (tmp_path / "manifest.json").write_text(
        json.dumps(incomplete), encoding="utf-8"
    )
    assert load_dataset(tmp_path, 7) is None


def test_point_in_time_replay_runs_every_operational_scan_without_dropping_timeframes():
    count = replay_10m_bar_count(1)
    start = datetime.datetime(2026, 6, 1, tzinfo=UTC)
    ten_minute = {
        "KRW-BTC": [
            _candle(
                "KRW-BTC",
                start + datetime.timedelta(minutes=10 * index),
                100.0,
            )
            for index in range(count)
        ]
    }
    daily_start = start - datetime.timedelta(days=replay_daily_bar_count(1))
    daily = {
        "KRW-BTC": [
            _candle(
                "KRW-BTC",
                daily_start + datetime.timedelta(days=index),
                100.0,
            )
            for index in range(replay_daily_bar_count(1))
        ]
    }

    observations = []
    report = run_point_in_time_replay(
        ten_minute,
        daily,
        {},
        {},
        evaluation_days=1,
        top_k=1,
        observation_sink=observations.append,
    )

    assert report.variants[REPLAY_VARIANT_BASELINE].scans == 144
    assert report.signal_model_version == config.SIGNAL_MODEL_VERSION
    assert report.variants[REPLAY_VARIANT_BASELINE].precision_at_k == 1.0
    assert report.variants[REPLAY_VARIANT_ATTENTION].selected_observations == 0
    assert report.warmup_daily_bars == 200
    assert report.attention_episode_count == 0
    assert report.attention_repeated_observations == 0
    assert report.scheduled_digest_scans == 48
    assert report.nonempty_digest_scans == 0
    assert any("orderbook snapshots" in warning for warning in report.warnings)
    assert report.evaluation_end == ten_minute["KRW-BTC"][-13].timestamp + datetime.timedelta(
        minutes=10
    )
    assert len(observations) == 144
    assert observations[0]["decision_at"] > observations[0]["signal_candle_start"]
    assert observations[0]["signal_model_version"] == config.SIGNAL_MODEL_VERSION
    assert REPLAY_VARIANT_BASELINE in observations[0]["variants"]
    assert REPLAY_VARIANT_PROGRESSION in observations[0]["variants"]
    assert "Precision@K" in report.to_markdown()
    assert "First-Visible Episode Quality" in report.to_markdown()
    assert "Scheduled/non-empty 30m digests: 48 / 0" in report.to_markdown()


def test_point_in_time_replay_measures_nonempty_attention_progression():
    count = replay_10m_bar_count(1)
    warmup = replay_warmup_10m_bars()
    start = datetime.datetime(2026, 6, 1, tzinfo=UTC)
    candles = []
    price = 100.0
    for index in range(count):
        if index < warmup:
            price *= 1.001 if index % 2 == 0 else 0.999
            volume = 100.0
        else:
            price *= 1.01 if index < warmup + 4 else 1.03
            volume = 1_000.0
        candles.append(
            CandleData(
                market="KRW-BTC",
                timestamp=start + datetime.timedelta(minutes=10 * index),
                open_price=price,
                high_price=price * 1.02,
                low_price=price * 0.98,
                close_price=price,
                volume=volume,
                trade_value=price * volume,
            )
        )
    daily_start = start - datetime.timedelta(days=replay_daily_bar_count(1))
    daily = {
        "KRW-BTC": [
            _candle(
                "KRW-BTC",
                daily_start + datetime.timedelta(days=index),
                100.0,
            )
            for index in range(replay_daily_bar_count(1))
        ]
    }

    report = run_point_in_time_replay(
        {"KRW-BTC": candles},
        daily,
        {},
        {},
        evaluation_days=1,
        top_k=1,
    )

    attention = report.variants[REPLAY_VARIANT_ATTENTION]
    assert attention.selected_observations > 0
    assert report.attention_episode_count > 0
    assert report.attention_repeated_observations > 0
    assert report.nonempty_digest_scans > 0
    assert report.progression_context_precision_lift is not None
    assert "building" in report.attention_stage_metrics
    assert "confirmed" in report.attention_stage_metrics


def test_replay_report_preserves_requested_market_coverage():
    count = replay_10m_bar_count(1)
    start = datetime.datetime(2026, 6, 1, tzinfo=UTC)
    ten_minute = {
        "KRW-BTC": [
            _candle(
                "KRW-BTC",
                start + datetime.timedelta(minutes=10 * index),
            )
            for index in range(count)
        ]
    }

    report = run_point_in_time_replay(
        ten_minute,
        {},
        {},
        {},
        evaluation_days=1,
        top_k=1,
        requested_market_count=2,
    )

    assert report.market_coverage_ratio == 0.5
    assert report.requested_market_count == 2
    assert not report.market_coverage_meets_minimum
    assert any("Excluded 1 market" in warning for warning in report.warnings)
    assert any("below the production minimum" in warning for warning in report.warnings)


def test_collection_retries_missing_ten_minute_markets_once(monkeypatch):
    as_of = datetime.datetime(2026, 7, 19, tzinfo=UTC)
    markets = ["KRW-BTC", "KRW-ETH"]
    tickers = [
        MarketTicker(
            market=market,
            acc_trade_price_24h=1.0,
            market_event=MarketEvent(warning=False, caution={}),
        )
        for market in markets
    ]
    minute_calls = []

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

    async def fake_tickers(_session):
        return tickers

    async def fake_candles(
        _session, requested, time_unit, **_kwargs
    ):
        if time_unit.value == "days":
            return {}
        minute_calls.append(list(requested))
        if len(minute_calls) == 1:
            return {"KRW-BTC": [_candle("KRW-BTC", as_of)]}
        return {"KRW-ETH": [_candle("KRW-ETH", as_of)]}

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr("replay_upbit.aiohttp.ClientSession", FakeSession)
    monkeypatch.setattr("replay_upbit.get_all_krw_tickers", fake_tickers)
    monkeypatch.setattr("replay_upbit.get_candles", fake_candles)
    monkeypatch.setattr("replay_upbit.asyncio.sleep", no_sleep)

    ten_minute, _daily, manifest = asyncio.run(collect_dataset(1, as_of))

    assert set(ten_minute) == set(markets)
    assert minute_calls == [markets, ["KRW-ETH"]]
    assert manifest["ten_minute_coverage_ratio"] == 1.0

import asyncio
import datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import config
from common.models import CandleData, MarketEvent, MarketTicker
from common.replay import (
    REPLAY_ANALYSIS_TIMEFRAMES,
    REPLAY_VARIANT_ATTENTION,
    REPLAY_VARIANT_BASELINE,
    REPLAY_VARIANT_PROGRESSION,
    REPLAY_VARIANT_V3_MATCHED,
    REPLAY_VARIANT_V3_SHADOW,
    ReplayEvidenceTier,
    aggregate_hourly_candles,
    replay_10m_bar_count,
    replay_daily_bar_count,
    replay_evidence_tier,
    replay_feature_history,
    replay_warmup_10m_bars,
    run_point_in_time_replay,
)
from replay_upbit import (
    _tmp_path,
    collect_dataset,
    load_dataset,
    run as run_replay,
    save_dataset,
)


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
    assert replay_10m_bar_count(30) < replay_10m_bar_count(60)
    assert replay_10m_bar_count(60) < replay_10m_bar_count(90)
    assert replay_10m_bar_count(30) == replay_warmup_10m_bars() + 30 * 144 + 12
    assert replay_daily_bar_count(30) == 232
    assert replay_10m_bar_count(90) == replay_warmup_10m_bars() + 90 * 144 + 12
    assert replay_daily_bar_count(90) == 292
    with pytest.raises(ValueError):
        replay_10m_bar_count(config.REPLAY_MAX_EVALUATION_DAYS + 1)


@pytest.mark.parametrize(
    ("evaluation_days", "expected"),
    [
        (1, ReplayEvidenceTier.SMOKE),
        (3, ReplayEvidenceTier.SMOKE),
        (7, ReplayEvidenceTier.REGRESSION),
        (30, ReplayEvidenceTier.OPERATING_ACCEPTANCE),
        (59, ReplayEvidenceTier.OPERATING_ACCEPTANCE),
        (60, ReplayEvidenceTier.REGIME_ROBUSTNESS),
        (90, ReplayEvidenceTier.REGIME_ROBUSTNESS),
    ],
)
def test_replay_evidence_tier_prevents_short_window_overclaiming(
    evaluation_days, expected
):
    assert replay_evidence_tier(evaluation_days) is expected


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
        "evaluation_days": 30,
        "ten_minute_bar_count": replay_10m_bar_count(30),
        "daily_bar_count": replay_daily_bar_count(30),
    }

    save_dataset(tmp_path, candles, candles, manifest)
    loaded = load_dataset(tmp_path, 30)

    assert loaded is not None
    assert load_dataset(tmp_path, 7, timestamp) is not None
    assert load_dataset(tmp_path, 30, timestamp) is not None
    assert load_dataset(tmp_path, 31, timestamp) is None
    assert (
        load_dataset(tmp_path, 30, timestamp + datetime.timedelta(minutes=10)) is None
    )
    assert loaded[0] == candles
    assert loaded[1] == candles
    assert _tmp_path(tmp_path) == tmp_path.resolve()
    with pytest.raises(ValueError):
        _tmp_path(Path("/var/tmp/not-tmp-output"))

    incomplete = {**loaded[2], "complete": False}
    (tmp_path / "manifest.json").write_text(json.dumps(incomplete), encoding="utf-8")
    assert load_dataset(tmp_path, 30) is None


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
    assert report.source_evaluation_days == 1
    assert report.evidence_tier is ReplayEvidenceTier.SMOKE
    assert report.analysis_timeframes == REPLAY_ANALYSIS_TIMEFRAMES
    assert report.variants[REPLAY_VARIANT_BASELINE].precision_at_k == 1.0
    assert report.variants[REPLAY_VARIANT_ATTENTION].selected_observations == 0
    assert report.warmup_daily_bars == 200
    assert report.attention_episode_count == 0
    assert report.attention_repeated_observations == 0
    assert report.scheduled_digest_scans == 48
    assert report.nonempty_digest_scans == 0
    assert any("orderbook snapshots" in warning for warning in report.warnings)
    assert report.evaluation_end == ten_minute["KRW-BTC"][
        -13
    ].timestamp + datetime.timedelta(minutes=10)
    assert len(observations) == 144
    assert observations[0]["decision_at"] > observations[0]["signal_candle_start"]
    assert observations[0]["signal_model_version"] == config.SIGNAL_MODEL_VERSION
    assert observations[0]["raw_market_coverage_ratio"] == 1.0
    assert REPLAY_VARIANT_BASELINE in observations[0]["variants"]
    assert REPLAY_VARIANT_PROGRESSION in observations[0]["variants"]
    assert REPLAY_VARIANT_V3_SHADOW in observations[0]["variants"]
    assert REPLAY_VARIANT_V3_MATCHED in observations[0]["variants"]
    assert "Precision@K" in report.to_markdown()
    assert "Evidence: `smoke`" in report.to_markdown()
    assert "Timeframes: 10m, 60m, 1d" in report.to_markdown()
    assert "First-Visible Episode Quality" in report.to_markdown()
    assert "Eligible context coverage" in report.to_markdown()
    assert "AttentionYield" in report.to_markdown()
    assert "Scheduled/non-empty 30m digests: 48 / 0" in report.to_markdown()
    assert any("smoke/debug evidence only" in warning for warning in report.warnings)


def test_replay_can_use_a_longer_same_end_cache_for_a_shorter_window():
    source_days = 2
    count = replay_10m_bar_count(source_days)
    start = datetime.datetime(2026, 6, 1, tzinfo=UTC)
    ten_minute = {
        "KRW-BTC": [
            _candle(
                "KRW-BTC",
                start + datetime.timedelta(minutes=10 * index),
                100.0,
            )
            for index in range(count)
        ],
        "KRW-NEW": [
            _candle(
                "KRW-NEW",
                start + datetime.timedelta(minutes=10 * index),
                100.0,
            )
            for index in range(
                count - replay_10m_bar_count(1),
                count,
            )
        ],
    }
    daily_start = start - datetime.timedelta(days=replay_daily_bar_count(source_days))
    daily = {
        "KRW-BTC": [
            _candle(
                "KRW-BTC",
                daily_start + datetime.timedelta(days=index),
                100.0,
            )
            for index in range(replay_daily_bar_count(source_days))
        ]
    }

    report = run_point_in_time_replay(
        ten_minute,
        daily,
        {},
        {},
        evaluation_days=1,
        source_evaluation_days=source_days,
        top_k=1,
        requested_market_count=2,
    )

    assert report.requested_evaluation_days == 1
    assert report.source_evaluation_days == source_days
    assert report.market_count == 1
    assert report.market_coverage_ratio == 0.5
    assert report.variants[REPLAY_VARIANT_BASELINE].scans == 144
    assert any("Reused a 2-day cache" in warning for warning in report.warnings)


def test_replay_run_reuses_superset_cache_and_separates_outputs(tmp_path, monkeypatch):
    source_days = 2
    count = replay_10m_bar_count(source_days)
    start = datetime.datetime(2026, 6, 1, tzinfo=UTC)
    as_of = start + datetime.timedelta(minutes=10 * count)
    candles = [
        _candle(
            "KRW-BTC",
            start + datetime.timedelta(minutes=10 * index),
            100.0,
        )
        for index in range(count)
    ]
    daily_start = start - datetime.timedelta(days=replay_daily_bar_count(source_days))
    daily = [
        _candle(
            "KRW-BTC",
            daily_start + datetime.timedelta(days=index),
            100.0,
        )
        for index in range(replay_daily_bar_count(source_days))
    ]
    cache_dir = tmp_path / "cache"
    output_dir = tmp_path / "report-1d"
    manifest = {
        "schema_version": 1,
        "as_of": as_of.isoformat(),
        "evaluation_days": source_days,
        "requested_market_count": 1,
        "ten_minute_market_count": 1,
        "daily_market_count": 1,
        "ten_minute_bar_count": count,
        "daily_bar_count": len(daily),
        "ten_minute_coverage_ratio": 1.0,
        "coverage_below_minimum": False,
        "requested_markets": ["KRW-BTC"],
    }
    save_dataset(
        cache_dir,
        {"KRW-BTC": candles},
        {"KRW-BTC": daily},
        manifest,
    )

    async def fake_sectors():
        return {}, {}

    monkeypatch.setattr("replay_upbit.load_and_process_sectors", fake_sectors)
    report = asyncio.run(
        run_replay(
            SimpleNamespace(
                cache_dir=str(cache_dir),
                output_dir=str(output_dir),
                as_of=as_of.isoformat(),
                refresh=False,
                evaluation_days=1,
                top_k=1,
            )
        )
    )

    assert report.source_evaluation_days == source_days
    assert not (cache_dir / "report.json").exists()
    assert (output_dir / "report.json").exists()
    assert (output_dir / "report.md").exists()
    assert (output_dir / "observations.ndjson").exists()
    payload = json.loads((output_dir / "report.json").read_text(encoding="utf-8"))
    assert payload["requested_evaluation_days"] == 1
    assert payload["source_evaluation_days"] == source_days
    assert payload["analysis_timeframes"] == list(REPLAY_ANALYSIS_TIMEFRAMES)
    assert "visible_precision_lift_vs_v3" in payload
    assert payload["v4_precision_lift_vs_v3"] is None


def test_point_in_time_replay_measures_nonempty_attention_progression(monkeypatch):
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
        ],
    }
    eth_candles = [
        candle.model_copy(update={"market": "KRW-ETH"}) for candle in candles
    ]
    daily["KRW-ETH"] = [
        candle.model_copy(update={"market": "KRW-ETH"}) for candle in daily["KRW-BTC"]
    ]

    observations = []
    report = run_point_in_time_replay(
        {"KRW-BTC": candles, "KRW-ETH": eth_candles},
        daily,
        {},
        {},
        evaluation_days=1,
        top_k=2,
        observation_sink=observations.append,
    )

    attention = report.variants[REPLAY_VARIANT_ATTENTION]
    assert attention.selected_observations > 0
    assert report.attention_episode_count > 0
    assert report.attention_repeated_observations > 0
    assert report.nonempty_digest_scans > 0
    assert report.attention_yield is not None
    assert report.eligible_context_coverage_ratio > 0
    assert report.variants[REPLAY_VARIANT_V3_SHADOW].selected_observations > 0
    assert all(
        len(observation["variants"][REPLAY_VARIANT_ATTENTION])
        == len(observation["variants"][REPLAY_VARIANT_V3_MATCHED])
        for observation in observations
    )
    assert any(
        len(observation["variants"][REPLAY_VARIANT_V3_SHADOW])
        > len(observation["variants"][REPLAY_VARIANT_V3_MATCHED])
        for observation in observations
    )
    assert report.visible_precision_lift_vs_v3 is not None
    assert report.v4_precision_lift_vs_v3 is None
    assert report.progression_context_precision_lift is not None
    assert "building" in report.attention_stage_metrics
    assert "confirmed" in report.attention_stage_metrics
    survivor_observation = next(
        observation for observation in observations if observation["attention_queue"]
    )
    assert survivor_observation["visible_attention_markets"]
    assert all(
        candidate["v3_shadow_rank"] is not None
        for candidate in survivor_observation["attention_queue"]
    )
    assert all(
        candidate["v4_shadow_rank"] is not None
        for candidate in survivor_observation["attention_queue"]
    )
    assert any(
        candidate["ridge_rank"] is not None
        for candidate in survivor_observation["attention_queue"]
    )
    assert all(
        "ridge_base_quality_score" in candidate
        and "ridge_base_exposures_60m" in candidate
        for candidate in survivor_observation["attention_queue"]
    )
    briefing_observation = next(
        observation
        for observation in observations
        if observation["briefing_attention_markets"]
    )
    assert any(
        candidate["displayed"] for candidate in briefing_observation["attention_queue"]
    )
    markdown = report.to_markdown()
    assert config.ATTENTION_RIDGE_MODEL_VERSION in markdown
    assert "v4 precision lift vs v3 shadow" not in markdown

    monkeypatch.setattr(
        config, "ATTENTION_VISIBLE_MODEL", config.ATTENTION_V4_MODEL_VERSION
    )
    monkeypatch.setattr(
        config, "SIGNAL_MODEL_VERSION", config.ATTENTION_V4_MODEL_VERSION
    )
    v4_report = run_point_in_time_replay(
        {"KRW-BTC": candles, "KRW-ETH": eth_candles},
        daily,
        {},
        {},
        evaluation_days=1,
        top_k=2,
    )

    assert v4_report.v4_precision_lift_vs_v3 == (v4_report.visible_precision_lift_vs_v3)
    assert v4_report.v4_recall_lift_vs_v3 == v4_report.visible_recall_lift_vs_v3
    assert config.ATTENTION_V4_MODEL_VERSION in v4_report.to_markdown()


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

    async def fake_candles(_session, requested, time_unit, **_kwargs):
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

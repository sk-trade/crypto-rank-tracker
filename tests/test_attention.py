import asyncio
import datetime

from common import state_manager
from common.attention import build_attention_queue, rank_attention_candidates
from common.attention import rank_structure_candidates
from common.execution import ExecutionDecision
from common.models import (
    AttentionStage,
    CandleData,
    EvidenceFamily,
    EvidenceVerdict,
    LiquidityTier,
    MarketRegime,
    MarketRegimeSnapshot,
    RejectionCode,
    TickerData,
    TrendState,
)


UTC = datetime.timezone.utc


def _ticker(
    market: str = "KRW-KAITO",
    *,
    latest_close: float = 100.0,
    prior_high: float = 101.0,
) -> TickerData:
    start = datetime.datetime(2026, 7, 18, tzinfo=UTC)
    candles = [
        CandleData(
            market=market,
            timestamp=start + datetime.timedelta(minutes=10 * index),
            open_price=100.0,
            high_price=prior_high,
            low_price=99.0,
            close_price=100.0,
            volume=100.0,
        )
        for index in range(20)
    ]
    candles.append(
        CandleData(
            market=market,
            timestamp=start + datetime.timedelta(minutes=200),
            open_price=100.0,
            high_price=max(latest_close, 100.0),
            low_price=min(latest_close, 100.0),
            close_price=latest_close,
            volume=1_000.0,
        )
    )
    return TickerData(
        market=market,
        candle_history=candles,
        price_change_10m=latest_close - 100.0,
        price_change_1h=1.2,
        price_change_4h=2.1,
        relative_volume=10.0,
        conditional_log_rvol_z_score=6.0,
        cross_sectional_log_rvol_z_score=4.0,
        price_surprise=3.0,
        rolling_turnover=100_000_000.0,
        liquidity_tier=LiquidityTier.HIGH,
    )


def test_filter_candidate_is_visible_before_a_breakout_or_alert():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker(latest_close=100.5, prior_high=101.0)

    queue, state = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {ticker.market: 60},
        signal_candidates=[],
        alerts=[],
        market_regime=MarketRegimeSnapshot(regime=MarketRegime.MEAN_REVERSION),
    )

    assert len(queue) == 1
    assert queue[0].stage is AttentionStage.DISCOVERED
    assert queue[0].market_rank_delta == 18
    assert queue[0].signal_type is None
    assert state.entries[ticker.market].episode_id == queue[0].episode_id
    assert {item.family for item in queue[0].evidence} == set(EvidenceFamily)
    activity = next(
        item for item in queue[0].evidence if item.family is EvidenceFamily.ACTIVITY
    )
    assert "직전24h 10분봉 중간 거래대금" in activity.summary
    assert activity.metrics["median_prior_10m_turnover_krw"] == 100_000_000.0


def test_consecutive_filter_observations_advance_to_building():
    first_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker(latest_close=100.5)
    _, first_state = build_attention_queue(
        first_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )

    queue, _ = build_attention_queue(
        first_at + datetime.timedelta(minutes=10),
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 39},
        {ticker.market: 42},
        [],
        [],
        previous_state=first_state,
    )

    assert queue[0].stage is AttentionStage.BUILDING
    assert queue[0].consecutive_observations == 2
    assert queue[0].first_seen_at == first_at
    assert "stage:discovered->building" in queue[0].change_reasons


def test_retrying_the_same_scan_is_idempotent_for_episode_progression():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker()
    first_queue, first_state = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )

    retry_queue, retry_state = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
        previous_state=first_state,
    )

    assert retry_queue[0].episode_id == first_queue[0].episode_id
    assert retry_queue[0].consecutive_observations == 1
    assert retry_queue[0].change_reasons == ["new_episode"]
    assert retry_state == first_state


def test_candidate_after_a_scan_gap_starts_a_new_episode():
    first_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker()
    first_queue, state = build_attention_queue(
        first_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )

    next_queue, _ = build_attention_queue(
        first_at + datetime.timedelta(minutes=20),
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 40},
        {ticker.market: 42},
        [],
        [],
        previous_state=state,
    )

    assert next_queue[0].episode_id != first_queue[0].episode_id
    assert next_queue[0].consecutive_observations == 1
    assert next_queue[0].change_reasons == ["new_episode"]


def test_structure_confirmation_is_a_stage_not_a_visibility_gate():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker(latest_close=102.0, prior_high=101.0)

    queue, _ = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 10},
        {},
        [],
        [],
    )

    assert queue[0].stage is AttentionStage.CONFIRMED
    assert queue[0].structure_level == 101.0
    assert queue[0].signal_type is not None


def test_execution_risk_is_shown_without_removing_the_candidate():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker()
    execution = ExecutionDecision(
        executable=False,
        rejection_reasons=[RejectionCode.SPREAD_ABOVE_MAXIMUM],
        spread_bps=45.0,
        expected_slippage_bps=2.0,
    )

    queue, _ = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
        execution_decisions={ticker.market: execution},
    )

    execution_evidence = next(
        item for item in queue[0].evidence if item.family is EvidenceFamily.EXECUTION
    )
    assert execution_evidence.verdict is EvidenceVerdict.RISK
    assert "spread_above_maximum" in execution_evidence.summary


def test_conflicting_hourly_and_daily_context_is_explicit_risk():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker(latest_close=100.5).model_copy(
        update={
            "hourly_candles": [_ticker().candle_history[-1]],
            "daily_candles": [_ticker().candle_history[-1]],
            "trend_1h_stable": TrendState.DOWN,
            "is_above_ma50_daily": False,
        }
    )

    queue, _ = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )

    context = next(
        item for item in queue[0].evidence if item.family is EvidenceFamily.CONTEXT
    )
    assert context.verdict is EvidenceVerdict.RISK


def test_attention_priority_prefers_building_before_late_or_failed_structure():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker()
    queue, _ = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )
    base = queue[0]
    ranked = rank_attention_candidates(
        [
            base.model_copy(update={"market": "KRW-FAIL", "stage": AttentionStage.FAILED}),
            base.model_copy(update={"market": "KRW-CONF", "stage": AttentionStage.CONFIRMED}),
            base.model_copy(update={"market": "KRW-BUILD", "stage": AttentionStage.BUILDING}),
        ]
    )

    assert [candidate.stage for candidate in ranked] == [
        AttentionStage.BUILDING,
        AttentionStage.CONFIRMED,
        AttentionStage.FAILED,
    ]


def test_attention_stage_outranks_a_newer_material_change():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker()
    queue, _ = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )
    base = queue[0]
    ranked = rank_attention_candidates(
        [
            base.model_copy(
                update={
                    "market": "KRW-NEW",
                    "stage": AttentionStage.DISCOVERED,
                    "material_change": True,
                }
            ),
            base.model_copy(
                update={
                    "market": "KRW-BUILD",
                    "stage": AttentionStage.BUILDING,
                    "material_change": False,
                    "consecutive_observations": 3,
                }
            ),
        ]
    )

    assert [candidate.market for candidate in ranked] == ["KRW-BUILD", "KRW-NEW"]


def test_structure_only_ranking_preserves_filter_order_after_structure():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker()
    queue, _ = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )
    base = queue[0]

    candidates = [
        base.model_copy(
            update={
                "market": "KRW-BUILD",
                "stage": AttentionStage.BUILDING,
                "conditional_volume_z": 20.0,
            }
        ),
        base.model_copy(
            update={
                "market": "KRW-DISC",
                "stage": AttentionStage.DISCOVERED,
                "conditional_volume_z": 1.0,
            }
        ),
        base.model_copy(
            update={
                "market": "KRW-CONF",
                "stage": AttentionStage.CONFIRMED,
                "conditional_volume_z": 0.5,
            }
        ),
    ]
    ranked = rank_structure_candidates(
        candidates,
        [
            "KRW-DISC",
            "KRW-BUILD",
            "KRW-CONF",
        ],
    )

    assert [candidate.market for candidate in ranked] == [
        "KRW-CONF",
        "KRW-DISC",
        "KRW-BUILD",
    ]


def test_candidate_cools_for_one_observation_after_leaving_the_filter():
    first_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker()
    _, state = build_attention_queue(
        first_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )

    cooling, cooling_state = build_attention_queue(
        first_at + datetime.timedelta(minutes=10),
        [],
        {ticker.market: ticker},
        {ticker.market: 50},
        {ticker.market: 42},
        [],
        [],
        previous_state=state,
    )
    expired, final_state = build_attention_queue(
        first_at + datetime.timedelta(minutes=20),
        [],
        {ticker.market: ticker},
        {ticker.market: 55},
        {ticker.market: 50},
        [],
        [],
        previous_state=cooling_state,
    )

    assert cooling[0].stage is AttentionStage.COOLING
    assert expired == []
    assert final_state.entries == {}


def test_attention_progression_persists_without_webhook_configuration():
    observed_at = datetime.datetime(2026, 7, 19, 0, 10, tzinfo=UTC)
    ticker = _ticker()
    _, state = build_attention_queue(
        observed_at,
        [ticker.market],
        {ticker.market: ticker},
        {ticker.market: 42},
        {},
        [],
        [],
    )

    async def round_trip():
        await state_manager.save_attention_state(state)
        return await state_manager.load_attention_state()

    assert asyncio.run(round_trip()) == state

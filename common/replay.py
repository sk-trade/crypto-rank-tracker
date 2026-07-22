"""Point-in-time replay for measuring attention-queue usefulness."""

from __future__ import annotations

import datetime
import statistics
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

import config
from common.analysis.deep_dive import enrich_deep_dive_tickers, get_market_regime
from common.analysis.scanner import (
    evaluate_candidate_eligibility,
    process_lightweight_indicators,
)
from common.attention import (
    attention_briefing_due,
    build_attention_result,
    rank_attention_candidates,
    rank_filter_candidates,
    rank_structure_candidates,
)
from common.models import (
    AttentionStage,
    AttentionState,
    CandleData,
    MarketRegimeSnapshot,
)
from common.residuals import assign_residual_momentum
from common.signals.detector import detect_anomalies, filter_market_wide_events


REPLAY_VARIANT_BASELINE = "turnover_baseline"
REPLAY_VARIANT_FILTER = "activity_price_filter"
REPLAY_VARIANT_STRUCTURE = "filter_plus_structure"
REPLAY_VARIANT_PROGRESSION = "filter_plus_structure_progression_context"
REPLAY_VARIANT_ATTENTION = "attention_full"
REPLAY_VARIANT_V3_SHADOW = "attention_v3_shadow"
REPLAY_VARIANT_V3_MATCHED = "attention_v3_matched"
REPLAY_ANALYSIS_TIMEFRAMES = ("10m", "60m", "1d")


class ReplayEvidenceTier(StrEnum):
    SMOKE = "smoke"
    REGRESSION = "regression"
    OPERATING_ACCEPTANCE = "operating_acceptance"
    REGIME_ROBUSTNESS = "regime_robustness"


class ReplayVariantMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    scans: int
    selected_observations: int
    true_positive_observations: int
    meaningful_mover_observations: int
    average_selected_count: float
    attention_compression_ratio: float
    precision_at_k: float | None
    recall_at_k: float | None
    average_abs_mfe_30m_pct: float | None
    average_abs_mfe_60m_pct: float | None
    average_abs_mfe_120m_pct: float | None
    median_time_to_1pct_minutes: float | None
    median_time_to_2pct_minutes: float | None


class ReplayStageMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    observations: int
    meaningful_observations: int
    meaningful_rate: float | None
    average_abs_mfe_120m_pct: float | None


class ReplayReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    generated_at: AwareDatetime
    signal_model_version: str
    evaluation_start: AwareDatetime
    evaluation_end: AwareDatetime
    requested_evaluation_days: int
    source_evaluation_days: int
    evidence_tier: ReplayEvidenceTier
    analysis_timeframes: tuple[str, ...]
    scan_interval_minutes: int
    top_k: int
    requested_market_count: int
    market_count: int
    market_coverage_ratio: float
    market_coverage_meets_minimum: bool
    eligible_candidate_observations: int
    eligible_context_observations: int
    eligible_context_coverage_ratio: float
    warmup_10m_bars: int
    warmup_daily_bars: int
    variants: Dict[str, ReplayVariantMetrics]
    precision_lift_vs_turnover: float | None
    recall_lift_vs_turnover: float | None
    structure_precision_lift: float | None
    structure_recall_lift: float | None
    progression_context_precision_lift: float | None
    progression_context_recall_lift: float | None
    retention_precision_lift: float | None
    retention_recall_lift: float | None
    visible_precision_lift_vs_v3: float | None
    visible_recall_lift_vs_v3: float | None
    visible_median_time_to_2pct_delta_vs_v3_minutes: float | None
    v4_precision_lift_vs_v3: float | None = None
    v4_recall_lift_vs_v3: float | None = None
    v4_median_time_to_2pct_delta_vs_v3_minutes: float | None = None
    attention_episode_count: int
    attention_meaningful_episode_count: int
    attention_episode_precision: float | None
    attention_episode_average_abs_mfe_120m_pct: float | None
    attention_episode_median_time_to_1pct_minutes: float | None
    attention_episode_median_time_to_2pct_minutes: float | None
    attention_yield: float | None
    attention_stage_metrics: Dict[str, ReplayStageMetrics]
    attention_visible_observations: int
    attention_repeated_observations: int
    average_attention_observations_per_episode: float | None
    repeated_observations_per_episode: float | None
    material_state_changes: int
    material_change_scans: int
    unchanged_repeat_observations: int
    scheduled_digest_scans: int
    nonempty_digest_scans: int
    warnings: List[str] = Field(default_factory=list)

    def to_markdown(self) -> str:
        lines = [
            "# Upbit Attention Queue Replay",
            "",
            f"- Signal model: `{self.signal_model_version}`",
            (
                f"- Window: `{self.evaluation_start.isoformat()}` to "
                f"`{self.evaluation_end.isoformat()}`"
            ),
            (
                f"- Evidence: `{self.evidence_tier.value}` from "
                f"{self.requested_evaluation_days} requested day(s); "
                f"source cache {self.source_evaluation_days} day(s)"
            ),
            f"- Timeframes: {', '.join(self.analysis_timeframes)}",
            (
                f"- Markets: {self.market_count}/{self.requested_market_count} "
                f"({self.market_coverage_ratio:.1%} coverage; "
                f"minimum {'met' if self.market_coverage_meets_minimum else 'not met'})"
            ),
            (
                "- Eligible context coverage: "
                f"{self.eligible_context_observations}/"
                f"{self.eligible_candidate_observations} "
                f"({self.eligible_context_coverage_ratio:.1%})"
            ),
            f"- Scan interval: {self.scan_interval_minutes} minutes; top K: {self.top_k}",
            (
                f"- Warm-up: {self.warmup_10m_bars} ten-minute bars and "
                f"{self.warmup_daily_bars} daily bars"
            ),
            "",
            "| Variant | Precision@K | Recall@K | Avg selected | Compression | 120m abs MFE |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for name, metrics in self.variants.items():
            lines.append(
                "| "
                + " | ".join(
                    [
                        name,
                        _fmt_ratio(metrics.precision_at_k),
                        _fmt_ratio(metrics.recall_at_k),
                        f"{metrics.average_selected_count:.2f}",
                        f"{metrics.attention_compression_ratio:.1%}",
                        _fmt_pct(metrics.average_abs_mfe_120m_pct),
                    ]
                )
                + " |"
            )
        lines.extend(
            [
                "",
                "## Incremental Lift",
                "",
                f"- Full attention precision vs turnover: {_fmt_points(self.precision_lift_vs_turnover)}",
                f"- Full attention recall vs turnover: {_fmt_points(self.recall_lift_vs_turnover)}",
                f"- Structure precision lift over filter: {_fmt_points(self.structure_precision_lift)}",
                f"- Structure recall lift over filter: {_fmt_points(self.structure_recall_lift)}",
                (
                    "- Progression/context precision lift over structure: "
                    f"{_fmt_points(self.progression_context_precision_lift)}"
                ),
                (
                    "- Progression/context recall lift over structure: "
                    f"{_fmt_points(self.progression_context_recall_lift)}"
                ),
                (
                    "- Cooling/failed retention precision effect: "
                    f"{_fmt_points(self.retention_precision_lift)}"
                ),
                (
                    "- Cooling/failed retention recall effect: "
                    f"{_fmt_points(self.retention_recall_lift)}"
                ),
                (
                    f"- {self.signal_model_version} precision lift vs v3 shadow: "
                    f"{_fmt_points(self.visible_precision_lift_vs_v3)}"
                ),
                (
                    f"- {self.signal_model_version} pre-event recall lift vs v3 shadow: "
                    f"{_fmt_points(self.visible_recall_lift_vs_v3)}"
                ),
                (
                    f"- {self.signal_model_version} median time-to-2% delta vs v3 shadow: "
                    f"{_fmt_minutes(self.visible_median_time_to_2pct_delta_vs_v3_minutes)}"
                ),
                "",
                "## First-Visible Episode Quality",
                "",
                f"- Episodes: {self.attention_episode_count}",
                f"- Meaningful episodes: {self.attention_meaningful_episode_count}",
                f"- Episode precision: {_fmt_ratio(self.attention_episode_precision)}",
                f"- AttentionYield: {_fmt_ratio(self.attention_yield)}",
                (
                    "- Average 120m abs MFE: "
                    f"{_fmt_pct(self.attention_episode_average_abs_mfe_120m_pct)}"
                ),
                (
                    "- Median time to 1% / 2% move: "
                    f"{_fmt_minutes(self.attention_episode_median_time_to_1pct_minutes)} / "
                    f"{_fmt_minutes(self.attention_episode_median_time_to_2pct_minutes)}"
                ),
                "",
                "## Stage Quality",
                "",
                "| Stage | Observations | Meaningful | Rate | 120m abs MFE |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        for stage, metrics in self.attention_stage_metrics.items():
            lines.append(
                "| "
                + " | ".join(
                    [
                        stage,
                        str(metrics.observations),
                        str(metrics.meaningful_observations),
                        _fmt_ratio(metrics.meaningful_rate),
                        _fmt_pct(metrics.average_abs_mfe_120m_pct),
                    ]
                )
                + " |"
            )
        lines.extend(
            [
                "",
                "## Notification Pressure",
                "",
                f"- Visible observations: {self.attention_visible_observations}",
                f"- Repeated episode observations: {self.attention_repeated_observations}",
                (
                    "- Average observations per episode: "
                    + (
                        f"{self.average_attention_observations_per_episode:.2f}"
                        if self.average_attention_observations_per_episode is not None
                        else "n/a"
                    )
                ),
                (
                    "- Repeated observations per episode: "
                    + (
                        f"{self.repeated_observations_per_episode:.2f}"
                        if self.repeated_observations_per_episode is not None
                        else "n/a"
                    )
                ),
                f"- Material state changes: {self.material_state_changes}",
                f"- Scans with a material change: {self.material_change_scans}",
                f"- Unchanged repeat observations: {self.unchanged_repeat_observations}",
                (
                    f"- Scheduled/non-empty {config.ATTENTION_BRIEFING_INTERVAL_MINUTES}m "
                    f"digests: {self.scheduled_digest_scans} / {self.nonempty_digest_scans}"
                ),
                "- Final structure alerts are immediate and are not included in digest counts.",
            ]
        )
        if self.warnings:
            lines.extend(["", "## Warnings", ""])
            lines.extend(f"- {warning}" for warning in self.warnings)
        return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class _FutureOutcome:
    meaningful: bool
    abs_mfe_30m_pct: float
    abs_mfe_60m_pct: float
    abs_mfe_120m_pct: float
    time_to_1pct_minutes: int | None
    time_to_2pct_minutes: int | None
    activity_persistence_ratio: float | None


@dataclass
class _StageAccumulator:
    observations: int = 0
    meaningful_observations: int = 0
    mfe_120m: List[float] = field(default_factory=list)

    def add(self, outcome: _FutureOutcome) -> None:
        self.observations += 1
        self.meaningful_observations += int(outcome.meaningful)
        self.mfe_120m.append(outcome.abs_mfe_120m_pct)

    def metrics(self) -> ReplayStageMetrics:
        return ReplayStageMetrics(
            observations=self.observations,
            meaningful_observations=self.meaningful_observations,
            meaningful_rate=(
                self.meaningful_observations / self.observations
                if self.observations
                else None
            ),
            average_abs_mfe_120m_pct=_mean(self.mfe_120m),
        )


@dataclass
class _VariantAccumulator:
    scans: int = 0
    selected_observations: int = 0
    true_positive_observations: int = 0
    meaningful_mover_observations: int = 0
    universe_observations: int = 0
    selected_counts: List[int] = field(default_factory=list)
    mfe_30m: List[float] = field(default_factory=list)
    mfe_60m: List[float] = field(default_factory=list)
    mfe_120m: List[float] = field(default_factory=list)
    time_to_1pct: List[int] = field(default_factory=list)
    time_to_2pct: List[int] = field(default_factory=list)
    last_selected_at: Dict[str, datetime.datetime] = field(default_factory=dict)

    def add(
        self,
        selected: Sequence[str],
        outcomes: Mapping[str, _FutureOutcome],
        universe_size: int,
        observed_at: datetime.datetime,
    ) -> None:
        selected_set = set(selected)
        meaningful = {
            market for market, outcome in outcomes.items() if outcome.meaningful
        }
        true_positives = selected_set & meaningful
        self.scans += 1
        self.selected_observations += len(selected_set)
        self.true_positive_observations += len(true_positives)
        self.meaningful_mover_observations += len(meaningful)
        self.universe_observations += universe_size
        self.selected_counts.append(len(selected_set))
        for market in selected_set:
            outcome = outcomes.get(market)
            if outcome is None:
                continue
            self.mfe_30m.append(outcome.abs_mfe_30m_pct)
            self.mfe_60m.append(outcome.abs_mfe_60m_pct)
            self.mfe_120m.append(outcome.abs_mfe_120m_pct)
            previous_selection = self.last_selected_at.get(market)
            is_episode_start = previous_selection != observed_at - datetime.timedelta(
                minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
            )
            if is_episode_start:
                if outcome.time_to_1pct_minutes is not None:
                    self.time_to_1pct.append(outcome.time_to_1pct_minutes)
                if outcome.time_to_2pct_minutes is not None:
                    self.time_to_2pct.append(outcome.time_to_2pct_minutes)
            self.last_selected_at[market] = observed_at

    def metrics(self) -> ReplayVariantMetrics:
        return ReplayVariantMetrics(
            scans=self.scans,
            selected_observations=self.selected_observations,
            true_positive_observations=self.true_positive_observations,
            meaningful_mover_observations=self.meaningful_mover_observations,
            average_selected_count=_mean(self.selected_counts) or 0.0,
            attention_compression_ratio=(
                1.0 - self.selected_observations / self.universe_observations
                if self.universe_observations
                else 0.0
            ),
            precision_at_k=(
                self.true_positive_observations / self.selected_observations
                if self.selected_observations
                else None
            ),
            recall_at_k=(
                self.true_positive_observations / self.meaningful_mover_observations
                if self.meaningful_mover_observations
                else None
            ),
            average_abs_mfe_30m_pct=_mean(self.mfe_30m),
            average_abs_mfe_60m_pct=_mean(self.mfe_60m),
            average_abs_mfe_120m_pct=_mean(self.mfe_120m),
            median_time_to_1pct_minutes=_median(self.time_to_1pct),
            median_time_to_2pct_minutes=_median(self.time_to_2pct),
        )


def replay_warmup_10m_bars() -> int:
    bars_per_week = 7 * 24 * (60 // config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES)
    conditional_history = config.CONDITIONAL_VOLUME_LOOKBACK_WEEKS * bars_per_week + 1
    return max(config.RECENT_SCAN_HISTORY_BARS, conditional_history)


def replay_10m_bar_count(evaluation_days: int) -> int:
    _validate_evaluation_days(evaluation_days)
    bars_per_day = 24 * (60 // config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES)
    outcome_bars = (
        config.REPLAY_OUTCOME_HORIZON_MINUTES
        // config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
    )
    return replay_warmup_10m_bars() + evaluation_days * bars_per_day + outcome_bars


def replay_daily_bar_count(evaluation_days: int) -> int:
    _validate_evaluation_days(evaluation_days)
    return 200 + evaluation_days + 2


def replay_feature_history(
    candles: Sequence[CandleData], index: int
) -> List[CandleData]:
    """Build the same three weekly samples plus recent bars used by live scans."""
    bars_per_week = 7 * 24 * (60 // config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES)
    recent_start = index - config.RECENT_SCAN_HISTORY_BARS + 1
    oldest_weekly = index - config.CONDITIONAL_VOLUME_LOOKBACK_WEEKS * bars_per_week
    if index >= len(candles) or recent_start < 0 or oldest_weekly < 0:
        raise ValueError("candle history does not contain the required feature warm-up")
    weekly_samples = [
        candles[index - weeks_ago * bars_per_week]
        for weeks_ago in range(config.CONDITIONAL_VOLUME_LOOKBACK_WEEKS, 0, -1)
    ]
    return [*weekly_samples, *candles[recent_start : index + 1]]


def aggregate_hourly_candles(
    candles_by_market: Mapping[str, Sequence[CandleData]],
) -> Dict[str, List[CandleData]]:
    """Aggregate synthesized 10-minute histories into fully completed UTC hours."""
    result: Dict[str, List[CandleData]] = {}
    for market, candles in candles_by_market.items():
        groups: Dict[datetime.datetime, List[CandleData]] = {}
        for candle in candles:
            hour = candle.timestamp.replace(minute=0, second=0, microsecond=0)
            groups.setdefault(hour, []).append(candle)
        hourly = []
        for hour, group in sorted(groups.items()):
            ordered = sorted(group, key=lambda candle: candle.timestamp)
            expected = [
                hour + datetime.timedelta(minutes=10 * index) for index in range(6)
            ]
            if [candle.timestamp for candle in ordered] != expected:
                continue
            hourly.append(
                CandleData(
                    market=market,
                    timestamp=hour,
                    open_price=ordered[0].open_price,
                    high_price=max(candle.high_price for candle in ordered),
                    low_price=min(candle.low_price for candle in ordered),
                    close_price=ordered[-1].close_price,
                    volume=sum(candle.volume for candle in ordered),
                    trade_value=sum(
                        candle.trade_value
                        if candle.trade_value is not None
                        else candle.close_price * candle.volume
                        for candle in ordered
                    ),
                )
            )
        if hourly:
            result[market] = hourly
    return result


def run_point_in_time_replay(
    candles_10m: Mapping[str, Sequence[CandleData]],
    candles_daily: Mapping[str, Sequence[CandleData]],
    sectors: Mapping[str, List[str]],
    reverse_sector_map: Mapping[str, List[str]],
    *,
    evaluation_days: int = config.REPLAY_DEFAULT_EVALUATION_DAYS,
    source_evaluation_days: int | None = None,
    top_k: int = config.REPLAY_DEFAULT_TOP_K,
    requested_market_count: int | None = None,
    progress: Callable[[int, int, datetime.datetime], None] | None = None,
    observation_sink: Callable[[Dict[str, Any]], None] | None = None,
) -> ReplayReport:
    """Replay production feature functions at each completed 10-minute decision point."""
    _validate_evaluation_days(evaluation_days)
    source_days = (
        evaluation_days if source_evaluation_days is None else source_evaluation_days
    )
    _validate_evaluation_days(source_days)
    if source_days < evaluation_days:
        raise ValueError(
            "source_evaluation_days cannot be shorter than evaluation_days"
        )
    if top_k < 1:
        raise ValueError("top_k must be positive")
    if "KRW-BTC" not in candles_10m:
        raise ValueError("KRW-BTC history is required for replay")
    source_required_count = replay_10m_bar_count(source_days)
    if len(candles_10m["KRW-BTC"]) < source_required_count:
        raise ValueError("source replay history is shorter than source_evaluation_days")

    required_count = replay_10m_bar_count(evaluation_days)
    source_aligned = _aligned_histories(candles_10m, source_required_count)
    if "KRW-BTC" not in source_aligned:
        raise ValueError("KRW-BTC history is incomplete or misaligned")
    aligned = {
        market: candles[-required_count:] for market, candles in source_aligned.items()
    }
    reference = aligned["KRW-BTC"]
    hourly = aggregate_hourly_candles(aligned)
    outcome_bars = (
        config.REPLAY_OUTCOME_HORIZON_MINUTES
        // config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
    )
    warmup = replay_warmup_10m_bars()
    evaluation_bars = (
        evaluation_days * 24 * (60 // config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES)
    )
    last_index = len(reference) - outcome_bars - 1
    first_index = last_index - evaluation_bars + 1
    if first_index < warmup - 1:
        raise ValueError("replay history does not contain the required warm-up")

    accumulators = {
        name: _VariantAccumulator()
        for name in [
            REPLAY_VARIANT_BASELINE,
            REPLAY_VARIANT_FILTER,
            REPLAY_VARIANT_STRUCTURE,
            REPLAY_VARIANT_PROGRESSION,
            REPLAY_VARIANT_ATTENTION,
            REPLAY_VARIANT_V3_SHADOW,
            REPLAY_VARIANT_V3_MATCHED,
        ]
    }
    previous_rankings: Dict[str, int] = {}
    attention_state = AttentionState()
    episode_outcomes: Dict[str, _FutureOutcome] = {}
    stage_accumulators: Dict[str, _StageAccumulator] = {}
    visible_observations = 0
    material_changes = 0
    material_change_scans = 0
    scheduled_digest_scans = 0
    nonempty_digest_scans = 0
    eligible_candidate_observations = 0
    eligible_context_observations = 0
    warnings = []
    warnings.append(
        "Historical orderbook snapshots are unavailable; execution-risk evidence "
        "is excluded from replay lift metrics."
    )
    evidence_tier = replay_evidence_tier(evaluation_days)
    if evidence_tier is ReplayEvidenceTier.SMOKE:
        warnings.append(
            "A 1-3 day replay is smoke/debug evidence only; it is not operating "
            "acceptance evidence."
        )
    elif evidence_tier is ReplayEvidenceTier.REGRESSION:
        warnings.append(
            f"This window is shorter than the {config.REPLAY_OPERATING_ACCEPTANCE_DAYS}-day "
            "operating acceptance window; use it for regression comparison only."
        )
    if source_days > evaluation_days:
        warnings.append(
            f"Reused a {source_days}-day cache for a {evaluation_days}-day evaluation; "
            "market coverage reflects markets with complete longer-window history."
        )
    expected_market_count = requested_market_count or len(candles_10m)
    if expected_market_count < len(candles_10m):
        raise ValueError("requested_market_count cannot be below collected coverage")
    market_coverage = (
        len(aligned) / expected_market_count if expected_market_count else 0.0
    )
    if len(aligned) != expected_market_count:
        warnings.append(
            f"Excluded {expected_market_count - len(aligned)} market(s) with incomplete or misaligned histories."
        )
    if market_coverage < config.CANDLE_SUCCESS_RATE_MINIMUM:
        warnings.append(
            f"Market coverage {market_coverage:.1%} is below the production minimum "
            f"of {config.CANDLE_SUCCESS_RATE_MINIMUM:.0%}; treat quality metrics as partial evidence."
        )
    daily_context_markets = sum(
        len(candles_daily.get(market, [])) >= 200 for market in aligned
    )
    if daily_context_markets != len(aligned):
        warnings.append(
            f"Daily MA context unavailable for {len(aligned) - daily_context_markets} "
            "replayed market(s); broad-filter observations remain eligible."
        )

    total_scans = last_index - first_index + 1
    for scan_number, index in enumerate(range(first_index, last_index + 1), 1):
        signal_candle_start = reference[index].timestamp
        observed_at = signal_candle_start + datetime.timedelta(
            minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
        )
        histories = {
            market: replay_feature_history(candles, index)
            for market, candles in aligned.items()
        }
        tickers = process_lightweight_indicators(histories)
        assign_residual_momentum(tickers, dict(sectors), dict(reverse_sector_map))
        decisions = evaluate_candidate_eligibility(tickers)
        candidate_markets = [
            market for market, decision in decisions.items() if decision.eligible
        ]
        current_rankings = _historical_turnover_rankings(histories)

        as_of = observed_at
        markets_to_enrich = sorted(
            set(candidate_markets) | set(attention_state.entries) | {"KRW-BTC"}
        )
        hourly_slice = {
            market: _completed_before(
                hourly.get(market, []), as_of, 200, datetime.timedelta(hours=1)
            )
            for market in markets_to_enrich
        }
        daily_slice = {
            market: _completed_before(
                candles_daily.get(market, []), as_of, 200, datetime.timedelta(days=1)
            )
            for market in markets_to_enrich
        }
        enriched = tickers.copy()
        enriched.update(
            enrich_deep_dive_tickers(
                {
                    market: tickers[market]
                    for market in markets_to_enrich
                    if market in tickers
                },
                hourly_slice,
                daily_slice,
                tickers,
            )
        )
        market_regime: MarketRegimeSnapshot = get_market_regime(enriched)
        signals = detect_anomalies(
            candidate_markets,
            enriched,
            dict(sectors),
            dict(reverse_sector_map),
        )
        signals = filter_market_wide_events(signals, enriched)
        attention_result = build_attention_result(
            observed_at,
            candidate_markets,
            enriched,
            current_rankings,
            previous_rankings,
            signals,
            [],
            previous_state=attention_state,
            market_regime=market_regime,
            limit=top_k,
        )
        all_attention = attention_result.all_candidates
        attention_state = attention_result.state

        baseline = [
            market
            for market, _rank in sorted(
                current_rankings.items(), key=lambda item: item[1]
            )[:top_k]
        ]
        filter_order = rank_filter_candidates(candidate_markets, enriched)
        filtered = filter_order[:top_k]
        active_markets = set(candidate_markets)
        active_attention = [
            candidate
            for candidate in all_attention
            if candidate.market in active_markets
        ]
        structured = [
            candidate.market
            for candidate in rank_structure_candidates(active_attention, filter_order)[
                :top_k
            ]
        ]
        progressed_candidates = rank_attention_candidates(active_attention)[:top_k]
        progressed = [candidate.market for candidate in progressed_candidates]
        full_candidates = attention_result.visible
        full = [candidate.market for candidate in full_candidates]
        briefing_due = attention_briefing_due(observed_at)
        displayed_candidates = full_candidates if briefing_due else []
        v3_shadow_candidates = sorted(
            all_attention,
            key=lambda candidate: candidate.v3_shadow_rank or 1_000_000,
        )[:top_k]
        v3_shadow = [candidate.market for candidate in v3_shadow_candidates]
        v3_matched = v3_shadow[: len(full_candidates)]
        outcomes = {
            market: _future_outcome(candles, index)
            for market, candles in aligned.items()
        }
        for candidate in displayed_candidates:
            outcome = outcomes[candidate.market]
            stage_accumulators.setdefault(
                candidate.stage.value, _StageAccumulator()
            ).add(outcome)
            episode_outcomes.setdefault(candidate.episode_id, outcome)
        selections = {
            REPLAY_VARIANT_BASELINE: baseline,
            REPLAY_VARIANT_FILTER: filtered,
            REPLAY_VARIANT_STRUCTURE: structured,
            REPLAY_VARIANT_PROGRESSION: progressed,
            REPLAY_VARIANT_ATTENTION: full,
            REPLAY_VARIANT_V3_SHADOW: v3_shadow,
            REPLAY_VARIANT_V3_MATCHED: v3_matched,
        }
        for name, selected in selections.items():
            accumulators[name].add(selected, outcomes, len(aligned), observed_at)

        if observation_sink:
            selected_markets = set().union(*map(set, selections.values()))
            meaningful_markets = {
                market for market, outcome in outcomes.items() if outcome.meaningful
            }
            included_outcomes = selected_markets | meaningful_markets
            observation_sink(
                {
                    "signal_model_version": config.SIGNAL_MODEL_VERSION,
                    "decision_at": observed_at.isoformat(),
                    "signal_candle_start": signal_candle_start.isoformat(),
                    "universe_size": len(aligned),
                    "raw_market_coverage_ratio": market_coverage,
                    "filter_candidate_count": len(candidate_markets),
                    "eligible_context_count": sum(
                        candidate.context_available
                        for candidate in all_attention
                        if candidate.market in active_markets
                    ),
                    "variants": selections,
                    "attention_queue": [
                        candidate.model_dump(mode="json") for candidate in all_attention
                    ],
                    "visible_attention_markets": full,
                    "briefing_attention_markets": [
                        candidate.market for candidate in displayed_candidates
                    ],
                    "meaningful_movers": sorted(meaningful_markets),
                    "outcomes": {
                        market: asdict(outcomes[market])
                        for market in sorted(included_outcomes)
                    },
                }
            )

        visible_observations += len(displayed_candidates)
        eligible_candidate_observations += len(candidate_markets)
        eligible_context_observations += sum(
            candidate.context_available
            for candidate in all_attention
            if candidate.market in active_markets
        )
        scan_material_changes = sum(
            candidate.material_change for candidate in displayed_candidates
        )
        material_changes += scan_material_changes
        material_change_scans += int(scan_material_changes > 0)
        if briefing_due:
            scheduled_digest_scans += 1
            nonempty_digest_scans += int(bool(full_candidates))
        previous_rankings = current_rankings
        if progress:
            progress(scan_number, total_scans, observed_at)

    variants = {
        name: accumulator.metrics() for name, accumulator in accumulators.items()
    }
    baseline_metrics = variants[REPLAY_VARIANT_BASELINE]
    filter_metrics = variants[REPLAY_VARIANT_FILTER]
    structure_metrics = variants[REPLAY_VARIANT_STRUCTURE]
    progression_metrics = variants[REPLAY_VARIANT_PROGRESSION]
    attention_metrics = variants[REPLAY_VARIANT_ATTENTION]
    v3_metrics = variants[REPLAY_VARIANT_V3_MATCHED]
    unchanged_repeats = max(visible_observations - material_changes, 0)
    episode_values = list(episode_outcomes.values())
    episode_count = len(episode_values)
    meaningful_episode_count = sum(outcome.meaningful for outcome in episode_values)
    repeated_observations = max(visible_observations - episode_count, 0)
    stage_metrics = {
        stage.value: stage_accumulators[stage.value].metrics()
        for stage in [
            AttentionStage.BUILDING,
            AttentionStage.CONFIRMED,
            AttentionStage.DISCOVERED,
            AttentionStage.FAILED,
            AttentionStage.COOLING,
        ]
        if stage.value in stage_accumulators
    }
    return ReplayReport(
        generated_at=datetime.datetime.now(datetime.timezone.utc),
        signal_model_version=config.SIGNAL_MODEL_VERSION,
        evaluation_start=reference[first_index].timestamp
        + datetime.timedelta(minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES),
        evaluation_end=reference[last_index].timestamp
        + datetime.timedelta(minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES),
        requested_evaluation_days=evaluation_days,
        source_evaluation_days=source_days,
        evidence_tier=evidence_tier,
        analysis_timeframes=REPLAY_ANALYSIS_TIMEFRAMES,
        scan_interval_minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES,
        top_k=top_k,
        requested_market_count=expected_market_count,
        market_count=len(aligned),
        market_coverage_ratio=market_coverage,
        market_coverage_meets_minimum=(
            market_coverage >= config.CANDLE_SUCCESS_RATE_MINIMUM
        ),
        eligible_candidate_observations=eligible_candidate_observations,
        eligible_context_observations=eligible_context_observations,
        eligible_context_coverage_ratio=(
            eligible_context_observations / eligible_candidate_observations
            if eligible_candidate_observations
            else 1.0
        ),
        warmup_10m_bars=warmup,
        warmup_daily_bars=200,
        variants=variants,
        precision_lift_vs_turnover=_difference(
            attention_metrics.precision_at_k, baseline_metrics.precision_at_k
        ),
        recall_lift_vs_turnover=_difference(
            attention_metrics.recall_at_k, baseline_metrics.recall_at_k
        ),
        structure_precision_lift=_difference(
            structure_metrics.precision_at_k, filter_metrics.precision_at_k
        ),
        structure_recall_lift=_difference(
            structure_metrics.recall_at_k, filter_metrics.recall_at_k
        ),
        progression_context_precision_lift=_difference(
            progression_metrics.precision_at_k, structure_metrics.precision_at_k
        ),
        progression_context_recall_lift=_difference(
            progression_metrics.recall_at_k, structure_metrics.recall_at_k
        ),
        retention_precision_lift=_difference(
            attention_metrics.precision_at_k, progression_metrics.precision_at_k
        ),
        retention_recall_lift=_difference(
            attention_metrics.recall_at_k, progression_metrics.recall_at_k
        ),
        visible_precision_lift_vs_v3=_difference(
            attention_metrics.precision_at_k, v3_metrics.precision_at_k
        ),
        visible_recall_lift_vs_v3=_difference(
            attention_metrics.recall_at_k, v3_metrics.recall_at_k
        ),
        visible_median_time_to_2pct_delta_vs_v3_minutes=_difference(
            attention_metrics.median_time_to_2pct_minutes,
            v3_metrics.median_time_to_2pct_minutes,
        ),
        v4_precision_lift_vs_v3=(
            _difference(
                attention_metrics.precision_at_k,
                v3_metrics.precision_at_k,
            )
            if config.ATTENTION_VISIBLE_MODEL == config.ATTENTION_V4_MODEL_VERSION
            else None
        ),
        v4_recall_lift_vs_v3=(
            _difference(
                attention_metrics.recall_at_k,
                v3_metrics.recall_at_k,
            )
            if config.ATTENTION_VISIBLE_MODEL == config.ATTENTION_V4_MODEL_VERSION
            else None
        ),
        v4_median_time_to_2pct_delta_vs_v3_minutes=(
            _difference(
                attention_metrics.median_time_to_2pct_minutes,
                v3_metrics.median_time_to_2pct_minutes,
            )
            if config.ATTENTION_VISIBLE_MODEL == config.ATTENTION_V4_MODEL_VERSION
            else None
        ),
        attention_episode_count=episode_count,
        attention_meaningful_episode_count=meaningful_episode_count,
        attention_episode_precision=(
            meaningful_episode_count / episode_count if episode_count else None
        ),
        attention_episode_average_abs_mfe_120m_pct=_mean(
            outcome.abs_mfe_120m_pct for outcome in episode_values
        ),
        attention_episode_median_time_to_1pct_minutes=_median(
            outcome.time_to_1pct_minutes
            for outcome in episode_values
            if outcome.time_to_1pct_minutes is not None
        ),
        attention_episode_median_time_to_2pct_minutes=_median(
            outcome.time_to_2pct_minutes
            for outcome in episode_values
            if outcome.time_to_2pct_minutes is not None
        ),
        attention_yield=(
            meaningful_episode_count / visible_observations
            if visible_observations
            else None
        ),
        attention_stage_metrics=stage_metrics,
        attention_visible_observations=visible_observations,
        attention_repeated_observations=repeated_observations,
        average_attention_observations_per_episode=(
            visible_observations / episode_count if episode_count else None
        ),
        repeated_observations_per_episode=(
            repeated_observations / episode_count if episode_count else None
        ),
        material_state_changes=material_changes,
        material_change_scans=material_change_scans,
        unchanged_repeat_observations=unchanged_repeats,
        scheduled_digest_scans=scheduled_digest_scans,
        nonempty_digest_scans=nonempty_digest_scans,
        warnings=warnings,
    )


def _aligned_histories(
    candles_by_market: Mapping[str, Sequence[CandleData]], required_count: int
) -> Dict[str, List[CandleData]]:
    if "KRW-BTC" not in candles_by_market:
        return {}
    reference = list(candles_by_market["KRW-BTC"])[-required_count:]
    reference_timestamps = [candle.timestamp for candle in reference]
    aligned = {}
    for market, candles in candles_by_market.items():
        selected = list(candles)[-required_count:]
        if (
            len(selected) == required_count
            and [candle.timestamp for candle in selected] == reference_timestamps
        ):
            aligned[market] = selected
    return aligned


def _historical_turnover_rankings(
    histories: Mapping[str, Sequence[CandleData]],
) -> Dict[str, int]:
    bars_per_day = 24 * (60 // config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES)
    turnover = {
        market: sum(
            candle.trade_value
            if candle.trade_value is not None
            else candle.close_price * candle.volume
            for candle in candles[-bars_per_day:]
        )
        for market, candles in histories.items()
    }
    ordered = sorted(turnover, key=lambda market: (-turnover[market], market))
    return {market: index for index, market in enumerate(ordered, 1)}


def _completed_before(
    candles: Sequence[CandleData],
    as_of: datetime.datetime,
    count: int,
    interval: datetime.timedelta,
) -> List[CandleData]:
    if not candles:
        return []
    completed = [candle for candle in candles if candle.timestamp + interval <= as_of]
    return completed[-count:]


def _future_outcome(candles: Sequence[CandleData], signal_index: int) -> _FutureOutcome:
    horizon_bars = (
        config.REPLAY_OUTCOME_HORIZON_MINUTES
        // config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
    )
    path = list(candles[signal_index + 1 : signal_index + 1 + horizon_bars])
    if len(path) != horizon_bars:
        raise ValueError("future outcome path is incomplete")
    entry_price = path[0].open_price

    def abs_mfe(bar_count: int) -> float:
        segment = path[:bar_count]
        upside = max(candle.high_price / entry_price - 1.0 for candle in segment)
        downside = max(1.0 - candle.low_price / entry_price for candle in segment)
        return max(upside, downside) * 100

    prior = candles[max(0, signal_index - 143) : signal_index + 1]
    baseline_volumes = [candle.volume for candle in prior if candle.volume > 0]
    baseline_volume = statistics.median(baseline_volumes) if baseline_volumes else 0.0
    future_volume = statistics.mean(candle.volume for candle in path[:6])
    persistence = future_volume / baseline_volume if baseline_volume > 0 else None
    mfe_120 = abs_mfe(12)
    meaningful = mfe_120 >= config.REPLAY_MEANINGFUL_MOVE_PCT or (
        mfe_120 >= config.REPLAY_MEANINGFUL_MOVE_PCT / 2
        and (persistence or 0.0) >= config.REPLAY_ACTIVITY_PERSISTENCE_RATIO
    )
    return _FutureOutcome(
        meaningful=meaningful,
        abs_mfe_30m_pct=abs_mfe(3),
        abs_mfe_60m_pct=abs_mfe(6),
        abs_mfe_120m_pct=mfe_120,
        time_to_1pct_minutes=_time_to_move(path, entry_price, 1.0),
        time_to_2pct_minutes=_time_to_move(path, entry_price, 2.0),
        activity_persistence_ratio=persistence,
    )


def _time_to_move(
    path: Sequence[CandleData], entry_price: float, threshold_pct: float
) -> int | None:
    for index, candle in enumerate(path, 1):
        upside = (candle.high_price / entry_price - 1.0) * 100
        downside = (1.0 - candle.low_price / entry_price) * 100
        if max(upside, downside) >= threshold_pct:
            return index * config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES
    return None


def _validate_evaluation_days(evaluation_days: int) -> None:
    if not (
        config.REPLAY_MIN_EVALUATION_DAYS
        <= evaluation_days
        <= config.REPLAY_MAX_EVALUATION_DAYS
    ):
        raise ValueError(
            f"evaluation_days must be between {config.REPLAY_MIN_EVALUATION_DAYS} "
            f"and {config.REPLAY_MAX_EVALUATION_DAYS}"
        )


def replay_evidence_tier(evaluation_days: int) -> ReplayEvidenceTier:
    _validate_evaluation_days(evaluation_days)
    if evaluation_days <= 3:
        return ReplayEvidenceTier.SMOKE
    if evaluation_days < config.REPLAY_OPERATING_ACCEPTANCE_DAYS:
        return ReplayEvidenceTier.REGRESSION
    if evaluation_days < config.REPLAY_ROBUSTNESS_MIN_EVALUATION_DAYS:
        return ReplayEvidenceTier.OPERATING_ACCEPTANCE
    return ReplayEvidenceTier.REGIME_ROBUSTNESS


def _mean(values: Iterable[float]) -> float | None:
    values = list(values)
    return statistics.fmean(values) if values else None


def _median(values: Iterable[float]) -> float | None:
    values = list(values)
    return float(statistics.median(values)) if values else None


def _difference(left: float | None, right: float | None) -> float | None:
    return left - right if left is not None and right is not None else None


def _fmt_ratio(value: float | None) -> str:
    return f"{value:.1%}" if value is not None else "n/a"


def _fmt_pct(value: float | None) -> str:
    return f"{value:.2f}%" if value is not None else "n/a"


def _fmt_minutes(value: float | None) -> str:
    return f"{value:.0f}m" if value is not None else "n/a"


def _fmt_points(value: float | None) -> str:
    return f"{value:+.1%}p" if value is not None else "n/a"

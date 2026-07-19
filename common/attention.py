"""Build a compact, explainable queue of markets that deserve human attention."""

from __future__ import annotations

import datetime
import hashlib
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Sequence

import config
from common.execution import ExecutionDecision
from common.models import (
    Alert,
    AttentionCandidate,
    AttentionEvidence,
    AttentionStage,
    AttentionState,
    AttentionStateEntry,
    EvidenceFamily,
    EvidenceVerdict,
    MarketRegimeSnapshot,
    SignalCandidate,
    SignalType,
    StructureDirection,
    TickerData,
    TrendState,
)


@dataclass(frozen=True)
class _StructureSnapshot:
    signal_type: SignalType | None = None
    level: float | None = None
    direction: StructureDirection | None = None
    distance_pct: float | None = None
    confirmed: bool = False
    failed: bool = False


def build_attention_queue(
    observed_at: datetime.datetime,
    candidate_markets: Iterable[str],
    tickers: Mapping[str, TickerData],
    current_rankings: Mapping[str, int],
    previous_rankings: Mapping[str, int],
    signal_candidates: Iterable[SignalCandidate],
    alerts: Iterable[Alert],
    previous_state: AttentionState | None = None,
    execution_decisions: Mapping[str, ExecutionDecision] | None = None,
    market_regime: MarketRegimeSnapshot | None = None,
    limit: int | None = None,
) -> tuple[List[AttentionCandidate], AttentionState]:
    """Advance attention episodes and return the highest-priority visible queue."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")

    previous_state = previous_state or AttentionState()
    execution_decisions = execution_decisions or {}
    signals_by_market = {candidate.market: candidate for candidate in signal_candidates}
    alerts_by_market = {alert.candidate.market: alert for alert in alerts}
    eligible_markets = {
        market
        for market in candidate_markets
        if market in tickers and tickers[market].candle_history
    }
    updated_entries: Dict[str, AttentionStateEntry] = {}
    candidates: List[AttentionCandidate] = []

    for market in sorted(eligible_markets):
        ticker = tickers[market]
        previous = previous_state.entries.get(market)
        signal = signals_by_market.get(market)
        alert = alerts_by_market.get(market)
        structure = _classify_structure(ticker, previous, alert)
        same_observation = previous is not None and previous.last_seen_at == observed_at
        continuous = previous is not None and _is_continuous(previous, observed_at)

        if same_observation:
            first_seen_at = previous.first_seen_at
            episode_id = previous.episode_id
            consecutive = previous.consecutive_observations
            new_episode = False
        elif continuous and previous.stage not in {AttentionStage.FAILED}:
            first_seen_at = previous.first_seen_at
            episode_id = previous.episode_id
            consecutive = previous.consecutive_observations + 1
            new_episode = False
        else:
            first_seen_at = observed_at
            episode_id = _episode_id(market, observed_at)
            consecutive = 1
            new_episode = True

        stage = _active_stage(structure, consecutive)
        signal_type = alert.signal_type if alert else structure.signal_type
        current_price = ticker.candle_history[-1].close_price
        market_rank = current_rankings.get(market)
        relative_volume = ticker.relative_volume

        if same_observation:
            material_change = previous.material_change
            change_reasons = list(previous.change_reasons)
        elif new_episode:
            material_change = True
            change_reasons = ["new_episode"]
        else:
            material_change, change_reasons = _material_changes(
                previous,
                stage=stage,
                market_rank=market_rank,
                current_price=current_price,
                relative_volume=relative_volume,
                signal_type=signal_type,
            )

        entry = AttentionStateEntry(
            market=market,
            episode_id=episode_id,
            first_seen_at=first_seen_at,
            last_seen_at=observed_at,
            consecutive_observations=consecutive,
            stage=stage,
            last_rank=market_rank,
            last_price=current_price,
            last_relative_volume=relative_volume,
            last_signal_score=signal.signal_score if signal else None,
            last_signal_type=signal_type,
            structure_level=structure.level,
            structure_direction=structure.direction,
            cooling_observations=0,
            material_change=material_change,
            change_reasons=change_reasons,
        )
        updated_entries[market] = entry
        candidates.append(
            _candidate_from_entry(
                entry,
                ticker,
                current_rankings,
                previous_rankings,
                signal,
                execution_decisions.get(market),
                market_regime,
                structure,
            )
        )

    for market, previous in previous_state.entries.items():
        if market in eligible_markets or market not in tickers or not tickers[market].candle_history:
            continue
        if previous.last_seen_at == observed_at and previous.stage in {
            AttentionStage.COOLING,
            AttentionStage.FAILED,
        }:
            entry = previous
        elif (
            _is_continuous(previous, observed_at)
            and previous.cooling_observations < config.ATTENTION_COOLING_OBSERVATIONS
        ):
            ticker = tickers[market]
            structure = _classify_structure(ticker, previous, None)
            stage = AttentionStage.FAILED if structure.failed else AttentionStage.COOLING
            entry = previous.model_copy(
                update={
                    "last_seen_at": observed_at,
                    "stage": stage,
                    "last_rank": current_rankings.get(market),
                    "last_price": ticker.candle_history[-1].close_price,
                    "last_relative_volume": ticker.relative_volume,
                    "last_signal_type": structure.signal_type,
                    "structure_level": structure.level or previous.structure_level,
                    "structure_direction": structure.direction
                    or previous.structure_direction,
                    "cooling_observations": previous.cooling_observations + 1,
                    "material_change": True,
                    "change_reasons": [f"stage:{previous.stage.value}->{stage.value}"],
                }
            )
        else:
            continue

        ticker = tickers[market]
        structure = _classify_structure(ticker, entry, None)
        updated_entries[market] = entry
        candidates.append(
            _candidate_from_entry(
                entry,
                ticker,
                current_rankings,
                previous_rankings,
                signals_by_market.get(market),
                execution_decisions.get(market),
                market_regime,
                structure,
            )
        )

    ranked = rank_attention_candidates(candidates)
    visible_limit = config.ATTENTION_QUEUE_LIMIT if limit is None else limit
    if visible_limit < 1:
        raise ValueError("attention queue limit must be positive")
    state = AttentionState(updated_at=observed_at, entries=updated_entries)
    return ranked[:visible_limit], state


def rank_attention_candidates(
    candidates: Sequence[AttentionCandidate],
) -> List[AttentionCandidate]:
    """Apply a deterministic priority order without exposing a fake precision score."""

    def sort_key(candidate: AttentionCandidate) -> tuple:
        stage_priority = {
            AttentionStage.BUILDING: 5,
            AttentionStage.CONFIRMED: 4,
            AttentionStage.DISCOVERED: 3,
            AttentionStage.FAILED: 2,
            AttentionStage.COOLING: 1,
        }
        progression = candidate.consecutive_observations
        material = int(candidate.material_change)
        structure = stage_priority[candidate.stage]
        context = candidate.signal_score or 0.0
        volume = candidate.conditional_volume_z or 0.0
        surprise = candidate.price_surprise or 0.0
        market_rank = candidate.market_rank or 1_000_000
        # Stage reflects observed quality; material change is a within-stage tiebreaker.
        return (
            -structure,
            -material,
            -progression,
            -context,
            -volume,
            -surprise,
            market_rank,
            candidate.market,
        )

    ordered = sorted(candidates, key=sort_key)
    return [
        candidate.model_copy(update={"attention_rank": index})
        for index, candidate in enumerate(ordered, 1)
    ]


def rank_filter_candidates(
    markets: Iterable[str], tickers: Mapping[str, TickerData]
) -> List[str]:
    """Rank broad-filter survivors using only their primary price/activity evidence."""

    def sort_key(market: str) -> tuple:
        ticker = tickers[market]
        volume_ratio = (ticker.conditional_log_rvol_z_score or 0.0) / max(
            config.rvol_z_score_minimum(ticker.liquidity_tier), 1e-9
        )
        price_ratio = (ticker.price_surprise or 0.0) / max(
            config.price_surprise_minimum(ticker.liquidity_tier), 1e-9
        )
        return (
            -volume_ratio,
            -price_ratio,
            -(ticker.rolling_turnover or 0.0),
            market,
        )

    return sorted(
        (market for market in markets if market in tickers),
        key=sort_key,
    )


def rank_structure_candidates(
    candidates: Sequence[AttentionCandidate], filter_order: Sequence[str]
) -> List[AttentionCandidate]:
    """Add only observed structure state while preserving the broad-filter order."""
    filter_positions = {
        market: index for index, market in enumerate(filter_order)
    }
    stage_priority = {
        AttentionStage.CONFIRMED: 2,
        AttentionStage.BUILDING: 1,
        AttentionStage.DISCOVERED: 1,
        AttentionStage.FAILED: 0,
        AttentionStage.COOLING: 0,
    }
    ordered = sorted(
        candidates,
        key=lambda candidate: (
            -stage_priority[candidate.stage],
            filter_positions.get(candidate.market, len(filter_positions)),
            candidate.market,
        ),
    )
    return [
        candidate.model_copy(update={"attention_rank": index})
        for index, candidate in enumerate(ordered, 1)
    ]


def attention_briefing_due(observed_at: datetime.datetime) -> bool:
    """Return whether this completed scan belongs to the digest cadence."""
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    elapsed_minutes = int(observed_at.timestamp() // 60)
    return elapsed_minutes % config.ATTENTION_BRIEFING_INTERVAL_MINUTES == 0


def _candidate_from_entry(
    entry: AttentionStateEntry,
    ticker: TickerData,
    current_rankings: Mapping[str, int],
    previous_rankings: Mapping[str, int],
    signal: SignalCandidate | None,
    execution: ExecutionDecision | None,
    market_regime: MarketRegimeSnapshot | None,
    structure: _StructureSnapshot,
) -> AttentionCandidate:
    market_rank = current_rankings.get(entry.market)
    previous_rank = previous_rankings.get(entry.market)
    rank_delta = (
        previous_rank - market_rank
        if previous_rank is not None and market_rank is not None
        else None
    )
    return AttentionCandidate(
        market=entry.market,
        attention_rank=1,
        market_rank=market_rank,
        market_rank_delta=rank_delta,
        stage=entry.stage,
        episode_id=entry.episode_id,
        first_seen_at=entry.first_seen_at,
        observed_at=entry.last_seen_at,
        consecutive_observations=entry.consecutive_observations,
        current_price=entry.last_price,
        price_change_10m=ticker.price_change_10m,
        price_change_1h=ticker.price_change_1h,
        price_change_4h=ticker.price_change_4h,
        relative_volume=ticker.relative_volume,
        conditional_volume_z=ticker.conditional_log_rvol_z_score,
        price_surprise=ticker.price_surprise,
        residual_momentum=ticker.residual_momentum_score,
        signal_score=signal.signal_score if signal else entry.last_signal_score,
        signal_type=entry.last_signal_type,
        structure_level=entry.structure_level,
        structure_direction=entry.structure_direction,
        material_change=entry.material_change,
        change_reasons=entry.change_reasons,
        evidence=_build_evidence(ticker, execution, market_regime, structure),
    )


def _build_evidence(
    ticker: TickerData,
    execution: ExecutionDecision | None,
    market_regime: MarketRegimeSnapshot | None,
    structure: _StructureSnapshot,
) -> List[AttentionEvidence]:
    z_score = ticker.conditional_log_rvol_z_score
    relative_volume = ticker.relative_volume
    activity_support = (z_score or 0.0) >= config.rvol_z_score_minimum(
        ticker.liquidity_tier
    ) or (relative_volume or 0.0) >= 2.0
    activity = AttentionEvidence(
        family=EvidenceFamily.ACTIVITY,
        verdict=EvidenceVerdict.SUPPORTING if activity_support else EvidenceVerdict.MIXED,
        summary=(
            f"RVOL {_fmt(relative_volume, 'x')} · 동일시간 Z {_fmt(z_score)} · "
            f"직전24h 10분봉 중간 거래대금 {_fmt_krw(ticker.rolling_turnover)}"
        ),
        metrics={
            "relative_volume": relative_volume,
            "conditional_volume_z": z_score,
            "cross_sectional_volume_z": ticker.cross_sectional_log_rvol_z_score,
            "median_prior_10m_turnover_krw": ticker.rolling_turnover,
        },
    )

    price_support = structure.confirmed or (
        ticker.price_surprise is not None
        and ticker.price_surprise >= config.price_surprise_minimum(ticker.liquidity_tier)
    )
    structure_text = "구조 n/a"
    if structure.level is not None and structure.distance_pct is not None:
        level_name = (
            "저항"
            if structure.direction is StructureDirection.BULLISH
            else "지지"
        )
        label = (
            f"{level_name} 확인"
            if structure.confirmed
            else f"{level_name}까지"
        )
        if structure.failed:
            label = f"{level_name} 실패"
        structure_text = f"{label} {structure.distance_pct:+.2f}%"
    price = AttentionEvidence(
        family=EvidenceFamily.PRICE_STRUCTURE,
        verdict=(
            EvidenceVerdict.RISK
            if structure.failed
            else EvidenceVerdict.SUPPORTING
            if price_support
            else EvidenceVerdict.MIXED
        ),
        summary=(
            f"10분 {_fmt_pct(ticker.price_change_10m)} · 1시간 {_fmt_pct(ticker.price_change_1h)} · "
            f"가격 surprise {_fmt(ticker.price_surprise)} · {structure_text}"
        ),
        metrics={
            "price_change_10m_pct": ticker.price_change_10m,
            "price_change_1h_pct": ticker.price_change_1h,
            "price_change_4h_pct": ticker.price_change_4h,
            "price_surprise": ticker.price_surprise,
            "structure_distance_pct": structure.distance_pct,
        },
    )

    direction_up = (ticker.price_change_10m or 0.0) >= 0
    trend_aligned = (
        ticker.trend_1h_stable is TrendState.UP
        if direction_up
        else ticker.trend_1h_stable is TrendState.DOWN
    )
    daily_aligned = (
        ticker.is_above_ma50_daily is True
        if direction_up
        else ticker.is_above_ma50_daily is False
    )
    trend_contradicts = (
        ticker.trend_1h_stable is TrendState.DOWN
        if direction_up
        else ticker.trend_1h_stable is TrendState.UP
    )
    daily_contradicts = (
        ticker.is_above_ma50_daily is False
        if direction_up
        else ticker.is_above_ma50_daily is True
    )
    context_available = bool(ticker.hourly_candles and ticker.daily_candles)
    context_support = trend_aligned or daily_aligned or abs(ticker.residual_momentum_score or 0) >= 2
    regime = market_regime.regime.value if market_regime else "UNKNOWN"
    context = AttentionEvidence(
        family=EvidenceFamily.CONTEXT,
        verdict=(
            EvidenceVerdict.UNAVAILABLE
            if not context_available
            else EvidenceVerdict.RISK
            if trend_contradicts and daily_contradicts
            else EvidenceVerdict.SUPPORTING
            if context_support
            else EvidenceVerdict.MIXED
        ),
        summary=(
            f"1시간 추세 {ticker.trend_1h_stable.value} · 일봉 MA50 {ticker.is_above_ma50_daily} · "
            f"시장잔차 {_fmt(ticker.residual_momentum_score)} · 체제 {regime}"
        ),
        metrics={
            "trend_1h": ticker.trend_1h_stable.value,
            "above_ma50_daily": ticker.is_above_ma50_daily,
            "above_ma200_daily": ticker.is_above_ma200_daily,
            "residual_momentum": ticker.residual_momentum_score,
            "market_regime": regime,
        },
    )

    if execution is None:
        execution_evidence = AttentionEvidence(
            family=EvidenceFamily.EXECUTION,
            verdict=EvidenceVerdict.UNAVAILABLE,
            summary="호가 실행성 n/a",
        )
    else:
        reasons = ", ".join(reason.value for reason in execution.rejection_reasons)
        execution_evidence = AttentionEvidence(
            family=EvidenceFamily.EXECUTION,
            verdict=(
                EvidenceVerdict.SUPPORTING
                if execution.executable
                else EvidenceVerdict.RISK
            ),
            summary=(
                f"호가 스프레드 {_fmt(execution.spread_bps, 'bps')} · "
                f"예상 슬리피지 {_fmt(execution.expected_slippage_bps, 'bps')}"
                + (f" · 위험코드 {reasons}" if reasons else "")
            ),
            metrics={
                "executable": execution.executable,
                "spread_bps": execution.spread_bps,
                "expected_slippage_bps": execution.expected_slippage_bps,
                "risk_codes": [reason.value for reason in execution.rejection_reasons],
            },
        )
    return [activity, price, context, execution_evidence]


def _classify_structure(
    ticker: TickerData,
    previous: AttentionStateEntry | None,
    alert: Alert | None,
) -> _StructureSnapshot:
    if not ticker.candle_history:
        return _StructureSnapshot()
    current_price = ticker.candle_history[-1].close_price

    if (
        previous
        and previous.structure_level
        and previous.structure_direction
        and previous.last_signal_type is not None
        and not previous.last_signal_type.is_failure
    ):
        level = previous.structure_level
        direction = previous.structure_direction
        distance = (current_price / level - 1.0) * 100
        if direction is StructureDirection.BULLISH:
            if current_price <= level:
                return _StructureSnapshot(
                    SignalType.BULL_MOMENTUM_FAILED,
                    level,
                    direction,
                    distance,
                    failed=True,
                )
            return _StructureSnapshot(
                alert.signal_type if alert else SignalType.BREAKOUT_START,
                level,
                direction,
                distance,
                confirmed=True,
            )
        if current_price >= level:
            return _StructureSnapshot(
                SignalType.BEAR_MOMENTUM_FAILED,
                level,
                direction,
                distance,
                failed=True,
            )
        return _StructureSnapshot(
            alert.signal_type if alert else SignalType.BREAKDOWN_START,
            level,
            direction,
            distance,
            confirmed=True,
        )

    lookback = config.BREAKOUT_STRUCTURE_LOOKBACK_BARS
    if len(ticker.candle_history) < lookback + 1:
        return _StructureSnapshot()
    previous_candles = ticker.candle_history[-lookback - 1 : -1]
    resistance = max(candle.high_price for candle in previous_candles)
    support = min(candle.low_price for candle in previous_candles)
    if current_price > resistance:
        return _StructureSnapshot(
            SignalType.BREAKOUT_START,
            resistance,
            StructureDirection.BULLISH,
            (current_price / resistance - 1.0) * 100,
            confirmed=True,
        )
    if current_price < support:
        return _StructureSnapshot(
            SignalType.BREAKDOWN_START,
            support,
            StructureDirection.BEARISH,
            (current_price / support - 1.0) * 100,
            confirmed=True,
        )
    resistance_distance = (current_price / resistance - 1.0) * 100
    support_distance = (current_price / support - 1.0) * 100
    if abs(resistance_distance) <= abs(support_distance):
        return _StructureSnapshot(
            level=resistance,
            direction=StructureDirection.BULLISH,
            distance_pct=resistance_distance,
        )
    return _StructureSnapshot(
        level=support,
        direction=StructureDirection.BEARISH,
        distance_pct=support_distance,
    )


def _active_stage(
    structure: _StructureSnapshot, consecutive_observations: int
) -> AttentionStage:
    if structure.failed:
        return AttentionStage.FAILED
    if structure.confirmed:
        return AttentionStage.CONFIRMED
    if consecutive_observations >= 2:
        return AttentionStage.BUILDING
    return AttentionStage.DISCOVERED


def _material_changes(
    previous: AttentionStateEntry | None,
    *,
    stage: AttentionStage,
    market_rank: int | None,
    current_price: float,
    relative_volume: float | None,
    signal_type: SignalType | None,
) -> tuple[bool, List[str]]:
    if previous is None:
        return True, ["new_episode"]
    reasons = []
    if previous.stage is not stage:
        reasons.append(f"stage:{previous.stage.value}->{stage.value}")
    if previous.last_rank and market_rank:
        rank_change = previous.last_rank - market_rank
        if abs(rank_change) >= config.ATTENTION_RANK_CHANGE_MINIMUM:
            reasons.append(f"rank:{rank_change:+d}")
    if previous.last_price > 0:
        price_change = (current_price / previous.last_price - 1.0) * 100
        if abs(price_change) >= config.ATTENTION_PRICE_CHANGE_MINIMUM_PCT:
            reasons.append(f"price:{price_change:+.2f}%")
    if previous.last_relative_volume and relative_volume:
        ratio = max(
            relative_volume / previous.last_relative_volume,
            previous.last_relative_volume / relative_volume,
        )
        if ratio >= config.ATTENTION_RVOL_RATIO_CHANGE_MINIMUM:
            reasons.append(f"rvol:{relative_volume:.2f}x")
    if signal_type is not previous.last_signal_type:
        reasons.append(
            f"signal:{signal_type.value if signal_type else 'none'}"
        )
    return bool(reasons), reasons


def _is_continuous(
    previous: AttentionStateEntry, observed_at: datetime.datetime
) -> bool:
    interval = datetime.timedelta(minutes=config.PRIMARY_EXECUTION_TIMEFRAME_MINUTES)
    elapsed = observed_at - previous.last_seen_at
    return datetime.timedelta(0) <= elapsed <= interval * 1.5


def _episode_id(market: str, observed_at: datetime.datetime) -> str:
    identity = f"{market}\0{observed_at.isoformat()}"
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]


def _fmt(value: float | None, suffix: str = "") -> str:
    return "n/a" if value is None else f"{value:.2f}{suffix}"


def _fmt_pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.2f}%"


def _fmt_krw(value: float | None) -> str:
    if value is None:
        return "n/a"
    if value >= 100_000_000:
        return f"{value / 100_000_000:.1f}억원"
    if value >= 10_000:
        return f"{value / 10_000:.1f}만원"
    return f"{value:.0f}원"

"""Validated domain models for market evidence and durable state."""

import datetime
import math
from enum import StrEnum
from typing import Annotated, Any

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    RootModel,
    StrictBool,
    field_validator,
    model_validator,
)


def _validate_krw_market(market: str) -> str:
    if not market.startswith("KRW-"):
        raise ValueError("market identifier must use the KRW- prefix")
    return market


KrwMarket = Annotated[
    str,
    Field(min_length=5),
    AfterValidator(_validate_krw_market),
]
UNTAGGED_SECTOR_CATEGORY = "Untagged"
LEGACY_SECTOR_PLACEHOLDER_CATEGORIES = frozenset(
    {
        UNTAGGED_SECTOR_CATEGORY,
        "API_Error",
        "CG_Not_Found",
        "CG_Symbol_Ambiguous",
        "Identity_Mismatch",
        "Invalid_Category",
        "Lookup_Failed",
        "No_Category",
        "Override_Invalid",
    }
)


def canonicalize_sector_categories(categories: list[Any]) -> list[Any]:
    canonical = [
        category
        for category in categories
        if not isinstance(category, str)
        or category not in LEGACY_SECTOR_PLACEHOLDER_CATEGORIES
    ]
    if len(canonical) == len(categories):
        return list(categories)
    return canonical or [UNTAGGED_SECTOR_CATEGORY]


class TrendState(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    NEUTRAL = "NEUTRAL"


class LiquidityTier(StrEnum):
    UNKNOWN = "UNKNOWN"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


class StructureDirection(StrEnum):
    BULLISH = "bullish"
    BEARISH = "bearish"


class SignalType(StrEnum):
    BREAKOUT_START = "BREAKOUT_START"
    BREAKDOWN_START = "BREAKDOWN_START"
    MOMENTUM_ACCELERATION = "MOMENTUM_ACCELERATION"
    DOWNTREND_ACCELERATION = "DOWNTREND_ACCELERATION"
    BULL_MOMENTUM_FAILED = "BULL_MOMENTUM_FAILED"
    BEAR_MOMENTUM_FAILED = "BEAR_MOMENTUM_FAILED"

    @property
    def starts_structure(self) -> bool:
        return self in {self.BREAKOUT_START, self.BREAKDOWN_START}

    @property
    def is_failure(self) -> bool:
        return self in {self.BULL_MOMENTUM_FAILED, self.BEAR_MOMENTUM_FAILED}

    @property
    def updates_existing_structure(self) -> bool:
        return self in {
            self.MOMENTUM_ACCELERATION,
            self.DOWNTREND_ACCELERATION,
            self.BULL_MOMENTUM_FAILED,
            self.BEAR_MOMENTUM_FAILED,
        }

    @property
    def structure_direction(self) -> StructureDirection:
        if self in {
            self.BREAKOUT_START,
            self.MOMENTUM_ACCELERATION,
            self.BULL_MOMENTUM_FAILED,
        }:
            return StructureDirection.BULLISH
        if self in {
            self.BREAKDOWN_START,
            self.DOWNTREND_ACCELERATION,
            self.BEAR_MOMENTUM_FAILED,
        }:
            return StructureDirection.BEARISH
        raise AssertionError(f"unmapped signal type: {self.value}")


class RejectionCode(StrEnum):
    PRICE_SURPRISE_UNAVAILABLE = "price_surprise_unavailable"
    CONDITIONAL_VOLUME_HISTORY_UNAVAILABLE = "conditional_volume_history_unavailable"
    SUSPECTED_WASH_TRADING = "suspected_wash_trading"
    VOLUME_ANOMALY_BELOW_THRESHOLD = "volume_anomaly_below_threshold"
    PRICE_SURPRISE_BELOW_THRESHOLD = "price_surprise_below_threshold"
    VOLUME_AND_PRICE_SURPRISE_BELOW_THRESHOLD = (
        "volume_and_price_surprise_below_threshold"
    )
    MARKET_WARNING = "market_warning"
    DAILY_TURNOVER_BELOW_MINIMUM = "daily_turnover_below_minimum"
    ORDERBOOK_UNAVAILABLE = "orderbook_unavailable"
    ORDERBOOK_INVALID = "orderbook_invalid"
    ORDERBOOK_DEPTH_BELOW_NOTIONAL = "orderbook_depth_below_notional"
    SPREAD_ABOVE_MAXIMUM = "spread_above_maximum"
    SLIPPAGE_ABOVE_MAXIMUM = "slippage_above_maximum"
    MOVE_DOES_NOT_COVER_ESTIMATED_COSTS = "move_does_not_cover_estimated_costs"
    MARKET_REGIME_UNKNOWN = "market_regime_unknown"
    COMPLETE_CANDLE_HISTORY_UNAVAILABLE = "complete_candle_history_unavailable"
    HIGHER_TIMEFRAME_CANDLE_HISTORY_UNAVAILABLE = (
        "higher_timeframe_candle_history_unavailable"
    )
    MARKET_UNIVERSE_EMPTY = "market_universe_empty"
    CANDLE_COVERAGE_BELOW_MINIMUM = "candle_coverage_below_minimum"
    BTC_CANDLE_HISTORY_UNAVAILABLE = "btc_candle_history_unavailable"
    IMMUTABLE_SCAN_EVENT_CONFLICT = "immutable_scan_event_conflict"


class ScanDecision(StrEnum):
    REJECTED_LIGHTWEIGHT = "rejected_lightweight"
    EXECUTION_BLOCKED = "execution_blocked"
    MARKET_REGIME_BLOCKED = "market_regime_blocked"
    DATA_QUALITY_BLOCKED = "data_quality_blocked"
    DEEP_DIVE_DATA_BLOCKED = "deep_dive_data_blocked"
    CANDIDATE_NOT_ALERTED = "candidate_not_alerted"
    ATTENTION_QUEUED = "attention_queued"
    ALERT_SELECTED = "alert_selected"


class AttentionStage(StrEnum):
    DISCOVERED = "discovered"
    BUILDING = "building"
    CONFIRMED = "confirmed"
    COOLING = "cooling"
    FAILED = "failed"


class AttentionLane(StrEnum):
    FOCUS = "focus_now"
    EARLY = "early_watch"
    ONGOING = "ongoing"
    COOLING_FAILED = "cooling_failed"
    DATA_LIMITED = "data_limited"


class EvidenceFamily(StrEnum):
    ACTIVITY = "activity"
    PRICE_STRUCTURE = "price_structure"
    CONTEXT = "context"
    EXECUTION = "execution"


class EvidenceVerdict(StrEnum):
    SUPPORTING = "supporting"
    MIXED = "mixed"
    RISK = "risk"
    UNAVAILABLE = "unavailable"


class Direction(StrEnum):
    LONG = "long"
    SHORT = "short"


class MarketRegime(StrEnum):
    UNKNOWN = "UNKNOWN"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"
    TRENDING_BULL = "TRENDING_BULL"
    TRENDING_BEAR = "TRENDING_BEAR"
    MEAN_REVERSION = "MEAN_REVERSION"


class NotificationKind(StrEnum):
    BRIEFING = "briefing"
    ALERT = "alert"
    DATA_QUALITY = "data_quality"


class NotificationStatus(StrEnum):
    PREPARED = "prepared"
    ATTEMPTING = "attempting"
    DELIVERED = "delivered"


class DispatchOutcome(StrEnum):
    SENT = "sent"
    SKIPPED = "skipped"
    QUEUED = "queued"
    FAILED = "failed"
    UNCERTAIN = "uncertain"


class DispatchCode(StrEnum):
    EMPTY_MESSAGE = "empty_message"
    WEBHOOK_NOT_CONFIGURED = "webhook_not_configured"
    ALREADY_QUEUED = "already_queued"
    SCAN_ALREADY_OWNED = "scan_already_owned"
    ACTIVE_OUTBOX_ALREADY_OWNED = "active_outbox_already_owned"
    QUEUED_BEHIND_PENDING = "queued_behind_pending"
    PENDING_CANCELED = "pending_canceled"
    PRIOR_DELIVERY_FINALIZED = "prior_delivery_finalized"
    HTTP_ERROR = "http_error"
    CONNECTION_ERROR = "connection_error"
    UNEXPECTED_ERROR = "unexpected_error"


class DeliveryState(StrEnum):
    NOT_CONFIRMED = "not_confirmed"
    CONFIRMED = "confirmed"
    UNCERTAIN = "uncertain"


class ScanHandoffState(StrEnum):
    NOT_DURABLE = "not_durable"
    DURABLE = "durable"
    UNCERTAIN = "uncertain"


class NotificationErrorCode(StrEnum):
    BACKLOG_CAPACITY_EXCEEDED = "backlog_capacity_exceeded"
    BACKLOG_WRITE_UNVERIFIED = "backlog_write_unverified"
    BACKLOG_WRITE_COMMITTED_WITHOUT_ACK = "backlog_write_committed_without_ack"
    BACKLOG_WRITE_NOT_PERSISTED = "backlog_write_not_persisted"
    QUEUED_HANDOFF_FINALIZATION_FAILED = "queued_handoff_finalization_failed"
    OUTBOX_WRITE_UNVERIFIED = "outbox_write_unverified"
    OUTBOX_WRITE_COMMITTED_WITHOUT_ACK = "outbox_write_committed_without_ack"
    OUTBOX_WRITE_NOT_PERSISTED = "outbox_write_not_persisted"
    PREPARED_ADVANCE_FAILED = "prepared_advance_failed"
    DELIVERY_FINALIZATION_FAILED = "delivery_finalization_failed"
    UNCERTAIN_DELIVERY_SCAN_COMPLETION_FAILED = (
        "uncertain_delivery_scan_completion_failed"
    )
    DELIVERY_OUTCOME_UNCERTAIN = "delivery_outcome_uncertain"
    RETRY_STATE_RESTORE_FAILED = "retry_state_restore_failed"
    DELIVERY_FAILED = "delivery_failed"
    PENDING_SCAN_HANDOFF_FAILED = "pending_scan_handoff_failed"
    PENDING_CANCELLATION_FAILED = "pending_cancellation_failed"
    AMBIGUOUS_ATTEMPT_REQUIRES_RECONCILIATION = (
        "ambiguous_attempt_requires_reconciliation"
    )
    CONFIRMED_DELIVERY_FINALIZATION_FAILED = "confirmed_delivery_finalization_failed"
    PENDING_ADVANCE_FAILED = "pending_advance_failed"


class ScanClaimStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"


class SectorTagStatus(StrEnum):
    TAGGED = "tagged"
    NO_CATEGORY = "no_category"
    SYMBOL_NOT_FOUND = "symbol_not_found"
    SYMBOL_AMBIGUOUS = "symbol_ambiguous"
    OVERRIDE_INVALID = "override_invalid"
    IDENTITY_MISMATCH = "identity_mismatch"
    LOOKUP_NOT_FOUND = "lookup_not_found"
    LOOKUP_FAILED = "lookup_failed"
    INVALID_CATEGORY = "invalid_category"


class CandidateDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    eligible: bool
    rejection_reasons: list[RejectionCode] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_decision(self) -> "CandidateDecision":
        if self.eligible and self.rejection_reasons:
            raise ValueError("eligible candidates cannot have rejection reasons")
        if not self.eligible and not self.rejection_reasons:
            raise ValueError(
                "rejected candidates require at least one rejection reason"
            )
        return self


class DataQualityIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    code: RejectionCode
    message: str
    details: dict[str, Any] = Field(default_factory=dict)


class MarketRegimeSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    regime: MarketRegime
    rsi: float | None = None
    atr_ratio: float | None = None


class SectorTagResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    market: KrwMarket
    status: SectorTagStatus
    categories: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_categories(self) -> "SectorTagResult":
        if self.status is SectorTagStatus.TAGGED and not self.categories:
            raise ValueError("tagged sector results require categories")
        if self.status is not SectorTagStatus.TAGGED and self.categories:
            raise ValueError("only tagged sector results may contain categories")
        if any(not category.strip() for category in self.categories):
            raise ValueError("sector categories must be non-empty strings")
        return self


class SectorTagBatch(RootModel[list[SectorTagResult]]):
    @model_validator(mode="after")
    def validate_markets(self) -> "SectorTagBatch":
        if not self.root:
            raise ValueError("sector tag results must not be empty")
        markets = [result.market for result in self.root]
        if len(markets) != len(set(markets)):
            raise ValueError("sector tag result markets must be unique")
        return self


class SectorMap(RootModel[dict[KrwMarket, list[str]]]):
    @field_validator("root", mode="before")
    @classmethod
    def migrate_legacy_placeholders(cls, assignments: Any) -> Any:
        if not isinstance(assignments, dict):
            return assignments
        return {
            market: canonicalize_sector_categories(categories)
            if isinstance(categories, list)
            else categories
            for market, categories in assignments.items()
        }

    @model_validator(mode="after")
    def validate_assignments(self) -> "SectorMap":
        if not self.root:
            raise ValueError("sector map must not be empty")
        for categories in self.root.values():
            if not categories or any(not category.strip() for category in categories):
                raise ValueError("sector map contains an invalid assignment")
        return self


def _positive_finite_float(value: Any) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        or float(value) <= 0
    ):
        raise ValueError("value must be a positive finite number")
    return float(value)


def _nonnegative_finite_float(value: Any) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        or float(value) < 0
    ):
        raise ValueError("value must be a nonnegative finite number")
    return float(value)


PositiveFiniteFloat = Annotated[float, BeforeValidator(_positive_finite_float)]
NonNegativeFiniteFloat = Annotated[float, BeforeValidator(_nonnegative_finite_float)]


class MarketEvent(BaseModel):
    """Current Upbit warning metadata returned by the detailed market endpoint."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    warning: StrictBool
    caution: dict[str, StrictBool]

    @property
    def blocks_execution(self) -> bool:
        return self.warning or any(self.caution.values())


class MarketListing(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    market: KrwMarket
    english_name: str | None = None
    market_event: MarketEvent | None = None


class MarketTicker(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    market: KrwMarket
    acc_trade_price_24h: NonNegativeFiniteFloat
    trade_price: PositiveFiniteFloat | None = None
    market_event: MarketEvent


class OrderBookUnit(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    bid_price: PositiveFiniteFloat
    bid_size: PositiveFiniteFloat
    ask_price: PositiveFiniteFloat
    ask_size: PositiveFiniteFloat


class OrderBookSnapshot(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    market: KrwMarket
    orderbook_units: list[OrderBookUnit] = Field(min_length=1)


class CandleData(BaseModel):
    """One completed market candle on a defined time grid."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: KrwMarket
    timestamp: datetime.datetime
    open_price: PositiveFiniteFloat
    high_price: PositiveFiniteFloat
    low_price: PositiveFiniteFloat
    close_price: PositiveFiniteFloat
    volume: NonNegativeFiniteFloat
    trade_value: NonNegativeFiniteFloat | None = None

    @model_validator(mode="after")
    def validate_ohlc(self) -> "CandleData":
        if self.low_price > min(self.open_price, self.close_price):
            raise ValueError("low_price cannot exceed the candle body")
        if self.high_price < max(self.open_price, self.close_price):
            raise ValueError("high_price cannot be below the candle body")
        return self


class TickerData(BaseModel):
    """Point-in-time features and higher-timeframe context for one market."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: KrwMarket
    candle_history: list[CandleData] = Field(default_factory=list)

    # Broad-scan features derived from completed 10-minute candles.
    price_change_10m: float | None = None
    relative_volume: float | None = None
    rvol_z_score: float | None = None
    conditional_log_rvol_z_score: float | None = None
    cross_sectional_log_rvol_z_score: float | None = None
    price_change_1h: float | None = None
    price_change_4h: float | None = None
    price_surprise: float | None = None
    rolling_turnover: float | None = None
    liquidity_tier: LiquidityTier = LiquidityTier.UNKNOWN
    execution_spread_bps: float | None = None
    expected_slippage_bps: float | None = None

    # Higher-timeframe evidence populated only for selected markets.
    decoupling_score: float | None = None
    residual_momentum_score: float | None = None
    hourly_candles: list[CandleData] = Field(default_factory=list)
    daily_candles: list[CandleData] = Field(default_factory=list)

    trend_1h_stable: TrendState = TrendState.NEUTRAL
    is_above_ma50_daily: bool | None = None
    is_above_ma200_daily: bool | None = None


class SignalCandidate(BaseModel):
    """A scored signal candidate emitted by deterministic analysis."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    market: KrwMarket
    signal_score: float
    price_change: float
    rvol: float
    rvol_z_score: float
    current_price: float


class AttentionEvidence(BaseModel):
    """One non-overlapping evidence family shown for an attention candidate."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    family: EvidenceFamily
    verdict: EvidenceVerdict
    summary: str
    metrics: dict[str, Any] = Field(default_factory=dict)


class AttentionStateEntry(BaseModel):
    """Durable progression state for one market attention episode."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    market: KrwMarket
    episode_id: str = Field(min_length=1)
    first_seen_at: AwareDatetime
    last_seen_at: AwareDatetime
    consecutive_observations: int = Field(ge=1)
    stage: AttentionStage
    last_rank: int | None = Field(default=None, ge=1)
    last_price: PositiveFiniteFloat
    last_relative_volume: NonNegativeFiniteFloat | None = None
    last_signal_score: float | None = None
    last_signal_type: SignalType | None = None
    structure_level: PositiveFiniteFloat | None = None
    structure_direction: StructureDirection | None = None
    cooling_observations: int = Field(default=0, ge=0)
    focus_observations: int = Field(default=0, ge=0)
    confirmed_transition_seen: bool = False
    primary_exposure_times: list[AwareDatetime] = Field(default_factory=list)
    # None marks legacy state whose prior visible-model provenance is unknown.
    ridge_base_exposure_times: list[AwareDatetime] | None = None
    last_lane: AttentionLane | None = None
    material_change: bool = False
    change_reasons: list[str] = Field(default_factory=list)


class AttentionState(BaseModel):
    """Latest bounded attention state, independent from webhook delivery state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    updated_at: AwareDatetime | None = None
    entries: dict[KrwMarket, AttentionStateEntry] = Field(default_factory=dict)


class AttentionCandidate(BaseModel):
    """Ranked item that tells a human which chart deserves attention and why."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    market: KrwMarket
    attention_rank: int = Field(ge=1)
    lane: AttentionLane
    lane_rank: int | None = Field(default=None, ge=1)
    primary_selected: bool = False
    displayed: bool = False
    display_rank: int | None = Field(default=None, ge=1)
    quality_score: float = Field(default=0.0, ge=0.0, le=1.0)
    ranking_score: float = 0.0
    max_similarity: float = Field(default=0.0, ge=0.0, le=1.0)
    primary_exposures_60m: int = Field(default=0, ge=0)
    ridge_base_quality_score: float = Field(default=0.0, ge=0.0, le=1.0)
    ridge_base_exposures_60m: int = Field(default=0, ge=0)
    ridge_base_rank: int | None = Field(default=None, ge=1)
    ridge_base_ranking_score: float | None = None
    v3_shadow_rank: int | None = Field(default=None, ge=1)
    v4_shadow_rank: int | None = Field(default=None, ge=1)
    ridge_rank: int | None = Field(default=None, ge=1)
    ridge_score: float | None = None
    score_version: str = Field(min_length=1)
    context_available: bool = False
    market_rank: int | None = Field(default=None, ge=1)
    market_rank_delta: int | None = None
    stage: AttentionStage
    episode_id: str = Field(min_length=1)
    first_seen_at: AwareDatetime
    observed_at: AwareDatetime
    consecutive_observations: int = Field(ge=1)
    current_price: PositiveFiniteFloat
    price_change_10m: float | None = None
    price_change_1h: float | None = None
    price_change_4h: float | None = None
    relative_volume: NonNegativeFiniteFloat | None = None
    conditional_volume_z: float | None = None
    price_surprise: float | None = None
    residual_momentum: float | None = None
    signal_score: float | None = None
    signal_type: SignalType | None = None
    structure_level: PositiveFiniteFloat | None = None
    structure_direction: StructureDirection | None = None
    material_change: bool
    change_reasons: list[str] = Field(default_factory=list)
    evidence: list[AttentionEvidence] = Field(default_factory=list)


class Alert(BaseModel):
    """A classified alert transition with its supporting ticker evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate: SignalCandidate
    ticker_data: TickerData
    signal_type: SignalType
    priority: int
    structure_level: float | None = None


class RankState(BaseModel):
    """One durable turnover-ranking snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    last_updated: AwareDatetime
    rankings: dict[KrwMarket, int] = Field(default_factory=dict)


class AnalysisState(BaseModel):
    """A bounded analysis snapshot for operational audit logs."""

    model_config = ConfigDict(extra="forbid")

    last_updated: datetime.datetime
    tickers: dict[KrwMarket, TickerData]
    rankings: dict[KrwMarket, int] = Field(default_factory=dict)


class ScanEvent(BaseModel):
    """Immutable pre-decision record for one market in one scan."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str
    observed_at: datetime.datetime
    market: KrwMarket
    feature_snapshot: dict[str, Any]
    candidate_eligible: bool
    rejection_reasons: list[RejectionCode]
    final_decision: ScanDecision
    model_version: str
    direction: Direction | None = None
    signal_score: float | None = None
    signal_candle_start: datetime.datetime | None = None
    attention_stage: AttentionStage | None = None
    attention_lane: AttentionLane | None = None
    attention_rank: int | None = Field(default=None, ge=1)
    attention_lane_rank: int | None = Field(default=None, ge=1)
    attention_primary_selected: bool | None = None
    attention_displayed: bool | None = None
    attention_display_rank: int | None = Field(default=None, ge=1)
    attention_quality_score: float | None = Field(default=None, ge=0.0, le=1.0)
    attention_ranking_score: float | None = None
    attention_v3_shadow_rank: int | None = Field(default=None, ge=1)
    attention_v4_shadow_rank: int | None = Field(default=None, ge=1)
    attention_ridge_rank: int | None = Field(default=None, ge=1)
    attention_ridge_score: float | None = None
    attention_ridge_base_rank: int | None = Field(default=None, ge=1)
    attention_ridge_base_ranking_score: float | None = None
    attention_ridge_base_quality_score: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    attention_ridge_base_exposures_60m: int | None = Field(default=None, ge=0)
    attention_primary_exposures_60m: int | None = Field(default=None, ge=0)
    attention_score_version: str | None = None
    attention_first_seen_at: AwareDatetime | None = None
    attention_episode_id: str | None = None


class ScanOutcome(BaseModel):
    """Post-hoc evaluation record joined to an immutable scan event by event_id."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str
    market: KrwMarket
    entry_candle_start: datetime.datetime
    exit_candle_start: datetime.datetime
    entry_price: float
    exit_price: float
    directional_net_return: float
    mfe: float
    mae: float


class AlertHistory(BaseModel):
    """Durable alert structure and cooldown state for one market."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: KrwMarket
    last_alert_timestamp: datetime.datetime
    last_signal_type: SignalType
    last_price: float
    last_rvol: float
    initial_timestamp: datetime.datetime
    initial_price: float
    structure_level: float | None = None
    structure_direction: StructureDirection | None = None


class NotificationOutbox(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    delivery_id: str = Field(min_length=1)
    status: NotificationStatus
    message: str = Field(min_length=1)
    alert_history: dict[KrwMarket, AlertHistory] | None = None
    previous_alert_history: dict[KrwMarket, AlertHistory] | None = None
    alert_markets: list[KrwMarket] = Field(default_factory=list)
    scan_key: str | None = None
    kind: NotificationKind = NotificationKind.BRIEFING
    mention_channel: bool = False

    @field_validator("alert_markets")
    @classmethod
    def unique_alert_markets(cls, markets: list[str]) -> list[str]:
        if any(not market for market in markets) or len(markets) != len(set(markets)):
            raise ValueError(
                "alert_markets must contain unique non-empty market identifiers"
            )
        return markets


class NotificationBacklog(RootModel[list[NotificationOutbox]]):
    @model_validator(mode="after")
    def validate_deferred_records(self) -> "NotificationBacklog":
        if any(item.status is not NotificationStatus.PREPARED for item in self.root):
            raise ValueError("notification backlog records must be prepared")
        delivery_ids = [item.delivery_id for item in self.root]
        if len(delivery_ids) != len(set(delivery_ids)):
            raise ValueError("notification backlog delivery ids must be unique")
        scan_keys = [item.scan_key for item in self.root if item.scan_key]
        if len(scan_keys) != len(set(scan_keys)):
            raise ValueError("notification backlog scan keys must be unique")
        return self


class ScanClaim(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    scan_key: str = Field(min_length=1)
    execution_id: str | None = Field(default=None, min_length=1)
    claimed_at: AwareDatetime | None = None
    status: ScanClaimStatus
    completed_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> "ScanClaim":
        if self.status is ScanClaimStatus.IN_PROGRESS and (
            self.claimed_at is None or self.completed_at is not None
        ):
            raise ValueError(
                "in-progress claims require claimed_at and forbid completed_at"
            )
        return self


class ScanClaimState(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    claims: list[ScanClaim] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_scan_keys(self) -> "ScanClaimState":
        scan_keys = [claim.scan_key for claim in self.claims]
        if len(scan_keys) != len(set(scan_keys)):
            raise ValueError("scan claim keys must be unique")
        return self

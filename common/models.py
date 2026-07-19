#common/models

import datetime
import math
from enum import StrEnum
from typing import Annotated, Any, Dict, List, Optional

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


def canonicalize_sector_categories(categories: List[Any]) -> List[Any]:
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
    VOLUME_AND_PRICE_SURPRISE_BELOW_THRESHOLD = "volume_and_price_surprise_below_threshold"
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
    HIGHER_TIMEFRAME_CANDLE_HISTORY_UNAVAILABLE = "higher_timeframe_candle_history_unavailable"
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
    ALERT_SELECTED = "alert_selected"


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
    UNCERTAIN_DELIVERY_SCAN_COMPLETION_FAILED = "uncertain_delivery_scan_completion_failed"
    DELIVERY_OUTCOME_UNCERTAIN = "delivery_outcome_uncertain"
    RETRY_STATE_RESTORE_FAILED = "retry_state_restore_failed"
    DELIVERY_FAILED = "delivery_failed"
    PENDING_SCAN_HANDOFF_FAILED = "pending_scan_handoff_failed"
    PENDING_CANCELLATION_FAILED = "pending_cancellation_failed"
    AMBIGUOUS_ATTEMPT_REQUIRES_RECONCILIATION = "ambiguous_attempt_requires_reconciliation"
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
    rejection_reasons: List[RejectionCode] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_decision(self) -> "CandidateDecision":
        if self.eligible and self.rejection_reasons:
            raise ValueError("eligible candidates cannot have rejection reasons")
        if not self.eligible and not self.rejection_reasons:
            raise ValueError("rejected candidates require at least one rejection reason")
        return self


class DataQualityIssue(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    code: RejectionCode
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)


class MarketRegimeSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    regime: MarketRegime
    rsi: Optional[float] = None
    atr_ratio: Optional[float] = None


class SectorTagResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    market: KrwMarket
    status: SectorTagStatus
    categories: List[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_categories(self) -> "SectorTagResult":
        if self.status is SectorTagStatus.TAGGED and not self.categories:
            raise ValueError("tagged sector results require categories")
        if self.status is not SectorTagStatus.TAGGED and self.categories:
            raise ValueError("only tagged sector results may contain categories")
        if any(not category.strip() for category in self.categories):
            raise ValueError("sector categories must be non-empty strings")
        return self


class SectorTagBatch(RootModel[List[SectorTagResult]]):
    @model_validator(mode="after")
    def validate_markets(self) -> "SectorTagBatch":
        if not self.root:
            raise ValueError("sector tag results must not be empty")
        markets = [result.market for result in self.root]
        if len(markets) != len(set(markets)):
            raise ValueError("sector tag result markets must be unique")
        return self


class SectorMap(RootModel[Dict[KrwMarket, List[str]]]):
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
            if (
                not categories
                or any(not category.strip() for category in categories)
            ):
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
NonNegativeFiniteFloat = Annotated[
    float, BeforeValidator(_nonnegative_finite_float)
]


class MarketEvent(BaseModel):
    """Current Upbit warning metadata returned by the detailed market endpoint."""

    model_config = ConfigDict(extra="ignore", frozen=True)

    warning: StrictBool
    caution: Dict[str, StrictBool]

    @property
    def blocks_execution(self) -> bool:
        return self.warning or any(self.caution.values())


class MarketListing(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    market: KrwMarket
    english_name: Optional[str] = None
    market_event: Optional[MarketEvent] = None

class MarketTicker(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    market: KrwMarket
    acc_trade_price_24h: NonNegativeFiniteFloat
    trade_price: Optional[PositiveFiniteFloat] = None
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
    orderbook_units: List[OrderBookUnit] = Field(min_length=1)


class CandleData(BaseModel):
    """단일 캔들(분봉) 데이터를 나타냅니다."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: KrwMarket
    timestamp: datetime.datetime
    open_price: PositiveFiniteFloat
    high_price: PositiveFiniteFloat
    low_price: PositiveFiniteFloat
    close_price: PositiveFiniteFloat
    volume: NonNegativeFiniteFloat

    @model_validator(mode="after")
    def validate_ohlc(self) -> "CandleData":
        if self.low_price > min(self.open_price, self.close_price):
            raise ValueError("low_price cannot exceed the candle body")
        if self.high_price < max(self.open_price, self.close_price):
            raise ValueError("high_price cannot be below the candle body")
        return self


class TickerData(BaseModel):
    """모든 기술적 분석 지표가 포함된 단일 티커의 종합 데이터입니다."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: KrwMarket
    candle_history: List[CandleData] = Field(default_factory=list)
    
    # --- 기본 분석용 ---
    price_change_10m: Optional[float] = None
    relative_volume: Optional[float] = None
    rvol_z_score: Optional[float] = None
    conditional_log_rvol_z_score: Optional[float] = None
    cross_sectional_log_rvol_z_score: Optional[float] = None
    price_change_1h: Optional[float] = None
    price_change_4h: Optional[float] = None
    price_surprise: Optional[float] = None
    rolling_turnover: Optional[float] = None
    liquidity_tier: LiquidityTier = LiquidityTier.UNKNOWN
    execution_spread_bps: Optional[float] = None
    expected_slippage_bps: Optional[float] = None
    

    # --- 심층 분석용 필드  ---
    decoupling_score: Optional[float] = None
    residual_momentum_score: Optional[float] = None
    hourly_candles: List[CandleData] = Field(default_factory=list)
    daily_candles: List[CandleData] = Field(default_factory=list)
    
    trend_1h_stable: TrendState = TrendState.NEUTRAL
    is_above_ma50_daily: Optional[bool] = None
    is_above_ma200_daily: Optional[bool] = None

class SignalCandidate(BaseModel):
    """1차 분석을 통해 생성된 잠재적 시그널 후보입니다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    market: KrwMarket
    signal_score: float
    price_change: float
    rvol: float
    rvol_z_score: float
    current_price: float


class Alert(BaseModel):
    """최종 알림 대상으로 확정된 시그널 객체입니다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate: SignalCandidate
    ticker_data: TickerData  # 포매팅 시 참고할 상세 데이터
    signal_type: SignalType
    priority: int
    structure_level: Optional[float] = None


class RankState(BaseModel):
    """거래대금 순위 변동을 비교하기 위한 상태 모델입니다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    last_updated: AwareDatetime
    rankings: Dict[KrwMarket, int] = Field(default_factory=dict)


class AnalysisState(BaseModel):
    """분석 결과 로그를 저장하기 위한 상태 모델입니다."""

    model_config = ConfigDict(extra="forbid")

    last_updated: datetime.datetime
    tickers: Dict[KrwMarket, TickerData]
    rankings: Dict[KrwMarket, int] = Field(default_factory=dict)


class ScanEvent(BaseModel):
    """Immutable pre-decision record for one market in one scan."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str
    observed_at: datetime.datetime
    market: KrwMarket
    feature_snapshot: Dict[str, Any]
    candidate_eligible: bool
    rejection_reasons: List[RejectionCode]
    final_decision: ScanDecision
    model_version: str
    direction: Optional[Direction] = None
    signal_score: Optional[float] = None
    signal_candle_start: Optional[datetime.datetime] = None


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
    """알림 쿨다운을 관리하기 위한 발송 기록 모델입니다."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    market: KrwMarket
    last_alert_timestamp: datetime.datetime
    last_signal_type: SignalType
    last_price: float
    last_rvol: float
    initial_timestamp: datetime.datetime
    initial_price: float
    structure_level: Optional[float] = None
    structure_direction: Optional[StructureDirection] = None


class NotificationOutbox(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    delivery_id: str = Field(min_length=1)
    status: NotificationStatus
    message: str = Field(min_length=1)
    alert_history: Optional[Dict[KrwMarket, AlertHistory]] = None
    previous_alert_history: Optional[Dict[KrwMarket, AlertHistory]] = None
    alert_markets: List[KrwMarket] = Field(default_factory=list)
    scan_key: Optional[str] = None
    kind: NotificationKind = NotificationKind.BRIEFING
    mention_channel: bool = False

    @field_validator("alert_markets")
    @classmethod
    def unique_alert_markets(cls, markets: List[str]) -> List[str]:
        if any(not market for market in markets) or len(markets) != len(set(markets)):
            raise ValueError("alert_markets must contain unique non-empty market identifiers")
        return markets


class NotificationBacklog(RootModel[List[NotificationOutbox]]):
    @model_validator(mode="after")
    def validate_deferred_records(self) -> "NotificationBacklog":
        if any(
            item.status is not NotificationStatus.PREPARED for item in self.root
        ):
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
    execution_id: Optional[str] = Field(default=None, min_length=1)
    claimed_at: Optional[AwareDatetime] = None
    status: ScanClaimStatus
    completed_at: Optional[AwareDatetime] = None

    @model_validator(mode="after")
    def validate_lifecycle(self) -> "ScanClaim":
        if self.status is ScanClaimStatus.IN_PROGRESS:
            if self.claimed_at is None or self.completed_at is not None:
                raise ValueError("in-progress claims require claimed_at and forbid completed_at")
        return self


class ScanClaimState(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    claims: List[ScanClaim] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_scan_keys(self) -> "ScanClaimState":
        scan_keys = [claim.scan_key for claim in self.claims]
        if len(scan_keys) != len(set(scan_keys)):
            raise ValueError("scan claim keys must be unique")
        return self

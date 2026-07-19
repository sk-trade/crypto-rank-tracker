# config.py

import os
from enum import StrEnum

from common.models import LiquidityTier

# -- ENVIRONMENT & STORAGE CONFIGURATION --

# 저장 방식 선택 ('GCS' 또는 'LOCAL')
STATE_STORAGE_METHOD = os.environ.get("STATE_STORAGE_METHOD", "LOCAL")
SHADOW_MODE = os.environ.get("SHADOW_MODE", "").strip().lower() in {
    "1",
    "true",
    "yes",
}
# GCP storage Settings
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")

# local storage Settings
LOCAL_STATE_DIR = os.path.join(os.path.dirname(__file__), "state")

STATE_FILE_NAME = "upbit_market_state.json"
RANK_STATE_FILE_NAME = "rank_state.json"
SECTOR_MAP_FILE_NAME = "sectors.json"
ALERT_HISTORY_FILE_NAME = "alert_history.json"
SHADOW_ALERT_HISTORY_FILE_NAME = "shadow_alert_history.json"
ATTENTION_STATE_FILE_NAME = "attention_state.json"

# -- EXTERNAL API & WEBHOOK CONFIGURATION --

# Coingecko API Key
CG_API_KEY = os.environ.get("CG_API_KEY")

# Webhook
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")

# -- APPLICATION SETTINGS --

APP_LOGGER_NAME = "CryptoRankTracker"
LOG_LEVEL = "INFO"
STATE_HISTORY_COUNT = 12  # 순위 분석에 사용할 과거 데이터 수
CANDLE_SUCCESS_RATE_MINIMUM = 0.95


class StorageMethod(StrEnum):
    LOCAL = "LOCAL"
    GCS = "GCS"


class ConfigErrorCode(StrEnum):
    INVALID_STORAGE_METHOD = "invalid_storage_method"
    GCS_BUCKET_REQUIRED = "gcs_bucket_required"


class StorageConfigError(RuntimeError):
    def __init__(self, code: ConfigErrorCode, field: str):
        super().__init__(code.value)
        self.code = code
        self.field = field


def storage_method() -> StorageMethod:
    try:
        return StorageMethod(STATE_STORAGE_METHOD)
    except ValueError as error:
        raise StorageConfigError(
            ConfigErrorCode.INVALID_STORAGE_METHOD, "STATE_STORAGE_METHOD"
        ) from error


def validate_storage_config() -> StorageMethod:
    """Fail early when the selected state backend is missing required settings."""
    method = storage_method()
    if method is StorageMethod.GCS and not GCS_BUCKET_NAME:
        raise StorageConfigError(
            ConfigErrorCode.GCS_BUCKET_REQUIRED, "GCS_BUCKET_NAME"
        )
    return method


# -- ANALYSIS & ALERTING POLICY --

# [통계 보정 설정]
MAX_Z_SCORE_CAP = 10.0          # Z-Score 상한선 (통계 왜곡 방지)
MIN_MAD_FLOOR = 0.001           # Z-Score 분모 0 방지
CONDITIONAL_VOLUME_MIN_SAMPLES = 3
# Recent indicators need 154 contiguous clock bars; conditional volume uses
# separate same-slot observations so the broad scan stays within runtime limits.
RECENT_SCAN_HISTORY_BARS = 154
CONDITIONAL_VOLUME_LOOKBACK_WEEKS = 3
RESIDUAL_MOMENTUM_LOOKBACK_BARS = 144
RESIDUAL_MOMENTUM_MIN_OBSERVATIONS = 30

# [Wash Trading 필터]
# 거래량 제한
WASH_TRADING_MIN_PRICE_CHANGE = 0.5 

# [Pre-fitted candidate policy]
# These parameters are fixed for validation; revisions require the validation workflow.
# Every value uses only candles completed before the current decision candle.
PRICE_SURPRISE_LOOKBACK_BARS = 144
PRICE_SURPRISE_MIN_RETURN_OBSERVATIONS = 30
ROLLING_TURNOVER_LOOKBACK_BARS = 144
LIQUIDITY_TIER_QUANTILES = (0.33, 0.67)
PRICE_SURPRISE_MINIMUMS = {
    LiquidityTier.HIGH: 2.0,
    LiquidityTier.MEDIUM: 2.5,
    LiquidityTier.LOW: 3.0,
}
RVOL_Z_SCORE_MINIMUMS = {
    LiquidityTier.HIGH: 3.0,
    LiquidityTier.MEDIUM: 4.0,
    LiquidityTier.LOW: 5.0,
}


def price_surprise_minimum(liquidity_tier: LiquidityTier) -> float:
    """Return the fixed policy threshold for a pre-decision liquidity tier."""
    tier = (
        LiquidityTier.LOW
        if liquidity_tier is LiquidityTier.UNKNOWN
        else liquidity_tier
    )
    return PRICE_SURPRISE_MINIMUMS[tier]


def rvol_z_score_minimum(liquidity_tier: LiquidityTier) -> float:
    """Return the fixed policy threshold for a pre-decision liquidity tier."""
    tier = (
        LiquidityTier.LOW
        if liquidity_tier is LiquidityTier.UNKNOWN
        else liquidity_tier
    )
    return RVOL_Z_SCORE_MINIMUMS[tier]

# [1차 필터링 공통]
SIGNAL_SCORE_CANDIDATE_MINIMUM = 0.5
ROBUST_Z_SCORE_THRESHOLD = 3.0  
DECOUPLING_MIN_DEVIATION_PCT = 3.0 

# [2차 알림 게이트키퍼]
ALERT_MIN_PRICE_CHANGE_10M = 0.8  # 최소 변동폭 (스캘핑 마지노선)
ALERT_MIN_SIGNAL_SCORE = 0.70

# [Evaluation target]
# Signals are evaluated from the next fully completed 10-minute bar's opening
# price over one hour. Costs are a fixed pre-trade estimate, not a fee quote.
PRIMARY_HOLDING_PERIOD_MINUTES = 60
PRIMARY_EXECUTION_TIMEFRAME_MINUTES = 10
ESTIMATED_ROUND_TRIP_COST_BPS = 10.0
EXECUTION_NOTIONAL_KRW = 1_000_000.0
EXECUTION_MIN_DAILY_TURNOVER_KRW = 50_000_000.0
EXECUTION_MAX_SPREAD_BPS = 30.0
EXECUTION_MAX_SLIPPAGE_BPS = 30.0
ATTENTION_V3_MODEL_VERSION = "attention-v3"
ATTENTION_V4_MODEL_VERSION = "attention-v4-c-guarded"
ATTENTION_VISIBLE_MODEL = os.environ.get(
    "ATTENTION_VISIBLE_MODEL", ATTENTION_V4_MODEL_VERSION
).strip()
SIGNAL_MODEL_VERSION = ATTENTION_VISIBLE_MODEL
BREAKOUT_STRUCTURE_LOOKBACK_BARS = 20
ATTENTION_QUEUE_LIMIT = 10
ATTENTION_FOCUS_SLOTS = 3
ATTENTION_EARLY_SLOTS = 1
ATTENTION_ONGOING_SLOTS = 1
ATTENTION_FOCUS_SCAN_LIMIT = 3
ATTENTION_PRIMARY_EXPOSURE_WINDOW_MINUTES = 60
ATTENTION_CONTEXT_MIN_HOURLY_BARS = 24
ATTENTION_CONTEXT_MIN_DAILY_BARS = 200
ATTENTION_V4_ACTIVITY_WEIGHT = 0.55
ATTENTION_V4_PRICE_SURPRISE_WEIGHT = 0.35
ATTENTION_V4_CONTEXT_ADJUSTMENT = 0.05
ATTENTION_V4_EXECUTION_ADJUSTMENT = 0.05
ATTENTION_V4_STRENGTH_SATURATION_RATIO = 1.25
ATTENTION_V4_RERANK_QUALITY_WINDOW = 0.08
ATTENTION_V4_SIMILARITY_PENALTY = 0.06
ATTENTION_V4_REPEAT_PENALTY = 0.02
ATTENTION_V4_ONGOING_REPEAT_PENALTY = 0.10
ATTENTION_V4_REPEAT_EXPOSURE_DIVISOR = 3
ATTENTION_RANK_CHANGE_MINIMUM = 5
ATTENTION_PRICE_CHANGE_MINIMUM_PCT = 1.0
ATTENTION_RVOL_RATIO_CHANGE_MINIMUM = 1.5
ATTENTION_COOLING_OBSERVATIONS = 1
ATTENTION_BRIEFING_INTERVAL_MINUTES = 30
REPLAY_DEFAULT_EVALUATION_DAYS = 7
REPLAY_DEFAULT_TOP_K = 5
REPLAY_MIN_EVALUATION_DAYS = 1
REPLAY_OPERATING_ACCEPTANCE_DAYS = 30
REPLAY_ROBUSTNESS_MIN_EVALUATION_DAYS = 60
REPLAY_MAX_EVALUATION_DAYS = 90
REPLAY_OUTCOME_HORIZON_MINUTES = 120
REPLAY_MEANINGFUL_MOVE_PCT = 2.0
REPLAY_ACTIVITY_PERSISTENCE_RATIO = 1.5
REPLAY_RATE_LIMIT_PER_SECOND = 2
REGIME_RSI_PERIOD = 14
REGIME_ATR_LONG_PERIOD = 24
REGIME_ATR_SHORT_PERIOD = 6
IDEMPOTENCY_KEY_HISTORY_LIMIT = 1_440

# [쿨다운]
ALERT_COOLDOWN_MINUTES = 60       
SUSTAINED_MOMENTUM_MIN_ADDITIONAL_CHANGE_PCT = 1.0

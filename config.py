# config.py

import os

# -- ENVIRONMENT & STORAGE CONFIGURATION --

# 저장 방식 선택 ('GCS' 또는 'LOCAL')
STATE_STORAGE_METHOD = os.environ.get("STATE_STORAGE_METHOD", "LOCAL")
# GCP storage Settings
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")

# local storage Settings
LOCAL_STATE_DIR = os.path.join(os.path.dirname(__file__), "state")

STATE_FILE_NAME = "upbit_market_state.json"
RANK_STATE_FILE_NAME = "rank_state.json"
SECTOR_MAP_FILE_NAME = "sectors.json"
ALERT_HISTORY_FILE_NAME = "alert_history.json"

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


def validate_storage_config() -> None:
    """Fail early when the selected state backend is missing required settings."""
    if STATE_STORAGE_METHOD == "GCS" and not GCS_BUCKET_NAME:
        raise RuntimeError("GCS_BUCKET_NAME is required when STATE_STORAGE_METHOD=GCS")


# -- ANALYSIS & ALERTING POLICY --

# [통계 보정 설정]
MAX_Z_SCORE_CAP = 10.0          # Z-Score 상한선 (통계 왜곡 방지)
MIN_MAD_FLOOR = 0.001           # Z-Score 분모 0 방지

# [Wash Trading 필터]
# 거래량 제한
WASH_TRADING_MIN_PRICE_CHANGE = 0.5 

# [Dynamic Thresholds - 순위별 차등 적용]
# 메이저 (Top 50)
RANK_THRESHOLD_MAJOR = 50
MAJOR_MIN_PRICE_CHANGE = 1.5
MAJOR_MIN_Z_SCORE = 3.0

# 중위권 (Top 50 ~ 100)
RANK_THRESHOLD_MID = 100
MID_MIN_PRICE_CHANGE = 2.5
MID_MIN_Z_SCORE = 4.0

# 하위권 (Top 100 밖 - 잡코인)
MINOR_MIN_PRICE_CHANGE = 5.0  # 5% 이상 급등해야 인정
MINOR_MIN_Z_SCORE = 5.0       # 거래량도 확실해야 함

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
SIGNAL_MODEL_VERSION = "heuristic-v1"

# [쿨다운]
ALERT_COOLDOWN_MINUTES = 60       
SUSTAINED_MOMENTUM_MIN_ADDITIONAL_CHANGE_PCT = 1.0

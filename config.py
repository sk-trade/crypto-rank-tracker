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


# -- ANALYSIS & ALERTING POLICY --


# --- 1. Signal Detector Settings (1차 후보군 필터링) ---
CONFIDENCE_THRESHOLD = 0.2      # 후보군으로 포함될 최소 신뢰도 점수
ROBUST_Z_SCORE_THRESHOLD = 3.5  # RVOL 이상 현상으로 판단할 최소 Z-score
DECOUPLING_MIN_DEVIATION_PCT = 2.0 # 강력한 디커플링으로 판단할 최소 편차 (%p)

# --- 2. Alert Gatekeeper Policy (최종 알림 발송 조건) ---
# 아래 기준을 모두 통과해야만 최종 알림이 발송됩니다.
ALERT_MIN_PRICE_CHANGE_10M = 2.0  # 최소 10분간 2% 이상 가격 변동
ALERT_MIN_CONFIDENCE = 0.65       # 최소 신뢰도 65% 이상

# --- 3. Alert Cooldown Policy (알림 반복 방지) ---
ALERT_COOLDOWN_MINUTES = 60       # 같은 코인은 60분 동안 재알림 금지 (단, 예외 있음)
SUSTAINED_MOMENTUM_MIN_ADDITIONAL_CHANGE_PCT = 0.5 # 쿨다운 중 추가 알림을 보낼 최소 변동률
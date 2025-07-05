import os

# 저장 방식 선택 ('GCS' 또는 'LOCAL')
STATE_STORAGE_METHOD = os.environ.get("STATE_STORAGE_METHOD", "LOCAL") 
# GCP storage Settings
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
GCS_STATE_FILE_NAME = "upbit_market_state.json"
# local storage Settings
LOCAL_STATE_DIR = os.path.join(os.path.dirname(__file__), "state")
LOCAL_STATE_FILE_NAME = "upbit_market_state.json"

# Webhook
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")

# Application Settings
APP_LOGGER_NAME = "CryptoRankTracker"
LOG_LEVEL = "INFO"

# --- 분석 및 알림 정책 설정 ---

# 1. 분석 파라미터
STATE_HISTORY_COUNT = 12          # 분석에 사용할 과거 데이터 수 (5회 체크 결과)
NOTIFY_TOP_N = 30                # '신규 진입/이탈'을 감지할 기준 순위 (TOP 30)

# 2. 알림 조건 파라미터
TRENDING_STREAK_THRESHOLD = 3    # '지속적인 추세'로 간주할 최소 연속 변동 횟수
SIGNIFICANT_RANK_CHANGE_THRESHOLD = 8 # '급변동'으로 간주할 최소 순위 변동 폭

# 3. 알림 출력 파라미터
MAX_ALERTS_PER_TYPE = 10         # 유형별(상승/하락/급변동) 최대 알림 개수
DISPLAY_TOP_N_RANKING = 30       # 최종 요약에 표시할 현재 순위 개수
import os

# 저장 방식 선택 ('GCS' 또는 'LOCAL')
STATE_STORAGE_METHOD = os.environ.get("STATE_STORAGE_METHOD", "LOCAL") 
# GCP storage Settings
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
GCS_STATE_FILE = "upbit_market_state.json"
# local storage Settings
LOCAL_STATE_DIR = os.path.join(os.path.dirname(__file__), "state")
LOCAL_STATE_FILE = "upbit_market_state.json"

# Webhook
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")

# Application Settings
APP_LOGGER_NAME = "UpbitRankTracker"
LOG_LEVEL = "INFO"

# 나중에 알림을 보낼 상위 N개 종목 (전체 순위는 매기되, 알림은 상위권 변동만)
NOTIFY_TOP_N = 100
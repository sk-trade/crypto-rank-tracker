import os

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

# Coingecko API Key
CG_API_KEY = os.environ.get("CG_API_KEY")

# Webhook
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "https://chat.home9634.duckdns.org/hooks/p1dpjm3wjpy18qexgs47jjcz7o")

# Application Settings
APP_LOGGER_NAME = "CryptoRankTracker"
LOG_LEVEL = "INFO"

# --- 분석 및 알림 정책 설정 ---

# 필터링 및 스코어링 설정
CONFIDENCE_THRESHOLD = 0.2  # 시그널을 알림으로 보낼 최소 신뢰도 점수 (0.0 ~ 1.0)

# 분석 파라미터 
STATE_HISTORY_COUNT = 12          # 분석에 사용할 과거 데이터 수

# 시그널 감지 임계값
RVOL_SURGE_THRESHOLD = 4.0         # 거래량 폭증: 평소 대비 4배
RVOL_BREAKOUT_THRESHOLD = 2.0      # 돌파 확인: 평소 대비 2배
PRICE_CHANGE_SURGE_THRESHOLD = 1.0  # 급등 기준: 10분간 1% 이상
PRICE_CHANGE_DUMP_THRESHOLD = 4.0   # 급락 기준: 10분간 4% 이상 (절대값)
BTC_DECOUPLING_MIN_CHANGE = 2.0     # 디커플링: BTC 보합 시 2% 이상 독립 상승


# 이 기준을 통과하지 못하면 어떤 알림도 보내지 않음
ALERT_MIN_PRICE_CHANGE_10M = 0.8  # 최소 10분간 0.8% 이상 가격 변동
ALERT_MIN_RVOL = 4.0              # 최소 RVOL 4.0배 이상
ALERT_MIN_CONFIDENCE = 0.5        # 최소 신뢰도 50% 이상

# 알림 쿨다운 정책
ALERT_COOLDOWN_MINUTES = 60       # 같은 코인은 60분 동안 재알림 금지 (단, 예외 있음)
SUSTAINED_MOMENTUM_MIN_ADDITIONAL_CHANGE_PCT = 0.5
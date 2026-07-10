#common/models

import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class CandleData(BaseModel):
    """단일 캔들(분봉) 데이터를 나타냅니다."""

    market: str
    timestamp: datetime.datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float


class TickerData(BaseModel):
    """모든 기술적 분석 지표가 포함된 단일 티커의 종합 데이터입니다."""

    market: str
    candle_history: List[CandleData] = []
    
    is_breakout: bool = False
    trend_1h: str = "NEUTRAL"
    trend_4h: str = "NEUTRAL"
    bb_status: str = "NORMAL"
    volatility_tier: str = "NORMAL"
    rvol_vs_yesterday: Optional[float] = None
    volume_acceleration: Optional[float] = None
    rvol_consistency_score: float = 0.0
    rvol_1h_avg: Optional[float] = None
    is_breaking_1h_high: bool = False
    
    # --- 기본 분석용 ---
    price_change_10m: Optional[float] = None
    relative_volume: Optional[float] = None
    rvol_z_score: Optional[float] = None
    price_change_1h: Optional[float] = None
    price_change_4h: Optional[float] = None
    price_surprise: Optional[float] = None
    rolling_turnover: Optional[float] = None
    liquidity_tier: str = "UNKNOWN"
    

    # --- 심층 분석용 필드  ---
    decoupling_status: str = "COUPLED"
    decoupling_score: Optional[float] = None
    hourly_candles: List[CandleData] = [] 
    daily_candles: List[CandleData] = []
    
    trend_1h_stable: str = "NEUTRAL"
    is_above_ma50_daily: Optional[bool] = None
    is_above_ma200_daily: Optional[bool] = None
    candle_shape: Dict[str, Any] = Field(default_factory=dict)

class SignalCandidate(BaseModel):
    """1차 분석을 통해 생성된 잠재적 시그널 후보입니다."""

    market: str
    signal_score: float
    price_change: float
    rvol: float
    rvol_z_score: float
    contexts: List[str]
    current_price: float


class Alert(BaseModel):
    """최종 알림 대상으로 확정된 시그널 객체입니다."""

    candidate: SignalCandidate
    ticker_data: TickerData  # 포매팅 시 참고할 상세 데이터
    signal_type: str
    priority: int
    structure_level: Optional[float] = None


class RankState(BaseModel):
    """거래대금 순위 변동을 비교하기 위한 상태 모델입니다."""

    last_updated: datetime.datetime
    rankings: Dict[str, int] = {}


class AnalysisState(BaseModel):
    """분석 결과 로그를 저장하기 위한 상태 모델입니다."""

    last_updated: datetime.datetime
    tickers: Dict[str, TickerData]
    rankings: Dict[str, int] = {}


class ScanEvent(BaseModel):
    """Immutable pre-decision record for one market in one scan."""

    event_id: str
    observed_at: datetime.datetime
    market: str
    feature_snapshot: Dict[str, Any]
    candidate_eligible: bool
    rejection_reasons: List[str]
    final_decision: str
    model_version: str
    direction: Optional[str] = None
    signal_score: Optional[float] = None
    signal_candle_start: Optional[datetime.datetime] = None


class ScanOutcome(BaseModel):
    """Post-hoc evaluation record joined to an immutable scan event by event_id."""

    event_id: str
    market: str
    entry_candle_start: datetime.datetime
    exit_candle_start: datetime.datetime
    entry_price: float
    exit_price: float
    directional_net_return: float
    mfe: float
    mae: float


class AlertHistory(BaseModel):
    """알림 쿨다운을 관리하기 위한 발송 기록 모델입니다."""

    market: str
    last_alert_timestamp: datetime.datetime
    last_signal_type: str
    last_price: float
    last_rvol: float
    initial_timestamp: datetime.datetime
    initial_price: float
    structure_level: Optional[float] = None
    structure_direction: Optional[str] = None

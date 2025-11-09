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
    price_change_10m: Optional[float] = None
    relative_volume: Optional[float] = None
    is_breakout: bool = False
    trend_1h: str = "NEUTRAL"
    trend_4h: str = "NEUTRAL"
    bb_status: str = "NORMAL"
    volatility_tier: str = "NORMAL"
    rvol_vs_yesterday: Optional[float] = None
    volume_acceleration: Optional[float] = None
    rvol_consistency_score: float = 0.0
    price_change_1h: Optional[float] = None
    price_change_4h: Optional[float] = None
    rvol_1h_avg: Optional[float] = None
    is_breaking_1h_high: bool = False
    rvol_z_score: Optional[float] = None
    decoupling_status: str = "COUPLED"  # e.g., "COUPLED", "STRONG_DECOUPLE"
    decoupling_score: Optional[float] = None
    candle_shape: Dict[str, Any] = Field(
        default_factory=dict
    )  # e.g., {'type': 'STRONG_MOMENTUM', 'reliability': 'HIGH'}


class SignalCandidate(BaseModel):
    """1차 분석을 통해 생성된 잠재적 시그널 후보입니다."""

    market: str
    confidence: float
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


class RankState(BaseModel):
    """거래대금 순위 변동을 비교하기 위한 상태 모델입니다."""

    last_updated: datetime.datetime
    rankings: Dict[str, int] = {}


class AnalysisState(BaseModel):
    """분석 결과 로그를 저장하기 위한 상태 모델입니다."""

    last_updated: datetime.datetime
    tickers: Dict[str, TickerData]
    rankings: Dict[str, int] = {}


class AlertHistory(BaseModel):
    """알림 쿨다운을 관리하기 위한 발송 기록 모델입니다."""

    market: str
    last_alert_timestamp: datetime.datetime
    last_signal_type: str
    last_price: float
    last_rvol: float
    initial_timestamp: datetime.datetime
    initial_price: float
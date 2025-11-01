#models
from pydantic import BaseModel, Field
from typing import Dict, Optional, List
import datetime

class CandleData(BaseModel):
    market: str
    timestamp: datetime.datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float

class TickerData(BaseModel):
    """분석 및 저장에 모두 사용되는 단일 티커 모델"""
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

class RankState(BaseModel):
    """순위 변동 비교용 상태 모델"""
    last_updated: datetime.datetime
    rankings: Dict[str, int] = {}

class AnalysisState(BaseModel):
    """분석 결과 로그 저장용 상태 모델"""
    last_updated: datetime.datetime
    tickers: Dict[str, TickerData]
    rankings: Dict[str, int] = {}

class AlertHistory(BaseModel):
    """알림 발송 기록을 저장하는 모델"""
    market: str
    last_alert_timestamp: datetime.datetime
    last_signal_type: str 
    last_price: float     
    last_rvol: float       
    initial_timestamp: datetime.datetime
    initial_price: float       
from pydantic import BaseModel, Field
from typing import Dict, Optional, List
import datetime

class TickerData(BaseModel):
    """개별 티커의 처리된 데이터 모델"""
    market: str
    price: Optional[float] = None
    
    # API의 acc_trade_price_24h 필드를 이 필드에 매핑합니다.
    trade_volume_24h_krw: Optional[float] = None
    
    # 분석 과정에서 추가되는 필드들
    rank: Optional[int] = None
    rank_history: List[Optional[int]] = []
    rank_change: int = 0
    volume_z_score: Optional[float] = 0.0 # 거래대금 Z-score
    trend_streak: int = 0

class State(BaseModel):
    """특정 시점의 전체 시장 상태를 나타내는 모델"""
    last_updated: datetime.datetime
    tickers: Dict[str, TickerData]
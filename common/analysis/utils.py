#common/analysis/utils

import logging
from typing import Dict, List

import config
from common.models import MarketTicker

logger = logging.getLogger(config.APP_LOGGER_NAME)

def calculate_rankings(raw_tickers: List[MarketTicker]) -> Dict[str, int]:
    """24시간 누적 거래대금을 기준으로 마켓 순위를 계산합니다."""
    sorted_tickers = sorted(
        raw_tickers,
        key=lambda ticker: ticker.acc_trade_price_24h,
        reverse=True,
    )
    return {ticker.market: rank for rank, ticker in enumerate(sorted_tickers, 1)}

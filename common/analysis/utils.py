"""Small deterministic analysis transforms shared by live scans."""

import logging

import config
from common.models import MarketTicker

logger = logging.getLogger(config.APP_LOGGER_NAME)


def rank_markets_by_24h_turnover(raw_tickers: list[MarketTicker]) -> dict[str, int]:
    """Rank markets by validated 24-hour notional turnover."""
    sorted_tickers = sorted(
        raw_tickers,
        key=lambda ticker: ticker.acc_trade_price_24h,
        reverse=True,
    )
    return {ticker.market: rank for rank, ticker in enumerate(sorted_tickers, 1)}

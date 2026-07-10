"""Point-in-time beta-adjusted residual momentum features."""

from typing import Dict, List

import numpy as np

import config
from common.models import TickerData


def assign_residual_momentum(
    tickers: Dict[str, TickerData], sectors: Dict[str, List[str]], reverse_sectors: Dict[str, List[str]]
) -> None:
    btc = tickers.get("KRW-BTC")
    if not btc:
        return
    returns = {market: _returns(ticker) for market, ticker in tickers.items()}
    if "KRW-BTC" not in returns:
        return
    common = min(len(values) for values in returns.values())
    if common < config.RESIDUAL_MOMENTUM_MIN_OBSERVATIONS + 1:
        return
    start = -(min(common - 1, config.RESIDUAL_MOMENTUM_LOOKBACK_BARS) + 1)
    market_factor = np.median(np.array([values[start:] for values in returns.values()]), axis=0)
    btc_factor = returns["KRW-BTC"][start:]
    for market, ticker in tickers.items():
        tags = reverse_sectors.get(market, [])
        if not tags or market == "KRW-BTC":
            continue
        peers = [coin for coin in sectors.get(tags[0].split("(")[0].strip(), []) if coin != market and coin in returns]
        if len(peers) < 2:
            continue
        sector_factor = np.median(np.array([returns[peer][start:] for peer in peers]), axis=0)
        asset = returns[market][start:]
        # Fit only the history before the current residual observation.
        features = np.column_stack((btc_factor[:-1], market_factor[:-1], sector_factor[:-1]))
        if len(features) < config.RESIDUAL_MOMENTUM_MIN_OBSERVATIONS:
            continue
        betas, *_ = np.linalg.lstsq(features, asset[:-1], rcond=None)
        residual_history = asset[:-1] - features @ betas
        volatility = float(np.std(residual_history, ddof=1))
        if volatility <= 0:
            continue
        expected = float(np.dot(np.array([btc_factor[-1], market_factor[-1], sector_factor[-1]]), betas))
        score = float((asset[-1] - expected) / volatility)
        ticker.residual_momentum_score = score
        ticker.decoupling_score = score


def _returns(ticker: TickerData) -> np.ndarray:
    closes = np.array([candle.close_price for candle in ticker.candle_history], dtype=float)
    if len(closes) < 2 or np.any(closes[:-1] <= 0):
        return np.array([])
    return closes[1:] / closes[:-1] - 1

import datetime

import pytest

from common.analysis.scanner import evaluate_candidate_eligibility, process_lightweight_indicators
from common.models import CandleData, LiquidityTier, RejectionCode, TickerData


def _with_close(candle: CandleData, close_price: float) -> CandleData:
    data = candle.model_dump(mode="python")
    data.update(
        close_price=close_price,
        high_price=max(candle.high_price, close_price),
        low_price=min(candle.low_price, close_price),
    )
    return CandleData.model_validate(data)


def _history(market: str, base_price: float, step_return: float, volume: float) -> list[CandleData]:
    candles = []
    price = base_price
    start = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    for index in range(40):
        price *= 1 + (step_return if index % 2 else -step_return)
        candles.append(
            CandleData(
                market=market,
                timestamp=start + datetime.timedelta(minutes=10 * index),
                open_price=price,
                high_price=price,
                low_price=price,
                close_price=price,
                volume=volume,
            )
        )
    candles[-1] = _with_close(candles[-1], candles[-1].close_price * 1.03)
    return candles


def test_price_surprise_normalizes_assets_with_different_volatility():
    low_volatility = _history("KRW-LOW", 100.0, 0.001, 1000.0)
    high_volatility = _history("KRW-HIGH", 100.0, 0.01, 1000.0)
    low_volatility[-1] = _with_close(
        low_volatility[-1], low_volatility[-2].close_price * 1.003
    )
    high_volatility[-1] = _with_close(
        high_volatility[-1], high_volatility[-2].close_price * 1.03
    )

    tickers = process_lightweight_indicators(
        {"KRW-LOW": low_volatility, "KRW-HIGH": high_volatility}
    )

    assert tickers["KRW-LOW"].price_surprise == pytest.approx(
        tickers["KRW-HIGH"].price_surprise
    )


def test_candidate_rejects_missing_historical_volatility_without_percent_fallback():
    ticker = TickerData(
        market="KRW-NEW",
        price_change_10m=10.0,
        liquidity_tier=LiquidityTier.UNKNOWN,
    )

    decision = evaluate_candidate_eligibility({"KRW-NEW": ticker})

    assert decision["KRW-NEW"].eligible is False
    assert decision["KRW-NEW"].rejection_reasons == [
        RejectionCode.PRICE_SURPRISE_UNAVAILABLE
    ]


def test_liquidity_tiers_come_from_prior_rolling_turnover_not_rank():
    low = _history("KRW-LOW", 100.0, 0.002, 10.0)
    middle = _history("KRW-MID", 100.0, 0.002, 100.0)
    high = _history("KRW-HIGH", 100.0, 0.002, 1000.0)

    tickers = process_lightweight_indicators(
        {"KRW-LOW": low, "KRW-MID": middle, "KRW-HIGH": high}
    )

    assert tickers["KRW-LOW"].liquidity_tier is LiquidityTier.LOW
    assert tickers["KRW-MID"].liquidity_tier is LiquidityTier.MEDIUM
    assert tickers["KRW-HIGH"].liquidity_tier is LiquidityTier.HIGH

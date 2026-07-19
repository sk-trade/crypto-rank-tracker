import datetime

from common.analysis.deep_dive import get_market_regime
from common.models import (
    CandleData,
    MarketRegime,
    MarketRegimeSnapshot,
    TickerData,
)


UTC = datetime.timezone.utc


def _btc_ticker(prices: list[float], high_offset: float = 1.0) -> TickerData:
    candles = [
        CandleData(
            market="KRW-BTC",
            timestamp=datetime.datetime(2026, 6, 18, tzinfo=UTC) + datetime.timedelta(hours=index),
            open_price=price,
            high_price=price + high_offset,
            low_price=price - high_offset,
            close_price=price,
            volume=1.0,
        )
        for index, price in enumerate(prices)
    ]
    return TickerData(market="KRW-BTC", hourly_candles=candles)


def test_regime_is_unknown_without_enough_btc_candles_for_true_range_and_rsi():
    assert get_market_regime(
        {"KRW-BTC": _btc_ticker([100.0] * 24)}
    ) == MarketRegimeSnapshot(regime=MarketRegime.UNKNOWN)


def test_regime_uses_recent_rsi_window_for_a_bull_trend():
    regime = get_market_regime({"KRW-BTC": _btc_ticker([100.0 + index for index in range(25)])})

    assert regime.regime is MarketRegime.TRENDING_BULL
    assert regime.rsi == 100.0


def test_true_range_includes_gaps_from_the_previous_close():
    prices = [100.0] * 19 + [110.0] + [111.0] * 5
    regime = get_market_regime({"KRW-BTC": _btc_ticker(prices, high_offset=0.5)})

    assert regime.atr_ratio > 1.0

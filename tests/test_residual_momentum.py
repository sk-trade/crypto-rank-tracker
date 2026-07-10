import datetime

from common.models import CandleData, TickerData
from common.residuals import assign_residual_momentum


def _ticker(market, returns):
    price, candles = 100.0, []
    start = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    for index, change in enumerate([0.0] + returns):
        price *= 1 + change
        candles.append(CandleData(market=market, timestamp=start + datetime.timedelta(minutes=10 * index), open_price=price, high_price=price, low_price=price, close_price=price, volume=1))
    return TickerData(market=market, candle_history=candles)


def test_residual_momentum_removes_btc_market_and_sector_expected_return():
    base = [0.001 * ((index % 5) - 2) for index in range(40)]
    btc = _ticker("KRW-BTC", base)
    peer_a = _ticker("KRW-P1", [value * 1.2 for value in base])
    peer_b = _ticker("KRW-P2", [value * 0.8 for value in base])
    target = _ticker("KRW-T", [value + 0.0001 * ((index % 3) - 1) + (0.0 if index < 39 else 0.01) for index, value in enumerate(base)])
    tickers = {ticker.market: ticker for ticker in [btc, peer_a, peer_b, target]}

    assign_residual_momentum(tickers, {"Layer": ["KRW-T", "KRW-P1", "KRW-P2"]}, {"KRW-T": ["Layer"], "KRW-P1": ["Layer"], "KRW-P2": ["Layer"]})

    assert target.residual_momentum_score is not None
    assert target.residual_momentum_score > 0
    assert target.decoupling_score == target.residual_momentum_score

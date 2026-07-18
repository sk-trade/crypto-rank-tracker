from common.execution import assess_execution
from common.models import (
    MarketEvent,
    MarketTicker,
    OrderBookSnapshot,
    RejectionCode,
    TickerData,
)


def _book():
    return OrderBookSnapshot.model_validate(
        {
            "market": "KRW-A",
            "orderbook_units": [
                {
                    "bid_price": 99_900,
                    "bid_size": 20,
                    "ask_price": 100_100,
                    "ask_size": 20,
                },
            ],
        }
    )


def _ticker(*, warning: bool = False, caution: bool = False) -> MarketTicker:
    return MarketTicker(
        market="KRW-A",
        acc_trade_price_24h=1_000_000_000,
        market_event=MarketEvent(
            warning=warning,
            caution={"PRICE_FLUCTUATIONS": caution},
        ),
    )


def test_execution_gate_accepts_a_liquid_uncautioned_market():
    decision = assess_execution(
        TickerData(market="KRW-A", price_change_10m=1.0),
        _ticker(),
        _book(),
    )
    assert decision.executable


def test_execution_gate_rejects_market_warning_and_caution_events():
    ticker = TickerData(market="KRW-A", price_change_10m=1.0)
    assert assess_execution(
        ticker,
        _ticker(warning=True),
        _book(),
    ).rejection_reasons == [RejectionCode.MARKET_WARNING]
    assert assess_execution(
        ticker,
        _ticker(caution=True),
        _book(),
    ).rejection_reasons == [RejectionCode.MARKET_WARNING]


def test_execution_gate_rejects_missing_orderbook():
    ticker = TickerData(market="KRW-A", price_change_10m=1.0)
    assert assess_execution(
        ticker,
        _ticker(),
        None,
    ).rejection_reasons == [RejectionCode.ORDERBOOK_UNAVAILABLE]

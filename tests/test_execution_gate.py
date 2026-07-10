from common.execution import assess_execution
from common.models import TickerData


def _book():
    return {"orderbook_units": [
        {"bid_price": 99_900, "bid_size": 20, "ask_price": 100_100, "ask_size": 20},
    ]}


def test_execution_gate_accepts_a_liquid_uncautioned_market():
    decision = assess_execution(
        TickerData(market="KRW-A", price_change_10m=1.0),
        {"acc_trade_price_24h": 1_000_000_000, "market_warning": "NONE"}, _book(),
    )
    assert decision.executable


def test_execution_gate_rejects_warning_and_missing_orderbook():
    ticker = TickerData(market="KRW-A", price_change_10m=1.0)
    assert assess_execution(ticker, {"acc_trade_price_24h": 1e9, "market_warning": "CAUTION"}, _book()).rejection_reasons == ["market_warning"]
    assert assess_execution(ticker, {"acc_trade_price_24h": 1e9, "market_warning": "NONE"}, None).rejection_reasons == ["orderbook_unavailable"]

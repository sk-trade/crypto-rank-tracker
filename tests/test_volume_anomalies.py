import datetime

from common.analysis.scanner import evaluate_candidate_eligibility, process_lightweight_indicators
from common.models import CandleData, TickerData


def _candle(market: str, timestamp: datetime.datetime, volume: float) -> CandleData:
    return CandleData(
        market=market, timestamp=timestamp, open_price=100.0, high_price=100.0,
        low_price=100.0, close_price=100.0, volume=volume,
    )


def test_conditional_and_cross_sectional_volume_anomalies_are_separate():
    start = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    # Three earlier same-weekday/time observations establish the conditional baseline.
    first = [_candle("KRW-A", start + datetime.timedelta(minutes=10 * index), 10.0) for index in range(3025)]
    second = [_candle("KRW-B", start + datetime.timedelta(minutes=10 * index), 10.0) for index in range(3025)]
    third = [_candle("KRW-C", start + datetime.timedelta(minutes=10 * index), 20.0) for index in range(3025)]
    first[-1] = _candle("KRW-A", first[-1].timestamp, 100.0)
    third[-1] = _candle("KRW-C", third[-1].timestamp, 40.0)

    tickers = process_lightweight_indicators({"KRW-A": first, "KRW-B": second, "KRW-C": third}, {})

    assert tickers["KRW-A"].conditional_log_rvol_z_score is not None
    assert tickers["KRW-A"].conditional_log_rvol_z_score > 0
    assert tickers["KRW-A"].cross_sectional_log_rvol_z_score is not None
    assert tickers["KRW-B"].conditional_log_rvol_z_score == 0.0
    assert tickers["KRW-B"].cross_sectional_log_rvol_z_score < 0


def test_candidate_selection_rejects_missing_conditional_history_even_with_cross_sectional_spike():
    ticker = TickerData(
        market="KRW-A",
        price_surprise=4.0,
        liquidity_tier="HIGH",
        cross_sectional_log_rvol_z_score=8.0,
    )

    decisions = evaluate_candidate_eligibility({"KRW-A": ticker}, {})

    assert decisions["KRW-A"].rejection_reasons == ["conditional_volume_history_unavailable"]

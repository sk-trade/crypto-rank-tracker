import datetime

from common.analysis.scanner import evaluate_candidate_eligibility, process_lightweight_indicators
from common.models import CandleData, TickerData


def _candle(market: str, timestamp: datetime.datetime, volume: float) -> CandleData:
    return CandleData(
        market=market, timestamp=timestamp, open_price=100.0, high_price=100.0,
        low_price=100.0, close_price=100.0, volume=volume,
    )


def test_conditional_and_cross_sectional_volume_anomalies_are_separate():
    latest = datetime.datetime(2026, 7, 13, 11, 50, tzinfo=datetime.timezone.utc)

    def history(market: str, baseline_volume: float, latest_volume: float):
        same_slot = [
            _candle(market, latest - datetime.timedelta(weeks=weeks), baseline_volume)
            for weeks in range(3, 0, -1)
        ]
        recent_start = latest - datetime.timedelta(minutes=10 * 153)
        recent = [
            _candle(market, recent_start + datetime.timedelta(minutes=10 * index), baseline_volume)
            for index in range(154)
        ]
        recent[-1] = _candle(market, latest, latest_volume)
        return [*same_slot, *recent]

    first = history("KRW-A", 10.0, 100.0)
    second = history("KRW-B", 10.0, 10.0)
    third = history("KRW-C", 20.0, 40.0)

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

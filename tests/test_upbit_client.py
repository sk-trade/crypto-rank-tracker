import datetime

from common.models import CandleData
from common.upbit_client import normalize_completed_candles
from main import filter_markets_with_complete_deep_dive_data


UTC = datetime.timezone.utc


def _candle(timestamp: str) -> CandleData:
    return CandleData(
        market="KRW-BTC",
        timestamp=datetime.datetime.fromisoformat(timestamp).replace(tzinfo=UTC),
        open_price=100.0,
        high_price=101.0,
        low_price=99.0,
        close_price=100.0,
        volume=1.0,
    )


def test_normalize_completed_candles_excludes_the_open_minute_candle():
    candles = [_candle("2026-06-18T11:40:00"), _candle("2026-06-18T11:50:00"), _candle("2026-06-18T12:00:00")]

    result = normalize_completed_candles(
        candles, "minutes", count=2, minutes_unit=10, as_of=datetime.datetime(2026, 6, 18, 12, 6, tzinfo=UTC)
    )

    assert [candle.timestamp for candle in result] == [
        datetime.datetime(2026, 6, 18, 11, 40, tzinfo=UTC),
        datetime.datetime(2026, 6, 18, 11, 50, tzinfo=UTC),
    ]


def test_normalize_completed_candles_rejects_missing_or_off_grid_minutes():
    missing = [_candle("2026-06-18T11:40:00"), _candle("2026-06-18T12:00:00")]
    off_grid = [_candle("2026-06-18T11:40:00"), _candle("2026-06-18T11:55:00")]
    as_of = datetime.datetime(2026, 6, 18, 12, 6, tzinfo=UTC)

    assert normalize_completed_candles(missing, "minutes", 2, 10, as_of) == []
    assert normalize_completed_candles(off_grid, "minutes", 2, 10, as_of) == []


def test_normalize_completed_daily_candles_uses_the_upbit_kst_boundary():
    candles = [_candle("2026-06-15T15:00:00"), _candle("2026-06-16T15:00:00"), _candle("2026-06-17T15:00:00")]

    result = normalize_completed_candles(
        candles, "days", count=2, as_of=datetime.datetime(2026, 6, 18, 1, tzinfo=UTC)
    )

    assert [candle.timestamp for candle in result] == [
        datetime.datetime(2026, 6, 15, 15, tzinfo=UTC),
        datetime.datetime(2026, 6, 16, 15, tzinfo=UTC),
    ]


def test_deep_dive_gate_blocks_a_candidate_missing_either_timeframe():
    assert filter_markets_with_complete_deep_dive_data(
        ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        {"KRW-BTC": [], "KRW-ETH": []},
        {"KRW-BTC": [], "KRW-XRP": []},
    ) == ["KRW-BTC"]

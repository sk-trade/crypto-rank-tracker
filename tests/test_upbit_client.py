import datetime
import asyncio

import pytest

from common.models import CandleData
from common.upbit_client import (
    UpbitAPIError,
    get_all_krw_tickers,
    get_candles,
    normalize_completed_candles,
)
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


class _Response:
    status = 200
    headers = {}

    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    async def json(self):
        return self.payload


class _Session:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def get(self, _url, params, timeout):
        self.calls.append(params)
        return _Response(self.pages.pop(0))


class _TickerSession:
    def __init__(self, payloads):
        self.payloads = list(payloads)

    def get(self, _url, **_kwargs):
        return _Response(self.payloads.pop(0))


def _raw_candle(timestamp: datetime.datetime) -> dict:
    return {
        "market": "KRW-BTC", "candle_date_time_utc": timestamp.isoformat().replace("+00:00", "Z"),
        "opening_price": 100.0, "high_price": 101.0, "low_price": 99.0,
        "trade_price": 100.0, "candle_acc_trade_volume": 1.0,
    }


def test_get_candles_paginates_before_normalizing_the_complete_grid():
    as_of = datetime.datetime(2026, 6, 18, 12, 6, tzinfo=UTC)
    latest = datetime.datetime(2026, 6, 18, 11, 50, tzinfo=UTC)
    chronological = [latest - datetime.timedelta(minutes=10 * offset) for offset in range(200, -1, -1)]
    newest_first = list(reversed([_raw_candle(timestamp) for timestamp in chronological]))
    session = _Session([newest_first[:200], newest_first[200:]])

    result = asyncio.run(get_candles(session, ["KRW-BTC"], "minutes", 201, 10, as_of))

    assert len(result["KRW-BTC"]) == 201
    assert len(session.calls) == 2
    assert session.calls[0]["count"] == 200
    assert session.calls[1]["count"] == 1
    assert "to" in session.calls[1]


def test_get_all_krw_tickers_rejects_partial_ticker_response():
    session = _TickerSession(
        [
            [
                {"market": "KRW-BTC", "market_warning": "NONE"},
                {"market": "KRW-ETH", "market_warning": "NONE"},
            ],
            [{"market": "KRW-BTC", "trade_price": 100.0}],
        ]
    )

    with pytest.raises(UpbitAPIError, match="KRW 마켓 범위가 목록과 일치하지 않습니다"):
        asyncio.run(get_all_krw_tickers(session))

import datetime
import asyncio

import pytest

from common.models import CandleData
from common.upbit_client import (
    UpbitAPIError,
    _retry_after_seconds,
    get_all_krw_tickers,
    get_candles,
    normalize_completed_candles,
    normalize_sparse_completed_candles,
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


def test_normalize_completed_daily_candles_uses_the_upbit_utc_boundary():
    candles = [
        _candle("2026-06-16T00:00:00"),
        _candle("2026-06-17T00:00:00"),
        _candle("2026-06-18T00:00:00"),
    ]

    result = normalize_completed_candles(
        candles, "days", count=2, as_of=datetime.datetime(2026, 6, 18, 1, tzinfo=UTC)
    )

    assert [candle.timestamp for candle in result] == [
        datetime.datetime(2026, 6, 16, 0, tzinfo=UTC),
        datetime.datetime(2026, 6, 17, 0, tzinfo=UTC),
    ]


def test_deep_dive_gate_blocks_a_candidate_missing_either_timeframe():
    assert filter_markets_with_complete_deep_dive_data(
        ["KRW-BTC", "KRW-ETH", "KRW-XRP"],
        {"KRW-BTC": [], "KRW-ETH": []},
        {"KRW-BTC": [], "KRW-XRP": []},
    ) == ["KRW-BTC"]


class _Response:
    def __init__(self, payload, status=200, headers=None):
        self.payload = payload
        self.status = status
        self.headers = headers or {}

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
        response = self.pages.pop(0)
        return response if isinstance(response, _Response) else _Response(response)


class _TickerSession:
    def __init__(self, payloads):
        self.payloads = list(payloads)

    def get(self, _url, **_kwargs):
        return _Response(self.payloads.pop(0))


def _raw_candle(
    timestamp: datetime.datetime,
    market: str = "KRW-BTC",
    price: float = 100.0,
    volume: float = 1.0,
) -> dict:
    return {
        "market": market, "candle_date_time_utc": timestamp.isoformat().replace("+00:00", "Z"),
        "opening_price": price, "high_price": price, "low_price": price,
        "trade_price": price, "candle_acc_trade_volume": volume,
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
    assert session.calls[0]["to"] == "2026-06-18T12:00:00Z"
    assert session.calls[1]["count"] == 1
    assert "to" in session.calls[1]


def test_sparse_normalization_fills_no_trade_slots_from_the_previous_close():
    as_of = datetime.datetime(2026, 7, 13, 12, 6, tzinfo=UTC)
    source = [
        _candle("2026-07-13T11:20:00"),
        CandleData(
            market="KRW-BTC",
            timestamp=datetime.datetime(2026, 7, 13, 11, 50, tzinfo=UTC),
            open_price=110.0,
            high_price=110.0,
            low_price=110.0,
            close_price=110.0,
            volume=5.0,
        ),
    ]

    result = normalize_sparse_completed_candles(source, 3, 10, as_of)

    assert [candle.timestamp.minute for candle in result] == [30, 40, 50]
    assert [candle.close_price for candle in result] == [100.0, 100.0, 110.0]
    assert [candle.volume for candle in result] == [0.0, 0.0, 5.0]


@pytest.mark.parametrize(
    "source",
    [
        [_candle("2026-07-13T11:50:00"), _candle("2026-07-13T11:50:00")],
        [_candle("2026-07-13T11:40:00"), _candle("2026-07-13T11:55:00")],
        [_candle("2026-07-13T11:50:00"), _candle("2026-07-13T12:00:00")],
    ],
)
def test_sparse_normalization_rejects_duplicate_off_grid_or_open_rows(source):
    as_of = datetime.datetime(2026, 7, 13, 12, 6, tzinfo=UTC)

    assert normalize_sparse_completed_candles(source, 1, 10, as_of) == []


def test_sparse_candle_collection_is_bounded_to_recent_and_weekly_slot_requests():
    as_of = datetime.datetime(2026, 7, 13, 12, 6, tzinfo=UTC)
    recent_page = [
        _raw_candle(datetime.datetime(2026, 7, 13, 11, 50, tzinfo=UTC), price=110.0, volume=5.0),
        _raw_candle(datetime.datetime(2026, 7, 13, 11, 20, tzinfo=UTC), price=100.0),
    ]
    exact_week = [
        _raw_candle(datetime.datetime(2026, 6, 22, 11, 50, tzinfo=UTC), price=80.0, volume=2.0)
    ]
    no_trade_week = [
        _raw_candle(datetime.datetime(2026, 6, 29, 11, 40, tzinfo=UTC), price=90.0, volume=3.0)
    ]
    session = _Session([recent_page, exact_week, no_trade_week, []])

    result = asyncio.run(
        get_candles(
            session,
            ["KRW-BTC"],
            "minutes",
            count=3,
            minutes_unit=10,
            as_of=as_of,
            synthesize_no_trade_intervals=True,
            same_slot_lookback_weeks=3,
        )
    )

    candles = result["KRW-BTC"]
    assert len(session.calls) == 4
    assert [call["count"] for call in session.calls] == [200, 1, 1, 1]
    assert len(candles) == 5
    assert candles[0].timestamp == datetime.datetime(2026, 6, 22, 11, 50, tzinfo=UTC)
    assert candles[1].timestamp == datetime.datetime(2026, 6, 29, 11, 50, tzinfo=UTC)
    assert candles[1].volume == 0.0
    assert [candle.timestamp.minute for candle in candles[-3:]] == [30, 40, 50]


def test_sparse_candle_collection_rejects_cross_market_payloads():
    as_of = datetime.datetime(2026, 7, 13, 12, 6, tzinfo=UTC)
    session = _Session(
        [[_raw_candle(datetime.datetime(2026, 7, 13, 11, 50, tzinfo=UTC), market="KRW-ETH")]]
    )

    result = asyncio.run(
        get_candles(
            session,
            ["KRW-BTC"],
            "minutes",
            count=1,
            minutes_unit=10,
            as_of=as_of,
            synthesize_no_trade_intervals=True,
        )
    )

    assert result == {}


def test_candle_transport_failure_exhausts_retries_and_fails_the_market(monkeypatch):
    as_of = datetime.datetime(2026, 7, 13, 12, 6, tzinfo=UTC)
    session = _Session([_Response([], status=500) for _ in range(3)])

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr("common.upbit_client.asyncio.sleep", no_sleep)
    result = asyncio.run(
        get_candles(
            session,
            ["KRW-BTC"],
            "minutes",
            count=1,
            minutes_unit=10,
            as_of=as_of,
            synthesize_no_trade_intervals=True,
        )
    )

    assert result == {}
    assert len(session.calls) == 3


def test_retry_after_accepts_http_date_and_caps_the_delay(monkeypatch):
    monkeypatch.setattr(
        "common.upbit_client.datetime.datetime",
        type(
            "FixedDateTime",
            (datetime.datetime,),
            {
                "now": classmethod(
                    lambda cls, tz=None: cls(2026, 7, 13, 12, 0, tzinfo=tz or UTC)
                )
            },
        ),
    )

    assert _retry_after_seconds("Mon, 13 Jul 2026 12:00:05 GMT") == 5.0
    assert _retry_after_seconds("Mon, 13 Jul 2026 13:00:00 GMT") == 60.0


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

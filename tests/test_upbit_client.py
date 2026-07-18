import datetime
import asyncio

import pytest

from common.models import (
    CandleData,
    MarketEvent,
    MarketTicker,
    OrderBookSnapshot,
)
from common.upbit_client import (
    CandleTimeUnit,
    UpbitAPIError,
    UpbitErrorCode,
    _retry_after_seconds,
    get_all_krw_tickers,
    get_candles,
    get_orderbooks,
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
        candles,
        CandleTimeUnit.MINUTES,
        count=2,
        minutes_unit=10,
        as_of=datetime.datetime(2026, 6, 18, 12, 6, tzinfo=UTC),
    )

    assert [candle.timestamp for candle in result] == [
        datetime.datetime(2026, 6, 18, 11, 40, tzinfo=UTC),
        datetime.datetime(2026, 6, 18, 11, 50, tzinfo=UTC),
    ]


def test_normalize_completed_candles_rejects_missing_or_off_grid_minutes():
    missing = [_candle("2026-06-18T11:40:00"), _candle("2026-06-18T12:00:00")]
    off_grid = [_candle("2026-06-18T11:40:00"), _candle("2026-06-18T11:55:00")]
    as_of = datetime.datetime(2026, 6, 18, 12, 6, tzinfo=UTC)

    assert normalize_completed_candles(
        missing, CandleTimeUnit.MINUTES, 2, 10, as_of
    ) == []
    assert normalize_completed_candles(
        off_grid, CandleTimeUnit.MINUTES, 2, 10, as_of
    ) == []


def test_normalize_completed_daily_candles_uses_the_upbit_utc_boundary():
    candles = [
        _candle("2026-06-16T00:00:00"),
        _candle("2026-06-17T00:00:00"),
        _candle("2026-06-18T00:00:00"),
    ]

    result = normalize_completed_candles(
        candles,
        CandleTimeUnit.DAYS,
        count=2,
        as_of=datetime.datetime(2026, 6, 18, 1, tzinfo=UTC),
    )

    assert [candle.timestamp for candle in result] == [
        datetime.datetime(2026, 6, 16, 0, tzinfo=UTC),
        datetime.datetime(2026, 6, 17, 0, tzinfo=UTC),
    ]


def test_candle_request_rejects_unsupported_or_invalid_contracts():
    with pytest.raises(UpbitAPIError) as unsupported:
        asyncio.run(get_candles(_Session([]), ["KRW-BTC"], "weeks"))
    assert unsupported.value.code is UpbitErrorCode.INVALID_CANDLE_REQUEST
    assert unsupported.value.details["field"] == "time_unit"

    with pytest.raises(UpbitAPIError) as invalid_count:
        asyncio.run(
            get_candles(
                _Session([]),
                ["KRW-BTC"],
                CandleTimeUnit.MINUTES,
                count=0,
                minutes_unit=10,
            )
        )
    assert invalid_count.value.code is UpbitErrorCode.INVALID_CANDLE_REQUEST
    assert invalid_count.value.details["field"] == "count"


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
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _Response(self.payloads.pop(0))


def _market_event_payload(*, warning: bool = False, caution: bool = False) -> dict:
    return {
        "warning": warning,
        "caution": {"PRICE_FLUCTUATIONS": caution},
    }


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

    result = asyncio.run(
        get_candles(
            session,
            ["KRW-BTC"],
            CandleTimeUnit.MINUTES,
            201,
            10,
            as_of,
        )
    )

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
    recent_week = [
        _raw_candle(datetime.datetime(2026, 7, 6, 11, 50, tzinfo=UTC), price=95.0, volume=4.0)
    ]
    session = _Session([recent_page, exact_week, no_trade_week, recent_week])

    result = asyncio.run(
        get_candles(
            session,
            ["KRW-BTC"],
            CandleTimeUnit.MINUTES,
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
    assert len(candles) == 6
    assert candles[0].timestamp == datetime.datetime(2026, 6, 22, 11, 50, tzinfo=UTC)
    assert candles[1].timestamp == datetime.datetime(2026, 6, 29, 11, 50, tzinfo=UTC)
    assert candles[1].volume == 0.0
    assert [candle.timestamp.minute for candle in candles[-3:]] == [30, 40, 50]


def test_sparse_candle_collection_rejects_missing_weekly_same_slot_history():
    as_of = datetime.datetime(2026, 7, 13, 12, 6, tzinfo=UTC)
    recent_page = [
        _raw_candle(datetime.datetime(2026, 7, 13, 11, 50, tzinfo=UTC), price=110.0),
        _raw_candle(datetime.datetime(2026, 7, 13, 11, 20, tzinfo=UTC), price=100.0),
    ]
    complete_week = [
        _raw_candle(datetime.datetime(2026, 6, 22, 11, 50, tzinfo=UTC), price=80.0)
    ]
    session = _Session([recent_page, complete_week, [], []])

    result = asyncio.run(
        get_candles(
            session,
            ["KRW-BTC"],
            CandleTimeUnit.MINUTES,
            count=3,
            minutes_unit=10,
            as_of=as_of,
            synthesize_no_trade_intervals=True,
            same_slot_lookback_weeks=3,
        )
    )

    assert result == {}


def test_sparse_candle_collection_rejects_cross_market_payloads():
    as_of = datetime.datetime(2026, 7, 13, 12, 6, tzinfo=UTC)
    session = _Session(
        [[_raw_candle(datetime.datetime(2026, 7, 13, 11, 50, tzinfo=UTC), market="KRW-ETH")]]
    )

    result = asyncio.run(
        get_candles(
            session,
            ["KRW-BTC"],
            CandleTimeUnit.MINUTES,
            count=1,
            minutes_unit=10,
            as_of=as_of,
            synthesize_no_trade_intervals=True,
        )
    )

    assert result == {}


@pytest.mark.parametrize(
    "updates",
    [
        {"opening_price": 0.0},
        {"trade_price": float("nan")},
        {"high_price": 99.0},
        {"low_price": 101.0},
        {"candle_acc_trade_volume": -1.0},
        {"candle_acc_trade_volume": float("inf")},
    ],
)
def test_sparse_candle_collection_rejects_invalid_numeric_domains(updates):
    as_of = datetime.datetime(2026, 7, 13, 12, 6, tzinfo=UTC)
    row = _raw_candle(datetime.datetime(2026, 7, 13, 11, 50, tzinfo=UTC))
    row.update(updates)
    session = _Session([[row]])

    result = asyncio.run(
        get_candles(
            session,
            ["KRW-BTC"],
            CandleTimeUnit.MINUTES,
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
            CandleTimeUnit.MINUTES,
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
                {"market": "KRW-BTC", "market_event": _market_event_payload()},
                {"market": "KRW-ETH", "market_event": _market_event_payload()},
            ],
            [{"market": "KRW-BTC", "trade_price": 100.0}],
        ]
    )

    with pytest.raises(UpbitAPIError) as error:
        asyncio.run(get_all_krw_tickers(session))
    assert error.value.code is UpbitErrorCode.TICKER_SCOPE_MISMATCH
    assert error.value.details == {
        "missing": ["KRW-ETH"],
        "unexpected": [],
        "duplicates": [],
        "invalid_rows": 0,
    }


def test_get_all_krw_tickers_rejects_duplicate_market_rows():
    session = _TickerSession(
        [
            [
                {"market": "KRW-BTC", "market_event": _market_event_payload()},
                {"market": "KRW-ETH", "market_event": _market_event_payload()},
            ],
            [
                {"market": "KRW-BTC", "acc_trade_price_24h": 300.0},
                {"market": "KRW-BTC", "acc_trade_price_24h": 100.0},
                {"market": "KRW-ETH", "acc_trade_price_24h": 200.0},
            ],
        ]
    )

    with pytest.raises(UpbitAPIError) as error:
        asyncio.run(get_all_krw_tickers(session))
    assert error.value.code is UpbitErrorCode.TICKER_SCOPE_MISMATCH
    assert error.value.details["duplicates"] == ["KRW-BTC"]


@pytest.mark.parametrize("turnover", [None, "100", -1.0, float("inf")])
def test_get_all_krw_tickers_rejects_invalid_ranking_turnover(turnover):
    session = _TickerSession(
        [
            [{"market": "KRW-BTC", "market_event": _market_event_payload()}],
            [{"market": "KRW-BTC", "acc_trade_price_24h": turnover}],
        ]
    )

    with pytest.raises(UpbitAPIError) as error:
        asyncio.run(get_all_krw_tickers(session))
    assert error.value.code is UpbitErrorCode.INVALID_TICKER_TURNOVER
    assert error.value.market == "KRW-BTC"


def test_get_all_krw_tickers_accepts_zero_turnover_for_per_market_execution_gating():
    session = _TickerSession(
        [
            [{"market": "KRW-BTC", "market_event": _market_event_payload()}],
            [{"market": "KRW-BTC", "acc_trade_price_24h": 0.0}],
        ]
    )

    result = asyncio.run(get_all_krw_tickers(session))

    assert result[0].acc_trade_price_24h == 0.0


def test_get_all_krw_tickers_returns_typed_market_contracts():
    session = _TickerSession(
        [
            [{"market": "KRW-BTC", "market_event": _market_event_payload()}],
            [
                {
                    "market": "KRW-BTC",
                    "trade_price": 100.0,
                    "acc_trade_price_24h": 1_000_000.0,
                    "future_api_field": "ignored at the external boundary",
                }
            ],
        ]
    )

    result = asyncio.run(get_all_krw_tickers(session))

    assert result == [
        MarketTicker(
            market="KRW-BTC",
            trade_price=100.0,
            acc_trade_price_24h=1_000_000.0,
            market_event=MarketEvent.model_validate(_market_event_payload()),
        )
    ]
    assert session.calls[0][1]["params"] == {"is_details": "true"}


def test_get_all_krw_tickers_rejects_missing_detailed_market_event():
    session = _TickerSession(
        [
            [{"market": "KRW-BTC"}],
            [{"market": "KRW-BTC", "acc_trade_price_24h": 1_000_000.0}],
        ]
    )

    with pytest.raises(UpbitAPIError) as error:
        asyncio.run(get_all_krw_tickers(session))

    assert error.value.code is UpbitErrorCode.INVALID_MARKET_RESPONSE
    assert error.value.market == "KRW-BTC"


def test_get_orderbooks_preserves_valid_markets_when_other_rows_are_malformed():
    valid = {
        "market": "KRW-BTC",
        "orderbook_units": [
            {
                "bid_price": 99.0,
                "bid_size": 2.0,
                "ask_price": 101.0,
                "ask_size": 2.0,
            }
        ],
    }
    session = _Session(
        [
            [
                valid,
                {"orderbook_units": valid["orderbook_units"]},
                {"market": "KRW-ETH", "orderbook_units": [{"bid_price": "bad"}]},
                {"market": "KRW-XRP", "orderbook_units": valid["orderbook_units"]},
            ]
        ]
    )

    result = asyncio.run(get_orderbooks(session, ["KRW-BTC", "KRW-ETH"]))

    assert result == {"KRW-BTC": OrderBookSnapshot.model_validate(valid)}

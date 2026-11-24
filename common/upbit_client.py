"""Validated, rate-limited access to Upbit market data."""

import asyncio
import datetime
import logging
from collections import Counter
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import Any
from weakref import WeakKeyDictionary

import aiohttp
from aiolimiter import AsyncLimiter
from pydantic import ValidationError

import config
from common.models import (
    CandleData,
    MarketListing,
    MarketTicker,
    OrderBookSnapshot,
)

logger = logging.getLogger(config.APP_LOGGER_NAME)

UPBIT_API_BASE_URL = "https://api.upbit.com/v1"

GLOBAL_RATE_LIMIT_PER_SECOND = 8
_LOOP_LIMITERS = WeakKeyDictionary()
_LOOP_COOLDOWN_UNTIL = WeakKeyDictionary()
MAX_CANDLES_PER_REQUEST = 200
MAX_RETRY_DELAY_SECONDS = 60.0


class UpbitErrorCode(StrEnum):
    INVALID_CANDLE_REQUEST = "invalid_candle_request"
    CANDLE_REQUEST_FAILED = "candle_request_failed"
    INVALID_CANDLE_RESPONSE = "invalid_candle_response"
    CANDLE_RETRIES_EXHAUSTED = "candle_retries_exhausted"
    MARKET_LIST_EMPTY = "market_list_empty"
    INVALID_MARKET_RESPONSE = "invalid_market_response"
    TICKER_LIST_EMPTY = "ticker_list_empty"
    TICKER_SCOPE_MISMATCH = "ticker_scope_mismatch"
    INVALID_TICKER_TURNOVER = "invalid_ticker_turnover"
    INVALID_TICKER_RESPONSE = "invalid_ticker_response"
    NETWORK_ERROR = "network_error"
    UNEXPECTED_ERROR = "unexpected_error"


class CandleTimeUnit(StrEnum):
    MINUTES = "minutes"
    DAYS = "days"


@dataclass(frozen=True)
class _CandleCollectionRequest:
    time_unit: CandleTimeUnit
    count: int
    minutes_unit: int | None
    as_of: datetime.datetime
    cutoff: datetime.datetime
    synthesize_no_trade_intervals: bool
    same_slot_lookback_weeks: int

    @property
    def endpoint(self) -> str:
        if self.time_unit is CandleTimeUnit.MINUTES:
            return f"{UPBIT_API_BASE_URL}/candles/minutes/{self.minutes_unit}"
        return f"{UPBIT_API_BASE_URL}/candles/{self.time_unit.value}"


class UpbitAPIError(Exception):
    """A typed Upbit request, response, or pagination failure."""

    def __init__(
        self,
        code: UpbitErrorCode,
        *,
        market: str | None = None,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(code.value)
        self.code = code
        self.market = market
        self.details = details or {}


def _global_limiter() -> AsyncLimiter:
    """Share one Upbit limiter within an event loop without leaking it across invocations."""
    loop = asyncio.get_running_loop()
    limiter = _LOOP_LIMITERS.get(loop)
    if limiter is None:
        # Smooth requests instead of spending the whole per-second allowance in a burst.
        limiter = AsyncLimiter(1, 1 / GLOBAL_RATE_LIMIT_PER_SECOND)
        _LOOP_LIMITERS[loop] = limiter
    return limiter


async def _wait_for_global_cooldown() -> None:
    """Pause all candle requests in this process after any peer receives a 429."""
    loop = asyncio.get_running_loop()
    deadline = _LOOP_COOLDOWN_UNTIL.get(loop, 0.0)
    delay = deadline - loop.time()
    if delay > 0:
        await asyncio.sleep(delay)


async def _acquire_request_slot() -> None:
    """Acquire rate capacity only after the shared cooldown has actually expired."""
    loop = asyncio.get_running_loop()
    while True:
        await _wait_for_global_cooldown()
        await _global_limiter().acquire()
        if _LOOP_COOLDOWN_UNTIL.get(loop, 0.0) <= loop.time():
            return


def _extend_global_cooldown(delay: float) -> None:
    loop = asyncio.get_running_loop()
    _LOOP_COOLDOWN_UNTIL[loop] = max(
        _LOOP_COOLDOWN_UNTIL.get(loop, 0.0), loop.time() + delay
    )


def _as_utc(timestamp: datetime.datetime) -> datetime.datetime:
    """Interpret Upbit's UTC timestamp strings consistently as aware UTC values."""
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=datetime.UTC)
    return timestamp.astimezone(datetime.UTC)


def _candle_grid(
    time_unit: CandleTimeUnit,
    minutes_unit: int | None,
    as_of: datetime.datetime,
) -> tuple[datetime.datetime, datetime.timedelta]:
    """Return the current open candle start and interval in Upbit's UTC grid."""
    now_utc = _as_utc(as_of)
    if time_unit is CandleTimeUnit.MINUTES:
        if not minutes_unit:
            raise UpbitAPIError(
                UpbitErrorCode.INVALID_CANDLE_REQUEST,
                details={"field": "minutes_unit"},
            )
        interval = datetime.timedelta(minutes=minutes_unit)
        elapsed = now_utc - datetime.datetime.min.replace(tzinfo=datetime.UTC)
        current_start = (
            datetime.datetime.min.replace(tzinfo=datetime.UTC)
            + (elapsed // interval) * interval
        )
    elif time_unit is CandleTimeUnit.DAYS:
        interval = datetime.timedelta(days=1)
        current_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        raise UpbitAPIError(
            UpbitErrorCode.INVALID_CANDLE_REQUEST,
            details={"field": "time_unit", "value": time_unit},
        )
    return current_start, interval


def _expected_candle_starts(
    time_unit: CandleTimeUnit,
    count: int,
    minutes_unit: int | None,
    as_of: datetime.datetime,
) -> list[datetime.datetime]:
    """Return the ordered grid of fully closed Upbit candle start times."""
    current_start, interval = _candle_grid(time_unit, minutes_unit, as_of)
    latest_completed = current_start - interval
    first = latest_completed - interval * (count - 1)
    return [first + interval * index for index in range(count)]


def normalize_completed_candles(
    candles: list[CandleData],
    time_unit: CandleTimeUnit,
    count: int,
    minutes_unit: int | None = None,
    as_of: datetime.datetime | None = None,
) -> list[CandleData]:
    """Reindex to the complete candle grid, rejecting partial or gapped histories."""
    expected_starts = _expected_candle_starts(
        time_unit, count, minutes_unit, as_of or datetime.datetime.now(datetime.UTC)
    )
    candles_by_start: dict[datetime.datetime, CandleData] = {}
    for candle in candles:
        timestamp = _as_utc(candle.timestamp)
        if timestamp in candles_by_start:
            return []
        candle.timestamp = timestamp
        candles_by_start[timestamp] = candle

    reindexed = [candles_by_start.get(timestamp) for timestamp in expected_starts]
    if any(candle is None for candle in reindexed):
        return []
    return [candle for candle in reindexed if candle is not None]


def normalize_sparse_completed_candles(
    candles: list[CandleData],
    count: int,
    minutes_unit: int,
    as_of: datetime.datetime,
) -> list[CandleData]:
    """Fill valid no-trade clock slots while rejecting malformed candle history."""
    if not candles:
        return []

    current_start, _ = _candle_grid(CandleTimeUnit.MINUTES, minutes_unit, as_of)
    expected_starts = _expected_candle_starts(
        CandleTimeUnit.MINUTES, count, minutes_unit, as_of
    )
    market = candles[0].market
    candles_by_start: dict[datetime.datetime, CandleData] = {}
    for candle in candles:
        timestamp = _as_utc(candle.timestamp)
        if candle.market != market or timestamp >= current_start:
            return []
        if (
            _candle_grid(CandleTimeUnit.MINUTES, minutes_unit, timestamp)[0]
            != timestamp
        ):
            return []
        if timestamp in candles_by_start:
            return []
        candles_by_start[timestamp] = candle.model_copy(update={"timestamp": timestamp})

    first_expected = expected_starts[0]
    seed_candidates = [
        candle
        for timestamp, candle in candles_by_start.items()
        if timestamp < first_expected
    ]
    previous_close = None
    if seed_candidates:
        seed = max(seed_candidates, key=lambda candle: candle.timestamp)
        previous_close = seed.close_price

    normalized = []
    for timestamp in expected_starts:
        candle = candles_by_start.get(timestamp)
        if candle is not None:
            previous_close = candle.close_price
            normalized.append(candle)
            continue
        if previous_close is None:
            return []
        normalized.append(
            CandleData(
                market=market,
                timestamp=timestamp,
                open_price=previous_close,
                high_price=previous_close,
                low_price=previous_close,
                close_price=previous_close,
                volume=0.0,
                trade_value=0.0,
            )
        )
    return normalized


def _retry_after_seconds(value: str | None) -> float:
    """Parse either legal Retry-After form and cap excessive server delays."""
    if not value:
        return 1.0
    try:
        delay = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            delay = (
                _as_utc(retry_at) - datetime.datetime.now(datetime.UTC)
            ).total_seconds()
        except (TypeError, ValueError, OverflowError):
            delay = 1.0
    return min(max(delay, 0.0), MAX_RETRY_DELAY_SECONDS)


async def _request_candle_page(
    session: aiohttp.ClientSession,
    url: str,
    params: dict[str, Any],
    market: str,
) -> list[dict[str, Any]]:
    """Fetch one candle page with bounded retries and explicit payload validation."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            await _acquire_request_slot()
            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 429:
                    retry_after = max(
                        _retry_after_seconds(response.headers.get("Retry-After")),
                        float(2**attempt),
                    )
                    _extend_global_cooldown(retry_after)
                    logger.warning(
                        "%s: Upbit rate limit; retrying in %.1fs (attempt %d/%d)",
                        market,
                        retry_after,
                        attempt + 1,
                        max_retries,
                    )
                    await asyncio.sleep(retry_after)
                    continue
                if 500 <= response.status < 600:
                    wait_time = 2**attempt
                    logger.warning(
                        "%s: Upbit HTTP %d; retrying in %ds (attempt %d/%d)",
                        market,
                        response.status,
                        wait_time,
                        attempt + 1,
                        max_retries,
                    )
                    await asyncio.sleep(wait_time)
                    continue
                response.raise_for_status()
                page = await response.json()
                if not isinstance(page, list):
                    raise ValueError("candle response must be a list")
                return page
        except aiohttp.ClientError as error:
            logger.warning(
                "%s: Upbit network error (attempt %d/%d): %s",
                market,
                attempt + 1,
                max_retries,
                error,
            )
            if attempt == max_retries - 1:
                raise UpbitAPIError(
                    UpbitErrorCode.CANDLE_REQUEST_FAILED, market=market
                ) from error
            await asyncio.sleep(2**attempt)
        except ValueError as error:
            raise UpbitAPIError(
                UpbitErrorCode.INVALID_CANDLE_RESPONSE, market=market
            ) from error
    raise UpbitAPIError(UpbitErrorCode.CANDLE_RETRIES_EXHAUSTED, market=market)


def _parse_candle_page(page: list[dict[str, Any]], market: str) -> list[CandleData]:
    candles = []
    for item in page:
        if not isinstance(item, dict) or item.get("market") != market:
            raise ValueError(f"{market}: candle response market mismatch")
        candles.append(
            CandleData.model_validate(
                {
                    "market": item["market"],
                    "timestamp": _as_utc(
                        datetime.datetime.fromisoformat(
                            item["candle_date_time_utc"].replace("Z", "+00:00")
                        )
                    ),
                    "open_price": item["opening_price"],
                    "high_price": item["high_price"],
                    "low_price": item["low_price"],
                    "close_price": item["trade_price"],
                    "volume": item["candle_acc_trade_volume"],
                    "trade_value": item.get("candle_acc_trade_price"),
                }
            )
        )
    return candles


def _same_slot_candle(
    candles: list[CandleData],
    market: str,
    target: datetime.datetime,
    minutes_unit: int,
) -> CandleData | None:
    """Return the target candle or a zero-volume carry-forward for a no-trade slot."""
    if not candles:
        return None
    if len(candles) != 1:
        raise ValueError(f"{market}: expected one same-slot seed candle")
    candle = candles[0]
    timestamp = _as_utc(candle.timestamp)
    if _candle_grid(CandleTimeUnit.MINUTES, minutes_unit, timestamp)[0] != timestamp:
        raise ValueError(f"{market}: off-grid same-slot seed candle")
    if timestamp > target:
        raise ValueError(f"{market}: same-slot seed is newer than its cutoff")
    if timestamp == target:
        return candle.model_copy(update={"timestamp": timestamp})
    return CandleData(
        market=market,
        timestamp=target,
        open_price=candle.close_price,
        high_price=candle.close_price,
        low_price=candle.close_price,
        close_price=candle.close_price,
        volume=0.0,
        trade_value=0.0,
    )


def _parse_krw_market_listings(payload: object) -> list[MarketListing]:
    if not isinstance(payload, list):
        raise UpbitAPIError(UpbitErrorCode.INVALID_MARKET_RESPONSE)

    listings: list[MarketListing] = []
    for row in payload:
        if not isinstance(row, dict) or not isinstance(row.get("market"), str):
            raise UpbitAPIError(UpbitErrorCode.INVALID_MARKET_RESPONSE)
        if not row["market"].startswith("KRW-"):
            continue
        try:
            listing = MarketListing.model_validate(row)
        except ValidationError as error:
            raise UpbitAPIError(
                UpbitErrorCode.INVALID_MARKET_RESPONSE,
                market=row["market"],
            ) from error
        if listing.market_event is None:
            raise UpbitAPIError(
                UpbitErrorCode.INVALID_MARKET_RESPONSE,
                market=listing.market,
            )
        listings.append(listing)

    if not listings:
        raise UpbitAPIError(UpbitErrorCode.MARKET_LIST_EMPTY)
    return listings


async def _fetch_krw_market_listings(
    session: aiohttp.ClientSession,
) -> list[MarketListing]:
    async with session.get(
        f"{UPBIT_API_BASE_URL}/market/all",
        params={"is_details": "true"},
        timeout=10,
    ) as response:
        response.raise_for_status()
        return _parse_krw_market_listings(await response.json())


def _ticker_scope_mismatch(
    payload: object,
    expected_markets: list[str],
) -> dict[str, Any] | None:
    if not isinstance(payload, list) or not payload:
        raise UpbitAPIError(UpbitErrorCode.TICKER_LIST_EMPTY)

    payload_markets = [
        row.get("market") if isinstance(row, dict) else None for row in payload
    ]
    market_counts = Counter(
        market for market in payload_markets if isinstance(market, str)
    )
    expected = set(expected_markets)
    missing = sorted(expected - set(market_counts))
    unexpected = sorted(set(market_counts) - expected)
    duplicates = sorted(market for market, count in market_counts.items() if count != 1)
    invalid_rows = sum(market is None for market in payload_markets)
    if (
        missing
        or unexpected
        or duplicates
        or invalid_rows
        or len(payload) != len(expected_markets)
    ):
        return {
            "missing": missing,
            "unexpected": unexpected,
            "duplicates": duplicates,
            "invalid_rows": invalid_rows,
        }
    return None


def _ticker_error_code(error: ValidationError) -> UpbitErrorCode:
    invalid_turnover = any(
        tuple(issue["loc"]) == ("acc_trade_price_24h",) for issue in error.errors()
    )
    return (
        UpbitErrorCode.INVALID_TICKER_TURNOVER
        if invalid_turnover
        else UpbitErrorCode.INVALID_TICKER_RESPONSE
    )


def _parse_market_tickers(
    payload: object,
    listings: list[MarketListing],
) -> list[MarketTicker]:
    expected_markets = [listing.market for listing in listings]
    if mismatch := _ticker_scope_mismatch(payload, expected_markets):
        raise UpbitAPIError(
            UpbitErrorCode.TICKER_SCOPE_MISMATCH,
            details=mismatch,
        )
    if not isinstance(payload, list):
        raise UpbitAPIError(UpbitErrorCode.TICKER_LIST_EMPTY)

    market_events = {listing.market: listing.market_event for listing in listings}
    parsed_tickers: list[MarketTicker] = []
    for row in payload:
        market = row["market"]
        try:
            parsed_tickers.append(
                MarketTicker.model_validate(
                    {
                        **row,
                        "market_event": market_events[market],
                    }
                )
            )
        except ValidationError as error:
            raise UpbitAPIError(
                _ticker_error_code(error),
                market=market,
            ) from error
    return parsed_tickers


async def _fetch_market_tickers(
    session: aiohttp.ClientSession,
    listings: list[MarketListing],
) -> list[MarketTicker]:
    markets = [listing.market for listing in listings]
    async with session.get(
        f"{UPBIT_API_BASE_URL}/ticker",
        params={"markets": ",".join(markets)},
        timeout=15,
    ) as response:
        response.raise_for_status()
        return _parse_market_tickers(await response.json(), listings)


async def get_all_krw_tickers(
    session: aiohttp.ClientSession,
) -> list[MarketTicker]:
    """Fetch and validate the complete Upbit KRW ticker universe."""
    try:
        listings = await _fetch_krw_market_listings(session)
        return await _fetch_market_tickers(session, listings)
    except UpbitAPIError:
        raise
    except aiohttp.ClientError as error:
        logger.error("Upbit market collection failed: %s", error)
        raise UpbitAPIError(UpbitErrorCode.NETWORK_ERROR) from error
    except Exception as error:
        logger.exception("Unexpected Upbit market collection failure")
        raise UpbitAPIError(UpbitErrorCode.UNEXPECTED_ERROR) from error


async def get_orderbooks(
    session: aiohttp.ClientSession, markets: list[str]
) -> dict[str, OrderBookSnapshot]:
    """Fetch orderbook snapshots; callers must fail closed for missing markets."""
    if not markets:
        return {}
    try:
        async with (
            _global_limiter(),
            session.get(
                f"{UPBIT_API_BASE_URL}/orderbook",
                params={"markets": ",".join(markets)},
                timeout=15,
            ) as response,
        ):
            response.raise_for_status()
            payload = await response.json()
            if not isinstance(payload, list):
                raise ValueError("orderbook response must be a list")
            requested_markets = set(markets)
            orderbooks = {}
            for raw_book in payload:
                try:
                    book = OrderBookSnapshot.model_validate(raw_book)
                except ValidationError:
                    market = (
                        raw_book.get("market") if isinstance(raw_book, dict) else None
                    )
                    if market in requested_markets:
                        logger.warning(
                            "Ignoring malformed orderbook row for market %s",
                            market,
                        )
                    continue
                if book.market not in requested_markets or book.market in orderbooks:
                    continue
                orderbooks[book.market] = book
            return orderbooks
    except (aiohttp.ClientError, ValueError) as error:
        logger.warning("Orderbook collection failed: %s", error)
        return {}


def _build_candle_collection_request(
    *,
    time_unit: CandleTimeUnit,
    count: int,
    minutes_unit: int | None,
    as_of: datetime.datetime | None,
    synthesize_no_trade_intervals: bool,
    same_slot_lookback_weeks: int,
) -> _CandleCollectionRequest:
    if count <= 0:
        raise UpbitAPIError(
            UpbitErrorCode.INVALID_CANDLE_REQUEST,
            details={"field": "count"},
        )
    if same_slot_lookback_weeks < 0:
        raise UpbitAPIError(
            UpbitErrorCode.INVALID_CANDLE_REQUEST,
            details={"field": "same_slot_lookback_weeks"},
        )
    if synthesize_no_trade_intervals and (
        time_unit is not CandleTimeUnit.MINUTES or not minutes_unit
    ):
        raise UpbitAPIError(
            UpbitErrorCode.INVALID_CANDLE_REQUEST,
            details={"field": "synthesize_no_trade_intervals"},
        )
    if same_slot_lookback_weeks and not synthesize_no_trade_intervals:
        raise UpbitAPIError(
            UpbitErrorCode.INVALID_CANDLE_REQUEST,
            details={"field": "same_slot_lookback_weeks"},
        )

    request_as_of = as_of or datetime.datetime.now(datetime.UTC)
    request_cutoff, _ = _candle_grid(time_unit, minutes_unit, request_as_of)
    return _CandleCollectionRequest(
        time_unit=time_unit,
        count=count,
        minutes_unit=minutes_unit,
        as_of=request_as_of,
        cutoff=request_cutoff,
        synthesize_no_trade_intervals=synthesize_no_trade_intervals,
        same_slot_lookback_weeks=same_slot_lookback_weeks,
    )


def _format_upbit_cutoff(timestamp: datetime.datetime) -> str:
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


async def _fetch_sparse_recent_candles(
    session: aiohttp.ClientSession,
    market: str,
    request: _CandleCollectionRequest,
) -> list[CandleData]:
    if request.minutes_unit is None:
        raise UpbitAPIError(
            UpbitErrorCode.INVALID_CANDLE_REQUEST,
            market=market,
            details={"field": "minutes_unit"},
        )

    first_expected = _expected_candle_starts(
        request.time_unit,
        request.count,
        request.minutes_unit,
        request.as_of,
    )[0]
    source_candles: list[CandleData] = []
    page_cutoff = request.cutoff
    previous_oldest: datetime.datetime | None = None
    maximum_pages = (
        request.count + MAX_CANDLES_PER_REQUEST - 1
    ) // MAX_CANDLES_PER_REQUEST + 2

    for _page_number in range(maximum_pages):
        raw_page = await _request_candle_page(
            session,
            request.endpoint,
            {
                "market": market,
                "count": MAX_CANDLES_PER_REQUEST,
                "to": _format_upbit_cutoff(page_cutoff),
            },
            market,
        )
        if not raw_page:
            break

        page_candles = _parse_candle_page(raw_page, market)
        source_candles.extend(page_candles)
        oldest_timestamp = min(candle.timestamp for candle in page_candles)
        if previous_oldest is not None and oldest_timestamp >= previous_oldest:
            raise ValueError(f"{market}: candle pagination did not advance")
        previous_oldest = oldest_timestamp

        if (
            oldest_timestamp <= first_expected
            or len(raw_page) < MAX_CANDLES_PER_REQUEST
        ):
            break
        page_cutoff = oldest_timestamp

    return normalize_sparse_completed_candles(
        source_candles,
        request.count,
        request.minutes_unit,
        request.as_of,
    )


async def _fetch_same_slot_candles(
    session: aiohttp.ClientSession,
    market: str,
    request: _CandleCollectionRequest,
    latest_completed_at: datetime.datetime,
) -> list[CandleData]:
    if request.minutes_unit is None:
        return []

    same_slot_candles: list[CandleData] = []
    for weeks_ago in range(request.same_slot_lookback_weeks, 0, -1):
        target_at = latest_completed_at - datetime.timedelta(weeks=weeks_ago)
        cutoff_at = target_at + datetime.timedelta(minutes=request.minutes_unit)
        raw_page = await _request_candle_page(
            session,
            request.endpoint,
            {
                "market": market,
                "count": 1,
                "to": _format_upbit_cutoff(cutoff_at),
            },
            market,
        )
        sample = _same_slot_candle(
            _parse_candle_page(raw_page, market),
            market,
            target_at,
            request.minutes_unit,
        )
        if sample is not None:
            same_slot_candles.append(sample)
    return same_slot_candles


async def _fetch_sparse_market_candles(
    session: aiohttp.ClientSession,
    market: str,
    request: _CandleCollectionRequest,
) -> list[CandleData]:
    try:
        recent_candles = await _fetch_sparse_recent_candles(session, market, request)
        if len(recent_candles) != request.count:
            logger.warning(
                "%s: rejected sparse candle history without a valid seed or grid.",
                market,
            )
            return []

        same_slot_candles = await _fetch_same_slot_candles(
            session,
            market,
            request,
            recent_candles[-1].timestamp,
        )
        if len(same_slot_candles) != request.same_slot_lookback_weeks:
            logger.warning(
                "%s: rejected candle history with incomplete weekly same-slot coverage.",
                market,
            )
            return []
        return [*same_slot_candles, *recent_candles]
    except (UpbitAPIError, KeyError, TypeError, ValueError) as error:
        logger.warning("%s: sparse candle collection failed: %s", market, error)
        return []


def _oldest_raw_candle_timestamp(
    raw_page: list[dict[str, Any]], market: str
) -> datetime.datetime:
    try:
        return min(
            _as_utc(
                datetime.datetime.fromisoformat(
                    row["candle_date_time_utc"].replace("Z", "+00:00")
                )
            )
            for row in raw_page
            if isinstance(row, dict) and row.get("market") == market
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"{market}: invalid candle timestamp payload") from error


async def _fetch_strict_market_candles(
    session: aiohttp.ClientSession,
    market: str,
    request: _CandleCollectionRequest,
) -> list[CandleData]:
    raw_candles: list[dict[str, Any]] = []
    remaining_count = request.count
    page_cutoff = request.cutoff

    while remaining_count:
        requested_page_size = min(remaining_count, MAX_CANDLES_PER_REQUEST)
        params = {
            "market": market,
            "count": requested_page_size,
            "to": _format_upbit_cutoff(page_cutoff),
        }
        try:
            raw_page = await _request_candle_page(
                session, request.endpoint, params, market
            )
        except UpbitAPIError as error:
            logger.error("%s: candle page collection failed: %s", market, error)
            return []
        if not raw_page:
            logger.error("%s: empty candle page returned.", market)
            return []

        raw_candles.extend(raw_page)
        remaining_count -= len(raw_page)
        if len(raw_page) < requested_page_size:
            logger.warning(
                "%s: requested %d candles but history ended early.",
                market,
                request.count,
            )
            return []
        try:
            page_cutoff = _oldest_raw_candle_timestamp(raw_page, market)
        except ValueError as error:
            logger.warning("%s: invalid candle payload: %s", market, error)
            return []

    try:
        parsed_candles = _parse_candle_page(raw_candles, market)
    except (KeyError, TypeError, ValueError) as error:
        logger.warning("%s: invalid candle payload: %s", market, error)
        return []

    complete_candles = normalize_completed_candles(
        parsed_candles,
        request.time_unit,
        request.count,
        request.minutes_unit,
        request.as_of,
    )
    if len(complete_candles) != request.count:
        logger.warning(
            "%s: rejected incomplete, duplicate, or off-grid paginated candle history.",
            market,
        )
        return []
    return complete_candles


async def _fetch_market_candles(
    session: aiohttp.ClientSession,
    market: str,
    request: _CandleCollectionRequest,
) -> tuple[str, list[CandleData]]:
    if request.synthesize_no_trade_intervals:
        candles = await _fetch_sparse_market_candles(session, market, request)
    else:
        candles = await _fetch_strict_market_candles(session, market, request)
    return market, candles


def _collect_candle_results(
    markets: list[str],
    results: list[tuple[str, list[CandleData]] | BaseException],
) -> dict[str, list[CandleData]]:
    candles_by_market: dict[str, list[CandleData]] = {}
    failed_markets: list[str] = []

    for requested_market, result in zip(markets, results, strict=True):
        if isinstance(result, BaseException):
            logger.error("'%s' candle task failed: %s", requested_market, result)
            failed_markets.append(requested_market)
            continue

        market, candles = result
        if candles:
            candles_by_market[market] = candles
        else:
            failed_markets.append(market)

    logger.info(
        "Candle collection completed: %d/%d markets succeeded.",
        len(candles_by_market),
        len(markets),
    )
    if failed_markets:
        preview = ", ".join(failed_markets[:10])
        if len(failed_markets) > 10:
            preview = f"{preview}..."
        logger.warning("Failed markets (%d): %s", len(failed_markets), preview)
    return candles_by_market


async def get_candles(
    session: aiohttp.ClientSession,
    markets: list[str],
    time_unit: CandleTimeUnit,
    count: int = 200,
    minutes_unit: int | None = None,
    as_of: datetime.datetime | None = None,
    synthesize_no_trade_intervals: bool = False,
    same_slot_lookback_weeks: int = 0,
) -> dict[str, list[CandleData]]:
    """Fetch complete candle histories for each market under one explicit contract."""
    if not markets:
        return {}

    request = _build_candle_collection_request(
        time_unit=time_unit,
        count=count,
        minutes_unit=minutes_unit,
        as_of=as_of,
        synthesize_no_trade_intervals=synthesize_no_trade_intervals,
        same_slot_lookback_weeks=same_slot_lookback_weeks,
    )
    tasks = [_fetch_market_candles(session, market, request) for market in markets]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return _collect_candle_results(markets, results)

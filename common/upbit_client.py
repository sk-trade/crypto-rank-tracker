#common/upbit_client

import asyncio
import datetime
import logging
from collections import Counter
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import Any, Dict, List, Optional
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

# --- 상수 및 사용자 정의 예외 ---
UPBIT_API_BASE_URL = "https://api.upbit.com/v1"

GLOBAL_RATE_LIMIT_PER_SECOND = 8
_LOOP_LIMITERS = WeakKeyDictionary()
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


class UpbitAPIError(Exception):
    """Upbit API 호출 실패 시 발생하는 사용자 정의 예외입니다."""

    def __init__(
        self,
        code: UpbitErrorCode,
        *,
        market: str | None = None,
        details: Dict[str, Any] | None = None,
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
        limiter = AsyncLimiter(GLOBAL_RATE_LIMIT_PER_SECOND, 1)
        _LOOP_LIMITERS[loop] = limiter
    return limiter


def _as_utc(timestamp: datetime.datetime) -> datetime.datetime:
    """Interpret Upbit's UTC timestamp strings consistently as aware UTC values."""
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=datetime.timezone.utc)
    return timestamp.astimezone(datetime.timezone.utc)


def _candle_grid(
    time_unit: CandleTimeUnit,
    minutes_unit: Optional[int],
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
        elapsed = now_utc - datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
        current_start = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc) + (
            elapsed // interval
        ) * interval
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
    minutes_unit: Optional[int],
    as_of: datetime.datetime,
) -> List[datetime.datetime]:
    """Return the ordered grid of fully closed Upbit candle start times."""
    current_start, interval = _candle_grid(time_unit, minutes_unit, as_of)
    latest_completed = current_start - interval
    first = latest_completed - interval * (count - 1)
    return [first + interval * index for index in range(count)]


def normalize_completed_candles(
    candles: List[CandleData],
    time_unit: CandleTimeUnit,
    count: int,
    minutes_unit: Optional[int] = None,
    as_of: Optional[datetime.datetime] = None,
) -> List[CandleData]:
    """Reindex to the complete candle grid, rejecting partial or gapped histories."""
    expected_starts = _expected_candle_starts(
        time_unit, count, minutes_unit, as_of or datetime.datetime.now(datetime.timezone.utc)
    )
    candles_by_start: Dict[datetime.datetime, CandleData] = {}
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
    candles: List[CandleData],
    count: int,
    minutes_unit: int,
    as_of: datetime.datetime,
) -> List[CandleData]:
    """Fill valid no-trade clock slots while rejecting malformed candle history."""
    if not candles:
        return []

    current_start, _ = _candle_grid(CandleTimeUnit.MINUTES, minutes_unit, as_of)
    expected_starts = _expected_candle_starts(
        CandleTimeUnit.MINUTES, count, minutes_unit, as_of
    )
    market = candles[0].market
    candles_by_start: Dict[datetime.datetime, CandleData] = {}
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
        candle for timestamp, candle in candles_by_start.items() if timestamp < first_expected
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
            )
        )
    return normalized


def _retry_after_seconds(value: Optional[str]) -> float:
    """Parse either legal Retry-After form and cap excessive server delays."""
    if not value:
        return 1.0
    try:
        delay = float(value)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            delay = (
                _as_utc(retry_at) - datetime.datetime.now(datetime.timezone.utc)
            ).total_seconds()
        except (TypeError, ValueError, OverflowError):
            delay = 1.0
    return min(max(delay, 0.0), MAX_RETRY_DELAY_SECONDS)


async def _request_candle_page(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    market: str,
) -> List[Dict[str, Any]]:
    """Fetch one candle page with bounded retries and explicit payload validation."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with _global_limiter():
                async with session.get(url, params=params, timeout=10) as response:
                    if response.status == 429:
                        retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                        logger.warning(
                            "%s: 429 Rate Limit. %.1f초 대기 (시도 %d/%d)",
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
                            "%s: %d 서버 에러. %d초 대기 (시도 %d/%d)",
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
                "%s: 네트워크 오류 (시도 %d/%d): %s",
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


def _parse_candle_page(page: List[Dict[str, Any]], market: str) -> List[CandleData]:
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
                }
            )
        )
    return candles


def _same_slot_candle(
    candles: List[CandleData],
    market: str,
    target: datetime.datetime,
    minutes_unit: int,
) -> Optional[CandleData]:
    """Return the target candle or a zero-volume carry-forward for a no-trade slot."""
    if not candles:
        return None
    if len(candles) != 1:
        raise ValueError(f"{market}: expected one same-slot seed candle")
    candle = candles[0]
    timestamp = _as_utc(candle.timestamp)
    if (
        _candle_grid(CandleTimeUnit.MINUTES, minutes_unit, timestamp)[0]
        != timestamp
    ):
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
    )

# --- API 호출 함수 ---
async def get_all_krw_tickers(
    session: aiohttp.ClientSession,
) -> List[MarketTicker]:
    """Upbit의 모든 KRW 마켓 티커 정보를 가져옵니다."""
    try:
        async with session.get(
            f"{UPBIT_API_BASE_URL}/market/all",
            params={"is_details": "true"},
            timeout=10,
        ) as response:
            response.raise_for_status()
            markets_data = await response.json()
            if not isinstance(markets_data, list):
                raise UpbitAPIError(UpbitErrorCode.INVALID_MARKET_RESPONSE)
            listings = []
            for row in markets_data:
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
            krw_markets = [listing.market for listing in listings]
            market_events = {
                listing.market: listing.market_event for listing in listings
            }

        if not krw_markets:
            raise UpbitAPIError(UpbitErrorCode.MARKET_LIST_EMPTY)

        params = {"markets": ",".join(krw_markets)}
        async with session.get(
            f"{UPBIT_API_BASE_URL}/ticker", params=params, timeout=15
        ) as response:
            response.raise_for_status()
            tickers = await response.json()
            if not isinstance(tickers, list) or not tickers:
                raise UpbitAPIError(UpbitErrorCode.TICKER_LIST_EMPTY)
            requested_markets = set(krw_markets)
            ticker_markets = [
                ticker.get("market") if isinstance(ticker, dict) else None
                for ticker in tickers
            ]
            market_counts = Counter(
                market for market in ticker_markets if isinstance(market, str)
            )
            missing_markets = sorted(requested_markets - set(market_counts))
            unexpected_markets = sorted(set(market_counts) - requested_markets)
            duplicate_markets = sorted(
                market for market, occurrence_count in market_counts.items() if occurrence_count != 1
            )
            invalid_rows = sum(market is None for market in ticker_markets)
            if (
                missing_markets
                or unexpected_markets
                or duplicate_markets
                or invalid_rows
                or len(tickers) != len(krw_markets)
            ):
                raise UpbitAPIError(
                    UpbitErrorCode.TICKER_SCOPE_MISMATCH,
                    details={
                        "missing": missing_markets,
                        "unexpected": unexpected_markets,
                        "duplicates": duplicate_markets,
                        "invalid_rows": invalid_rows,
                    },
                )
            parsed_tickers = []
            for ticker in tickers:
                market = ticker["market"]
                try:
                    parsed_tickers.append(
                        MarketTicker.model_validate(
                            {
                                **ticker,
                                "market_event": market_events[market],
                            }
                        )
                    )
                except ValidationError as error:
                    code = (
                        UpbitErrorCode.INVALID_TICKER_TURNOVER
                        if any(
                            tuple(issue["loc"]) == ("acc_trade_price_24h",)
                            for issue in error.errors()
                        )
                        else UpbitErrorCode.INVALID_TICKER_RESPONSE
                    )
                    raise UpbitAPIError(
                        code, market=market
                    ) from error
            return parsed_tickers

    except UpbitAPIError:
        raise
    except aiohttp.ClientError as e:
        logger.error(f"Upbit API 클라이언트 오류 (get_all_krw_tickers): {e}")
        raise UpbitAPIError(UpbitErrorCode.NETWORK_ERROR) from e
    except Exception as e:
        logger.error(f"Upbit API 예상치 못한 오류 (get_all_krw_tickers): {e}")
        raise UpbitAPIError(UpbitErrorCode.UNEXPECTED_ERROR) from e


async def get_orderbooks(
    session: aiohttp.ClientSession, markets: List[str]
) -> Dict[str, OrderBookSnapshot]:
    """Fetch orderbook snapshots; callers must fail closed for missing markets."""
    if not markets:
        return {}
    try:
        async with _global_limiter():
            async with session.get(
                f"{UPBIT_API_BASE_URL}/orderbook", params={"markets": ",".join(markets)}, timeout=15
            ) as response:
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
                            raw_book.get("market")
                            if isinstance(raw_book, dict)
                            else None
                        )
                        if market in requested_markets:
                            logger.warning(
                                "Ignoring malformed orderbook row for market %s",
                                market,
                            )
                        continue
                    if (
                        book.market not in requested_markets
                        or book.market in orderbooks
                    ):
                        continue
                    orderbooks[book.market] = book
                return orderbooks
    except (aiohttp.ClientError, ValueError) as error:
        logger.warning("Orderbook collection failed: %s", error)
        return {}


async def get_candles(
    session: aiohttp.ClientSession,
    markets: List[str],
    time_unit: CandleTimeUnit,
    count: int = 200,
    minutes_unit: Optional[int] = None,
    as_of: Optional[datetime.datetime] = None,
    synthesize_no_trade_intervals: bool = False,
    same_slot_lookback_weeks: int = 0,
) -> Dict[str, List[CandleData]]:
    """
    지정된 시간 단위(분, 일)에 대한 캔들 데이터를 병렬로 가져옵니다.

    Args:
        session: aiohttp ClientSession.
        markets: 마켓 코드 리스트.
        time_unit: `CandleTimeUnit.MINUTES` 또는 `CandleTimeUnit.DAYS`.
        count: strict mode에서는 요청할 캔들 수, sparse mode에서는 최근 clock bar 수.
        minutes_unit: 분봉일 경우, 분 단위 (e.g., 1, 3, 5, 10, ...).
        synthesize_no_trade_intervals: 거래가 없어서 생략된 최근 분봉을 carry-forward로 채웁니다.
        same_slot_lookback_weeks: sparse mode에서 별도로 조회할 이전 주 동일 시각 수.
    """
    if not markets:
        return {}
    if count <= 0 or same_slot_lookback_weeks < 0:
        raise UpbitAPIError(
            UpbitErrorCode.INVALID_CANDLE_REQUEST,
            details={"field": "count"},
        )
    if synthesize_no_trade_intervals and (
        time_unit is not CandleTimeUnit.MINUTES
        or not minutes_unit
        or count >= MAX_CANDLES_PER_REQUEST
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

    request_as_of = as_of or datetime.datetime.now(datetime.timezone.utc)
    request_cutoff, _ = _candle_grid(time_unit, minutes_unit, request_as_of)

    async def _fetch_single_market(market: str) -> tuple[str, List[CandleData]]:
        """단일 마켓의 캔들 데이터를 가져오는 내부 헬퍼 함수입니다."""
        if time_unit is CandleTimeUnit.MINUTES:
            url = f"{UPBIT_API_BASE_URL}/candles/minutes/{minutes_unit}"
        else:
            url = f"{UPBIT_API_BASE_URL}/candles/{time_unit.value}"
        if synthesize_no_trade_intervals:
            try:
                recent_page = await _request_candle_page(
                    session,
                    url,
                    {
                        "market": market,
                        "count": MAX_CANDLES_PER_REQUEST,
                        "to": request_cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                    market,
                )
                recent_source = _parse_candle_page(recent_page, market)
                recent_candles = normalize_sparse_completed_candles(
                    recent_source, count, minutes_unit, request_as_of
                )
                del recent_page, recent_source
                if len(recent_candles) != count:
                    logger.warning(
                        "%s: rejected sparse candle history without a valid seed or grid.", market
                    )
                    return market, []

                latest_completed = recent_candles[-1].timestamp
                same_slot_candles = []
                for weeks_ago in range(same_slot_lookback_weeks, 0, -1):
                    target = latest_completed - datetime.timedelta(weeks=weeks_ago)
                    cutoff = target + datetime.timedelta(minutes=minutes_unit)
                    page = await _request_candle_page(
                        session,
                        url,
                        {
                            "market": market,
                            "count": 1,
                            "to": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        },
                        market,
                    )
                    sample = _same_slot_candle(
                        _parse_candle_page(page, market), market, target, minutes_unit
                    )
                    if sample is not None:
                        same_slot_candles.append(sample)
                if len(same_slot_candles) != same_slot_lookback_weeks:
                    logger.warning(
                        "%s: rejected candle history with incomplete weekly same-slot coverage.",
                        market,
                    )
                    return market, []
                return market, [*same_slot_candles, *recent_candles]
            except (UpbitAPIError, KeyError, TypeError, ValueError) as error:
                logger.warning("%s: sparse candle collection failed: %s", market, error)
                return market, []

        raw_candles = []
        remaining = count
        to = request_cutoff

        while remaining:
            params = {"market": market, "count": min(remaining, MAX_CANDLES_PER_REQUEST)}
            if to:
                params["to"] = to.strftime("%Y-%m-%dT%H:%M:%SZ")
            try:
                page = await _request_candle_page(session, url, params, market)
            except UpbitAPIError as error:
                logger.error("%s: candle page collection failed: %s", market, error)
                return market, []
            if not page:
                logger.error(f"{market}: empty candle page returned.")
                return market, []
            raw_candles.extend(page)
            remaining -= len(page)
            if len(page) < params["count"]:
                logger.warning(f"{market}: requested {count} candles but history ended early.")
                return market, []
            oldest = min(
                datetime.datetime.fromisoformat(item["candle_date_time_utc"].replace("Z", "+00:00"))
                for item in page
            )
            to = _as_utc(oldest)

        try:
            candles = _parse_candle_page(raw_candles, market)
        except (KeyError, TypeError, ValueError) as error:
            logger.warning("%s: invalid candle payload: %s", market, error)
            return market, []
        complete_candles = normalize_completed_candles(
            candles, time_unit, count, minutes_unit, request_as_of
        )
        if len(complete_candles) != count:
            logger.warning("%s: rejected incomplete, duplicate, or off-grid paginated candle history.", market)
            return market, []
        return market, complete_candles

    tasks = [_fetch_single_market(m) for m in markets]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    candles_dict = {}
    failed_markets = []
    for i, res in enumerate(results):
        market_name = markets[i]
        if isinstance(res, Exception):
            logger.error(f"'{market_name}' 작업 중 예외 발생: {res}")
            failed_markets.append(market_name)
        elif res and isinstance(res, tuple):
            market, candles = res
            if candles:
                candles_dict[market] = candles
            else:
                failed_markets.append(market)

    success_count = len(candles_dict)
    total_count = len(markets)
    logger.info(f"캔들 수집 완료: {success_count}/{total_count}개 마켓 성공.")

    if failed_markets:
        failed_list_str = ", ".join(failed_markets[:10])
        if len(failed_markets) > 10:
            failed_list_str += "..."
        logger.warning(f"실패한 마켓 ({len(failed_markets)}개): {failed_list_str}")

    return candles_dict

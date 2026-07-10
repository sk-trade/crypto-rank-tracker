#common/upbit_client

import asyncio
import datetime
import logging
from typing import Any, Dict, List, Literal, Optional
from zoneinfo import ZoneInfo

import aiohttp
from aiolimiter import AsyncLimiter

import config
from common.models import CandleData

logger = logging.getLogger(config.APP_LOGGER_NAME)

# --- 상수 및 사용자 정의 예외 ---
UPBIT_API_BASE_URL = "https://api.upbit.com/v1"

GLOBAL_LIMITER = AsyncLimiter(8, 1) 
KST = ZoneInfo("Asia/Seoul")
MAX_CANDLES_PER_REQUEST = 200

class UpbitAPIError(Exception):
    """Upbit API 호출 실패 시 발생하는 사용자 정의 예외입니다."""

    pass


def _as_utc(timestamp: datetime.datetime) -> datetime.datetime:
    """Interpret Upbit's UTC timestamp strings consistently as aware UTC values."""
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=datetime.timezone.utc)
    return timestamp.astimezone(datetime.timezone.utc)


def _expected_candle_starts(
    time_unit: Literal["minutes", "days", "weeks", "months"],
    count: int,
    minutes_unit: Optional[int],
    as_of: datetime.datetime,
) -> List[datetime.datetime]:
    """Return the ordered grid of fully closed Upbit candle start times."""
    now_utc = _as_utc(as_of)
    if time_unit == "minutes":
        if not minutes_unit:
            raise ValueError("minutes_unit is required for minute candles")
        interval = datetime.timedelta(minutes=minutes_unit)
        elapsed = now_utc - datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
        current_start = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc) + (
            elapsed // interval
        ) * interval
        latest_completed = current_start - interval
    elif time_unit == "days":
        local_today = now_utc.astimezone(KST).replace(hour=0, minute=0, second=0, microsecond=0)
        latest_completed = (local_today - datetime.timedelta(days=1)).astimezone(datetime.timezone.utc)
        interval = datetime.timedelta(days=1)
    else:
        raise ValueError(f"Unsupported candle integrity grid: {time_unit}")

    first = latest_completed - interval * (count - 1)
    return [first + interval * index for index in range(count)]


def normalize_completed_candles(
    candles: List[CandleData],
    time_unit: Literal["minutes", "days", "weeks", "months"],
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

# --- API 호출 함수 ---
async def get_all_krw_tickers(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """Upbit의 모든 KRW 마켓 티커 정보를 가져옵니다."""
    try:
        async with session.get(
            f"{UPBIT_API_BASE_URL}/market/all", timeout=10
        ) as response:
            response.raise_for_status()
            markets_data = await response.json()
            krw_markets = [
                m["market"] for m in markets_data if m["market"].startswith("KRW-")
            ]

        if not krw_markets:
            raise UpbitAPIError("KRW 마켓 목록을 가져오지 못했습니다.")

        params = {"markets": ",".join(krw_markets)}
        async with session.get(
            f"{UPBIT_API_BASE_URL}/ticker", params=params, timeout=15
        ) as response:
            response.raise_for_status()
            tickers = await response.json()
            if not tickers:
                raise UpbitAPIError("티커 정보를 가져오지 못했습니다.")
            return tickers

    except aiohttp.ClientError as e:
        logger.error(f"Upbit API 클라이언트 오류 (get_all_krw_tickers): {e}")
        raise UpbitAPIError(f"네트워크/클라이언트 오류: {e}") from e
    except Exception as e:
        logger.error(f"Upbit API 예상치 못한 오류 (get_all_krw_tickers): {e}")
        raise UpbitAPIError(f"알 수 없는 오류: {e}") from e


async def get_candles(
    session: aiohttp.ClientSession,
    markets: List[str],
    time_unit: Literal["minutes", "days", "weeks", "months"],
    count: int = 200,
    minutes_unit: Optional[int] = None,
    as_of: Optional[datetime.datetime] = None,
) -> Dict[str, List[CandleData]]:
    """
    지정된 시간 단위(분, 일, 주, 월)에 대한 캔들 데이터를 병렬로 가져옵니다.

    Args:
        session: aiohttp ClientSession.
        markets: 마켓 코드 리스트.
        time_unit: 'minutes', 'days', 'weeks', 'months' 중 하나.
        count: 요청할 캔들 수.
        minutes_unit: 분봉일 경우, 분 단위 (e.g., 1, 3, 5, 10, ...).
    """
    if not markets:
        return {}

    limiter = GLOBAL_LIMITER

    async def _fetch_single_market(market: str) -> tuple[str, List[CandleData]]:
        """단일 마켓의 캔들 데이터를 가져오는 내부 헬퍼 함수입니다."""
        if time_unit == "minutes":
            url = f"{UPBIT_API_BASE_URL}/candles/minutes/{minutes_unit}"
        else:
            url = f"{UPBIT_API_BASE_URL}/candles/{time_unit}"
        max_retries = 3
        raw_candles = []
        remaining = count
        to: datetime.datetime | None = None

        while remaining:
            params = {"market": market, "count": min(remaining, MAX_CANDLES_PER_REQUEST)}
            if to:
                params["to"] = to.strftime("%Y-%m-%dT%H:%M:%SZ")
            page = None
            for attempt in range(max_retries):
                try:
                    async with limiter:
                        async with session.get(url, params=params, timeout=10) as response:
                            if response.status == 429:
                                retry_after = int(response.headers.get("Retry-After", 1))
                                logger.warning(
                                    f"{market}: 429 Rate Limit. {retry_after}초 대기 (시도 {attempt + 1}/{max_retries})"
                                )
                                await asyncio.sleep(retry_after)
                                continue
                            if 500 <= response.status < 600:
                                wait_time = 2**attempt
                                logger.warning(
                                    f"{market}: {response.status} 서버 에러. {wait_time}초 대기 (시도 {attempt + 1}/{max_retries})"
                                )
                                await asyncio.sleep(wait_time)
                                continue
                            response.raise_for_status()
                            page = await response.json()
                    if page:
                        break

                except aiohttp.ClientError as e:
                    logger.warning(
                        f"{market}: 네트워크 오류 (시도 {attempt + 1}/{max_retries}): {e}"
                    )
                    if attempt == max_retries - 1:
                        logger.error(f"{market}: 최종 재시도 실패 (네트워크 오류).")
                        return market, []
                    await asyncio.sleep(2**attempt)
                except Exception as e:
                    logger.error(f"{market}: 캔들 데이터 처리 중 예상치 못한 오류: {e}")
                    return market, []

            if not page:
                logger.error(f"{market}: 최대 재시도 횟수({max_retries})를 초과했습니다.")
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

        candles = [
            CandleData.model_validate({
                "market": item["market"],
                "timestamp": _as_utc(datetime.datetime.fromisoformat(item["candle_date_time_utc"].replace("Z", "+00:00"))),
                "open_price": item["opening_price"], "high_price": item["high_price"],
                "low_price": item["low_price"], "close_price": item["trade_price"],
                "volume": item["candle_acc_trade_volume"],
            }) for item in raw_candles
        ]
        complete_candles = normalize_completed_candles(candles, time_unit, count, minutes_unit, as_of)
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

#common/upbit_client

import asyncio
import logging
from typing import Any, Dict, List

import aiohttp
from aiolimiter import AsyncLimiter

import config
from common.models import CandleData

logger = logging.getLogger(config.APP_LOGGER_NAME)

# --- 상수 및 사용자 정의 예외 ---
UPBIT_API_BASE_URL = "https://api.upbit.com/v1"


class UpbitAPIError(Exception):
    """Upbit API 호출 실패 시 발생하는 사용자 정의 예외입니다."""

    pass


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


async def get_minutes_candles(
    session: aiohttp.ClientSession,
    markets: List[str],
    unit: int = 10,
    count: int = 200,
) -> Dict[str, List[CandleData]]:
    """여러 마켓의 분봉 캔들 데이터를 병렬로 가져옵니다."""
    if not markets:
        return {}

    limiter = AsyncLimiter(5, 1)

    async def _fetch_single_market(market: str) -> tuple[str, List[CandleData]]:
        """단일 마켓의 캔들 데이터를 가져오는 내부 헬퍼 함수입니다."""
        url = f"{UPBIT_API_BASE_URL}/candles/minutes/{unit}"
        params = {"market": market, "count": count}
        max_retries = 3

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
                        raw_candles = await response.json()

                        if not raw_candles:
                            logger.warning(f"{market}의 캔들 데이터가 비어 있습니다.")
                            return market, []

                        candles = [
                            CandleData.model_validate(
                                {
                                    "market": r["market"],
                                    "timestamp": r["candle_date_time_utc"],
                                    "open_price": r["opening_price"],
                                    "high_price": r["high_price"],
                                    "low_price": r["low_price"],
                                    "close_price": r["trade_price"],
                                    "volume": r["candle_acc_trade_volume"],
                                }
                            )
                            for r in raw_candles
                        ]

                        candles.sort(key=lambda c: c.timestamp)
                        return market, candles

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

        logger.error(f"{market}: 최대 재시도 횟수({max_retries})를 초과했습니다.")
        return market, []

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
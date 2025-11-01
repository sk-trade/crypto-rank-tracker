import asyncio
import aiohttp
from typing import List, Dict, Any
import logging
from common.models import CandleData 
from aiolimiter import AsyncLimiter

logger = logging.getLogger(__name__)

# 업비트 API URL 상수
UPBIT_API_BASE_URL = "https://api.upbit.com/v1"


class UpbitAPIError(Exception):
    """업비트 API 호출 실패 시 발생하는 사용자 정의 예외"""
    pass


async def get_all_krw_tickers(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """업비트의 모든 KRW 마켓 티커 정보를 가져옵니다."""
    try:
        async with session.get(f"{UPBIT_API_BASE_URL}/market/all", timeout=10) as response:
            response.raise_for_status()
            markets_data = await response.json()
            markets = [m['market'] for m in markets_data if m['market'].startswith('KRW-')]
        
        if not markets:
            raise UpbitAPIError("KRW 마켓 목록을 가져오지 못했습니다.")

        async with session.get(f"{UPBIT_API_BASE_URL}/ticker", params={'markets': ','.join(markets)}, timeout=15) as response:
            response.raise_for_status()
            tickers = await response.json()
            if not tickers:
                raise UpbitAPIError("티커 정보를 가져오지 못했습니다.")
            return tickers
            
    except aiohttp.ClientError as e:
        logger.error(f"Upbit API 클라이언트 오류: {e}")
        raise UpbitAPIError(f"네트워크/클라이언트 오류: {e}") from e
    except Exception as e:
        logger.error(f"Upbit API 예상치 못한 오류: {e}")
        raise UpbitAPIError(f"알 수 없는 오류: {e}") from e


async def get_minutes_candles(
    session: aiohttp.ClientSession,
    markets: List[str],
    unit: int = 10,
    count: int = 200
) -> Dict[str, List[CandleData]]:
    """여러 마켓의 분봉 캔들 데이터를 병렬로 가져옵니다."""
    if not markets:
        return {}
    
    limiter = AsyncLimiter(5, 1)
    async def fetch_single_market(market: str) -> tuple[str, List[CandleData]]:
        """단일 마켓의 캔들 데이터를 가져오는 내부 함수."""
        url = f"{UPBIT_API_BASE_URL}/candles/minutes/{unit}"
        params = {'market': market, 'count': count}
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                async with limiter:
                    async with session.get(url, params=params, timeout=10) as response:
                        # 429 처리: Retry-After 헤더 확인
                        if response.status == 429:
                            retry_after = int(response.headers.get('Retry-After', 1))
                            logger.warning(f"{market} - 429 Too Many Requests. {retry_after}초 대기 (시도 {attempt+1}/{max_retries})")
                            await asyncio.sleep(retry_after)
                            continue  # 재시도
                        
                        # 5xx 서버 에러: 지수 백오프
                        if 500 <= response.status < 600:
                            wait_time = 2 ** attempt  # 1, 2, 4초
                            logger.warning(f"{market} - {response.status} 서버 에러. {wait_time}초 대기 (시도 {attempt+1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        # 그 외 에러는 즉시 실패
                        response.raise_for_status()
                        raw_candles = await response.json()
                        
                        if not raw_candles:
                            logger.warning(f"{market} 캔들 데이터가 비어 있습니다.")
                            return market, []
                        
                        # CandleData 객체로 변환
                        candles = [CandleData.model_validate({
                            'market': market,
                            'timestamp': r['candle_date_time_utc'],
                            'open_price': r['opening_price'],
                            'high_price': r['high_price'],
                            'low_price': r['low_price'],
                            'close_price': r['trade_price'],
                            'volume': r['candle_acc_trade_volume']
                        }) for r in raw_candles]
                        
                        # 시간순 정렬
                        candles.sort(key=lambda c: c.timestamp)
                        logger.debug(f"{market} - {len(candles)}개 캔들 수집 완료")
                        return market, candles
                        
            except aiohttp.ClientError as e:
                if attempt == max_retries - 1:
                    logger.error(f"{market} 최종 실패 (네트워크 오류): {e}")
                    return market, []
                logger.warning(f"{market} 네트워크 오류 (시도 {attempt+1}/{max_retries}): {e}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"{market} 예상치 못한 오류: {e}")
                return market, []
        
        # 최대 재시도 초과
        logger.error(f"{market} 최대 재시도 횟수 초과")
        return market, []
    
    # 모든 마켓에 대해 병렬 실행
    tasks = [fetch_single_market(m) for m in markets]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # 결과 수집
    candles_dict = {}
    failed_markets = []
    
    for i, result in enumerate(results):
        market_name = markets[i]
        if isinstance(result, Exception):
            logger.error(f"'{market_name}' 예외 발생: {type(result).__name__} - {result}")
            failed_markets.append(market_name)
        elif result and isinstance(result, tuple):
            market, candles = result
            if candles:
                candles_dict[market] = candles
            else:
                failed_markets.append(market)
    
    success_count = len(candles_dict)
    total_count = len(markets)
    logger.info(f"캔들 수집 완료: {success_count}/{total_count} 성공")
    
    if failed_markets:
        logger.warning(f"실패한 마켓 ({len(failed_markets)}개): {', '.join(failed_markets[:10])}")
    
    return candles_dict

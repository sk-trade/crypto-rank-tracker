import aiohttp
from typing import List, Dict, Any
import logging
import tenacity

logger = logging.getLogger(__name__)

# 업비트 API URL 상수
UPBIT_API_BASE_URL = "https://api.upbit.com/v1"

class UpbitAPIError(Exception):
    """업비트 API 호출 실패 시 발생하는 사용자 정의 예외"""
    pass

# 재시도 데코레이터 설정
retry_decarator = tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=10),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type((aiohttp.ClientError, UpbitAPIError)),
    reraise=True 
)

@retry_decarator
async def get_all_krw_tickers(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """업비트의 모든 KRW 마켓 티커 정보를 가져옵니다. """
    try:
        # 모든 마켓 코드 조회
        async with session.get(f"{UPBIT_API_BASE_URL}/market/all", timeout=10) as response:
            response.raise_for_status()
            markets_data = await response.json()
            markets = [m['market'] for m in markets_data if m['market'].startswith('KRW-')]
        
        if not markets:
            raise UpbitAPIError("KRW 마켓 목록을 가져오지 못했습니다.")

        # 모든 KRW 마켓의 티커 정보 일괄 조회
        async with session.get(f"{UPBIT_API_BASE_URL}/ticker", params={'markets': ','.join(markets)}, timeout=15) as response:
            response.raise_for_status()
            tickers = await response.json()
            if not tickers:
                raise UpbitAPIError("티커 정보를 가져오지 못했습니다 (결과가 비어 있음).")
            return tickers
            
    except aiohttp.ClientError as e:
        logger.error(f"Upbit API 클라이언트 오류: {e}")
        raise UpbitAPIError(f"네트워크/클라이언트 오류: {e}") from e
    except Exception as e:
        logger.error(f"Upbit API에서 티커 정보를 가져오는 중 예상치 못한 오류 발생: {e}")
        raise UpbitAPIError(f"알 수 없는 오류: {e}") from e
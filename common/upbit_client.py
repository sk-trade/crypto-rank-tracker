import aiohttp
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

# 업비트 API URL 상수
UPBIT_API_BASE_URL = "https://api.upbit.com/v1"

async def get_all_krw_tickers(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """업비트의 모든 KRW 마켓 티커 정보를 가져옵니다."""
    try:
        # 1. 모든 마켓 코드 조회
        async with session.get(f"{UPBIT_API_BASE_URL}/market/all") as response:
            response.raise_for_status()
            markets = [m['market'] for m in await response.json() if m['market'].startswith('KRW-')]
        
        # 2. 모든 KRW 마켓의 티커 정보 일괄 조회
        async with session.get(f"{UPBIT_API_BASE_URL}/ticker", params={'markets': ','.join(markets)}) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        logger.error(f"Error fetching tickers from Upbit: {e}")
        return []
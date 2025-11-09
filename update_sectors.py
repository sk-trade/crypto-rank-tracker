#update_sectors

import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp
from tqdm.asyncio import tqdm_asyncio

import config
from common.storage_client import save_json

# --- 로거 및 상수 설정 ---
logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")

CG_API_KEY = config.CG_API_KEY
UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all"
CG_BASE_URL = "https://api.coingecko.com/api/v3"
CG_COINS_LIST_URL = f"{CG_BASE_URL}/coins/list"
CG_COIN_DETAIL_URL = f"{CG_BASE_URL}/coins/"


# --- RateLimiter 클래스 ---
class RateLimiter:
    """CoinGecko API의 호출 속도를 제어합니다."""

    def __init__(self, calls_per_minute: int = 28):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = time.time()
        self.lock = asyncio.Lock()

    async def wait(self):
        """API 호출 전, 필요한 경우 대기합니다."""
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_call_time
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self.last_call_time = time.time()


rate_limiter = RateLimiter(calls_per_minute=28)


# --- API 호출 함수들 ---
async def get_upbit_krw_markets(session: aiohttp.ClientSession) -> Dict[str, str]:
    """Upbit의 모든 KRW 마켓 정보를 가져옵니다."""
    try:
        async with session.get(UPBIT_MARKET_URL) as response:
            response.raise_for_status()
            markets = await response.json()
            return {
                m["market"].split("-")[1].lower(): m["market"]
                for m in markets
                if m["market"].startswith("KRW-")
            }
    except Exception as e:
        logging.error(f"Upbit 마켓 목록 조회 실패: {e}")
        return {}


async def get_coingecko_coins_list(session: aiohttp.ClientSession) -> Dict[str, str]:
    """CoinGecko의 모든 코인 목록(symbol -> id)을 가져옵니다."""
    try:
        await rate_limiter.wait()

        headers = {"x-cg-demo-api-key": CG_API_KEY}
        async with session.get(CG_COINS_LIST_URL, headers=headers) as response:
            response.raise_for_status()
            coins = await response.json()
            return {c["symbol"].lower(): c["id"] for c in coins}
    except Exception as e:
        logging.error(f"CoinGecko 코인 목록 조회 실패: {e}")
        return {}


async def get_coin_categories(
    session: aiohttp.ClientSession, coin_id: str
) -> Optional[List[str]]:
    """특정 코인의 카테고리(섹터) 정보를 가져옵니다."""
    await rate_limiter.wait()

    url = f"{CG_COIN_DETAIL_URL}{coin_id}"
    headers = {"x-cg-demo-api-key": CG_API_KEY}
    max_retries = 3

    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("categories", [])
                elif response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    logging.warning(f"Rate limit ({coin_id}). {retry_after}초 대기...")
                    await asyncio.sleep(retry_after)
                    continue
                elif response.status == 404:
                    logging.warning(f"CoinGecko에서 {coin_id}를 찾을 수 없습니다 (404).")
                    return None
        except Exception as e:
            logging.error(f"카테고리 조회 중 에러 ({coin_id}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(5)

    return ["Untagged", "API_Error"]


async def tag_market(
    session: aiohttp.ClientSession,
    symbol: str,
    market_name: str,
    cg_symbol_to_id: Dict[str, str],
) -> Tuple[str, List[str]]:
    """단일 Upbit 마켓에 CoinGecko 카테고리를 태깅합니다."""
    if symbol in cg_symbol_to_id:
        coin_id = cg_symbol_to_id[symbol]
        categories = await get_coin_categories(session, coin_id)
        if categories is not None:
            return market_name, categories if categories else ["Untagged", "No_Category"]
        else:
            return market_name, ["Untagged", "Lookup_Failed"]
    else:
        return market_name, ["Untagged", "CG_Not_Found"]


async def main():
    """스크립트의 메인 실행 함수입니다."""
    start_time = datetime.now()
    gcs_client = None

    if config.STATE_STORAGE_METHOD == "GCS":
        try:
            from google.cloud import storage

            gcs_client = storage.Client()
            logging.info("GCS 저장 모드로 실행됩니다.")
        except ImportError:
            logging.error("GCS 모듈이 설치되지 않았습니다. 로컬 저장으로 대체합니다.")
        except Exception as e:
            logging.error(f"GCS 클라이언트 초기화 실패: {e}. 로컬 파일 저장으로 대체합니다.")
    else:
        logging.info("로컬 파일 저장 모드로 실행됩니다.")

    async with aiohttp.ClientSession() as session:
        logging.info("1. Upbit KRW 마켓 목록 가져오기...")
        upbit_markets = await get_upbit_krw_markets(session)
        if not upbit_markets:
            return
        logging.info(f"   -> {len(upbit_markets)}개 KRW 마켓 확인.")

        logging.info("2. CoinGecko 전체 코인 목록 가져오기...")
        cg_symbol_to_id = await get_coingecko_coins_list(session)
        if not cg_symbol_to_id:
            return
        logging.info(f"   -> {len(cg_symbol_to_id)}개 코인 ID 확인.")

        logging.info(f"3. {len(upbit_markets)}개 마켓에 대한 자동 태깅 시작...")
        tasks = [
            tag_market(session, symbol, market_name, cg_symbol_to_id)
            for symbol, market_name in upbit_markets.items()
        ]
        results = await tqdm_asyncio.gather(*tasks, desc="태깅 진행 중")
        sector_map = dict(results)

        logging.info("4. sectors.json 파일 저장 시작...")
        try:
            await save_json(config.SECTOR_MAP_FILE_NAME, sector_map, gcs_client)
            storage_type = "GCS" if gcs_client else "로컬"
            logging.info(f"'{config.SECTOR_MAP_FILE_NAME}' 파일 저장 완료 ({storage_type}).")
        except Exception as e:
            logging.error(f"'{config.SECTOR_MAP_FILE_NAME}' 파일 저장 중 오류 발생: {e}")

        _print_summary(sector_map, start_time)


def _print_summary(sector_map: Dict, start_time: datetime):
    """작업 완료 후 최종 요약 정보를 출력합니다."""
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    total_count = len(sector_map)
    if total_count == 0:
        logging.warning("처리된 마켓이 없어 요약을 생략합니다.")
        return

    tagged_count = sum(1 for tags in sector_map.values() if "Untagged" not in tags)
    untagged_count = total_count - tagged_count

    summary = f"""
============================================================
     자동 태깅 작업 완료 - 최종 요약
============================================================
  - 총 대상 마켓: {total_count}개
  - ✅ 성공적으로 태깅: {tagged_count}개 ({tagged_count/total_count:.1%})
  - ❌ 태깅 실패/누락: {untagged_count}개
  - ⏱️ 총 소요 시간: {elapsed:.2f}초 ({elapsed/60:.2f}분)
  - ⚙️ 평균 처리 시간: {elapsed/total_count:.2f}초/개
============================================================
'sectors.json' 파일이 생성되었습니다.
"""
    print(summary)


if __name__ == "__main__":
    asyncio.run(main())
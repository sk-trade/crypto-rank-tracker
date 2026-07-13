#update_sectors

import asyncio
import logging
import time
import json
import math
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import aiohttp
from tqdm.asyncio import tqdm_asyncio

import config
from common.storage_client import create_gcs_client, load_json, save_json

# --- 로거 및 상수 설정 ---
logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")

CG_API_KEY = config.CG_API_KEY
UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all"
CG_BASE_URL = "https://api.coingecko.com/api/v3"
CG_COINS_LIST_URL = f"{CG_BASE_URL}/coins/list"
CG_COIN_DETAIL_URL = f"{CG_BASE_URL}/coins/"
CG_SYMBOL_OVERRIDES = json.loads(__import__("os").environ.get("CG_SYMBOL_OVERRIDES", "{}"))
SECTOR_MAP_ROLLBACK_FILE_NAME = "sectors.previous.json"
MAX_SECTOR_MAP_CHANGE_RATIO = 0.30
MIN_SECTOR_BOOTSTRAP_USABLE_RATIO = 0.50
TRANSIENT_SECTOR_TAGS = {"API_Error", "Lookup_Failed", "Invalid_Category"}


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


def validate_coin_identity(upbit_name: str | None, coin: Dict, override: Dict | None = None) -> bool:
    """Require an explicit name and optional network/platform match before tagging."""
    expected_name = (override or {}).get("name") or upbit_name
    if expected_name and coin.get("name", "").casefold() != expected_name.casefold():
        return False
    expected_network = (override or {}).get("network")
    if expected_network and expected_network not in (coin.get("platforms") or {}):
        return False
    return True


# --- API 호출 함수들 ---
async def get_upbit_krw_markets(session: aiohttp.ClientSession) -> Dict[str, Dict]:
    """Upbit의 모든 KRW 마켓 정보를 가져옵니다."""
    try:
        async with session.get(UPBIT_MARKET_URL) as response:
            response.raise_for_status()
            markets = await response.json()
            return {
                m["market"].split("-")[1].lower(): {"market": m["market"], "english_name": m.get("english_name")}
                for m in markets
                if m["market"].startswith("KRW-")
            }
    except Exception as e:
        logging.error(f"Upbit 마켓 목록 조회 실패: {e}")
        return {}


async def get_coingecko_coins_list(session: aiohttp.ClientSession) -> Dict[str, List[str]]:
    """Return every CoinGecko id for a symbol; never silently choose a collision."""
    try:
        await rate_limiter.wait()

        headers = {"x-cg-demo-api-key": CG_API_KEY}
        async with session.get(CG_COINS_LIST_URL, headers=headers) as response:
            response.raise_for_status()
            coins = await response.json()
            mapping: Dict[str, List[str]] = {}
            for coin in coins:
                mapping.setdefault(coin["symbol"].lower(), []).append(coin["id"])
            return mapping
    except Exception as e:
        logging.error(f"CoinGecko 코인 목록 조회 실패: {e}")
        return {}


async def get_coin_detail(
    session: aiohttp.ClientSession, coin_id: str
) -> Optional[Dict]:
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
                    return data
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

    return {"categories": ["Untagged", "API_Error"]}


async def tag_market(
    session: aiohttp.ClientSession,
    symbol: str,
    market_name: str,
    cg_symbol_to_id: Dict[str, List[str]],
    upbit_name: str | None = None,
) -> Tuple[str, List[str]]:
    """단일 Upbit 마켓에 CoinGecko 카테고리를 태깅합니다."""
    if symbol in cg_symbol_to_id:
        candidates = cg_symbol_to_id[symbol]
        configured_override = CG_SYMBOL_OVERRIDES.get(symbol)
        override = (
            configured_override
            if isinstance(configured_override, dict)
            else {"id": configured_override}
            if configured_override
            else None
        )
        coin_id = (override or {}).get("id")
        if coin_id and coin_id not in candidates:
            return market_name, ["Untagged", "Override_Invalid"]
        if not coin_id and len(candidates) != 1:
            return market_name, ["Untagged", "CG_Symbol_Ambiguous"]
        coin_id = coin_id or candidates[0]
        detail = await get_coin_detail(session, coin_id)
        if detail is not None:
            if not validate_coin_identity(upbit_name, detail, override):
                return market_name, ["Untagged", "Identity_Mismatch"]
            categories = detail.get("categories", [])
            if categories and (
                not isinstance(categories, list)
                or any(
                    not isinstance(category, str) or not category.strip()
                    for category in categories
                )
            ):
                logging.error("CoinGecko category schema is invalid for %s", coin_id)
                return market_name, ["Untagged", "Invalid_Category"]
            return market_name, categories if categories else ["Untagged", "No_Category"]
        else:
            return market_name, ["Untagged", "Lookup_Failed"]
    else:
        return market_name, ["Untagged", "CG_Not_Found"]


def validate_sector_map_change(previous: Dict, proposed: Dict) -> None:
    """Reject a suspicious bulk remap instead of replacing a known-good map."""
    if not previous:
        return
    shared = set(previous) & set(proposed)
    changed = sum(previous[market] != proposed[market] for market in shared)
    removed = len(set(previous) - set(proposed))
    ratio = (changed + removed) / len(previous)
    if ratio > MAX_SECTOR_MAP_CHANGE_RATIO:
        raise RuntimeError(f"Sector map change ratio {ratio:.1%} exceeds the {MAX_SECTOR_MAP_CHANGE_RATIO:.0%} safety limit.")


def validate_sector_map_schema(sector_map: Dict, *, label: str) -> None:
    """Require the canonical sector map to remain a non-empty dict[str, list[str]]."""
    if not isinstance(sector_map, dict) or not sector_map:
        raise RuntimeError(f"Sector map {label} schema must be a non-empty JSON object.")
    invalid_markets = [
        market
        for market, tags in sector_map.items()
        if not isinstance(market, str)
        or not market.startswith("KRW-")
        or not isinstance(tags, list)
        or not tags
        or any(not isinstance(tag, str) or not tag.strip() for tag in tags)
    ]
    if invalid_markets:
        raise RuntimeError(
            f"Sector map {label} schema is invalid for {len(invalid_markets)} market(s)."
        )


def validate_sector_map_bootstrap(previous: Dict, proposed: Dict) -> None:
    """Require meaningful usable coverage before publishing the first canonical map."""
    if previous:
        return
    usable_markets = [
        market
        for market, tags in proposed.items()
        if isinstance(tags, list)
        and tags
        and "Untagged" not in tags
        and not TRANSIENT_SECTOR_TAGS.intersection(tags)
    ]
    if not usable_markets:
        raise RuntimeError(
            "Sector map bootstrap has no usable CoinGecko categories; canonical save aborted."
        )
    required_usable = math.ceil(
        len(proposed) * MIN_SECTOR_BOOTSTRAP_USABLE_RATIO
    )
    if len(usable_markets) < required_usable:
        ratio = len(usable_markets) / len(proposed)
        raise RuntimeError(
            f"Sector map bootstrap coverage {ratio:.1%} is below the "
            f"{MIN_SECTOR_BOOTSTRAP_USABLE_RATIO:.0%} minimum."
        )


async def save_validated_sector_map(sector_map: Dict, gcs_client=None) -> None:
    previous = await load_json(config.SECTOR_MAP_FILE_NAME, gcs_client)
    previous = previous if isinstance(previous, dict) else {}
    if previous:
        validate_sector_map_schema(previous, label="existing")
    validate_sector_map_schema(sector_map, label="proposed")
    validate_sector_map_bootstrap(previous, sector_map)
    sector_map = {
        market: previous[market]
        if market in previous and TRANSIENT_SECTOR_TAGS.intersection(tags)
        else tags
        for market, tags in sector_map.items()
    }
    validate_sector_map_change(previous, sector_map)
    if previous:
        await save_json(SECTOR_MAP_ROLLBACK_FILE_NAME, previous, gcs_client)
    try:
        await save_json(config.SECTOR_MAP_FILE_NAME, sector_map, gcs_client)
    except Exception:
        if previous:
            await save_json(config.SECTOR_MAP_FILE_NAME, previous, gcs_client)
        raise


async def main():
    """스크립트의 메인 실행 함수입니다."""
    config.validate_storage_config()

    start_time = datetime.now()
    gcs_client = None

    if config.STATE_STORAGE_METHOD == "GCS":
        try:
            gcs_client = create_gcs_client()
            logging.info("GCS 저장 모드로 실행됩니다.")
        except ImportError as e:
            raise RuntimeError(
                "GCS 모드로 설정되었으나 google-cloud-storage 라이브러리가 설치되지 않았습니다."
            ) from e
        except Exception as e:
            raise RuntimeError(f"GCS 클라이언트 초기화 실패: {e}") from e
    else:
        logging.info("로컬 파일 저장 모드로 실행됩니다.")

    async with aiohttp.ClientSession() as session:
        logging.info("1. Upbit KRW 마켓 목록 가져오기...")
        upbit_markets = await get_upbit_krw_markets(session)
        if not upbit_markets:
            logging.error("Upbit KRW 마켓 목록이 비어 있습니다.")
            raise RuntimeError("Upbit KRW 마켓 목록 조회 결과가 비어 있습니다.")
        logging.info(f"   -> {len(upbit_markets)}개 KRW 마켓 확인.")

        logging.info("2. CoinGecko 전체 코인 목록 가져오기...")
        cg_symbol_to_id = await get_coingecko_coins_list(session)
        if not cg_symbol_to_id:
            logging.error("CoinGecko 코인 목록이 비어 있습니다.")
            raise RuntimeError("CoinGecko 코인 목록 조회 결과가 비어 있습니다.")
        logging.info(f"   -> {len(cg_symbol_to_id)}개 코인 ID 확인.")

        logging.info(f"3. {len(upbit_markets)}개 마켓에 대한 자동 태깅 시작...")
        tasks = [
            tag_market(
                session, symbol,
                market_info["market"] if isinstance(market_info, dict) else market_info,
                cg_symbol_to_id,
                market_info.get("english_name") if isinstance(market_info, dict) else None,
            )
            for symbol, market_info in upbit_markets.items()
        ]
        results = await tqdm_asyncio.gather(*tasks, desc="태깅 진행 중")
        sector_map = dict(results)

        logging.info("4. sectors.json 파일 저장 시작...")
        try:
            await save_validated_sector_map(sector_map, gcs_client)
            storage_type = "GCS" if gcs_client else "로컬"
            logging.info(f"'{config.SECTOR_MAP_FILE_NAME}' 파일 저장 완료 ({storage_type}).")
        except Exception as e:
            logging.error(f"'{config.SECTOR_MAP_FILE_NAME}' 파일 저장 중 오류 발생: {e}")
            raise

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

#update_sectors

import asyncio
import logging
import time
import json
import math
import os
from datetime import datetime
from enum import StrEnum
from typing import Any, Dict, List, Optional

import aiohttp
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError
from tqdm.asyncio import tqdm_asyncio

import config
from common.models import (
    MarketListing,
    SectorMap,
    SectorTagBatch,
    SectorTagResult,
    SectorTagStatus,
    UNTAGGED_SECTOR_CATEGORY,
    canonicalize_sector_categories,
)
from common.storage_client import (
    StateBackendUnavailable,
    StateErrorCode,
    create_gcs_client,
    load_json,
    save_json,
)

# --- 로거 및 상수 설정 ---
logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")

CG_API_KEY = config.CG_API_KEY
UPBIT_MARKET_URL = "https://api.upbit.com/v1/market/all"
CG_BASE_URL = "https://api.coingecko.com/api/v3"
CG_COINS_LIST_URL = f"{CG_BASE_URL}/coins/list"
CG_COIN_DETAIL_URL = f"{CG_BASE_URL}/coins/"
SECTOR_MAP_ROLLBACK_FILE_NAME = "sectors.previous.json"
MAX_SECTOR_MAP_CHANGE_RATIO = 0.30
MIN_SECTOR_BOOTSTRAP_USABLE_RATIO = 0.50


class SectorUpdateErrorCode(StrEnum):
    INVALID_OVERRIDE_CONFIG = "invalid_override_config"
    INVALID_TAG_RESULTS = "invalid_tag_results"
    UPBIT_MARKETS_UNAVAILABLE = "upbit_markets_unavailable"
    COINGECKO_LIST_UNAVAILABLE = "coingecko_list_unavailable"
    COIN_DETAIL_UNAVAILABLE = "coin_detail_unavailable"
    INVALID_EXISTING_MAP = "invalid_existing_map"
    SUSPICIOUS_MAP_CHANGE = "suspicious_map_change"
    BOOTSTRAP_COVERAGE_INSUFFICIENT = "bootstrap_coverage_insufficient"


class SectorUpdateError(RuntimeError):
    def __init__(
        self, code: SectorUpdateErrorCode, *, detail: str | None = None
    ):
        super().__init__(detail or code.value)
        self.code = code
        self.detail = detail


class CoinGeckoOverride(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(min_length=1)
    name: str | None = Field(default=None, min_length=1)
    network: str | None = Field(default=None, min_length=1)


class CoinGeckoListing(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)


class CoinGeckoDetail(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    name: str = ""
    platforms: Dict[str, Any] = Field(default_factory=dict)
    categories: Any = None


_override_map_adapter = TypeAdapter(Dict[str, CoinGeckoOverride])


def parse_symbol_overrides(raw_value: str | None) -> Dict[str, CoinGeckoOverride]:
    """Parse the optional symbol override mapping, treating blank config as absent."""
    raw_value = (raw_value or "").strip()
    if not raw_value:
        return {}
    try:
        overrides = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise SectorUpdateError(
            SectorUpdateErrorCode.INVALID_OVERRIDE_CONFIG
        ) from error
    if not isinstance(overrides, dict):
        raise SectorUpdateError(SectorUpdateErrorCode.INVALID_OVERRIDE_CONFIG)
    normalized = {
        symbol: {"id": override} if isinstance(override, str) else override
        for symbol, override in overrides.items()
    }
    try:
        return _override_map_adapter.validate_python(normalized)
    except ValidationError as error:
        raise SectorUpdateError(
            SectorUpdateErrorCode.INVALID_OVERRIDE_CONFIG
        ) from error


CG_SYMBOL_OVERRIDES = parse_symbol_overrides(os.environ.get("CG_SYMBOL_OVERRIDES"))


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


def validate_coin_identity(
    coin: CoinGeckoDetail,
    override: CoinGeckoOverride | None = None,
) -> bool:
    """Validate only the explicit constraints attached to an operator override."""
    if override is None:
        return True
    expected_name = override.name
    if expected_name and coin.name.casefold() != expected_name.casefold():
        return False
    expected_network = override.network
    if expected_network and expected_network not in coin.platforms:
        return False
    return True


# --- API 호출 함수들 ---
async def get_upbit_krw_markets(
    session: aiohttp.ClientSession,
) -> Dict[str, MarketListing]:
    """Upbit의 모든 KRW 마켓 정보를 가져옵니다."""
    try:
        async with session.get(UPBIT_MARKET_URL) as response:
            response.raise_for_status()
            markets = await response.json()
            if not isinstance(markets, list):
                raise SectorUpdateError(
                    SectorUpdateErrorCode.UPBIT_MARKETS_UNAVAILABLE
                )
            listings = {}
            for raw_market in markets:
                if (
                    not isinstance(raw_market, dict)
                    or not isinstance(raw_market.get("market"), str)
                ):
                    raise SectorUpdateError(
                        SectorUpdateErrorCode.UPBIT_MARKETS_UNAVAILABLE
                    )
                if not raw_market["market"].startswith("KRW-"):
                    continue
                listing = MarketListing.model_validate(raw_market)
                symbol = listing.market.removeprefix("KRW-").lower()
                listings[symbol] = listing
            return listings
    except SectorUpdateError:
        raise
    except Exception as e:
        logging.error(f"Upbit 마켓 목록 조회 실패: {e}")
        raise SectorUpdateError(
            SectorUpdateErrorCode.UPBIT_MARKETS_UNAVAILABLE, detail=str(e)
        ) from e


async def get_coingecko_coins_list(session: aiohttp.ClientSession) -> Dict[str, List[str]]:
    """Return every CoinGecko id for a symbol; never silently choose a collision."""
    try:
        await rate_limiter.wait()

        headers = {"x-cg-demo-api-key": CG_API_KEY}
        async with session.get(CG_COINS_LIST_URL, headers=headers) as response:
            response.raise_for_status()
            coins = await response.json()
            if not isinstance(coins, list):
                raise SectorUpdateError(
                    SectorUpdateErrorCode.COINGECKO_LIST_UNAVAILABLE
                )
            mapping: Dict[str, List[str]] = {}
            for raw_coin in coins:
                coin = CoinGeckoListing.model_validate(raw_coin)
                mapping.setdefault(coin.symbol.casefold(), []).append(coin.id)
            return mapping
    except SectorUpdateError:
        raise
    except Exception as e:
        logging.error(f"CoinGecko 코인 목록 조회 실패: {e}")
        raise SectorUpdateError(
            SectorUpdateErrorCode.COINGECKO_LIST_UNAVAILABLE, detail=str(e)
        ) from e


async def get_coin_detail(
    session: aiohttp.ClientSession, coin_id: str
) -> Optional[CoinGeckoDetail]:
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
                    return CoinGeckoDetail.model_validate(data)
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

    raise SectorUpdateError(
        SectorUpdateErrorCode.COIN_DETAIL_UNAVAILABLE, detail=coin_id
    )


async def tag_market(
    session: aiohttp.ClientSession,
    symbol: str,
    market_name: str,
    cg_symbol_to_id: Dict[str, List[str]],
) -> SectorTagResult:
    """단일 Upbit 마켓에 CoinGecko 카테고리를 태깅합니다."""
    if symbol in cg_symbol_to_id:
        candidates = cg_symbol_to_id[symbol]
        override = CG_SYMBOL_OVERRIDES.get(symbol)
        coin_id = override.id if override else None
        if coin_id and coin_id not in candidates:
            return SectorTagResult(
                market=market_name, status=SectorTagStatus.OVERRIDE_INVALID
            )
        if not coin_id and len(candidates) != 1:
            return SectorTagResult(
                market=market_name, status=SectorTagStatus.SYMBOL_AMBIGUOUS
            )
        coin_id = coin_id or candidates[0]
        try:
            detail = await get_coin_detail(session, coin_id)
        except SectorUpdateError as error:
            if error.code is not SectorUpdateErrorCode.COIN_DETAIL_UNAVAILABLE:
                raise
            return SectorTagResult(
                market=market_name, status=SectorTagStatus.LOOKUP_FAILED
            )
        if detail is not None:
            if not validate_coin_identity(detail, override):
                return SectorTagResult(
                    market=market_name, status=SectorTagStatus.IDENTITY_MISMATCH
                )
            categories = detail.categories
            if not isinstance(categories, list) or any(
                not isinstance(category, str) or not category.strip()
                for category in categories
            ):
                logging.error("CoinGecko category schema is invalid for %s", coin_id)
                return SectorTagResult(
                    market=market_name, status=SectorTagStatus.INVALID_CATEGORY
                )
            if categories:
                return SectorTagResult(
                    market=market_name,
                    status=SectorTagStatus.TAGGED,
                    categories=categories,
                )
            return SectorTagResult(
                market=market_name, status=SectorTagStatus.NO_CATEGORY
            )
        else:
            return SectorTagResult(
                market=market_name, status=SectorTagStatus.LOOKUP_NOT_FOUND
            )
    else:
        return SectorTagResult(
            market=market_name, status=SectorTagStatus.SYMBOL_NOT_FOUND
        )


def validate_sector_map_change(previous: Dict, proposed: Dict) -> None:
    """Reject a suspicious bulk remap instead of replacing a known-good map."""
    if not previous:
        return
    shared = set(previous) & set(proposed)
    changed = sum(previous[market] != proposed[market] for market in shared)
    removed = len(set(previous) - set(proposed))
    ratio = (changed + removed) / len(previous)
    if ratio > MAX_SECTOR_MAP_CHANGE_RATIO:
        raise SectorUpdateError(
            SectorUpdateErrorCode.SUSPICIOUS_MAP_CHANGE,
            detail=f"change_ratio={ratio:.6f}",
        )


def validate_sector_map_bootstrap(
    previous: Dict[str, List[str]], results: List[SectorTagResult]
) -> None:
    """Require meaningful usable coverage before publishing the first canonical map."""
    if previous:
        return
    usable_markets = [
        result.market
        for result in results
        if result.status is SectorTagStatus.TAGGED
    ]
    if not usable_markets:
        raise SectorUpdateError(
            SectorUpdateErrorCode.BOOTSTRAP_COVERAGE_INSUFFICIENT,
            detail="usable_markets=0",
        )
    required_usable = math.ceil(
        len(results) * MIN_SECTOR_BOOTSTRAP_USABLE_RATIO
    )
    if len(usable_markets) < required_usable:
        ratio = len(usable_markets) / len(results)
        raise SectorUpdateError(
            SectorUpdateErrorCode.BOOTSTRAP_COVERAGE_INSUFFICIENT,
            detail=f"usable_ratio={ratio:.6f}",
        )


def build_sector_map(
    previous: Dict[str, List[str]], results: List[SectorTagResult]
) -> Dict[str, List[str]]:
    transient_statuses = {
        SectorTagStatus.LOOKUP_FAILED,
        SectorTagStatus.LOOKUP_NOT_FOUND,
        SectorTagStatus.INVALID_CATEGORY,
    }
    sector_map = {}
    for result in results:
        if result.status is SectorTagStatus.TAGGED:
            sector_map[result.market] = canonicalize_sector_categories(
                result.categories
            )
        elif result.status in transient_statuses and result.market in previous:
            sector_map[result.market] = canonicalize_sector_categories(
                previous[result.market]
            )
        else:
            sector_map[result.market] = [UNTAGGED_SECTOR_CATEGORY]
    return sector_map


async def save_validated_sector_map(
    results: List[SectorTagResult], gcs_client=None
) -> Dict[str, List[str]]:
    try:
        validated_results = SectorTagBatch.model_validate(results).root
    except ValidationError as error:
        raise SectorUpdateError(SectorUpdateErrorCode.INVALID_TAG_RESULTS) from error
    previous = await load_json(config.SECTOR_MAP_FILE_NAME, gcs_client)
    if previous is None:
        previous_map = {}
    else:
        try:
            previous_map = SectorMap.model_validate(previous).root
        except ValidationError as error:
            raise SectorUpdateError(
                SectorUpdateErrorCode.INVALID_EXISTING_MAP
            ) from error
    validate_sector_map_bootstrap(previous_map, validated_results)
    sector_map = build_sector_map(previous_map, validated_results)
    try:
        SectorMap.model_validate(sector_map)
    except ValidationError as error:
        raise SectorUpdateError(SectorUpdateErrorCode.INVALID_TAG_RESULTS) from error
    validate_sector_map_change(previous_map, sector_map)
    if previous_map:
        await save_json(SECTOR_MAP_ROLLBACK_FILE_NAME, previous_map, gcs_client)
    await save_json(config.SECTOR_MAP_FILE_NAME, sector_map, gcs_client)
    return sector_map


async def main():
    """스크립트의 메인 실행 함수입니다."""
    storage_method = config.validate_storage_config()

    start_time = datetime.now()
    gcs_client = None

    if storage_method is config.StorageMethod.GCS:
        try:
            gcs_client = create_gcs_client()
            logging.info("GCS 저장 모드로 실행됩니다.")
        except ImportError as e:
            raise StateBackendUnavailable(
                StateErrorCode.BACKEND_UNAVAILABLE,
                config.GCS_BUCKET_NAME or "GCS",
            ) from e
        except Exception as e:
            raise StateBackendUnavailable(
                StateErrorCode.BACKEND_UNAVAILABLE,
                config.GCS_BUCKET_NAME or "GCS",
                detail=str(e),
            ) from e
    else:
        logging.info("로컬 파일 저장 모드로 실행됩니다.")

    async with aiohttp.ClientSession() as session:
        logging.info("1. Upbit KRW 마켓 목록 가져오기...")
        upbit_markets = await get_upbit_krw_markets(session)
        if not upbit_markets:
            raise SectorUpdateError(SectorUpdateErrorCode.UPBIT_MARKETS_UNAVAILABLE)
        logging.info(f"   -> {len(upbit_markets)}개 KRW 마켓 확인.")

        logging.info("2. CoinGecko 전체 코인 목록 가져오기...")
        cg_symbol_to_id = await get_coingecko_coins_list(session)
        if not cg_symbol_to_id:
            raise SectorUpdateError(SectorUpdateErrorCode.COINGECKO_LIST_UNAVAILABLE)
        logging.info(f"   -> {len(cg_symbol_to_id)}개 코인 ID 확인.")

        logging.info(f"3. {len(upbit_markets)}개 마켓에 대한 자동 태깅 시작...")
        tasks = [
            tag_market(
                session,
                symbol,
                market_info.market,
                cg_symbol_to_id,
            )
            for symbol, market_info in upbit_markets.items()
        ]
        results = await tqdm_asyncio.gather(*tasks, desc="태깅 진행 중")

        logging.info("4. sectors.json 파일 저장 시작...")
        try:
            await save_validated_sector_map(results, gcs_client)
            storage_type = "GCS" if gcs_client else "로컬"
            logging.info(f"'{config.SECTOR_MAP_FILE_NAME}' 파일 저장 완료 ({storage_type}).")
        except Exception as e:
            logging.error(f"'{config.SECTOR_MAP_FILE_NAME}' 파일 저장 중 오류 발생: {e}")
            raise

        _print_summary(results, start_time)


def _print_summary(results: List[SectorTagResult], start_time: datetime):
    """작업 완료 후 최종 요약 정보를 출력합니다."""
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    total_count = len(results)
    if total_count == 0:
        logging.warning("처리된 마켓이 없어 요약을 생략합니다.")
        return

    tagged_count = sum(
        result.status is SectorTagStatus.TAGGED for result in results
    )
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

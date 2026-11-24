"""Refresh the durable Upbit-to-CoinGecko sector map."""

import asyncio
import json
import logging
import math
import os
import time
from enum import StrEnum
from typing import Any

import aiohttp
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError
from tqdm.asyncio import tqdm_asyncio

import config
from common.models import (
    UNTAGGED_SECTOR_CATEGORY,
    MarketListing,
    SectorMap,
    SectorTagBatch,
    SectorTagResult,
    SectorTagStatus,
    canonicalize_sector_categories,
)
from common.storage_client import (
    StateBackendUnavailableError,
    StateErrorCode,
    create_gcs_client,
    load_json,
    save_json,
)

logging.basicConfig(level="INFO", format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(config.APP_LOGGER_NAME)

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
    def __init__(self, code: SectorUpdateErrorCode, *, detail: str | None = None):
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
    platforms: dict[str, Any] = Field(default_factory=dict)
    categories: Any = None


_override_map_adapter = TypeAdapter(dict[str, CoinGeckoOverride])


def parse_symbol_overrides(raw_value: str | None) -> dict[str, CoinGeckoOverride]:
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


class RateLimiter:
    """Serialize CoinGecko requests to a fixed per-minute rate."""

    def __init__(self, calls_per_minute: int = 28):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = time.time()
        self.lock = asyncio.Lock()

    async def wait_for_slot(self) -> None:
        """Wait until the next request slot is available."""
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
    return not expected_network or expected_network in coin.platforms


async def fetch_upbit_krw_market_listings(
    session: aiohttp.ClientSession,
) -> dict[str, MarketListing]:
    """Fetch validated Upbit KRW listings keyed by lower-case symbol."""
    try:
        async with session.get(UPBIT_MARKET_URL) as response:
            response.raise_for_status()
            markets = await response.json()
            if not isinstance(markets, list):
                raise SectorUpdateError(SectorUpdateErrorCode.UPBIT_MARKETS_UNAVAILABLE)
            listings = {}
            for raw_market in markets:
                if not isinstance(raw_market, dict) or not isinstance(
                    raw_market.get("market"), str
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
    except Exception as error:
        logger.exception("Failed to fetch the Upbit KRW market list")
        raise SectorUpdateError(
            SectorUpdateErrorCode.UPBIT_MARKETS_UNAVAILABLE,
            detail=str(error),
        ) from error


async def fetch_coingecko_coin_ids_by_symbol(
    session: aiohttp.ClientSession,
) -> dict[str, list[str]]:
    """Return every CoinGecko id for a symbol; never silently choose a collision."""
    try:
        await rate_limiter.wait_for_slot()

        headers = {"x-cg-demo-api-key": CG_API_KEY}
        async with session.get(CG_COINS_LIST_URL, headers=headers) as response:
            response.raise_for_status()
            coins = await response.json()
            if not isinstance(coins, list):
                raise SectorUpdateError(
                    SectorUpdateErrorCode.COINGECKO_LIST_UNAVAILABLE
                )
            mapping: dict[str, list[str]] = {}
            for raw_coin in coins:
                coin = CoinGeckoListing.model_validate(raw_coin)
                mapping.setdefault(coin.symbol.casefold(), []).append(coin.id)
            return mapping
    except SectorUpdateError:
        raise
    except Exception as error:
        logger.exception("Failed to fetch the CoinGecko coin list")
        raise SectorUpdateError(
            SectorUpdateErrorCode.COINGECKO_LIST_UNAVAILABLE,
            detail=str(error),
        ) from error


async def fetch_coingecko_coin_detail(
    session: aiohttp.ClientSession, coin_id: str
) -> CoinGeckoDetail | None:
    """Fetch one CoinGecko identity and its category metadata."""
    await rate_limiter.wait_for_slot()

    url = f"{CG_COIN_DETAIL_URL}{coin_id}"
    headers = {"x-cg-demo-api-key": CG_API_KEY}
    max_retries = 3

    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    return CoinGeckoDetail.model_validate(data)
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    logger.warning(
                        "CoinGecko rate limit for %s; retrying in %d seconds",
                        coin_id,
                        retry_after,
                    )
                    await asyncio.sleep(retry_after)
                    continue
                if response.status == 404:
                    logger.warning("CoinGecko coin id not found: %s", coin_id)
                    return None
        except Exception as error:
            logger.warning(
                "CoinGecko detail request failed for %s on attempt %d/%d: %s",
                coin_id,
                attempt + 1,
                max_retries,
                error,
            )
            if attempt < max_retries - 1:
                await asyncio.sleep(5)

    raise SectorUpdateError(
        SectorUpdateErrorCode.COIN_DETAIL_UNAVAILABLE, detail=coin_id
    )


async def resolve_market_sector_tags(
    session: aiohttp.ClientSession,
    symbol: str,
    market: str,
    coin_ids_by_symbol: dict[str, list[str]],
) -> SectorTagResult:
    """Resolve one Upbit market to validated CoinGecko categories."""
    if symbol in coin_ids_by_symbol:
        candidate_coin_ids = coin_ids_by_symbol[symbol]
        override = CG_SYMBOL_OVERRIDES.get(symbol)
        coin_id = override.id if override else None
        if coin_id and coin_id not in candidate_coin_ids:
            return SectorTagResult(
                market=market, status=SectorTagStatus.OVERRIDE_INVALID
            )
        if not coin_id and len(candidate_coin_ids) != 1:
            return SectorTagResult(
                market=market, status=SectorTagStatus.SYMBOL_AMBIGUOUS
            )
        coin_id = coin_id or candidate_coin_ids[0]
        try:
            detail = await fetch_coingecko_coin_detail(session, coin_id)
        except SectorUpdateError as error:
            if error.code is not SectorUpdateErrorCode.COIN_DETAIL_UNAVAILABLE:
                raise
            return SectorTagResult(market=market, status=SectorTagStatus.LOOKUP_FAILED)
        if detail is not None:
            if not validate_coin_identity(detail, override):
                return SectorTagResult(
                    market=market, status=SectorTagStatus.IDENTITY_MISMATCH
                )
            categories = detail.categories
            if not isinstance(categories, list) or any(
                not isinstance(category, str) or not category.strip()
                for category in categories
            ):
                logger.error("CoinGecko category schema is invalid for %s", coin_id)
                return SectorTagResult(
                    market=market, status=SectorTagStatus.INVALID_CATEGORY
                )
            if categories:
                return SectorTagResult(
                    market=market,
                    status=SectorTagStatus.TAGGED,
                    categories=categories,
                )
            return SectorTagResult(market=market, status=SectorTagStatus.NO_CATEGORY)
        return SectorTagResult(market=market, status=SectorTagStatus.LOOKUP_NOT_FOUND)
    return SectorTagResult(market=market, status=SectorTagStatus.SYMBOL_NOT_FOUND)


def validate_sector_map_change(previous: dict, proposed: dict) -> None:
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
    previous: dict[str, list[str]], results: list[SectorTagResult]
) -> None:
    """Require meaningful usable coverage before publishing the first canonical map."""
    if previous:
        return
    usable_markets = [
        result.market for result in results if result.status is SectorTagStatus.TAGGED
    ]
    if not usable_markets:
        raise SectorUpdateError(
            SectorUpdateErrorCode.BOOTSTRAP_COVERAGE_INSUFFICIENT,
            detail="usable_markets=0",
        )
    required_usable = math.ceil(len(results) * MIN_SECTOR_BOOTSTRAP_USABLE_RATIO)
    if len(usable_markets) < required_usable:
        ratio = len(usable_markets) / len(results)
        raise SectorUpdateError(
            SectorUpdateErrorCode.BOOTSTRAP_COVERAGE_INSUFFICIENT,
            detail=f"usable_ratio={ratio:.6f}",
        )


def build_sector_map(
    previous: dict[str, list[str]], results: list[SectorTagResult]
) -> dict[str, list[str]]:
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
    results: list[SectorTagResult], gcs_client=None
) -> dict[str, list[str]]:
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


async def run_sector_update() -> None:
    """Refresh and persist the validated Upbit sector map."""
    storage_method = config.validate_storage_config()
    started_at = time.monotonic()
    gcs_client = None

    if storage_method is config.StorageMethod.GCS:
        try:
            gcs_client = create_gcs_client()
        except ImportError as error:
            raise StateBackendUnavailableError(
                StateErrorCode.BACKEND_UNAVAILABLE,
                config.GCS_BUCKET_NAME or "GCS",
            ) from error
        except Exception as error:
            raise StateBackendUnavailableError(
                StateErrorCode.BACKEND_UNAVAILABLE,
                config.GCS_BUCKET_NAME or "GCS",
                detail=str(error),
            ) from error
        logger.info("Using GCS sector-map storage")
    else:
        logger.info("Using local sector-map storage")

    async with aiohttp.ClientSession() as session:
        upbit_markets = await fetch_upbit_krw_market_listings(session)
        if not upbit_markets:
            raise SectorUpdateError(SectorUpdateErrorCode.UPBIT_MARKETS_UNAVAILABLE)
        logger.info("Fetched %d Upbit KRW markets", len(upbit_markets))

        coin_ids_by_symbol = await fetch_coingecko_coin_ids_by_symbol(session)
        if not coin_ids_by_symbol:
            raise SectorUpdateError(SectorUpdateErrorCode.COINGECKO_LIST_UNAVAILABLE)
        logger.info(
            "Fetched CoinGecko identities for %d symbols",
            len(coin_ids_by_symbol),
        )

        tasks = [
            resolve_market_sector_tags(
                session,
                symbol,
                listing.market,
                coin_ids_by_symbol,
            )
            for symbol, listing in upbit_markets.items()
        ]
        results = await tqdm_asyncio.gather(*tasks, desc="Resolving sector tags")
        await save_validated_sector_map(results, gcs_client)
        storage_backend = "GCS" if gcs_client else "local"
        logger.info(
            "Saved %s to %s storage",
            config.SECTOR_MAP_FILE_NAME,
            storage_backend,
        )
        _log_sector_update_summary(results, started_at)


def _log_sector_update_summary(
    results: list[SectorTagResult], started_at: float
) -> None:
    """Log one machine-readable summary for the completed sector refresh."""
    total_markets = len(results)
    if total_markets == 0:
        logger.warning("Sector update completed without any market results")
        return

    tagged_markets = sum(result.status is SectorTagStatus.TAGGED for result in results)
    untagged_markets = total_markets - tagged_markets
    elapsed_seconds = time.monotonic() - started_at
    logger.info(
        "Sector update completed: total=%d tagged=%d tagged_ratio=%.1f%% "
        "untagged=%d elapsed_seconds=%.2f average_seconds_per_market=%.3f",
        total_markets,
        tagged_markets,
        tagged_markets / total_markets * 100,
        untagged_markets,
        elapsed_seconds,
        elapsed_seconds / total_markets,
    )


if __name__ == "__main__":
    asyncio.run(run_sector_update())

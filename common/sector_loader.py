"""Load and validate the durable market-to-sector map."""

import logging

from pydantic import ValidationError

import config
from common.models import (
    UNTAGGED_SECTOR_CATEGORY,
    SectorMap,
    canonicalize_sector_categories,
)
from common.storage_client import StateErrorCode, StateLoadError, load_json

logger = logging.getLogger(config.APP_LOGGER_NAME)


def build_sector_maps(
    sector_map: SectorMap,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Build sector-to-market and market-to-sector indexes."""
    sectors_by_market = {
        market: [
            sector
            for sector in canonicalize_sector_categories(sectors)
            if sector != UNTAGGED_SECTOR_CATEGORY
        ]
        for market, sectors in sector_map.root.items()
    }
    markets_by_sector: dict[str, list[str]] = {}
    for market, market_sectors in sectors_by_market.items():
        for sector in market_sectors:
            markets_by_sector.setdefault(sector, []).append(market)
    return markets_by_sector, sectors_by_market


async def load_sector_maps(
    gcs_client=None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Load the canonical sector map and return both lookup directions."""
    raw_sector_map = await load_json(
        config.SECTOR_MAP_FILE_NAME,
        gcs_client,
        reject_null=True,
    )
    if raw_sector_map is None:
        logger.warning("Sector map is unavailable: %s", config.SECTOR_MAP_FILE_NAME)
        return {}, {}
    try:
        sector_map = SectorMap.model_validate(raw_sector_map)
    except ValidationError as error:
        raise StateLoadError(
            StateErrorCode.INVALID_SCHEMA,
            config.SECTOR_MAP_FILE_NAME,
        ) from error

    markets_by_sector, sectors_by_market = build_sector_maps(sector_map)
    logger.info(
        "Loaded sector map: sectors=%d tagged_markets=%d",
        len(markets_by_sector),
        len(sectors_by_market),
    )
    return markets_by_sector, sectors_by_market

# common/sector_loader.py
import logging
from typing import Dict, List, Tuple

from pydantic import ValidationError

import config
from common.models import (
    SectorMap,
    UNTAGGED_SECTOR_CATEGORY,
    canonicalize_sector_categories,
)
from common.storage_client import StateErrorCode, StateLoadError, load_json

logger = logging.getLogger(config.APP_LOGGER_NAME)

def process_sector_data(
    sector_map: SectorMap,
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """로드한 원본 데이터를 정방향/역방향 맵으로 가공합니다."""
    sectors: Dict[str, List[str]] = {}
    reverse_map = {
        market: [
            tag
            for tag in canonicalize_sector_categories(tags)
            if tag != UNTAGGED_SECTOR_CATEGORY
        ]
        for market, tags in sector_map.root.items()
    }

    for market, tags in reverse_map.items():
        for tag in tags:
            sectors.setdefault(tag, []).append(market)

    return sectors, reverse_map


async def load_and_process_sectors(
    gcs_client=None,
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """섹터 맵 파일을 로드하고 분석에 필요한 형태로 처리합니다."""
    logger.info("섹터 맵 데이터 로딩 시작...")
    raw_map = await load_json(
        config.SECTOR_MAP_FILE_NAME, gcs_client, reject_null=True
    )

    if raw_map is None:
        logger.warning("섹터 맵 파일을 찾을 수 없습니다.")
        return {}, {}
    try:
        sector_map = SectorMap.model_validate(raw_map)
    except ValidationError as error:
        raise StateLoadError(
            StateErrorCode.INVALID_SCHEMA, config.SECTOR_MAP_FILE_NAME
        ) from error

    sectors, reverse_map = process_sector_data(sector_map)
    logger.info(f"{len(sectors)}개 섹터, {len(reverse_map)}개 마켓 태그 로드 완료.")
    return sectors, reverse_map

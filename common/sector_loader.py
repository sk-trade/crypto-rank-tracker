# common/sector_loader.py
import logging
from typing import Dict, Tuple
from common.storage_client import load_json

import config

logger = logging.getLogger(config.APP_LOGGER_NAME)

def process_sector_data(raw_map: Dict) -> Tuple[Dict, Dict]:
    """
    로드한 데이터를 정방향/역방향 맵으로 가공합니다.
    - SECTORS: {'AI': ['KRW-RNDR', ...]}
    - REVERSE_SECTOR_MAP: {'KRW-RNDR': ['AI', ...]}
    """
    sectors = {}
    reverse_map = raw_map
    
    for market, tags in raw_map.items():
        for tag in tags:
            simple_tag = tag.split('(')[0].strip()
            if simple_tag not in sectors:
                sectors[simple_tag] = []
            sectors[simple_tag].append(market)
            
    return sectors, reverse_map

async def load_and_process_sectors(gcs_client= None) -> Tuple[Dict, Dict]:
    logger.info("섹터 맵 데이터 로딩 시작...")
    raw_map = await load_json(config.SECTOR_MAP_FILE_NAME, gcs_client)
    if not raw_map or not isinstance(raw_map, dict):
        logger.warning("섹터 맵 파일을 찾을 수 없거나 형식이 올바르지 않습니다.")
        return {}, {}
    sectors, reverse_map = process_sector_data(raw_map)
    logger.info(f"{len(sectors)}개 섹터, {len(reverse_map)}개 마켓 태그 로드 완료.")
    return sectors, reverse_map
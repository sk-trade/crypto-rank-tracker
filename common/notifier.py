import logging
import aiohttp
from typing import Dict, Any, List
import config

logger = logging.getLogger(config.APP_LOGGER_NAME)

def analyze_changes(
    new_state: Dict[str, Any], 
    old_state: Dict[str, Any]
) -> List[str]:
    """두 상태를 비교하여 변경 사항을 분석하고 메시지 리스트를 반환합니다."""
    
    old_tickers = old_state.get("tickers", {})
    new_tickers = new_state.get("tickers", {})
    
    messages = []
    
    # 상위 N개 종목만 필터링 (알림용)
    old_top_n = {m: d for m, d in old_tickers.items() if d.get('rank', 999) <= config.NOTIFY_TOP_N}
    new_top_n = {m: d for m, d in new_tickers.items() if d.get('rank', 999) <= config.NOTIFY_TOP_N}

    # 순위 변경, 신규 진입, 이탈 감지 (상위 N개 내에서)
    for market, new_data in new_top_n.items():
        old_data = old_top_n.get(market)
        new_rank = new_data['rank']
        
        if old_data:
            old_rank = old_data['rank']
            if new_rank != old_rank:
                arrow = "🔼" if new_rank < old_rank else "🔽"
                messages.append(f"{arrow} {market}: 순위 변동 {old_rank}위 → {new_rank}위")
        else: # 신규 진입
             messages.append(f"✨ {market}: TOP {config.NOTIFY_TOP_N} 신규 진입 ({new_rank}위)")

    removed_markets = set(old_top_n.keys()) - set(new_top_n.keys())
    for market in removed_markets:
        messages.append(f"❌ {market}: TOP {config.NOTIFY_TOP_N}에서 이탈")
        
    return messages

def format_notification(change_messages: List[str], new_state: Dict[str, Any]) -> str:
    """분석된 변경 사항과 전체 순위로 최종 알림 메시지를 포맷팅합니다."""
    if not change_messages:
        return ""
        
    # 상위 N개만 추려서 순위 목록 생성
    top_tickers_list = sorted(
        new_state['tickers'].values(), 
        key=lambda x: x.get('rank', 999)
    )[:config.NOTIFY_TOP_N]
    
    rank_list_str = "\n".join([f"{t['rank']:>2}. {t['market']}" for t in top_tickers_list])

    summary = "📈 **거래대금 순위 변동 알림**\n\n" + "\n".join(change_messages)
    full_message = f"{summary}\n\n---\n\n**현재 TOP {config.NOTIFY_TOP_N} 순위**\n{rank_list_str}"
    
    return full_message
    
async def send_notification(session: aiohttp.ClientSession, message: str):
    """실제로 웹훅을 통해 메시지를 보냅니다."""
    if not message or not config.WEBHOOK_URL:
        return
        
    payload = {"text": message}
    try:
        async with session.post(config.WEBHOOK_URL, json=payload, timeout=10) as response:
            if response.status == 200:
                logger.info("웹훅 알림 전송 성공.")
            else:
                logger.error(f"웹훅 전송 실패: {await response.text()}")
    except Exception as e:
        logger.error(f"웹훅 전송 중 예외 발생: {e}", exc_info=True)
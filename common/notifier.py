import logging
import aiohttp
from typing import Dict, Any, List
import config

logger = logging.getLogger(config.APP_LOGGER_NAME)

def analyze_and_format_notification(
    new_state: Dict[str, Any], 
    old_states: List[Dict[str, Any]]
) -> str:
    """
    과거 히스토리와 현재 상태를 비교 분석하여 최종 알림 메시지를 생성합니다.
    의미 있는 변동이 없더라도 현재 순위는 항상 포맷팅하여 반환합니다.
    """
    # 1. 데이터 보강: 각 티커에 분석 정보 추가
    enriched_tickers = _enrich_ticker_data(new_state.get("tickers", {}), old_states)

    # 2. 알림 메시지 생성 (변동 사항만)
    change_messages = {
        "trending_up": [],
        "trending_down": [],
        "significant_change": [],
        "entry_exit": [],
    }

    # 이전 상태가 있을 경우에만 변동 분석 수행
    if old_states:
        _analyze_entry_exit(change_messages["entry_exit"], enriched_tickers, old_states)
        _analyze_trends_and_changes(change_messages, enriched_tickers)

    # 3. 최종 메시지 포맷팅
    # 이제 이 함수는 변동사항이 없어도 항상 순위표를 포함한 메시지를 생성합니다.
    return _format_final_message(change_messages, enriched_tickers)


def _enrich_ticker_data(new_tickers, old_states):
    """각 티커에 과거 순위, 추세 등 분석용 데이터를 추가합니다."""
    old_tickers_history = [s.get("tickers", {}) for s in old_states]
    
    for market, data in new_tickers.items():
        # 과거 순위 리스트
        data['rank_history'] = [h.get(market, {}).get('rank') for h in old_tickers_history]
        
        # 직전 순위 및 변동폭
        if data['rank_history']:
            last_rank = data['rank_history'][-1]
            if last_rank:
                data['rank_change'] = last_rank - data['rank'] # 양수:상승, 음수:하락
            else:
                data['rank_change'] = 0
        else:
            data['rank_change'] = 0

        # 연속 상승/하락 추세(streak) 계산
        streak = 0
        current_rank = data['rank']
        history_for_streak = [r for r in data['rank_history'] if r] + [current_rank]
        
        if len(history_for_streak) > 1:
            # 상승 추세 확인
            if all(history_for_streak[i] > history_for_streak[i+1] for i in range(len(history_for_streak)-1)):
                streak = len(history_for_streak) - 1
            # 하락 추세 확인
            elif all(history_for_streak[i] < history_for_streak[i+1] for i in range(len(history_for_streak)-1)):
                streak = -(len(history_for_streak) - 1)
        data['trend_streak'] = streak
    
    return new_tickers

def _analyze_entry_exit(messages, enriched_tickers, old_states):
    """TOP N 순위권 진입/이탈 분석"""
    old_top_n_set = {m for m, d in old_states[-1].get("tickers", {}).items() if d.get('rank', 999) <= config.NOTIFY_TOP_N}
    new_top_n_set = {m for m, d in enriched_tickers.items() if d.get('rank', 999) <= config.NOTIFY_TOP_N}
    
    entered = new_top_n_set - old_top_n_set
    exited = old_top_n_set - new_top_n_set

    for market in entered:
        rank = enriched_tickers[market]['rank']
        messages.append(f"✨ {market}: TOP {config.NOTIFY_TOP_N} 신규 진입 ({rank}위)")

    for market in exited:
        old_rank = old_states[-1]['tickers'][market]['rank']
        messages.append(f"❌ {market}: TOP {config.NOTIFY_TOP_N} 에서 이탈 (이전 {old_rank}위)")

def _analyze_trends_and_changes(messages, enriched_tickers):
    """지속적인 추세 및 급변동 분석"""
    processed_markets = set()

    for market, data in enriched_tickers.items():
        # 추세 감지
        streak = data.get('trend_streak', 0)
        if abs(streak) >= config.TRENDING_STREAK_THRESHOLD:
            oldest_rank = data['rank_history'][-(abs(streak)):][0]
            current_rank = data['rank']
            if streak > 0: # 상승
                messages["trending_up"].append({
                    "text": f"🚀 {market}: {streak}회 연속 상승 ({oldest_rank}위 → {current_rank}위)",
                    "sort_key": streak
                })
            else: # 하락
                messages["trending_down"].append({
                    "text": f"📉 {market}: {abs(streak)}회 연속 하락 ({oldest_rank}위 → {current_rank}위)",
                    "sort_key": abs(streak)
                })
            processed_markets.add(market)

        # 급변동 감지 (추세가 아닌 경우에만)
        if market not in processed_markets:
            rank_change = data.get('rank_change', 0)
            if abs(rank_change) >= config.SIGNIFICANT_RANK_CHANGE_THRESHOLD:
                old_rank = data['rank'] + rank_change
                current_rank = data['rank']
                arrow = "⏫" if rank_change > 0 else "⏬"
                change_text = "상승" if rank_change > 0 else "하락"
                messages["significant_change"].append({
                    "text": f"{arrow} {market}: 순위 급변 ({old_rank}위 → {current_rank}위, {rank_change:+}계단)",
                    "sort_key": abs(rank_change)
                })

def _format_final_message(change_messages: Dict[str, list], enriched_tickers: Dict) -> str:
    """
    분석된 변경 사항과 현재 순위표를 조합하여 최종 알림 메시지를 생성합니다.
    """
    final_message_parts = []
    has_changes = False

    # --- 1. 변동 사항 요약 부분 (있을 경우에만 추가) ---
    summary_parts = ["📊 **업비트 거래대금 순위 동향**\n"]
    sections = {
        "trending_up": "🚀 **지속 상승**",
        "trending_down": "📉 **지속 하락**",
        "significant_change": "⚡ **주요 급변동**",
        "entry_exit": f"✨ **TOP {config.NOTIFY_TOP_N} 변동**"
    }

    for key, title in sections.items():
        msg_list = change_messages[key]
        if not msg_list:
            continue
        
        has_changes = True # 변동 사항이 하나라도 있음을 표시
        summary_parts.append(f"\n{title}")

        if msg_list and isinstance(msg_list[0], dict):
            sorted_msgs = sorted(msg_list, key=lambda x: x['sort_key'], reverse=True)
            msg_texts = [m['text'] for m in sorted_msgs[:config.MAX_ALERTS_PER_TYPE]]
            summary_parts.extend(msg_texts)
        else:
            summary_parts.extend(msg_list[:config.MAX_ALERTS_PER_TYPE])

    # 변동 사항이 있을 경우에만 요약 섹션을 최종 메시지에 추가
    if has_changes:
        final_message_parts.extend(summary_parts)

    # --- 2. 현재 순위표 부분 (항상 추가) ---
    top_tickers_list = sorted(
        enriched_tickers.values(), 
        key=lambda x: x.get('rank', 999)
    )[:config.DISPLAY_TOP_N_RANKING]
    
    rank_list_str = "\n".join([f"{t['rank']:>2}. {t['market']}" for t in top_tickers_list])
    
    # 변동 사항이 있었는지 여부에 따라 헤더와 구분선을 다르게 처리
    if has_changes:
        # 변동 사항이 있으면, 구분선과 함께 순위표 추가
        final_message_parts.append(f"\n\n---\n\n🏆 **현재 TOP {config.DISPLAY_TOP_N_RANKING} 순위**\n{rank_list_str}")
    else:
        # 변동 사항이 없으면, 순위표가 메인 컨텐츠가 됨
        final_message_parts.append(f"📊 **현재 거래대금 TOP {config.DISPLAY_TOP_N_RANKING} 순위**\n\n{rank_list_str}")

    return "\n".join(final_message_parts)



async def send_notification(session: aiohttp.ClientSession, message: str):
    """웹훅을 통해 메시지를 보냅니다."""
    if not message or not config.WEBHOOK_URL or "YOUR_DISCORD_OR_SLACK_WEBHOOK_URL" in config.WEBHOOK_URL:
        if "YOUR_DISCORD_OR_SLACK_WEBHOOK_URL" in config.WEBHOOK_URL:
             logger.warning("웹훅 URL이 설정되지 않았습니다. 알림을 보내지 않습니다.")
        return
        
    # 메시지가 너무 길 경우 4000자로 제한 (Discord 제한)
    if len(message) > 4000:
        message = message[:3950] + "\n... (메시지가 너무 길어 생략됨)"
        
    payload = {"text": message} 
    try:
        async with session.post(config.WEBHOOK_URL, json=payload, timeout=10) as response:
            if response.ok:
                logger.info("웹훅 알림 전송 성공.")
            else:
                logger.error(f"웹훅 전송 실패 ({response.status}): {await response.text()}")
    except Exception as e:
        logger.error(f"웹훅 전송 중 예외 발생: {e}", exc_info=True)
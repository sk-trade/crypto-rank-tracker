import logging
import aiohttp
from typing import Dict, Any, List
import numpy as np
import config
from common.models import State, TickerData

logger = logging.getLogger(config.APP_LOGGER_NAME)

def analyze_and_format_notification(
    new_state: State, 
    old_states: List[State]
) -> str:
    """
    과거 히스토리와 현재 상태를 비교 분석하여 최종 알림 메시지를 생성합니다.
    의미 있는 변동이 없더라도 현재 순위는 항상 포맷팅하여 반환합니다.
    """
    # 데이터 보강: 각 티커에 분석 정보 추가
    enriched_tickers = _enrich_ticker_data(new_state.tickers, old_states)

    # 알림 메시지 생성 (변동 사항만)
    change_messages = {
        "volume_surge": [],
        "volume_drop": [],
        "trending_up": [],
        "trending_down": [],
        "significant_change": [],
        "entry_exit": [],
    }

    # 이전 상태가 있을 경우에만 변동 분석 수행
    if old_states:
        _analyze_entry_exit(change_messages["entry_exit"], enriched_tickers, old_states)
        _analyze_trends_and_changes(change_messages, enriched_tickers)
        _analyze_volume_changes(change_messages, enriched_tickers)

    # 최종 메시지 포맷팅
    return _format_final_message(change_messages, enriched_tickers)


def _enrich_ticker_data(new_tickers: Dict[str, TickerData], old_states: List[State]) -> Dict[str, TickerData]:
    """각 티커에 과거 순위, 추세, Z-score 등 분석용 데이터를 추가합니다."""
    
    for market, data in new_tickers.items():
        # 과거 거래대금 및 순위 히스토리 추출
        volume_history = []
        rank_history = []
        for old_state in old_states:
            if market in old_state.tickers:
                old_ticker = old_state.tickers[market]
                volume_history.append(old_ticker.trade_volume_24h_krw)
                if old_ticker.rank:
                    rank_history.append(old_ticker.rank)
        
        data.rank_history = rank_history
        
        # 직전 순위 및 변동폭
        if rank_history:
            last_rank = rank_history[-1]
            if last_rank and data.rank:
                data.rank_change = last_rank - data.rank # 양수:상승, 음수:하락
        
        # 거래대금 Z-score 계산
        if len(volume_history) >= config.Z_SCORE_LOOKBACK_PERIOD:
            # 최근 N-1개 과거 데이터와 현재 데이터를 합쳐 Z-score 계산
            recent_volumes = volume_history[-(config.Z_SCORE_LOOKBACK_PERIOD-1):] + [data.trade_volume_24h_krw]
            mean_vol = np.mean(recent_volumes)
            std_vol = np.std(recent_volumes)
            
            if std_vol > 0:
                z_score = (data.trade_volume_24h_krw - mean_vol) / std_vol
                data.volume_z_score = z_score
        
        # 연속 상승/하락 추세(streak) 계산
        streak = 0
        current_rank = data.rank
        # None 값을 제외한 유효한 순위 기록만으로 추세 계산
        history_for_streak = [r for r in data.rank_history if r] + ([current_rank] if current_rank else [])
        
        if len(history_for_streak) > 1:
            # 상승 추세 확인
            if all(history_for_streak[i] > history_for_streak[i+1] for i in range(len(history_for_streak)-1)):
                streak = len(history_for_streak) - 1
            # 하락 추세 확인
            elif all(history_for_streak[i] < history_for_streak[i+1] for i in range(len(history_for_streak)-1)):
                streak = -(len(history_for_streak) - 1)
        data.trend_streak = streak
    
    return new_tickers

def _analyze_entry_exit(messages: list, enriched_tickers: Dict[str, TickerData], old_states: List[State]):
    """TOP N 순위권 진입/이탈 분석"""
    old_top_n_set = {m for m, d in old_states[-1].tickers.items() if d.rank and d.rank <= config.NOTIFY_TOP_N}
    new_top_n_set = {m for m, d in enriched_tickers.items() if d.rank and d.rank <= config.NOTIFY_TOP_N}
    
    entered = new_top_n_set - old_top_n_set
    exited = old_top_n_set - new_top_n_set

    for market in entered:
        rank = enriched_tickers[market].rank
        messages.append(f"✨ {market}: TOP {config.NOTIFY_TOP_N} 신규 진입 ({rank}위)")

    for market in exited:
        old_rank = old_states[-1].tickers[market].rank
        messages.append(f"❌ {market}: TOP {config.NOTIFY_TOP_N} 에서 이탈 (이전 {old_rank}위)")

def _analyze_volume_changes(messages: Dict[str, list], enriched_tickers: Dict[str, TickerData]):
    """🚀 Z-score 기반 거래대금 급증/급감 분석"""
    volume_surges = []
    volume_drops = []
    
    for market, data in enriched_tickers.items():
        z_score = data.volume_z_score or 0.0
        
        # 거래대금 급증 감지 (Z-score 사용)
        if z_score >= config.VOLUME_SURGE_Z_SCORE_THRESHOLD:
            volume_surges.append({
                "text": f"🔥 {market}: 거래대금 폭증 (Z-score: {z_score:.2f}, {data.rank}위)",
                "sort_key": z_score
            })

        # 거래대금 급감 감지 (Z-score 사용)
        elif z_score <= config.VOLUME_DROP_Z_SCORE_THRESHOLD:
            volume_drops.append({
                "text": f"🧊 {market}: 거래대금 급감 (Z-score: {z_score:.2f}, {data.rank}위)",
                "sort_key": abs(z_score)
            })

    if volume_surges:
        messages["volume_surge"] = sorted(volume_surges, key=lambda x: x['sort_key'], reverse=True)
    
    if volume_drops:
        messages["volume_drop"] = sorted(volume_drops, key=lambda x: x['sort_key'], reverse=True)


def _analyze_trends_and_changes(messages: Dict[str, list], enriched_tickers: Dict[str, TickerData]):
    """지속적인 추세 및 급변동 분석"""
    processed_markets = set()

    for market, data in enriched_tickers.items():
        if not data.rank: continue # 순위가 없는 티커는 분석에서 제외

        # 추세 감지
        streak = data.trend_streak
        if abs(streak) >= config.TRENDING_STREAK_THRESHOLD:
            # 추세 시작점의 순위를 찾기
            valid_history = [r for r in data.rank_history if r]
            if len(valid_history) >= abs(streak):
                oldest_rank = valid_history[-(abs(streak))]
                current_rank = data.rank
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
            rank_change = data.rank_change
            if abs(rank_change) >= config.SIGNIFICANT_RANK_CHANGE_THRESHOLD:
                old_rank = data.rank + rank_change
                current_rank = data.rank
                arrow = "⏫" if rank_change > 0 else "⏬"
                messages["significant_change"].append({
                    "text": f"{arrow} {market}: 순위 급변 ({old_rank}위 → {current_rank}위, {rank_change:+}계단)",
                    "sort_key": abs(rank_change)
                })

def _format_final_message(change_messages: Dict[str, list], enriched_tickers: Dict[str, TickerData]) -> str:
    """
    분석된 변경 사항과 현재 순위표를 조합하여 최종 알림 메시지를 생성합니다.
    """
    final_message_parts = []
    has_changes = False

    # --- 1. 변동 사항 요약 부분 (있을 경우에만 추가) ---
    summary_parts = ["📊 **업비트 거래대금 순위 동향**\n"]
    sections = {
        "volume_surge": "🚀 **거래대금 급증 (Z-score 기반)**",
        "trending_up": "📈 **지속 상승**",
        "significant_change": "⚡ **주요 급변동**",
        "entry_exit": f"✨ **TOP {config.NOTIFY_TOP_N} 변동**",
        "trending_down": "📉 **지속 하락**",
        "volume_drop": "🧊 **거래대금 급감 (Z-score 기반)**",
    }

    for key, title in sections.items():
        msg_list = change_messages.get(key, []) 
        if not msg_list:
            continue
        
        has_changes = True # 변동 사항이 하나라도 있음을 표시
        summary_parts.append(f"\n{title}")

        if msg_list and isinstance(msg_list[0], dict):
            # sort_key를 기준으로 정렬
            sorted_msgs = sorted(msg_list, key=lambda x: x['sort_key'], reverse=True)
            msg_texts = [m['text'] for m in sorted_msgs[:config.MAX_ALERTS_PER_TYPE]]
            summary_parts.extend(msg_texts)
        else: # dict가 아닌 단순 문자열 리스트인 경우 (e.g., entry_exit)
            summary_parts.extend(msg_list[:config.MAX_ALERTS_PER_TYPE])

    # 변동 사항이 있을 경우에만 요약 섹션을 최종 메시지에 추가
    if has_changes:
        final_message_parts.extend(summary_parts)

    # --- 2. 현재 순위표 부분 (항상 추가) ---
    top_tickers_list = sorted(
        [t for t in enriched_tickers.values() if t.rank], 
        key=lambda x: x.rank
    )[:config.DISPLAY_TOP_N_RANKING]
    
    rank_list_parts = []
    for t in top_tickers_list:
        rank_change = t.rank_change
        change_str = ""
        if rank_change > 0:
            change_str = f" (↑{rank_change})"
        elif rank_change < 0:
            change_str = f" (↓{abs(rank_change)})"
        rank_list_parts.append(f"{t.rank:>2}. {t.market}{change_str}")
    rank_list_str = "\n".join(rank_list_parts)
    
    # 변동 사항이 있었는지 여부에 따라 헤더와 구분선을 다르게 처리
    if has_changes:
        # 변동 사항이 있으면, 구분선과 함께 순위표 추가
        final_message_parts.append(f"\n\n---\n\n🏆 **현재 TOP {config.DISPLAY_TOP_N_RANKING} 순위**\n{rank_list_str}")
    else:
        # 변동 사항이 없으면, 순위표가 메인 컨텐츠가 됨
        final_message_parts.append(f"📊 **현재 거래대금 TOP {config.DISPLAY_TOP_N_RANKING} 순위**\n\n{rank_list_str}")

    # 생성된 메시지가 비어있으면 빈 문자열 반환
    if not final_message_parts:
        return ""
        
    final_message = "\n".join(final_message_parts)

    # has_changes 플래그가 True일 때만 메시지 맨 앞에 @channel 태그 추가
    if has_changes:
        return f"@channel\n{final_message}"
    else:
        return final_message

async def send_notification(session: aiohttp.ClientSession, message: str):
    """웹훅을 통해 메시지를 보냅니다."""
    if not message or not config.WEBHOOK_URL or "YOUR_SLACK_WEBHOOK_URL" in config.WEBHOOK_URL:
        if "YOUR_SLACK_WEBHOOK_URL" in config.WEBHOOK_URL:
             logger.warning("웹훅 URL이 설정되지 않았습니다. 알림을 보내지 않습니다.")
        return
    
    # 멘션 유무 확인
    use_channel_mention = message.strip().startswith("@channel")
        
    # 메시지가 너무 길 경우 4000자로 제한 
    if len(message) > 4000:
        message = message[:3950] + "\n... (메시지가 너무 길어 생략됨)"
        
    payload = {"text": message}
    
    if use_channel_mention:
        payload["link_names"] = 1
    
    try:
        async with session.post(config.WEBHOOK_URL, json=payload, timeout=10) as response:
            if response.ok:
                logger.info("웹훅 알림 전송 성공.")
            else:
                logger.error(f"웹훅 전송 실패 ({response.status}): {await response.text()}")
    except Exception as e:
        logger.error(f"웹훅 전송 중 예외 발생: {e}", exc_info=True)
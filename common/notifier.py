# common/notifier.py

import logging
import aiohttp
from typing import Dict, List, Any, Tuple
import numpy as np
import datetime

import config
from common.models import TickerData, AlertHistory
from common.signals.detector import detect_anomalies
from common.state_manager import load_alert_history, save_alert_history

logger = logging.getLogger(config.APP_LOGGER_NAME)


# --- 게이트키퍼 함수 ---

def is_worth_alerting(anomaly: Dict) -> bool:
    """
    이 시그널이 알림을 보낼 최소한의 가치가 있는지 판단합니다.
    이 관문을 통과하지 못하면 어떤 경우에도 알림이 나가지 않습니다.
    """
    price_change_abs = abs(anomaly.get('price_change', 0))
    
    if price_change_abs < config.ALERT_MIN_PRICE_CHANGE_10M:
        return False
    if anomaly.get('rvol', 0) < config.ALERT_MIN_RVOL:
        return False
    if anomaly.get('confidence', 0) < config.ALERT_MIN_CONFIDENCE:
        return False
        
    return True


# --- 알림 판단 함수 ---

def get_alert_type_and_priority(
    anomaly: Dict,
    history: Dict[str, AlertHistory]
) -> Tuple[str | None, int]:
    """
    게이트키퍼를 통과한 시그널의 유형과 우선순위를 판단합니다.
    """
    market = anomaly['market']
    previous_alert = history.get(market)

    # Case 1: 이전 알림이 없음 -> 새로운 이벤트
    if not previous_alert:
        return "INITIAL_BREAKOUT", 2

    # Case 2: 이전 알림이 있음 -> 후속 움직임 판단
    cooldown_delta = datetime.datetime.now(datetime.timezone.utc) - previous_alert.last_alert_timestamp
    
    if cooldown_delta.total_seconds() >= config.ALERT_COOLDOWN_MINUTES * 60:
        # 쿨다운 기간이 지났으면, 다시 새로운 이벤트로 간주
        return "INITIAL_BREAKOUT", 2
    else:
        # 쿨다운 기간 내라면, '의미 있는 지속성'이 있는지 엄격하게 판단
        current_price = anomaly['current_price']
        previous_price = previous_alert.last_price
        additional_change_pct = (current_price / previous_price - 1) * 100
        
        is_momentum = abs(additional_change_pct) >= config.SUSTAINED_MOMENTUM_MIN_ADDITIONAL_CHANGE_PCT
        
        if is_momentum:
            return "MOMENTUM_SUSTAINED", 1
        
        logger.debug(f"{market}는 쿨다운 중이며, 의미있는 추가 변동이 없어 건너뜁니다.")
        return None, 0


# --- 메시지 포맷팅 헬퍼 함수 ---

def format_signal_message(
    signal_type: str,
    anomaly: Dict,
    ticker: TickerData,
    previous_alert: AlertHistory | None,
    reverse_sector_map: Dict
) -> str:
    market = anomaly['market']
    price_change_10m = anomaly['price_change']
    rvol = anomaly['rvol']
    tag = reverse_sector_map.get(market, [""])[0]
    tag_str = f" ({tag})" if tag else ""
    
    if signal_type == "INITIAL_BREAKOUT":
        icon = "🔥" if price_change_10m > 0 else "🧊"
        header = f"{icon} **{market}{tag_str}: 초기 급등/급락 포착**"
    else: 
        icon = "📈" if price_change_10m > 0 else "📉"
        header = f"{icon} **{market}{tag_str}: 모멘텀 지속**"

    phenomenon = f"- **현상:** 최근 10분간 `{price_change_10m:+.2f}%` 변동, RVOL `{rvol:.1f}x`"
    
    contexts = []
    if ticker.price_change_1h is not None:
        contexts.append(f"1시간 추세 `{ticker.price_change_1h:+.2f}%`")
    if ticker.is_breaking_1h_high and price_change_10m > 0:
        contexts.append("`1시간 고점 돌파`")
    context_str = f"- **맥락:** {', '.join(contexts)}" if contexts else ""

    interpretation = ""
    if signal_type == "MOMENTUM_SUSTAINED" and previous_alert:
        total_change_pct = (anomaly['current_price'] / previous_alert.initial_price - 1) * 100
        elapsed_time = (datetime.datetime.now(datetime.timezone.utc) - previous_alert.initial_timestamp).total_seconds() / 60
        interpretation = f"- **누적:** 최초 알림 후 `{elapsed_time:.0f}분` 동안 `{total_change_pct:+.2f}%` 변동"

    return "\n".join(filter(None, [header, phenomenon, context_str, interpretation]))


# --- 주도 섹터 및 시장 현황 분석 함수 ---

def analyze_leading_sectors(tickers_data: Dict[str, TickerData], SECTORS: Dict) -> List[str]:
    sector_performance = {}
    for sector_name, coins in SECTORS.items():
        returns_1h = [t.price_change_1h for t in [tickers_data.get(c) for c in coins] if t and t.price_change_1h is not None]
        if len(returns_1h) < 3: continue

        avg_return_1h = np.mean(returns_1h)
        rising_count = sum(1 for r in returns_1h if r > 0)
        
        if avg_return_1h > 1.5 and (rising_count / len(returns_1h)) >= 0.6:
            sector_performance[sector_name] = {'avg_return': avg_return_1h, 'consistency': f"{rising_count}/{len(returns_1h)} 상승"}
            
    if not sector_performance:
        return ["- 주도 섹터 없음"]

    sorted_sectors = sorted(sector_performance.items(), key=lambda item: item[1]['avg_return'], reverse=True)
    return [f"- **{name} ({perf['consistency']}):** 1시간 평균 `{perf['avg_return']:.2f}%` 상승" for name, perf in sorted_sectors[:3]]


# --- 메인 분석 및 포맷팅 함수 ---

async def analyze_and_format_notification(
    enriched_tickers: Dict[str, TickerData],
    raw_tickers: List[Dict[str, Any]],
    current_rankings: Dict[str, int],
    previous_rankings: Dict[str, int],
    SECTORS: Dict,
    REVERSE_SECTOR_MAP: Dict,
    gcs_client=None
) -> str:
    alert_history = await load_alert_history(gcs_client)
    anomalies_raw = detect_anomalies(enriched_tickers, SECTORS, REVERSE_SECTOR_MAP)
    
    candidate_anomalies = [a for a in anomalies_raw if is_worth_alerting(a)]
    
    final_signals = []
    for anomaly in candidate_anomalies:
        anomaly['current_price'] = enriched_tickers[anomaly['market']].candle_history[-1].close_price
        
        signal_type, priority = get_alert_type_and_priority(anomaly, alert_history)
        
        if signal_type:
            anomaly.update({'signal_type': signal_type, 'priority': priority})
            final_signals.append(anomaly)

            now = datetime.datetime.now(datetime.timezone.utc)
            current_price = anomaly['current_price']
            
            if signal_type == "INITIAL_BREAKOUT":
                alert_history[anomaly['market']] = AlertHistory(
                    market=anomaly['market'], last_alert_timestamp=now, last_signal_type=signal_type,
                    last_price=current_price, last_rvol=anomaly['rvol'],
                    initial_timestamp=now, initial_price=current_price
                )
            elif signal_type == "MOMENTUM_SUSTAINED":
                alert_history[anomaly['market']].last_alert_timestamp = now
                alert_history[anomaly['market']].last_signal_type = signal_type
                alert_history[anomaly['market']].last_price = current_price
                alert_history[anomaly['market']].last_rvol = anomaly['rvol']

    gainers = sum(1 for t in enriched_tickers.values() if t.price_change_10m is not None and t.price_change_10m > 0)
    losers = sum(1 for t in enriched_tickers.values() if t.price_change_10m is not None and t.price_change_10m < 0)
    market_mood_str = "강세" if gainers > losers * 1.2 else "약세" if losers > gainers * 1.2 else "보합"
    
    total_24h = sum(t.get('acc_trade_price_24h', 0) for t in raw_tickers if t.get('acc_trade_price_24h'))
    major_24h = sum(t.get('acc_trade_price_24h', 0) for t in raw_tickers if t['market'] in ['KRW-BTC', 'KRW-ETH'] and t.get('acc_trade_price_24h'))
    major_pct = (major_24h / total_24h * 100) if total_24h > 0 else 0
    
    leading_sectors = analyze_leading_sectors(enriched_tickers, SECTORS)

    if not final_signals and not any("주도 섹터 없음" not in s for s in leading_sectors):
        await save_alert_history(alert_history, gcs_client)
        return ""

    message_parts = [f"📊 **업비트 마켓 브리핑 ({datetime.datetime.now().strftime('%H:%M')})**"]
    
    market_status_lines = [
        f"**시장 현황:**",
        f"- **분위기:** {market_mood_str} (상승 {gainers} : 하락 {losers})",
        f"- **자금 흐름:** 메이저 {major_pct:.1f}%, 알트 {(100-major_pct):.1f}%"
    ]
    message_parts.append("\n".join(market_status_lines))

    if leading_sectors:
        message_parts.extend(["\n---", "🔥 **주도 섹터 (1시간 기준)**", *leading_sectors])

    if final_signals:
        final_signals.sort(key=lambda x: x['priority'], reverse=True)
        message_parts.extend(["\n---", "⚡ **실시간 마켓 이벤트**"])
        for anomaly in final_signals[:5]:
            message_parts.append(format_signal_message(
                anomaly['signal_type'], anomaly, enriched_tickers[anomaly['market']],
                alert_history.get(anomaly['market']), REVERSE_SECTOR_MAP
            ))

    top_10_ranked = sorted([(m, r) for m, r in current_rankings.items() if r <= 10], key=lambda item: item[1])
    rank_strs = []
    for market, rank in top_10_ranked:
        prev_rank = previous_rankings.get(market)
        change_str = ""
        if prev_rank:
            change = prev_rank - rank
            if change > 0: change_str = f" (↑{change})"
            elif change < 0: change_str = f" (↓{abs(change)})"
        rank_strs.append(f"{rank}. {market.split('-')[1]}{change_str}")
    
    message_parts.append(f"\n---\n🏆 **24h 거래대금 TOP 10:**\n" + " | ".join(rank_strs))

    use_channel_mention = any(s['signal_type'] == 'INITIAL_BREAKOUT' for s in final_signals)
    
    final_message = "\n".join(message_parts)
    await save_alert_history(alert_history, gcs_client)
    return f"@channel\n{final_message}" if use_channel_mention else final_message

# --- 알림 전송 함수  ---
async def send_notification(session: aiohttp.ClientSession, message: str):
    """웹훅을 통해 메시지를 보냅니다."""
    if not message or not config.WEBHOOK_URL:
        if not config.WEBHOOK_URL:
             logger.warning("웹훅 URL이 설정되지 않았습니다. 알림을 보내지 않습니다.")
        return
    
    use_channel_mention = message.strip().startswith("@channel")
        
    if len(message) > 4000:
        message = message[:3950] + "\n... (메시지가 너무 길어 생략됨)"
        
    payload: Dict[str, Any] = {"text": message}
    
    if use_channel_mention:
        payload["link_names"] = True
    
    try:
        async with session.post(config.WEBHOOK_URL, json=payload, timeout=10) as response:
            if response.ok:
                logger.info("웹훅 알림 전송 성공.")
            else:
                logger.error(f"웹훅 전송 실패 ({response.status}): {await response.text()}")
    except Exception as e:
        logger.error(f"웹훅 전송 중 예외 발생: {e}", exc_info=True)
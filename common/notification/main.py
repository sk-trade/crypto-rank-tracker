# common/notification/main

import asyncio
import datetime
import logging
from typing import Any, Dict, List, Optional

import aiohttp

import config
from common.models import Alert, AlertHistory, TickerData
from common.notification.engine import AlertEngine
from common.notification.formatter import NotificationFormatter
from common.signals.detector import detect_anomalies, filter_market_wide_events
from common.state_manager import load_alert_history, save_alert_history

logger = logging.getLogger(config.APP_LOGGER_NAME)


async def create_and_dispatch_notification(
    raw_tickers: List[Dict[str, Any]],
    enriched_tickers: Dict[str, TickerData],
    current_rankings: Dict[str, int],
    previous_rankings: Dict[str, int],
    SECTORS: Dict[str, List[str]],
    REVERSE_SECTOR_MAP: Dict[str, List[str]],
    alert_history: Dict[str, AlertHistory], 
    market_regime: Dict[str, Any],
    final_alerts: Optional[List[Alert]] = None,
    gcs_client=None,
) -> None:
    """시장 브리핑을 생성하고, 최종 알림이 있을 경우 함께 전송합니다."""

    # 메시지 포매팅
    formatter = NotificationFormatter()
    message = formatter.format_daily_briefing(
        alerts=final_alerts or [], # final_alerts가 None이면 빈 리스트 전달
        raw_tickers=raw_tickers,
        enriched_tickers=enriched_tickers,
        current_rankings=current_rankings,
        previous_rankings=previous_rankings,
        SECTORS=SECTORS,
        REVERSE_SECTOR_MAP=REVERSE_SECTOR_MAP,
        alert_history=alert_history,
        market_regime=market_regime,
    )
    
    # 알림 전송
    if not message:
        logger.info("알림을 보낼 메시지가 없습니다.")
        return

    # 우선순위 높은 알림이 있을 때만 @channel 멘션
    use_channel_mention = False
    if final_alerts:
        top_alert = final_alerts[0]
        if top_alert.priority >= 2: 
            use_channel_mention = True

    final_message = f"@channel\n{message}" if use_channel_mention else message
    await send_notification(final_message)
    logger.info("알림 메시지를 생성하여 전송했습니다.")

    # 상태 저장 (알림이 발생했을 때만 히스토리 업데이트)
    if final_alerts:
        updated_history = _update_alert_history(alert_history, final_alerts)
        await save_alert_history(updated_history, gcs_client)


def _update_alert_history(
    history: Dict[str, AlertHistory], alerts: List[Alert]
) -> Dict[str, AlertHistory]:
    """알림 발송 내역을 기반으로 히스토리를 업데이트합니다."""
    now = datetime.datetime.now(datetime.timezone.utc)
    for alert in alerts:
        candidate = alert.candidate
        market = candidate.market
        signal_type = alert.signal_type

        if signal_type in [
            "BREAKOUT_START",
            "MOMENTUM_ACCELERATION",
            "BREAKDOWN_START",
            "DOWNTREND_ACCELERATION",
        ]:
            history[market] = AlertHistory(
                market=market,
                last_alert_timestamp=now,
                last_signal_type=signal_type,
                last_price=candidate.current_price,
                last_rvol=candidate.rvol,
                initial_timestamp=now,
                initial_price=candidate.current_price,
            )
        elif "SUSTAINED" in signal_type or "FAILED" in signal_type:
            if market in history:
                history[market].last_alert_timestamp = now
                history[market].last_signal_type = signal_type
                history[market].last_price = candidate.current_price
                history[market].last_rvol = candidate.rvol
    return history


async def send_notification(message: str):
    """웹훅을 통해 메시지를 보냅니다."""
    if not config.WEBHOOK_URL:
        logger.warning("웹훅 URL이 설정되지 않았습니다. 알림을 보내지 않습니다.")
        return

    payload: Dict[str, Any] = {"text": message}
    if message.strip().startswith("@channel"):
        payload["link_names"] = True

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                config.WEBHOOK_URL, json=payload, timeout=10
            ) as response:
                if response.ok:
                    logger.info("웹훅 알림 전송 성공.")
                else:
                    logger.error(
                        f"웹훅 전송 실패 ({response.status}): {await response.text()}"
                    )
    except Exception as e:
        logger.error(f"웹훅 전송 중 예외 발생: {e}", exc_info=True)
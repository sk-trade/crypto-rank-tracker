# common/notification/main

import asyncio
import datetime
import logging
from typing import Any, Dict, List

import aiohttp

import config
from common.models import Alert, AlertHistory, TickerData
from common.notification.engine import AlertEngine
from common.notification.formatter import NotificationFormatter
from common.signals.detector import detect_anomalies, filter_market_wide_events
from common.state_manager import load_alert_history, save_alert_history

logger = logging.getLogger(config.APP_LOGGER_NAME)


async def create_and_dispatch_notification(
    enriched_tickers: Dict[str, TickerData],
    raw_tickers: List[Dict[str, Any]],
    current_rankings: Dict[str, int],
    previous_rankings: Dict[str, int],
    SECTORS: Dict[str, List[str]],
    REVERSE_SECTOR_MAP: Dict[str, List[str]],
    gcs_client=None,
) -> None:
    """전체 알림 생성 및 발송 파이프라인을 실행합니다."""
    # 1. 상태 로드
    alert_history = await load_alert_history(gcs_client)

    # 2. 시그널 탐지 및 필터링
    raw_candidates = detect_anomalies(enriched_tickers, SECTORS, REVERSE_SECTOR_MAP)
    # 시장 전체 이벤트 필터 적용
    filtered_candidates = filter_market_wide_events(raw_candidates, enriched_tickers)

    # 3. 시그널 분류
    engine = AlertEngine()
    final_alerts = engine.process_signals(
        filtered_candidates, enriched_tickers, alert_history
    )

    # 4. 메시지 포매팅
    formatter = NotificationFormatter()
    message = formatter.format_daily_briefing(
        alerts=final_alerts,
        raw_tickers=raw_tickers,
        enriched_tickers=enriched_tickers,
        current_rankings=current_rankings,
        previous_rankings=previous_rankings,
        SECTORS=SECTORS,
        REVERSE_SECTOR_MAP=REVERSE_SECTOR_MAP,
        alert_history=alert_history,
    )

    # 5. 알림 전송
    if not message:
        logger.info("알림을 보낼 유의미한 시그널이 없습니다.")
    else:
        use_channel_mention = False
        if final_alerts:
            top_alert = final_alerts[0]
            if top_alert.priority >= 2 and top_alert.candidate.confidence >= 0.75:
                use_channel_mention = True

        final_message = f"@channel\n{message}" if use_channel_mention else message
        await send_notification(final_message)
        logger.info("알림 메시지를 생성하여 전송했습니다.")

    # 6. 상태 저장
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
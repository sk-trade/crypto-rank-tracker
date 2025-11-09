# common/notification/engine.py


import datetime
import logging
from typing import Dict, List, Tuple

import config
from common.models import Alert, AlertHistory, SignalCandidate, TickerData

logger = logging.getLogger(config.APP_LOGGER_NAME)


class AlertEngine:
    """시그널 후보군을 평가하여 최종 알림 대상을 결정하고 분류합니다."""

    def process_signals(
        self,
        candidates: List[SignalCandidate],
        enriched_tickers: Dict[str, TickerData],
        history: Dict[str, AlertHistory],
    ) -> List[Alert]:
        """후보 시그널 목록을 필터링하고 분류하여 최종 알림 목록을 반환합니다."""
        # 1. 게이트키퍼: 알림을 보낼 최소 가치가 있는지 1차 필터링
        valuable_candidates = [c for c in candidates if self._is_worth_alerting(c)]

        # 2. 분류: 각 시그널의 유형과 우선순위 결정
        alerts = []
        for candidate in valuable_candidates:
            ticker = enriched_tickers.get(candidate.market)
            if not ticker:
                logger.warning(
                    f"AlertEngine: {candidate.market}의 TickerData를 찾을 수 없습니다."
                )
                continue

            signal_type, priority = self._get_alert_type_and_priority(
                candidate, ticker, history
            )

            if signal_type:
                alerts.append(
                    Alert(
                        candidate=candidate,
                        ticker_data=ticker,
                        signal_type=signal_type,
                        priority=priority,
                    )
                )

        # 3. 우선순위가 높은 순으로 정렬
        return sorted(alerts, key=lambda x: x.priority, reverse=True)

    def _is_worth_alerting(self, candidate: SignalCandidate) -> bool:
        """시그널이 알림을 보낼 최소 기준(가격변동, 신뢰도)을 충족하는지 확인합니다."""
        if abs(candidate.price_change) < config.ALERT_MIN_PRICE_CHANGE_10M:
            return False
        if candidate.confidence < config.ALERT_MIN_CONFIDENCE:
            return False
        return True

    def _get_alert_type_and_priority(
        self,
        candidate: SignalCandidate,
        ticker: TickerData,
        history: Dict[str, AlertHistory],
    ) -> Tuple[str | None, int]:
        """시그널의 유형과 우선순위를 신규/후속 이벤트 여부에 따라 결정합니다."""
        market = candidate.market
        previous_alert = history.get(market)

        price_change_10m = ticker.price_change_10m or 0.0
        price_change_1h = ticker.price_change_1h or 0.0

        # --- Case 1: 신규 이벤트 (이전 알림이 없거나 쿨다운이 지난 경우) ---
        is_new_event = not previous_alert or (
            (datetime.datetime.now(datetime.timezone.utc) - previous_alert.last_alert_timestamp).total_seconds()
            >= config.ALERT_COOLDOWN_MINUTES * 60
        )

        if is_new_event:
            if price_change_10m > 0:
                return (
                    ("MOMENTUM_ACCELERATION", 2)
                    if price_change_1h > 2.0
                    else ("BREAKOUT_START", 3)
                )
            elif price_change_10m < 0:
                return (
                    ("DOWNTREND_ACCELERATION", 2)
                    if price_change_1h < -2.0
                    else ("BREAKDOWN_START", 3)
                )
            return "UNUSUAL_ACTIVITY", 1

        # --- Case 2: 후속 움직임 (쿨다운 기간 내) ---
        else:
            if not previous_alert:
                logger.warning(f"AlertEngine: {market}의 이전 알림이 없습니다.")
                return None, 0
            
            current_price = candidate.current_price
            previous_price = previous_alert.last_price
            additional_change_pct = (current_price / previous_price - 1) * 100

            if abs(additional_change_pct) >= config.SUSTAINED_MOMENTUM_MIN_ADDITIONAL_CHANGE_PCT:
                was_bullish = "BREAKOUT" in previous_alert.last_signal_type or "ACCELERATION" in previous_alert.last_signal_type
                was_bearish = "BREAKDOWN" in previous_alert.last_signal_type or "DOWNTREND" in previous_alert.last_signal_type

                if was_bullish and additional_change_pct > 0:
                    return "BULL_MOMENTUM_SUSTAINED", 1
                if was_bullish and additional_change_pct < 0:
                    return "BULL_MOMENTUM_FAILED", 2
                if was_bearish and additional_change_pct < 0:
                    return "BEAR_MOMENTUM_SUSTAINED", 1
                if was_bearish and additional_change_pct > 0:
                    return "BEAR_MOMENTUM_FAILED", 2

            logger.debug(f"{market}는 쿨다운 중이며, 의미있는 추가 변동이 없어 건너뜁니다.")
            return None, 0
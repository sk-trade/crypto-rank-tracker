# common/notification/engine.py


import datetime
import logging
from typing import Dict, List, Tuple

import config
from common.models import (
    Alert,
    AlertHistory,
    SignalCandidate,
    SignalType,
    StructureDirection,
    TickerData,
)

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
        # 1. Classify price structure before filtering. A failed breakout may
        # matter even if the latest bar's percentage move is small.
        alerts = []
        for candidate in candidates:
            ticker = enriched_tickers.get(candidate.market)
            if not ticker:
                logger.warning(
                    f"AlertEngine: {candidate.market}의 TickerData를 찾을 수 없습니다."
                )
                continue

            signal_type, priority, structure_level = self._get_alert_type_and_priority(
                candidate, ticker, history
            )

            if signal_type and self._is_worth_alerting(candidate, signal_type):
                alerts.append(
                    Alert(
                        candidate=candidate,
                        ticker_data=ticker,
                        signal_type=signal_type,
                        priority=priority,
                        structure_level=structure_level,
                    )
                )

        # 3. 우선순위가 높은 순으로 정렬
        return sorted(alerts, key=lambda x: x.priority, reverse=True)

    def _is_worth_alerting(
        self, candidate: SignalCandidate, signal_type: SignalType
    ) -> bool:
        """시그널이 알림을 보낼 최소 기준(가격변동, signal score)을 충족하는지 확인합니다."""
        if (
            not signal_type.is_failure
            and abs(candidate.price_change) < config.ALERT_MIN_PRICE_CHANGE_10M
        ):
            return False
        if candidate.signal_score < config.ALERT_MIN_SIGNAL_SCORE:
            return False
        return True

    def _get_alert_type_and_priority(
        self,
        candidate: SignalCandidate,
        ticker: TickerData,
        history: Dict[str, AlertHistory],
    ) -> Tuple[SignalType | None, int, float | None]:
        """Classify independent breakout, acceleration, and failure transitions."""
        market = candidate.market
        previous_alert = history.get(market)
        current_price = candidate.current_price
        if previous_alert:
            if previous_alert.structure_level is not None:
                level = previous_alert.structure_level
                if previous_alert.structure_direction is StructureDirection.BULLISH:
                    if current_price <= level:
                        return SignalType.BULL_MOMENTUM_FAILED, 3, level
                    if current_price > previous_alert.last_price and self._continuation_is_alertable(
                        previous_alert, current_price
                    ):
                        return SignalType.MOMENTUM_ACCELERATION, 2, level
                elif previous_alert.structure_direction is StructureDirection.BEARISH:
                    if current_price >= level:
                        return SignalType.BEAR_MOMENTUM_FAILED, 3, level
                    if current_price < previous_alert.last_price and self._continuation_is_alertable(
                        previous_alert, current_price
                    ):
                        return SignalType.DOWNTREND_ACCELERATION, 2, level
                return None, 0, None

            if not self._cooldown_expired(previous_alert):
                return None, 0, None

        return self._breakout_transition(ticker)

    def _cooldown_expired(self, previous_alert: AlertHistory) -> bool:
        cooldown = datetime.timedelta(minutes=config.ALERT_COOLDOWN_MINUTES)
        elapsed = datetime.datetime.now(datetime.timezone.utc) - previous_alert.last_alert_timestamp
        return elapsed >= cooldown

    def _continuation_is_alertable(
        self, previous_alert: AlertHistory, current_price: float
    ) -> bool:
        if self._cooldown_expired(previous_alert):
            return True
        if previous_alert.last_price <= 0:
            return False
        additional_change_pct = abs(current_price / previous_alert.last_price - 1.0) * 100
        return additional_change_pct >= config.SUSTAINED_MOMENTUM_MIN_ADDITIONAL_CHANGE_PCT

    def _breakout_transition(
        self, ticker: TickerData
    ) -> Tuple[SignalType | None, int, float | None]:
        candles = ticker.candle_history
        lookback = config.BREAKOUT_STRUCTURE_LOOKBACK_BARS
        if len(candles) < lookback + 1:
            return None, 0, None
        previous = candles[-lookback - 1 : -1]
        close = candles[-1].close_price
        resistance = max(candle.high_price for candle in previous)
        support = min(candle.low_price for candle in previous)
        if close > resistance:
            return SignalType.BREAKOUT_START, 3, resistance
        if close < support:
            return SignalType.BREAKDOWN_START, 3, support
        return None, 0, None

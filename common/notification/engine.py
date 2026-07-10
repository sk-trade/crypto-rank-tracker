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

    def _is_worth_alerting(self, candidate: SignalCandidate, signal_type: str) -> bool:
        """시그널이 알림을 보낼 최소 기준(가격변동, signal score)을 충족하는지 확인합니다."""
        if (
            signal_type not in {"BULL_MOMENTUM_FAILED", "BEAR_MOMENTUM_FAILED"}
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
    ) -> Tuple[str | None, int, float | None]:
        """Classify independent breakout, acceleration, and failure transitions."""
        market = candidate.market
        previous_alert = history.get(market)
        current_price = candidate.current_price
        if previous_alert and previous_alert.structure_level is not None:
            level = previous_alert.structure_level
            if previous_alert.structure_direction == "bullish":
                if current_price <= level:
                    return "BULL_MOMENTUM_FAILED", 3, level
                if current_price > previous_alert.last_price:
                    return "MOMENTUM_ACCELERATION", 2, level
            elif previous_alert.structure_direction == "bearish":
                if current_price >= level:
                    return "BEAR_MOMENTUM_FAILED", 3, level
                if current_price < previous_alert.last_price:
                    return "DOWNTREND_ACCELERATION", 2, level
            return None, 0, None

        return self._breakout_transition(ticker)

    def _breakout_transition(self, ticker: TickerData) -> Tuple[str | None, int, float | None]:
        candles = ticker.candle_history
        lookback = config.BREAKOUT_STRUCTURE_LOOKBACK_BARS
        if len(candles) < lookback + 1:
            return None, 0, None
        previous = candles[-lookback - 1 : -1]
        close = candles[-1].close_price
        resistance = max(candle.high_price for candle in previous)
        support = min(candle.low_price for candle in previous)
        if close > resistance:
            return "BREAKOUT_START", 3, resistance
        if close < support:
            return "BREAKDOWN_START", 3, support
        return None, 0, None

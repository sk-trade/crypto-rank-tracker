"""Select alert transitions from scored signal candidates."""

import datetime
import logging

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
    """Classify alert state transitions and apply delivery thresholds."""

    def select_alerts(
        self,
        candidates: list[SignalCandidate],
        enriched_tickers: dict[str, TickerData],
        history: dict[str, AlertHistory],
    ) -> list[Alert]:
        """Return prioritized alerts for valid state transitions."""
        # 1. Classify price structure before filtering. A failed breakout may
        # matter even if the latest bar's percentage move is small.
        alerts = []
        for candidate in candidates:
            ticker = enriched_tickers.get(candidate.market)
            if not ticker:
                logger.warning(
                    "Skipping alert candidate without ticker context: %s",
                    candidate.market,
                )
                continue

            signal_type, priority, structure_level = self._classify_alert_transition(
                candidate, ticker, history
            )

            if signal_type and self._meets_alert_thresholds(candidate, signal_type):
                alerts.append(
                    Alert(
                        candidate=candidate,
                        ticker_data=ticker,
                        signal_type=signal_type,
                        priority=priority,
                        structure_level=structure_level,
                    )
                )

        return sorted(
            alerts,
            key=lambda alert: alert.priority,
            reverse=True,
        )

    def _meets_alert_thresholds(
        self, candidate: SignalCandidate, signal_type: SignalType
    ) -> bool:
        """Return whether a classified transition meets alert thresholds."""
        if (
            not signal_type.is_failure
            and abs(candidate.price_change) < config.ALERT_MIN_PRICE_CHANGE_10M
        ):
            return False
        return candidate.signal_score >= config.ALERT_MIN_SIGNAL_SCORE

    def _classify_alert_transition(
        self,
        candidate: SignalCandidate,
        ticker: TickerData,
        history: dict[str, AlertHistory],
    ) -> tuple[SignalType | None, int, float | None]:
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
                    if (
                        current_price > previous_alert.last_price
                        and self._continuation_is_alertable(
                            previous_alert, current_price
                        )
                    ):
                        return SignalType.MOMENTUM_ACCELERATION, 2, level
                elif previous_alert.structure_direction is StructureDirection.BEARISH:
                    if current_price >= level:
                        return SignalType.BEAR_MOMENTUM_FAILED, 3, level
                    if (
                        current_price < previous_alert.last_price
                        and self._continuation_is_alertable(
                            previous_alert, current_price
                        )
                    ):
                        return SignalType.DOWNTREND_ACCELERATION, 2, level
                return None, 0, None

            if not self._cooldown_expired(previous_alert):
                return None, 0, None

        return self._breakout_transition(ticker)

    def _cooldown_expired(self, previous_alert: AlertHistory) -> bool:
        cooldown = datetime.timedelta(minutes=config.ALERT_COOLDOWN_MINUTES)
        elapsed = (
            datetime.datetime.now(datetime.UTC) - previous_alert.last_alert_timestamp
        )
        return elapsed >= cooldown

    def _continuation_is_alertable(
        self, previous_alert: AlertHistory, current_price: float
    ) -> bool:
        if self._cooldown_expired(previous_alert):
            return True
        if previous_alert.last_price <= 0:
            return False
        additional_change_pct = (
            abs(current_price / previous_alert.last_price - 1.0) * 100
        )
        return (
            additional_change_pct >= config.SUSTAINED_MOMENTUM_MIN_ADDITIONAL_CHANGE_PCT
        )

    def _breakout_transition(
        self, ticker: TickerData
    ) -> tuple[SignalType | None, int, float | None]:
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

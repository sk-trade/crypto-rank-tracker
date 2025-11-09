# common/notification/formatter.py

# common/notification/formatter.py
"""분석된 데이터를 기반으로 사용자에게 보여질 최종 알림 메시지를 생성합니다."""

import datetime
from typing import Any, Dict, List, Optional

import numpy as np

from common.models import Alert, AlertHistory, TickerData


class NotificationFormatter:
    """분석된 데이터와 알림 객체를 기반으로 사용자 메시지를 생성하는 클래스입니다."""

    def format_daily_briefing(
        self,
        alerts: List[Alert],
        raw_tickers: List[Dict[str, Any]],
        enriched_tickers: Dict[str, TickerData],
        current_rankings: Dict[str, int],
        previous_rankings: Dict[str, int],
        SECTORS: Dict[str, List[str]],
        REVERSE_SECTOR_MAP: Dict[str, List[str]],
        alert_history: Dict[str, AlertHistory],
    ) -> str:
        """시장 브리핑 전체 메시지를 조립합니다."""
        kst = datetime.timezone(datetime.timedelta(hours=9))
        now_kst = datetime.datetime.now(kst)

        parts = [f"📊 **업비트 마켓 브리핑 ({now_kst.strftime('%H:%M')} KST)**"]
        parts.append(self._format_market_status(raw_tickers, enriched_tickers))

        if leading_sectors_str := self._format_leading_sectors(enriched_tickers, SECTORS):
            parts.extend(["\n---", "🔥 **주도 섹터 (1시간 기준)**", leading_sectors_str])

        if alerts:
            parts.extend(["\n---", "⚡ **실시간 마켓 이벤트**"])
            for alert in alerts[:10]:  # 최대 10개 알림
                previous_alert = alert_history.get(alert.candidate.market)
                parts.append(
                    self._format_single_alert(alert, REVERSE_SECTOR_MAP, previous_alert)
                )

        parts.append(self._format_top_10_ranking(current_rankings, previous_rankings))

        return "\n".join(parts)

    def _format_market_status(
        self, raw_tickers: List[Dict[str, Any]], enriched_tickers: Dict[str, TickerData]
    ) -> str:
        """시장 현황 요약 문자열을 생성합니다."""
        gainers = sum(
            1
            for t in enriched_tickers.values()
            if t.price_change_10m is not None and t.price_change_10m > 0
        )
        losers = sum(
            1
            for t in enriched_tickers.values()
            if t.price_change_10m is not None and t.price_change_10m < 0
        )
        mood = "강세" if gainers > losers * 1.2 else "약세" if losers > gainers * 1.2 else "보합"

        total_24h = sum(t.get("acc_trade_price_24h", 0) for t in raw_tickers)
        major_24h = sum(
            t.get("acc_trade_price_24h", 0)
            for t in raw_tickers
            if t["market"] in ["KRW-BTC", "KRW-ETH"]
        )
        major_pct = (major_24h / total_24h * 100) if total_24h > 0 else 0

        return "\n".join(
            [
                "**시장 현황:**",
                f"- **분위기:** {mood} (상승 {gainers} : 하락 {losers})",
                f"- **자금 흐름:** 메이저 {major_pct:.1f}%, 알트 {(100-major_pct):.1f}%",
            ]
        )

    def _format_leading_sectors(
        self, enriched_tickers: Dict[str, TickerData], SECTORS: Dict[str, List[str]]
    ) -> Optional[str]:
        """주도 섹터 분석 및 문자열을 생성합니다."""
        sector_perf = {}
        for name, coins in SECTORS.items():
            returns = [
                t.price_change_1h
                for c in coins
                if (t := enriched_tickers.get(c)) and t.price_change_1h is not None
            ]
            if len(returns) < 3:
                continue

            avg_return = np.mean(returns)
            rising_count = sum(1 for r in returns if r > 0)

            if avg_return > 1.5 and (rising_count / len(returns)) >= 0.6:
                sector_perf[name] = {
                    "avg_return": avg_return,
                    "consistency": f"{rising_count}/{len(returns)} 상승",
                }

        if not sector_perf:
            return None

        sorted_sectors = sorted(
            sector_perf.items(), key=lambda item: item[1]["avg_return"], reverse=True
        )
        lines = [
            f"- **{name} ({perf['consistency']}):** 1시간 평균 `{perf['avg_return']:.2f}%` 상승"
            for name, perf in sorted_sectors[:3]
        ]
        return "\n".join(lines)

    def _format_top_10_ranking(
        self, current_rankings: Dict[str, int], previous_rankings: Dict[str, int]
    ) -> str:
        """24시간 거래대금 TOP 10 문자열을 생성합니다."""
        top_10 = sorted(
            [(m, r) for m, r in current_rankings.items() if r <= 10],
            key=lambda item: item[1],
        )
        rank_strs = []
        for market, rank in top_10:
            prev_rank = previous_rankings.get(market)
            change_str = ""
            if prev_rank:
                change = prev_rank - rank
                if change > 0:
                    change_str = f" (↑{change})"
                elif change < 0:
                    change_str = f" (↓{abs(change)})"
            rank_strs.append(f"{rank}. {market.split('-')[1]}{change_str}")

        return f"\n---\n🏆 **24h 거래대금 TOP 10:**\n" + " | ".join(rank_strs)

    def _format_single_alert(
        self,
        alert: Alert,
        reverse_sector_map: Dict[str, List[str]],
        previous_alert: Optional[AlertHistory],
    ) -> str:
        """단일 알림에 대한 사용자 친화적인 메시지를 생성합니다."""
        candidate = alert.candidate
        ticker = alert.ticker_data
        market = candidate.market
        tag = reverse_sector_map.get(market, [""])[0]

        signal_map = {
            "BREAKOUT_START": "초기 돌파 시작", "MOMENTUM_ACCELERATION": "상승 모멘텀 가속",
            "BREAKDOWN_START": "초기 이탈 시작", "DOWNTREND_ACCELERATION": "하락 모멘텀 가속",
            "BULL_MOMENTUM_SUSTAINED": "모멘텀 지속", "BULL_MOMENTUM_FAILED": "상승 모멘텀 실패",
            "BEAR_MOMENTUM_SUSTAINED": "모멘텀 지속", "BEAR_MOMENTUM_FAILED": "하락 모멘텀 실패",
            "UNUSUAL_ACTIVITY": "특이 거래 활동",
        }
        signal_title = signal_map.get(alert.signal_type, "주요 변동")
        icon = "🔥" if "BULL" in alert.signal_type or "BREAKOUT" in alert.signal_type or "ACCELERATION" in alert.signal_type else "🧊"

        header = (
            f"{icon} **{market}{f' ({tag})' if tag else ''}: {candidate.price_change:+.1f}%** "
            f"({signal_title})\n   현재가: `{candidate.current_price:,.4f}`원"
        )
        features = self._build_alert_features(candidate, ticker)
        interpretation = self._build_alert_interpretation(alert, previous_alert)
        risk = f"⚠️ **리스크:** `{'높음' if ticker.volatility_tier in ['VERY_HIGH', 'EXTREME'] else '중간'}` (변동성: {ticker.volatility_tier})"

        return "\n\n".join(filter(None, [header, features, interpretation, risk]))

    def _build_alert_features(self, candidate, ticker) -> str:
        """알림의 주요 특징(거래량, 시장 관계 등) 문자열을 생성합니다."""
        parts = []
        z = candidate.rvol_z_score
        rarity = "★★★ (극도로 이례적)" if z > 7 else "★★☆ (매우 이례적)" if z > 5 else "★☆☆ (이례적)"
        parts.append(f"• **거래량:** 평소의 `{candidate.rvol:.1f}배` (특이성: {rarity})")

        if ticker.decoupling_score is not None:
            desc = ""
            if ticker.decoupling_status == "STRONG_DECOUPLE": desc = "BTC/ETH 역행"
            elif "AMPLIFIED" in ticker.decoupling_status: desc = "시장 모멘텀 증폭"
            if desc: parts.append(f"• **시장 관계:** {desc} (`{ticker.decoupling_score:+.1f}%p`)")

        if isinstance(ticker.candle_shape, dict) and ticker.candle_shape.get("type") != "NORMAL":
            shape_map = {"STRONG_REJECTION_UP": "강한 상방 저항", "STRONG_SUPPORT_DOWN": "강한 하방 지지", "STRONG_MOMENTUM": "강한 모멘텀"}
            if shape_text := shape_map.get(ticker.candle_shape["type"]):
                parts.append(f"• **캔들 분석:** `{shape_text}` (신뢰도: {ticker.candle_shape['reliability']})")
        
        if candidate.contexts: parts.append(f"• **추가 맥락:** {', '.join(candidate.contexts)}")
        return "\n".join(parts)

    def _build_alert_interpretation(self, alert, previous_alert) -> str:
        """알림에 대한 종합 해석 문자열을 생성합니다."""
        parts = []
        ticker = alert.ticker_data
        candidate = alert.candidate
        market_short = candidate.market.split("-")[1]

        if ticker.decoupling_status == "STRONG_DECOUPLE" and ticker.decoupling_score > 0:
            parts.append(f"시장의 조용한 흐름에도 불구하고, **{market_short}에만 집중된 강력한 매수세**가 유입된 것으로 보입니다.")
        
        if isinstance(ticker.candle_shape, dict) and ticker.candle_shape.get("reliability") == "HIGH":
            shape_type = ticker.candle_shape.get("type")
            if shape_type == "STRONG_MOMENTUM": parts.append("거래량을 동반한 꽉 찬 양봉은 현재 상승 방향에 대한 시장의 강한 확신을 보여줍니다.")
            elif shape_type == "STRONG_SUPPORT_DOWN": parts.append("하락 시도가 강력한 매수세에 의해 차단되며, 단기적인 저점 방어에 성공한 모습입니다.")

        if candidate.rvol_z_score > 5.0: parts.append("통계적으로 매우 이례적인 거래량은 기관 또는 고래의 개입을 강하게 시사합니다.")

        if "SUSTAINED" in alert.signal_type and previous_alert:
            change = (candidate.current_price / previous_alert.initial_price - 1) * 100
            elapsed = (datetime.datetime.now(datetime.timezone.utc) - previous_alert.initial_timestamp).total_seconds() / 60
            parts.append(f"최초 알림 후 `{elapsed:.0f}분` 동안 모멘텀이 이어져 `{change:+.2f}%` 누적 변동되었습니다.")
        
        return "**[종합 해석]**\n" + "\n".join(f"→ {p}" for p in parts) if parts else ""
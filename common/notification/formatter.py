# common/notification/formatter.py

"""분석된 데이터를 기반으로 사용자에게 보여질 최종 알림 메시지를 생성합니다."""

import datetime
from typing import Dict, List, Optional

import numpy as np

from common.models import (
    Alert,
    AlertHistory,
    DataQualityIssue,
    MarketTicker,
    MarketRegimeSnapshot,
    SignalType,
    TickerData,
)


class NotificationFormatter:
    """분석된 데이터와 알림 객체를 기반으로 사용자 메시지를 생성하는 클래스입니다."""

    def format_data_quality_alert(self, issues: List[DataQualityIssue]) -> str:
        """Format an operational incident without presenting market analysis as valid."""
        kst = datetime.timezone(datetime.timedelta(hours=9))
        now_kst = datetime.datetime.now(kst)
        details = "\n".join(f"- [{issue.code.value}] {issue.message}" for issue in issues)
        return "\n".join(
            [
                f"🚨 **시장 데이터 품질 장애 ({now_kst.strftime('%H:%M')} KST)**",
                "이번 스캔의 시장 데이터가 기준에 미달해 시그널과 일반 브리핑을 생성하지 않았습니다.",
                details,
            ]
        )

    def format_daily_briefing(
        self,
        alerts: List[Alert],
        raw_tickers: List[MarketTicker],
        enriched_tickers: Dict[str, TickerData],
        current_rankings: Dict[str, int],
        previous_rankings: Dict[str, int],
        SECTORS: Dict[str, List[str]],
        REVERSE_SECTOR_MAP: Dict[str, List[str]],
        alert_history: Dict[str, AlertHistory],
        market_regime: MarketRegimeSnapshot,
    ) -> str:
        """시장 브리핑 전체 메시지를 조립합니다."""
        kst = datetime.timezone(datetime.timedelta(hours=9))
        now_kst = datetime.datetime.now(kst)

        parts = [f"📊 **업비트 마켓 브리핑 ({now_kst.strftime('%H:%M')} KST)**"]
        parts.append(self._format_market_status(raw_tickers, enriched_tickers))

        if leading_sectors_str := self._format_leading_sectors(
            enriched_tickers, SECTORS
        ):
            parts.extend(
                ["\n---", "🔥 **주도 섹터 (1시간 기준)**", leading_sectors_str]
            )

        if alerts:
            parts.extend(["\n---", "⚡ **실시간 마켓 이벤트**"])
            for alert in alerts[:10]:
                parts.append(self._format_single_alert(alert, REVERSE_SECTOR_MAP, market_regime))
        else:
            parts.append("\n---")
            parts.append("✅ 특이사항 없음. 시장을 계속 주시합니다.")

        parts.append(self._format_top_10_ranking(current_rankings, previous_rankings))

        return "\n".join(parts)

    def _format_market_status(
        self,
        raw_tickers: List[MarketTicker],
        enriched_tickers: Dict[str, TickerData],
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

        total_24h = sum(ticker.acc_trade_price_24h for ticker in raw_tickers)
        major_24h = sum(
            ticker.acc_trade_price_24h
            for ticker in raw_tickers
            if ticker.market in {"KRW-BTC", "KRW-ETH"}
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
        """
        [Advanced] 시장 대비 초과 수익(Alpha)과 다중 시간대(1H, 4H) 검증을 통한 주도 섹터 발굴
        """
        # 시장 전체 평균 (Benchmark) 계산
        all_changes_1h = [t.price_change_1h for t in enriched_tickers.values() if t.price_change_1h is not None]
        all_changes_4h = [t.price_change_4h for t in enriched_tickers.values() if t.price_change_4h is not None]
        
        if not all_changes_1h or not all_changes_4h:
            return None
        
        market_avg_1h = np.mean(all_changes_1h)
        market_avg_4h = np.mean(all_changes_4h)
        
        # 시장 상황(Regime)에 따른 동적 임계값 설정
        if market_avg_1h > 0:
            min_alpha_1h = 2.0  # 상승장: 시장보다 2%p 더 강해야 함
            header_icon = "🚀"
            header_text = "주도 섹터 (상승장)"
        else:
            min_alpha_1h = 1.5  # 하락장: 1.5%p만 방어해도 훌륭함
            header_icon = "🛡️"
            header_text = "방어 섹터 (하락장)"

        sector_perf = {}
        
        for name, coins in SECTORS.items():
            # 데이터 유효성 체크
            tickers = [enriched_tickers.get(c) for c in coins if enriched_tickers.get(c)]
            valid_tickers = [t for t in tickers if t.price_change_1h is not None and t.price_change_4h is not None]
            
            if len(valid_tickers) < 4:
                continue  # 최소 4개 종목 이상인 섹터만 분석
            
            # 섹터 지표 계산
            avg_return_1h = np.mean([t.price_change_1h for t in valid_tickers])
            avg_return_4h = np.mean([t.price_change_4h for t in valid_tickers])
            avg_rvol = np.mean([t.relative_volume or 1.0 for t in valid_tickers])
            
            # Alpha (초과 수익)
            alpha_1h = avg_return_1h - market_avg_1h
            alpha_4h = avg_return_4h - market_avg_4h
            
            # Breadth (너비) - 대장주 착시 방지
            beating_market_count = sum(1 for t in valid_tickers if t.price_change_1h > market_avg_1h)
            breadth_ratio = beating_market_count / len(valid_tickers)
            
            # 섹터 크기에 따른 Breadth 기준 차등 
            min_breadth = 0.6 if len(valid_tickers) >= 10 else 0.75

            # 필터링 
            if (
                alpha_1h > min_alpha_1h and
                alpha_4h > 0.5 and          
                avg_rvol > 1.5 and          
                breadth_ratio >= min_breadth
            ):
                sector_perf[name] = {
                    "alpha_1h": alpha_1h,
                    "alpha_4h": alpha_4h,
                    "avg_rvol": avg_rvol,
                    "consistency": f"{beating_market_count}/{len(valid_tickers)}",
                    "score": alpha_1h * 0.7 + alpha_4h * 0.3
                }

        if not sector_perf:
            return None

        # 점수순 정렬
        sorted_sectors = sorted(
            sector_perf.items(), key=lambda item: item[1]["score"], reverse=True
        )
        
        lines = []
        for name, perf in sorted_sectors[:5]: 
            lines.append(
                f"- **{name} ({perf['consistency']}):** "
                f"Alpha `{perf['alpha_1h']:+.1f}%p` (RVOL {perf['avg_rvol']:.1f}x)"
            )
            
        return f"\n---\n{header_icon} **{header_text}**\n" + "\n".join(lines)

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
            rank_strs.append(
                f"{rank}. {market.removeprefix('KRW-')}{change_str}"
            )

        return "\n---\n🏆 **24h 거래대금 TOP 10:**\n" + " | ".join(rank_strs)

    def _format_single_alert(
        self,
        alert: Alert,
        reverse_sector_map: Dict[str, List[str]],
        market_regime: MarketRegimeSnapshot,
    ) -> str:
        """단일 알림을 객관적인 Signal Checklist 포맷으로 생성합니다."""
        candidate = alert.candidate
        ticker = alert.ticker_data
        market = candidate.market
        tags = reverse_sector_map.get(market, [])
        tag = tags[0] if tags else ""

        signal_map = {
            SignalType.MOMENTUM_ACCELERATION: "상승 모멘텀 가속",
            SignalType.DOWNTREND_ACCELERATION: "하락 모멘텀 가속",
            SignalType.BREAKOUT_START: "초기 돌파 시작",
            SignalType.BREAKDOWN_START: "초기 이탈 시작",
            SignalType.BULL_MOMENTUM_FAILED: "상승 모멘텀 실패",
            SignalType.BEAR_MOMENTUM_FAILED: "하락 모멘텀 실패",
        }
        signal_title = signal_map[alert.signal_type]
        
        icon = "🔥" if (candidate.price_change or 0) > 0 else "🧊"
        header = (
            f"{icon} **{market.removeprefix('KRW-')}{f' ({tag})' if tag else ''}: "
            f"{signal_title}** (Signal score: {candidate.signal_score:.2f})"
        )

        residual_score = ticker.residual_momentum_score

        checklist = [
            "```",
            "--- Signal Checklist ---",
            f"[10min] Price Change    : {candidate.price_change:+.2f}%",
            f"[10min] RVOL            : {candidate.rvol:.1f}x (Z-Score: {candidate.rvol_z_score:.1f})",
            f"[ 1hr ] Trend           : {ticker.trend_1h_stable}",
            f"[Daily] Above MA50      : {ticker.is_above_ma50_daily}",
            f"[Daily] Above MA200     : {ticker.is_above_ma200_daily}",
            f"[Market] Regime        : {market_regime.regime.value}",
        ]
        if residual_score is not None:
            checklist.append(f"[Market] Residual momentum: {residual_score:+.2f} sigma")
        
        checklist.append("```")
        
        return "\n".join([header] + checklist)

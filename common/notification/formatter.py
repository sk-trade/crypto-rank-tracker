"""Render validated market evidence as localized operator notifications."""

import datetime

import numpy as np

import config
from common.models import (
    Alert,
    AttentionCandidate,
    AttentionLane,
    AttentionStage,
    DataQualityIssue,
    EvidenceFamily,
    EvidenceVerdict,
    MarketRegimeSnapshot,
    MarketTicker,
    RejectionCode,
    SignalType,
    TickerData,
)

KOREA_STANDARD_TIME = datetime.timezone(datetime.timedelta(hours=9))


def _format_kst_clock(observed_at: datetime.datetime) -> str:
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return observed_at.astimezone(KOREA_STANDARD_TIME).strftime("%H:%M")


class NotificationFormatter:
    """Format market evidence without changing its decision semantics."""

    def format_data_quality_alert(
        self,
        issues: list[DataQualityIssue],
        observed_at: datetime.datetime,
    ) -> str:
        """Format an operational incident without presenting market analysis as valid."""
        details = "\n".join(self._format_data_quality_issue(issue) for issue in issues)
        return "\n".join(
            [
                f"🚨 **시장 데이터 품질 장애 ({_format_kst_clock(observed_at)} KST 기준)**",
                "이번 스캔은 시장 데이터가 안전 기준을 충족하지 못해 관심종목과 일반 브리핑을 제공하지 않았습니다.",
                details,
                "다음 예약 실행에서 새 시장 데이터를 다시 확인합니다.",
            ]
        )

    def _format_data_quality_issue(self, issue: DataQualityIssue) -> str:
        if issue.code is RejectionCode.CANDLE_COVERAGE_BELOW_MINIMUM:
            successful = issue.details.get("successful_markets")
            total = issue.details.get("total_markets")
            success_rate = issue.details.get("success_rate")
            minimum = issue.details.get("minimum_success_rate")
            if all(
                isinstance(value, (int, float))
                for value in (successful, total, success_rate, minimum)
            ):
                return (
                    f"- 10분 캔들 확보: {successful}/{total} ({success_rate:.1%}), "
                    f"필요한 최소 {minimum:.0%}에 미달"
                )
            return "- 충분한 10분 캔들을 확보하지 못했습니다."
        if issue.code is RejectionCode.BTC_CANDLE_HISTORY_UNAVAILABLE:
            return "- 시장 기준점인 BTC의 완료된 10분 캔들을 확보하지 못했습니다."
        if issue.code is RejectionCode.MARKET_UNIVERSE_EMPTY:
            return "- Upbit KRW 시장 목록을 확보하지 못했습니다."
        if issue.code is RejectionCode.IMMUTABLE_SCAN_EVENT_CONFLICT:
            conflict_count = issue.details.get("conflicting_event_count")
            suffix = (
                f" ({conflict_count}건)"
                if isinstance(conflict_count, int) and conflict_count > 0
                else ""
            )
            return f"- 재시도 결과가 최초 저장된 스캔 근거와 달라 최초 기록을 유지했습니다{suffix}."
        return "- 시장 데이터 검증에 실패했습니다."

    def format_daily_briefing(
        self,
        alerts: list[Alert],
        raw_tickers: list[MarketTicker],
        enriched_tickers: dict[str, TickerData],
        current_rankings: dict[str, int],
        previous_rankings: dict[str, int],
        markets_by_sector: dict[str, list[str]],
        sectors_by_market: dict[str, list[str]],
        market_regime: MarketRegimeSnapshot,
        observed_at: datetime.datetime,
        attention_queue: list[AttentionCandidate] | None = None,
    ) -> str:
        """Assemble one localized market briefing from validated inputs."""
        parts = [
            f"📊 **업비트 마켓 브리핑 ({_format_kst_clock(observed_at)} KST 기준)**"
        ]

        if attention_queue:
            parts.extend(
                [
                    "\n---",
                    self._format_attention_queue(attention_queue, sectors_by_market),
                ]
            )

        if alerts:
            parts.extend(["\n---", "⚡ **중요 상태 변경**"])
            parts.extend(
                self._format_single_alert(alert, sectors_by_market, market_regime)
                for alert in alerts[:10]
            )
        elif not attention_queue:
            parts.extend(["\n---", "✅ 현재 관심 필터를 통과한 종목이 없습니다."])

        parts.extend(
            ["\n---", self._format_market_status(raw_tickers, enriched_tickers)]
        )

        if leading_sectors_section := self._format_leading_sectors(
            enriched_tickers,
            markets_by_sector,
        ):
            parts.extend(["\n---", leading_sectors_section])

        parts.extend(
            [
                "\n---",
                self._format_top_10_ranking(current_rankings, previous_rankings),
            ]
        )

        return "\n".join(parts)

    def _format_attention_queue(
        self,
        candidates: list[AttentionCandidate],
        sectors_by_market: dict[str, list[str]],
    ) -> str:
        """Render guarded lane cards plus every folded broad-filter survivor."""
        lane_labels = {
            AttentionLane.FOCUS: "Focus Now",
            AttentionLane.EARLY: "Early Watch",
            AttentionLane.ONGOING: "Ongoing",
            AttentionLane.COOLING_FAILED: "Cooling / Failed",
            AttentionLane.DATA_LIMITED: "Data-limited",
        }
        slot_limits = {
            AttentionLane.FOCUS: config.ATTENTION_FOCUS_SLOTS,
            AttentionLane.EARLY: config.ATTENTION_EARLY_SLOTS,
            AttentionLane.ONGOING: config.ATTENTION_ONGOING_SLOTS,
        }
        visible = [candidate for candidate in candidates if candidate.displayed]
        if config.ATTENTION_VISIBLE_MODEL in {
            config.ATTENTION_RIDGE_MODEL_VERSION,
            config.ATTENTION_V3_MODEL_VERSION,
        }:
            lines = [
                f"🎯 **관심종목 큐 (주요 {len(visible)} / 전체 {len(candidates)})**"
            ]
            for candidate in sorted(
                visible, key=lambda item: item.display_rank or 1_000_000
            ):
                lines.extend(
                    self._format_attention_candidate(candidate, sectors_by_market)
                )
            folded = [candidate for candidate in candidates if not candidate.displayed]
            if folded:
                symbols = ", ".join(
                    candidate.market.removeprefix("KRW-") for candidate in folded
                )
                lines.append(f"\n📁 **추가 필터 통과 ({len(folded)}개)**")
                lines.append(f"- {symbols}")
            return "\n".join(lines)

        lines = [f"🎯 **관심종목 큐 (주요 {len(visible)} / 전체 {len(candidates)})**"]
        for lane in [
            AttentionLane.FOCUS,
            AttentionLane.EARLY,
            AttentionLane.ONGOING,
        ]:
            lane_visible = [
                candidate for candidate in visible if candidate.lane is lane
            ]
            lines.append(
                f"\n**{lane_labels[lane]}** ({len(lane_visible)}/{slot_limits[lane]})"
            )
            if not lane_visible:
                lines.append("- 비어 있음")
                continue
            for candidate in lane_visible:
                lines.extend(
                    self._format_attention_candidate(candidate, sectors_by_market)
                )

        folded = [candidate for candidate in candidates if not candidate.displayed]
        if folded:
            lines.append(f"\n📁 **추가 필터 통과 ({len(folded)}개)**")
            for lane in AttentionLane:
                lane_candidates = [
                    candidate for candidate in folded if candidate.lane is lane
                ]
                if not lane_candidates:
                    continue
                symbols = ", ".join(
                    candidate.market.removeprefix("KRW-")
                    for candidate in lane_candidates
                )
                lines.append(f"- {lane_labels[lane]}: {symbols}")
        return "\n".join(lines)

    def _format_attention_candidate(
        self,
        candidate: AttentionCandidate,
        sectors_by_market: dict[str, list[str]],
    ) -> list[str]:
        """Format one primary card with observational, non-predictive direction text."""
        stage_labels = {
            AttentionStage.DISCOVERED: "발견",
            AttentionStage.BUILDING: "누적",
            AttentionStage.CONFIRMED: "구조확인",
            AttentionStage.COOLING: "약화",
            AttentionStage.FAILED: "구조실패",
        }
        symbol = candidate.market.removeprefix("KRW-")
        tags = sectors_by_market.get(candidate.market, [])
        tag = f" · {tags[0]}" if tags else ""
        marker = "●" if candidate.material_change else "○"
        market_rank = (
            f"24h #{candidate.market_rank}"
            if candidate.market_rank is not None
            else "24h rank n/a"
        )
        if candidate.market_rank_delta:
            arrow = "↑" if candidate.market_rank_delta > 0 else "↓"
            market_rank += f" {arrow}{abs(candidate.market_rank_delta)}"
        first_seen = candidate.first_seen_at.astimezone(KOREA_STANDARD_TIME).strftime(
            "%H:%M"
        )
        chart_url = f"https://upbit.com/exchange?code=CRIX.UPBIT.{candidate.market}"
        lines = [
            f"{candidate.display_rank or candidate.lane_rank}. {marker} **{symbol}**{tag} "
            f"[{stage_labels[candidate.stage]}] · {market_rank} · "
            f"{first_seen}부터 {candidate.consecutive_observations}회 · "
            f"[차트]({chart_url})"
        ]

        evidence = {item.family: item for item in candidate.evidence}
        activity = evidence.get(EvidenceFamily.ACTIVITY)
        price = evidence.get(EvidenceFamily.PRICE_STRUCTURE)
        if activity and price:
            lines.append(f"   - 근거: {activity.summary} | {price.summary}")

        context = evidence.get(EvidenceFamily.CONTEXT)
        lines.append(
            f"   - 관찰: {self._format_direction_observation(candidate, context)}"
        )
        if context:
            label = (
                "보조근거(n/a)"
                if context.verdict is EvidenceVerdict.UNAVAILABLE
                else "보조근거"
            )
            lines.append(f"   - {label}: {context.summary}")
            contrary = self._direction_contrary_evidence(candidate, context)
            if contrary:
                lines.append(f"   - 반대 관찰: {' | '.join(contrary)}")
        risks = [
            item.summary
            for item in candidate.evidence
            if item.verdict is EvidenceVerdict.RISK
        ]
        if risks:
            lines.append(f"   - 반대/위험: {' | '.join(risks)}")
        return lines

    def _format_direction_observation(
        self,
        candidate: AttentionCandidate,
        context,
    ) -> str:
        ten_minute = (
            "상방 움직임"
            if (candidate.price_change_10m or 0.0) > 0
            else "하방 움직임"
            if (candidate.price_change_10m or 0.0) < 0
            else "보합"
        )
        trend = context.metrics.get("trend_1h") if context else None
        hourly = {
            "UP": "상방 정렬",
            "DOWN": "하방 정렬",
            "NEUTRAL": "혼재",
        }.get(trend, "확인 불가")
        daily_value = context.metrics.get("above_ma50_daily") if context else None
        daily = (
            "MA50 위"
            if daily_value is True
            else "MA50 아래"
            if daily_value is False
            else "확인 불가"
        )
        return f"10분 {ten_minute} · 60분 {hourly} · 일봉 {daily} · 방향 예측 아님"

    def _direction_contrary_evidence(
        self,
        candidate: AttentionCandidate,
        context,
    ) -> list[str]:
        direction_up = (candidate.price_change_10m or 0.0) > 0
        direction_down = (candidate.price_change_10m or 0.0) < 0
        trend = context.metrics.get("trend_1h")
        daily = context.metrics.get("above_ma50_daily")
        contrary = []
        if direction_up and trend == "DOWN":
            contrary.append("60분 하방 정렬")
        elif direction_down and trend == "UP":
            contrary.append("60분 상방 정렬")
        if direction_up and daily is False:
            contrary.append("일봉 MA50 아래")
        elif direction_down and daily is True:
            contrary.append("일봉 MA50 위")
        return contrary

    def _format_market_status(
        self,
        raw_tickers: list[MarketTicker],
        enriched_tickers: dict[str, TickerData],
    ) -> str:
        """Summarize breadth and 24-hour major-market turnover share."""
        gainers = sum(
            ticker.price_change_10m is not None and ticker.price_change_10m > 0
            for ticker in enriched_tickers.values()
        )
        losers = sum(
            ticker.price_change_10m is not None and ticker.price_change_10m < 0
            for ticker in enriched_tickers.values()
        )
        mood = (
            "강세"
            if gainers > losers * 1.2
            else "약세"
            if losers > gainers * 1.2
            else "보합"
        )

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
                f"- **자금 흐름:** 메이저 {major_pct:.1f}%, 알트 {(100 - major_pct):.1f}%",
            ]
        )

    def _format_leading_sectors(
        self,
        enriched_tickers: dict[str, TickerData],
        markets_by_sector: dict[str, list[str]],
    ) -> str | None:
        """Render sectors with sustained excess return, breadth, and activity."""
        market_returns_1h = [
            ticker.price_change_1h
            for ticker in enriched_tickers.values()
            if ticker.price_change_1h is not None
        ]
        market_returns_4h = [
            ticker.price_change_4h
            for ticker in enriched_tickers.values()
            if ticker.price_change_4h is not None
        ]
        if not market_returns_1h or not market_returns_4h:
            return None

        market_average_1h = float(np.mean(market_returns_1h))
        market_average_4h = float(np.mean(market_returns_4h))
        if market_average_1h > 0:
            minimum_alpha_1h = config.LEADING_SECTOR_BULL_MIN_ALPHA_1H
            section_icon = "🚀"
            section_title = "주도 섹터 (상승장)"
        else:
            minimum_alpha_1h = config.LEADING_SECTOR_BEAR_MIN_ALPHA_1H
            section_icon = "🛡️"
            section_title = "방어 섹터 (하락장)"

        sector_metrics_by_name: dict[str, dict[str, float | str]] = {}
        for sector_name, sector_markets in markets_by_sector.items():
            sector_tickers = [
                ticker
                for market in sector_markets
                if (ticker := enriched_tickers.get(market)) is not None
                and ticker.price_change_1h is not None
                and ticker.price_change_4h is not None
            ]
            if len(sector_tickers) < config.LEADING_SECTOR_MIN_MARKETS:
                continue

            average_return_1h = float(
                np.mean([ticker.price_change_1h for ticker in sector_tickers])
            )
            average_return_4h = float(
                np.mean([ticker.price_change_4h for ticker in sector_tickers])
            )
            average_relative_volume = float(
                np.mean([ticker.relative_volume or 1.0 for ticker in sector_tickers])
            )
            alpha_1h = average_return_1h - market_average_1h
            alpha_4h = average_return_4h - market_average_4h

            # Breadth prevents one large constituent from defining the sector.
            beating_market_count = sum(
                ticker.price_change_1h > market_average_1h for ticker in sector_tickers
            )
            breadth_ratio = beating_market_count / len(sector_tickers)
            minimum_breadth = (
                config.LEADING_SECTOR_LARGE_MIN_BREADTH
                if len(sector_tickers) >= config.LEADING_SECTOR_LARGE_MARKET_COUNT
                else config.LEADING_SECTOR_SMALL_MIN_BREADTH
            )
            if (
                alpha_1h <= minimum_alpha_1h
                or alpha_4h <= config.LEADING_SECTOR_MIN_ALPHA_4H
                or average_relative_volume <= config.LEADING_SECTOR_MIN_AVERAGE_RVOL
                or breadth_ratio < minimum_breadth
            ):
                continue

            sector_metrics_by_name[sector_name] = {
                "alpha_1h": alpha_1h,
                "average_relative_volume": average_relative_volume,
                "constituent_breadth": (
                    f"{beating_market_count}/{len(sector_tickers)}"
                ),
                "score": alpha_1h * 0.7 + alpha_4h * 0.3,
            }

        if not sector_metrics_by_name:
            return None
        ranked_sectors = sorted(
            sector_metrics_by_name.items(),
            key=lambda item: item[1]["score"],
            reverse=True,
        )
        lines = [f"{section_icon} **{section_title}**"]
        for sector_name, metrics in ranked_sectors[: config.LEADING_SECTOR_MAX_DISPLAY]:
            lines.append(
                f"- **{sector_name} ({metrics['constituent_breadth']}):** "
                f"Alpha `{metrics['alpha_1h']:+.1f}%p` "
                f"(RVOL {metrics['average_relative_volume']:.1f}x)"
            )
        return "\n".join(lines)

    def _format_top_10_ranking(
        self,
        current_rankings: dict[str, int],
        previous_rankings: dict[str, int],
    ) -> str:
        """Render the current top-ten turnover ranking and rank changes."""
        top_ten = sorted(
            [(market, rank) for market, rank in current_rankings.items() if rank <= 10],
            key=lambda item: item[1],
        )
        ranking_entries = []
        for market, rank in top_ten:
            previous_rank = previous_rankings.get(market)
            change_text = ""
            if previous_rank is not None:
                rank_change = previous_rank - rank
                if rank_change > 0:
                    change_text = f" (↑{rank_change})"
                elif rank_change < 0:
                    change_text = f" (↓{abs(rank_change)})"
            ranking_entries.append(
                f"{rank}. {market.removeprefix('KRW-')}{change_text}"
            )
        return "🏆 **24h 거래대금 TOP 10:**\n" + " | ".join(ranking_entries)

    def _format_single_alert(
        self,
        alert: Alert,
        sectors_by_market: dict[str, list[str]],
        market_regime: MarketRegimeSnapshot,
    ) -> str:
        """Render one classified alert as an objective evidence checklist."""
        candidate = alert.candidate
        ticker = alert.ticker_data
        market = candidate.market
        tags = sectors_by_market.get(market, [])
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
            f"{signal_title}**"
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

        return "\n".join([header, *checklist])

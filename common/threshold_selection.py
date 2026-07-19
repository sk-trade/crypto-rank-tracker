"""Choose alert score thresholds by fixed-volume net expected value."""

from collections import defaultdict
from dataclasses import dataclass
import math
from typing import Iterable

from common.models import ScanEvent, ScanOutcome


@dataclass(frozen=True)
class ThresholdSelection:
    threshold: float
    net_expected_return: float
    selected_events: int


def select_threshold(events: Iterable[ScanEvent], outcomes: Iterable[ScanOutcome], daily_alert_limit: int) -> ThresholdSelection:
    outcome_by_id = {outcome.event_id: outcome for outcome in outcomes}
    rows = [(event, outcome_by_id[event.event_id]) for event in events if event.signal_score is not None and event.event_id in outcome_by_id]
    if daily_alert_limit < 1 or not rows:
        raise ValueError("insufficient scored outcomes for threshold selection")
    if any(
        not math.isfinite(event.signal_score)
        or not math.isfinite(outcome.directional_net_return)
        for event, outcome in rows
    ):
        raise ValueError("scored outcomes must contain finite values")
    thresholds = sorted({event.signal_score for event, _ in rows})
    candidates = []
    for threshold in thresholds:
        by_day = defaultdict(list)
        for event, outcome in rows:
            if event.signal_score >= threshold:
                by_day[event.observed_at.date()].append(
                    (event.signal_score, event.event_id, outcome.directional_net_return)
                )
        selected = [
            item
            for values in by_day.values()
            for item in sorted(values, key=lambda item: (-item[0], item[1]))[
                :daily_alert_limit
            ]
        ]
        if selected:
            candidates.append(
                ThresholdSelection(
                    threshold,
                    sum(value for _, _, value in selected) / len(selected),
                    len(selected),
                )
            )
    if not candidates:
        raise ValueError("no threshold selects an outcome")
    return max(candidates, key=lambda item: (item.net_expected_return, item.threshold))

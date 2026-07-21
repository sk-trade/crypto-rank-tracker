# Changelog

All notable changes to this project are documented in this file.

## [0.6.0] - 2026-07-22

### Added

- Added the sealed `ridge_early_bonus_0p3` artifact and exact 38-feature scorer.
  The policy reranks only the v3 top five, uses the native v4 visible-card count,
  applies the strict `+0.30` early bonus, and retains the lower v3 rank as its
  deterministic tie-break.
- Added frozen ridge score/rank, v4 shadow rank, execution-neutral base quality,
  and separate ridge-base exposure evidence to attention events and replay rows.
- Added golden-score, pool-boundary, budget, tie-break, rollback, formatter, and
  shadow-exposure regression coverage for the promoted policy.

### Changed

- Made `attention-v5-ridge-early-0p3` the default visible ranking after seven
  distinct 90-day windows (90,720 scans) improved meaningful precision from
  66.97% to 70.27% and early episode precision from 52.27% to 56.99% at the
  same 228,174-card budget.
- Kept `attention-v4-c-guarded` and `attention-v3` as immediate runtime rollback
  options while preserving the multi-timeframe collection and complete survivor
  queue.
- Added active-model replay lift fields and labels; legacy v4-specific fields
  remain compatible but are null unless v4 is actually the visible model.

### Fixed

- Rendered ridge-selected `Cooling / Failed` and `Data-limited` candidates in
  display-rank order instead of dropping them from the lane-only formatter.
- Isolated active ridge exposure history from the hypothetical execution-neutral
  v4 base history used by the frozen feature contract, preventing deployment
  feedback from silently changing the validated policy.
- Added a budget-matched v3 replay baseline for valid active-model lift while
  retaining the full v3 shadow pool for later counterfactual evaluation, and
  reset provenance-unknown legacy exposure history instead of attributing it to
  the execution-neutral ridge base.

## [0.5.0] - 2026-07-20

### Added

- Added the `attention-v4-c-guarded` lane model with independent `Focus Now`
  (3), `Early Watch` (1), and `Ongoing` (1) card budgets plus folded
  `Cooling / Failed` and `Data-limited` queues.
- Added a bounded conservative quality score, Focus-only diversity/repeat
  reranking, 60-minute primary-card exposure history, and an immediate
  `ATTENTION_VISIBLE_MODEL=attention-v3` rollback switch.
- Added per-survivor v3 shadow ranks, lane/display state, score provenance,
  raw and eligible-context coverage, and first-seen evidence to immutable scan
  events and replay observations.
- Added replay comparison fields for v4 versus v3 shadow precision, pre-event
  recall, time-to-move, and `AttentionYield` while retaining 10-minute,
  60-minute, and daily inputs.

### Changed

- Made v4 visible by default while keeping every broad-filter survivor in the
  complete queue; empty lane slots are no longer filled by a different lane.
- Limited Early to the first discovered observation, Focus to the first three
  building/confirmed observations plus one first confirmation transition, and
  moved older live episodes to Ongoing.
- Required 24 completed hourly bars and 200 completed daily bars for normal
  lane placement. Incomplete candidates remain measurable as Data-limited
  instead of receiving a missing-context advantage.
- Reworded card direction as observed 10-minute/60-minute/daily alignment with
  explicit contrary evidence and a non-predictive disclaimer.

### Fixed

- Counted repeat exposure only on scheduled briefing scans rather than every
  internal 10-minute ranking pass.
- Preserved all folded survivors in event and replay logs instead of measuring
  only the production-visible Top K.

## [0.4.0] - 2026-07-19

### Added

- Added a ranked attention queue with durable candidate episodes, mechanical
  progression states, grouped evidence families, rank movement, persistence,
  and direct chart-review links.
- Added `/tmp`-only all-market Upbit candle caching and configurable 1-30 day
  point-in-time replay with separate feature warm-up, multi-timeframe evidence,
  Precision@K, Recall@K, lead-time, MFE, lift, and repeat-pressure reporting.

### Changed

- Kept broad-filter candidates visible before structure confirmation and moved
  execution feasibility, market regime, and higher-timeframe availability from
  visibility gates to explicit supporting or contradictory evidence.
- Extended sparse candle pagination beyond one API page while preserving
  completed-grid validation, smoothed request pacing, shared 429 cooldown, and
  a conservative bulk-replay request rate.
- Batched the continuously persisted attention queue into deterministic
  30-minute webhook digests while keeping final structure alerts immediate.
- Prioritized empirically stronger candidate stages before the material-change
  marker so a new discovery cannot displace an established building setup.
- Advanced persisted scan-event model identity to `attention-v3` for the final
  stage-first queue semantics.

### Fixed

- Persisted attention progression and immutable event identity independently
  from webhook delivery, including no-webhook runs and idempotent scan retries.
- Preserved the previous completed ranking snapshot when a retry finds the
  current scan timestamp already stored.
- Corrected replay repeat-pressure reporting to count repeated episode
  exposures, and added first-visible episode, stage-quality, material-scan,
  and scheduled-digest metrics.
- Separated structure-only ordering, active progression/context ordering, and
  cooling/failed retention while preserving the exact broad-filter tie order,
  so replay lift attribution does not mix ranking formulas or queue filling.
- Bound explicit replay end timestamps to cache identity so `--as-of` cannot
  silently reuse a dataset collected for a different decision window.
- Kept higher-timeframe unavailability in immutable attention events and made
  the displayed prior-turnover metric describe its actual 24-hour median window.
- Prevented delayed historical scheduler retries from mixing current ticker,
  orderbook, cooldown, or alert evidence into older scan timestamps; stale
  retries now close without analysis, state mutation, or notifications.
- Retained completed 60-minute and daily evidence for one-step cooling/failed
  candidates in both live scans and point-in-time replay.
- Stamped replay reports and per-decision evidence with the persisted signal
  model version to prevent cross-version comparisons from being mistaken as
  same-model evidence.
- Matched replay feature input to the live three-weekly-sample plus 154-recent-
  bar layout, avoiding repeated scans across the entire 3,025-bar warm-up.

## [0.3.0] - 2026-07-19

### Added

- Added execution-aware signal classification, beta-adjusted residual momentum,
  point-in-time baselines, purged time-series validation, out-of-sample feature
  approval, net-value threshold selection, and shadow promotion gates.
- Added durable scan event outcomes, idempotent scan claims, recoverable
  notification handoffs, and typed state transitions for local and GCS storage.

### Changed

- Hardened Upbit market, ticker, and completed-candle validation so incomplete or
  restricted market data fails closed before signal execution.
- Made sector identity, ranking retention, data-quality reporting, and deployment
  workflows preserve explicit failure and freshness semantics.
- Added isolated `SHADOW_MODE` cooldown persistence so webhook-free shadow runs
  suppress repeated selections without mutating production alert history.

### Fixed

- Prevented malformed or legacy alert history from bypassing cooldown rules and
  blocked repeated structure starts until the cooldown or structure reset allows them.
- Preserved retry and recovery identity across storage conflicts, notification
  uncertainty, scheduler retries, and transient sector lookup failures.

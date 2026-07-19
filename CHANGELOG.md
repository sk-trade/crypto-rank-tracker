# Changelog

All notable changes to this project are documented in this file.

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

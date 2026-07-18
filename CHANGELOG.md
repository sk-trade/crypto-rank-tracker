# Changelog

All notable changes to this project are documented in this file.

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

# crypto-rank-tracker

Scheduled attention tracker for every Upbit KRW market. The service narrows the full market universe to a ranked queue of charts where price, activity, or relative behavior is changing. Signals, multi-timeframe structure, market regime, and execution checks explain or challenge each candidate; they are not automatic trade instructions.

## Configuration

Set these environment variables for runtime behavior:

- `STATE_STORAGE_METHOD`: state backend selector; omit it for `LOCAL`, or set it explicitly to `LOCAL` or `GCS`. Any other value fails at startup.
- `GCS_BUCKET_NAME`: required bucket name when `STATE_STORAGE_METHOD=GCS`.
- `WEBHOOK_URL`: outbound webhook destination for briefing and alert delivery. Local runs read it directly; production syncs the GitHub secret into Google Secret Manager and injects it as a runtime secret.
- `SHADOW_MODE`: set to `true`, `1`, or `yes` for no-webhook evaluations that must retain production-equivalent cooldown state in isolated `shadow_alert_history.json` without advancing delivery-backed `alert_history.json`.
- `CG_API_KEY`: CoinGecko API key used by the sector updater.
- `GCP_PROJECT_ID`: Google Cloud project identifier. Production workflows require it; local GCS use may omit it only when Application Default Credentials can infer the project.
- `CG_SYMBOL_OVERRIDES`: optional JSON object mapping an ambiguous lower-case symbol to an explicit CoinGecko id, for example `{"pay":"tenx"}`. Values may also provide explicit `name` and `network` constraints. Unique CoinGecko symbols do not depend on provider display-name equality; ambiguous symbols without a valid override are left untagged.

## Local setup

Install dependencies with:

```bash
uv sync --frozen
```

## Local verification

Run the same checks used for local validation:

```bash
uv run python -m pytest
uv run python -m compileall main.py config.py update_sectors.py replay_upbit.py common tests
uv build
WHEEL_PATH="$(realpath dist/*.whl)"
TEMP_DIR="$(mktemp -d)"
(cd "$TEMP_DIR" && uv run --no-cache --isolated --with "$WHEEL_PATH" python -c \
  "import main, update_sectors, replay_upbit, config, common.upbit_client, common.notification.main")
```

## Local execution

Run the main service entrypoint with:

```bash
uv run python main.py
```

Run the sector updater with:

```bash
uv run python update_sectors.py
```

Both commands can trigger live network traffic and service side effects. They may read external market APIs, write state, and send webhook requests depending on configuration. Use them only when those effects are intended.

## Attention queue

- A broad price/activity filter decides which markets deserve inspection.
- Candidates progress through `discovered`, `building`, `confirmed`, `cooling`, and `failed` episodes.
- Structure confirmation, 1-hour and daily context, market regime, and orderbook feasibility remain visible evidence. Missing or risky supporting evidence does not silently remove a broad-filter candidate.
- The webhook briefing shows rank movement, first-seen time, persistence, grouped activity/price/context/execution evidence, and a direct chart link. `signal_score` remains an internal ordering input and is not presented as probability.
- State and immutable events retain every 10-minute queue transition. The current queue is sent as a deterministic 30-minute digest, while final structure alerts bypass the digest; scans between digests remain available for replay without producing broad-filter webhook churn.
- Attention state and immutable scan events are stored independently from webhook delivery, so a no-webhook run still produces evaluation evidence.

## Point-in-time replay

Collect and replay the current Upbit KRW universe with disposable `/tmp` storage:

```bash
uv run python replay_upbit.py --evaluation-days 7
```

An installed wheel also exposes the equivalent `crypto-rank-replay` command.

The evaluation window accepts 1 through 30 days. Treat 1-3 day runs as smoke/debug evidence, use 7 days for fast regression comparisons, and use a same-end-time 30-day replay before making operating-like quality claims. A long-window result is partial evidence when warm-up-complete market coverage is below the production minimum (currently 95%). The collector adds the required feature warm-up separately: three weeks of 10-minute same-slot history, the recent 10-minute feature window, 200 completed daily bars, derived completed 60-minute bars, and 120 minutes of future outcome data. Upbit's 200-candle limit is paginated through the shared rate limiter, and historical turnover ranks use the API's actual candle KRW trade value rather than `close * volume` approximation.

The default cache and reports are written to `/tmp/crypto-rank-tracker-replay`. Any custom `--cache-dir` outside `/tmp` is rejected. Reuse the cache by default or pass `--refresh` to recollect it. Bulk replay collection uses a lower request rate than the scheduled scanner so both can share an IP without treating the API limit as normal control flow. The live queue retains up to 10 charts, while replay defaults to top 5 so ordering changes remain measurable when the broad filter returns fewer than 10 markets. `report.json` and `report.md` compare turnover ranking, the broad filter, structure ordering, active-candidate progression/context ordering, and cooling/failed retention in the full attention queue. Metrics include compression, Precision@K, Recall@K, 30/60/120-minute MFE, time-to-move, isolated incremental lift, first-visible episode quality, stage-conditioned quality, true repeat exposure, and scheduled digest pressure. `observations.ndjson` retains the concrete per-scan queues, comparison selections, meaningful movers, and joined future outcomes behind those aggregates.

Historical candle replay does not reconstruct past orderbooks. Execution evidence is therefore marked unavailable and excluded from replay lift attribution; live scans continue to show current spread, depth, slippage, warning, and estimated-cost risk.

Every replay report and per-decision observation records `SIGNAL_MODEL_VERSION`, preventing results from interim and final queue semantics from being silently mixed.

## Signal Safety

- The broad scan builds 154 recent 10-minute clock bars per market and separately fetches three prior weekly same-slot observations. Upbit no-trade intervals carry the previous OHLC with zero volume; malformed responses, missing conditional samples, or unavailable orderbooks still block signals rather than falling back to a weaker rule.
- Candidate execution checks require sufficient 24-hour turnover, two-sided orderbook depth for the configured KRW notional, acceptable spread/slippage, and movement that covers estimated round-trip costs.
- Local state is stored under `state/`. Rank snapshots retain the most recent `STATE_HISTORY_COUNT` entries; malformed state files fail explicitly rather than silently resetting history.
- The baseline model, threshold selector, and shadow-promotion policies are offline evaluation tools. They do not replace production alerts until frozen shadow-operation criteria are met.

## Deployment

GitHub Actions is used for deployment flow control:

- Pull requests to any target branch run verification only.
- Pushes to `main` and manual `workflow_dispatch` runs from `main` deploy after verification.
- The deployment target is Cloud Function `crypto-rank-tracker` in `asia-northeast1`.
- Cloud Scheduler runs every 10 minutes.
- The deploy workflow exports requirements without development dependencies.
- Configure distinct GitHub Secrets for `GCP_DEPLOYER_SA_EMAIL`, `GCP_RUNTIME_SA_EMAIL`,
  and `GCP_SCHEDULER_SA_EMAIL`. The deployer authenticates GitHub Actions, the runtime
  account accesses application resources, and the Scheduler account invokes the function.
- Configure the `GCP_PROJECT_ID` and `GCP_WIF_PROVIDER` secrets for explicit project selection and Workload Identity Federation, plus the
  `WEBHOOK_URL` GitHub secret when live notifications are required. The deploy workflow creates or updates `crypto-rank-tracker-webhook-url` in Secret Manager and injects only its resource reference into the Cloud Function revision.
- Production workflows pin `STATE_STORAGE_METHOD=GCS` so the scheduled function and sector
  updater share durable state. Configure the required `GCS_BUCKET_NAME` repository variable;
  deployment and sector refresh fail before authentication when it is missing.
- Configure `CG_SYMBOL_OVERRIDES` when symbol collisions need explicit CoinGecko identities.
  The sector workflow also requires the `CG_API_KEY` secret.
- Grant the deployer permission to deploy Cloud Functions, act as the runtime service account,
  update the Cloud Run invoker policy, manage the Scheduler job, and create/update the designated
  Secret Manager secret and its IAM policy. The workflow grants the runtime account
  `roles/secretmanager.secretAccessor` on that secret and grants the Scheduler service account
  `roles/run.invoker` on the deployed Gen2 service.
- The Gen2 function is deployed with 512 MB memory, a 540-second service timeout, one maximum instance,
  and one request per instance. Scheduler updates are idempotent, use the function URL
  as their OIDC audience, and retry failed executions three times with bounded backoff. Retries use
  `X-CloudScheduler-ScheduleTime` to keep the original completed-candle scan identity across time boundaries.
- Configured webhook deliveries use durable outbox state and expose a stable `X-Webhook-Delivery-ID` header for receiver-side reconciliation. Definitive HTTP/connect failures remain retryable, while an in-flight attempt with an unknown outcome is held for operator review instead of being silently cleared or resent.
- Market scans continue while an older delivery is pending. Later alerts and data-quality incidents are retained in FIFO order in `notification_backlog.json`; ordinary no-alert briefings coalesce to the latest scan. The backlog is bounded at 144 retained records, and a full backlog fails the scan explicitly so the notification is not silently discarded.
- For an outbox in `attempting`, check the receiver for its delivery ID before editing state. If the receiver confirms delivery, preserve the record and change only its status to `delivered`; if it confirms no delivery, change only the status to `prepared`. Leave an unresolved attempt untouched. Removing `WEBHOOK_URL` cancels prepared and deferred work, but preserves an ambiguous active attempt and its delivery ID until the operator resolves it.

## Operational notes

- Keep runtime configuration in sync with the deployment environment before pushing changes.
- Confirm webhook and storage settings before running local commands that can mutate state or notify downstream systems.

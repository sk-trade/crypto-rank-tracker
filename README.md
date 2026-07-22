# crypto-rank-tracker

Scheduled attention tracker for every Upbit KRW market. The service narrows the full market universe to a ranked queue of charts where price, activity, or relative behavior is changing. Signals, multi-timeframe structure, market regime, and execution checks explain or challenge each candidate; they are not automatic trade instructions.

## Configuration

Set these environment variables for runtime behavior:

- `STATE_STORAGE_METHOD`: state backend selector; omit it for `LOCAL`, or set it explicitly to `LOCAL` or `GCS`. Any other value fails at startup.
- `GCS_BUCKET_NAME`: required bucket name when `STATE_STORAGE_METHOD=GCS`.
- `WEBHOOK_URL`: outbound webhook destination for briefing and alert delivery. Local runs read it directly; production syncs the GitHub secret into Google Secret Manager and injects it as a runtime secret.
- `SHADOW_MODE`: set to `true`, `1`, or `yes` for no-webhook evaluations that must retain production-equivalent cooldown state in isolated `shadow_alert_history.json` without advancing delivery-backed `alert_history.json`.
- `ATTENTION_VISIBLE_MODEL`: defaults to `attention-v5-ridge-early-0p3`; set it to `attention-v4-c-guarded` or `attention-v3` for an immediate visible-ranking rollback while all three orders remain measurable.
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
- The default `attention-v5-ridge-early-0p3` policy reranks only the native v3 top five. It uses the exact frozen 38-feature artifact `sha256:cabde76...897866`, adds `0.30` only while absolute 1-hour movement is below 2% and absolute 4-hour movement is below 5%, and breaks ties with the lower v3 rank.
- The number of primary cards remains the exact count that `attention-v4-c-guarded` would expose for the same scan. The ridge policy changes ordering, not queue volume, and never expands its candidate pool beyond v3's top five.
- v4 retains independent budgets of `Focus Now` 3, `Early Watch` 1, and `Ongoing` 1 as the card-count guard and immediate fallback. Its quality score uses capped activity and price-surprise strength with small context and execution adjustments.
- Ridge scoring uses a separate execution-neutral v4 representation because historical replay had no orderbook snapshots. Execution risk remains visible evidence but does not silently move a validated attention candidate in or out of the ridge order.
- Active ridge exposures and the hypothetical v4-base exposures are persisted separately, preventing the new policy from changing its own validated repeat-exposure feature path. Legacy state has no trustworthy prior-model provenance, so its bounded ridge-base exposure history starts empty and becomes fully native within 60 minutes.
- Normal lane placement requires at least 24 completed 60-minute bars and 200 completed daily bars. Missing context is neutral in scoring and explicitly listed as `Data-limited`, never rewarded or silently discarded.
- The webhook briefing shows rank movement, first-seen time, grouped activity/price/context/execution evidence, contrary timeframes, and a direct chart link. Direction text describes observed alignment and explicitly says it is not a direction prediction; the internal score is not presented as probability.
- State and immutable events retain every 10-minute queue transition. The current queue is sent as a deterministic 30-minute digest, while final structure alerts bypass the digest; scans between digests remain available for replay without producing broad-filter webhook churn.
- Attention state and immutable scan events are stored independently from webhook delivery, so a no-webhook run still produces evaluation evidence.
- Every survivor records v3 and v4 shadow ranks plus the frozen ridge score/rank. Set `ATTENTION_VISIBLE_MODEL=attention-v4-c-guarded` or `attention-v3` to roll the visible list back without losing comparison evidence.

## Point-in-time replay

Collect and replay the current Upbit KRW universe with disposable `/tmp` storage:

```bash
uv run python replay_upbit.py --evaluation-days 7
```

An installed wheel also exposes the equivalent `crypto-rank-replay` command.

The evaluation window accepts 1 through 90 days. Treat 1-3 day runs as smoke/debug evidence, keep the 7-day default for fast regression comparisons, require a same-end-time 30-day replay before making operating-like quality claims, and use explicit 60- or 90-day runs to check whether conclusions survive broader market regimes. Every report labels that evidence tier. A long-window result is partial evidence when warm-up-complete market coverage is below the production minimum (currently 95%). The collector adds feature warm-up separately: three weeks of 10-minute same-slot history, the recent 10-minute feature window, 200 completed daily bars, derived completed 60-minute bars, and 120 minutes of future outcome data. These 10-minute, 60-minute, and daily inputs remain part of every replay tier. Upbit's 200-candle limit is paginated through the shared rate limiter, and historical turnover ranks use the API's actual candle KRW trade value rather than a `close * volume` approximation.

The default cache and reports are written to `/tmp/crypto-rank-tracker-replay`. Any custom `--cache-dir` or `--output-dir` outside `/tmp` is rejected. Reuse the cache by default or pass `--refresh` to recollect it. A complete longer cache can feed a shorter same-end-time replay; the report records the source window and warns that coverage then reflects the stable longer-history cohort. Use separate output directories so comparisons do not overwrite one another:

```bash
AS_OF=2026-07-19T01:20:00Z
CACHE=/tmp/crypto-rank-replay-90d

uv run python replay_upbit.py --evaluation-days 90 --as-of "$AS_OF" --cache-dir "$CACHE" --output-dir /tmp/crypto-rank-report-90d --refresh
uv run python replay_upbit.py --evaluation-days 60 --as-of "$AS_OF" --cache-dir "$CACHE" --output-dir /tmp/crypto-rank-report-60d
uv run python replay_upbit.py --evaluation-days 30 --as-of "$AS_OF" --cache-dir "$CACHE" --output-dir /tmp/crypto-rank-report-30d
```

If superset-cache coverage falls below 95%, collect a dedicated 30-day cache before treating the 30-day result as operating acceptance evidence. Bulk replay collection uses a lower request rate than the scheduled scanner so both can share an IP without treating the API limit as normal control flow. The active queue uses the native v4 lane budget of at most five primary cards, while replay keeps every survivor and the full v3 shadow pool. A separate budget-matched v3 variant is used only for valid active-model lift calculations, so later counterfactual evaluation can still inspect all v3 top-k candidates. `report.json` and `report.md` compare turnover ranking, the broad filter, structure ordering, active-candidate progression/context ordering, active visible cards, full v3 shadow selection, and the matched v3 baseline. Metrics include raw and eligible-context coverage, compression, Precision@K, Recall@K, 30/60/120-minute MFE, time-to-move, active-vs-v3 lift, first-visible episode quality, `AttentionYield`, stage-conditioned quality, true briefing exposure, and scheduled digest pressure. `observations.ndjson` retains every survivor, visible selections, comparison variants, meaningful movers, and joined future outcomes behind those aggregates.

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

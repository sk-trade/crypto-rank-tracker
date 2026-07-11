# crypto-rank-tracker

Scheduled tracker for Upbit KRW market ranking and anomaly reporting. The service polls market data on a schedule, derives sector/ranking signals, and sends webhook briefing or alert messages when configured to do so.

## Configuration

Set these environment variables for runtime behavior:

- `STATE_STORAGE_METHOD`: state backend selector; omit it for `LOCAL`, or set it explicitly to `LOCAL` or `GCS`. Any other value fails at startup.
- `GCS_BUCKET_NAME`: required bucket name when `STATE_STORAGE_METHOD=GCS`.
- `WEBHOOK_URL`: outbound webhook destination for briefing and alert delivery.
- `CG_API_KEY`: CoinGecko API key used by the sector updater.
- `GCP_PROJECT_ID`: Google Cloud project identifier used by deployment/runtime integration.
- `CG_SYMBOL_OVERRIDES`: optional JSON object mapping an ambiguous lower-case symbol to an explicit CoinGecko id, for example `{"pay":"tenx"}`. Ambiguous symbols without a valid override are left untagged.

## Local setup

Install dependencies with:

```bash
uv sync --frozen
```

## Local verification

Run the same checks used for local validation:

```bash
uv run python -m pytest
uv run python -m compileall main.py config.py update_sectors.py common tests
uv build
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

## Signal Safety

- The scanner fetches 3,025 completed 10-minute candles per market to establish a three-week same-weekday/time volume baseline. Incomplete history, missing conditional samples, or unavailable orderbooks block signals rather than falling back to a weaker rule.
- Candidate execution checks require sufficient 24-hour turnover, two-sided orderbook depth for the configured KRW notional, acceptable spread/slippage, and movement that covers estimated round-trip costs.
- Local state is stored under `state/`. Rank snapshots retain the most recent `STATE_HISTORY_COUNT` entries; malformed state files fail explicitly rather than silently resetting history.
- The baseline model, threshold selector, and shadow-promotion policies are offline evaluation tools. They do not replace production alerts until frozen shadow-operation criteria are met.

## Deployment

GitHub Actions is used for deployment flow control:

- Pull requests run verification only.
- Pushes to `main` and manual `workflow_dispatch` runs from `main` deploy after verification.
- The deployment target is Cloud Function `crypto-rank-tracker` in `asia-northeast1`.
- Cloud Scheduler runs every 10 minutes.
- The deploy workflow exports requirements without development dependencies.
- Configure distinct GitHub Secrets for `GCP_DEPLOYER_SA_EMAIL`, `GCP_RUNTIME_SA_EMAIL`,
  and `GCP_SCHEDULER_SA_EMAIL`. The deployer authenticates GitHub Actions, the runtime
  account accesses application resources, and the Scheduler account invokes the function.
- Configure the `GCP_WIF_PROVIDER` secret for Workload Identity Federation, plus the
  `WEBHOOK_URL` secret when live notifications are required.
- Configure repository variables `STATE_STORAGE_METHOD` (optional, defaults to `LOCAL`),
  `GCS_BUCKET_NAME` (required for `GCS`), and `CG_SYMBOL_OVERRIDES` when symbol collisions
  need explicit CoinGecko identities. The sector workflow also requires the `CG_API_KEY` secret.
- Grant the deployer permission to deploy Cloud Functions, act as the runtime service account,
  update the Cloud Run invoker policy, and manage the Scheduler job. The workflow grants the
  Scheduler service account `roles/run.invoker` on the deployed Gen2 service.
- The Gen2 function is deployed with a 540-second service timeout, one maximum instance,
  and one request per instance. Scheduler updates are idempotent and use the function URL
  as their OIDC audience.

## Operational notes

- Keep runtime configuration in sync with the deployment environment before pushing changes.
- Confirm webhook and storage settings before running local commands that can mutate state or notify downstream systems.

# crypto-rank-tracker

Scheduled tracker for Upbit KRW market ranking and anomaly reporting. The service polls market data on a schedule, derives sector/ranking signals, and sends webhook briefing or alert messages when configured to do so.

## Configuration

Set these environment variables for runtime behavior:

- `STATE_STORAGE_METHOD`: state backend selector used by the service.
- `GCS_BUCKET_NAME`: bucket name for state storage when GCS-backed state is enabled.
- `WEBHOOK_URL`: outbound webhook destination for briefing and alert delivery.
- `CG_API_KEY`: CoinGecko API key used for market data requests.
- `GCP_PROJECT_ID`: Google Cloud project identifier used by deployment/runtime integration.

## Local setup

Install dependencies with:

```bash
uv sync --frozen
```

## Local verification

Run the same checks used for local validation:

```bash
uv run python -m pytest
uv run python -m compileall main.py config.py common tests
uv build
```

## Local execution

Run the main service entrypoint with:

```bash
python main.py
```

Run the sector updater with:

```bash
python update_sectors.py
```

Both commands can trigger live network traffic and service side effects. They may read external market APIs, write state, and send webhook requests depending on configuration. Use them only when those effects are intended.

## Deployment

GitHub Actions is used for deployment flow control:

- Pull requests run verification only.
- Manual deployment is allowed only from `main`.
- The deployment target is Cloud Function `crypto-rank-tracker` in `asia-northeast1`.
- Cloud Scheduler runs every 10 minutes.
- The deploy workflow exports requirements without development dependencies.

## Operational notes

- Keep runtime configuration in sync with the deployment environment before pushing changes.
- Confirm webhook and storage settings before running local commands that can mutate state or notify downstream systems.

# Repository guide

## Scope

This repository runs a scheduled Upbit KRW attention scan, persists decision evidence, delivers recoverable webhook notifications, and evaluates the same decision path with point-in-time replay.

## Module boundaries

- Keep `main.py` as orchestration. Put provider parsing in `common/upbit_client.py`, persistence in `common/state_manager.py`, notification state transitions in `common/notification/`, and deterministic ranking logic in `common/attention.py`.
- Validate external payloads before they enter analysis code. Do not replace a missing required input with a weaker heuristic.
- Preserve immutable scan events. Outcome resolution creates separate records; it must not rewrite the original decision evidence.
- Live and replay paths must reuse the same feature and attention functions. Replay-only approximations must be explicit in report warnings.
- Keep local/GCS state transitions idempotent. Notification delivery must retain the distinction between confirmed, failed, and uncertain external effects.

## Change rules

- Use descriptive domain names. A collection name is plural; a mapping name states its key or purpose; exceptions end in `Error`.
- Prefer a small dataclass for data passed through several pipeline stages. Do not add generic manager, helper, base-service, or utility layers without a concrete repeated contract.
- Keep cyclomatic complexity at or below 10. Split by responsibility rather than suppressing `C90`.
- Update tests and `CHANGELOG.md` with behavior or operational-contract changes. Do not add issue or task templates unless a recurring workflow requires them.

## Verification

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run python -m pytest
uv run python -m compileall main.py config.py update_sectors.py replay_upbit.py common tests
rm -rf dist
uv build
WHEEL_PATH="$(realpath dist/*.whl)"
TEMP_DIR="$(mktemp -d)"
(cd "$TEMP_DIR" && uv run --no-cache --isolated --with "$WHEEL_PATH" python -c \
  "import main, update_sectors, replay_upbit, config, common.upbit_client, common.notification.main")
```

`main.py`, `update_sectors.py`, and `replay_upbit.py` can call external services and mutate state. Do not run them as verification commands.

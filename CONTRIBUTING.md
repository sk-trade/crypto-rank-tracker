# Contributing

## Before changing code

Create a focused branch from the current target branch. Confirm the behavior to preserve with an existing test or add a failing regression test first. Keep one change set centered on one operational problem; unrelated formatting or dependency updates belong in separate changes.

## Local checks

Use the locked environment and run the same sequence as CI:

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

Tests isolate local state and disable outbound webhooks. The three executable entrypoints can perform live network requests and state mutations, so run them only for an intentional integration or operational check.

## Design expectations

Keep provider validation, deterministic analysis, persistence, and external delivery in their existing module boundaries. New state formats require explicit validation and migration behavior. New notification paths require tests for durable, failed, and uncertain handoffs. A replay change must show that its point-in-time inputs still match the live feature contract.

Use typed domain objects at module boundaries and descriptive names inside them. Avoid broad dictionaries when a value crosses multiple stages, duplicated fallback logic, hidden exception swallowing, and comments that only restate the code.

## Pull requests

Describe the problem, the behavioral change, and the exact verification commands that passed. Call out state-schema, deployment, credential, webhook, or replay-evidence impact. Add an `Unreleased` changelog entry for user-visible or operational changes.

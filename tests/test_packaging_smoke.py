from importlib import import_module
from pathlib import Path


def test_main_imports():
    import_module("main")
    import_module("replay_upbit")


def test_readme_documents_the_uncached_isolated_wheel_import_smoke():
    readme = Path("README.md").read_text(encoding="utf-8")

    assert 'WHEEL_PATH="$(realpath dist/*.whl)"' in readme
    assert 'uv run --no-cache --isolated --with "$WHEEL_PATH"' in readme
    assert (
        "import main, update_sectors, replay_upbit, config, "
        "common.upbit_client, common.notification.main"
    ) in readme


def test_readme_documents_the_full_pull_request_verification_sequence():
    readme = Path("README.md").read_text(encoding="utf-8")

    commands = [
        "uv run ruff check .",
        "uv run ruff format --check .",
        "uv run python -m pytest",
        "uv run python -m compileall main.py config.py update_sectors.py replay_upbit.py common tests",
        "uv build",
    ]
    positions = [readme.index(command) for command in commands]
    assert positions == sorted(positions)

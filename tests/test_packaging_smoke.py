from importlib import import_module
from pathlib import Path


def test_main_imports():
    import_module("main")


def test_readme_documents_the_uncached_isolated_wheel_import_smoke():
    readme = Path("README.md").read_text(encoding="utf-8")

    assert 'WHEEL_PATH="$(realpath dist/*.whl)"' in readme
    assert 'uv run --no-cache --isolated --with "$WHEEL_PATH"' in readme
    assert "import main, update_sectors, config, common.upbit_client, common.notification.main" in readme

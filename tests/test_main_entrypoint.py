import pytest

import main as app


def test_cloud_function_entrypoint_returns_ok_when_run_check_succeeds(monkeypatch):
    async def run_check_success():
        return None

    monkeypatch.setattr(app, "run_check", run_check_success)

    assert app.main(None) == ("OK", 200)


def test_cloud_function_entrypoint_returns_500_when_run_check_fails(monkeypatch):
    async def run_check_failure():
        raise RuntimeError("boom")

    monkeypatch.setattr(app, "run_check", run_check_failure)

    assert app.main(None) == ("Internal Server Error", 500)

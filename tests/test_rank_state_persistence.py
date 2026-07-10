import asyncio
import datetime

import pytest

import config
from common.models import RankState
from common.state_manager import load_rank_state_history, save_rank_state_history
from common.storage_client import StateLoadError


def test_rank_state_persists_across_reload_and_trims_oldest_entries(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    monkeypatch.setattr(config, "STATE_HISTORY_COUNT", 2)

    async def scenario():
        history = []
        for index in range(3):
            state = RankState(last_updated=datetime.datetime(2026, 1, 1, index, tzinfo=datetime.timezone.utc), rankings={"KRW-BTC": index})
            await save_rank_state_history(state, history)
            history = await load_rank_state_history()
        return await load_rank_state_history()

    restored = asyncio.run(scenario())
    assert [state.rankings["KRW-BTC"] for state in restored] == [1, 2]


def test_rank_state_corruption_is_explicit_not_empty_history(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(config, "LOCAL_STATE_DIR", str(tmp_path))
    (tmp_path / config.RANK_STATE_FILE_NAME).write_text("not json", encoding="utf-8")

    with pytest.raises(StateLoadError):
        asyncio.run(load_rank_state_history())

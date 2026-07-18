import asyncio
from pathlib import Path

import config
from common import state_manager


def test_maintainer_verification_uses_ephemeral_local_state(tmp_path):
    product_state_dir = Path(config.__file__).resolve().parent / "state"

    assert config.storage_method() is config.StorageMethod.LOCAL
    assert config.WEBHOOK_URL is None
    assert Path(config.LOCAL_STATE_DIR).resolve() == tmp_path.resolve()
    assert Path(config.LOCAL_STATE_DIR).resolve() != product_state_dir.resolve()

    asyncio.run(state_manager.save_notification_backlog([]))

    assert (tmp_path / state_manager.NOTIFICATION_BACKLOG_FILE_NAME).is_file()

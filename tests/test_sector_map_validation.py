import asyncio

import pytest

import update_sectors


def test_sector_map_rejects_suspicious_bulk_change():
    with pytest.raises(RuntimeError, match="change ratio"):
        update_sectors.validate_sector_map_change(
            {"KRW-A": ["A"], "KRW-B": ["B"]}, {"KRW-A": ["Changed"]}
        )


def test_sector_map_backup_and_rollback_preserve_previous_valid_map(monkeypatch):
    saved = []

    async def load(*_args):
        return {"KRW-A": ["A"]}

    async def save(filename, value, *_args):
        saved.append((filename, value))
        if filename == update_sectors.config.SECTOR_MAP_FILE_NAME and len(saved) == 2:
            raise RuntimeError("write failed")

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", save)

    with pytest.raises(RuntimeError, match="write failed"):
        asyncio.run(update_sectors.save_validated_sector_map({"KRW-A": ["A"]}))
    assert saved[-1] == (update_sectors.config.SECTOR_MAP_FILE_NAME, {"KRW-A": ["A"]})


def test_sector_map_preserves_known_good_tags_on_partial_lookup_failure(monkeypatch):
    saved = []

    async def load(*_args):
        return {
            "KRW-A": ["Layer 1"],
            "KRW-B": ["DeFi"],
            "KRW-C": ["Gaming"],
            "KRW-D": ["Payments"],
        }

    async def save(filename, value, *_args):
        saved.append((filename, value))

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", save)

    asyncio.run(
        update_sectors.save_validated_sector_map(
            {
                "KRW-A": ["Untagged", "API_Error"],
                "KRW-B": ["DeFi"],
                "KRW-C": ["Gaming"],
                "KRW-D": ["Payments"],
            }
        )
    )

    assert saved[-1] == (
        update_sectors.config.SECTOR_MAP_FILE_NAME,
        {
            "KRW-A": ["Layer 1"],
            "KRW-B": ["DeFi"],
            "KRW-C": ["Gaming"],
            "KRW-D": ["Payments"],
        },
    )

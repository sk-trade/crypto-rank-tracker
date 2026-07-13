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


def test_sector_map_rejects_all_failed_fresh_bootstrap_without_writing(monkeypatch):
    save = pytest.fail

    async def load(*_args):
        return None

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", save)

    with pytest.raises(RuntimeError, match="no usable CoinGecko categories"):
        asyncio.run(
            update_sectors.save_validated_sector_map(
                {
                    "KRW-BTC": ["Untagged", "API_Error"],
                    "KRW-ETH": ["Untagged", "Lookup_Failed"],
                }
            )
        )


def test_sector_map_allows_fresh_bootstrap_with_a_usable_category(monkeypatch):
    saved = []

    async def load(*_args):
        return None

    async def save(filename, value, *_args):
        saved.append((filename, value))

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", save)

    sector_map = {
        "KRW-BTC": ["Layer 1"],
        "KRW-ETH": ["Untagged", "API_Error"],
    }
    asyncio.run(update_sectors.save_validated_sector_map(sector_map))

    assert saved == [(update_sectors.config.SECTOR_MAP_FILE_NAME, sector_map)]


def test_sector_map_rejects_near_total_fresh_bootstrap_failure(monkeypatch):
    async def load(*_args):
        return None

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", pytest.fail)
    sector_map = {"KRW-VALID": ["Layer 1"]}
    sector_map.update(
        {
            f"KRW-FAILED-{index}": ["Untagged", "API_Error"]
            for index in range(9)
        }
    )

    with pytest.raises(RuntimeError, match="bootstrap coverage"):
        asyncio.run(update_sectors.save_validated_sector_map(sector_map))


@pytest.mark.parametrize(
    "sector_map",
    [
        {"KRW-BTC": "DeFi"},
        {"KRW-BTC": [123]},
        {"KRW-BTC": []},
        {123: ["Layer 1"]},
    ],
)
def test_sector_map_rejects_invalid_canonical_schema(monkeypatch, sector_map):
    async def load(*_args):
        return None

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", pytest.fail)

    with pytest.raises(RuntimeError, match="schema"):
        asyncio.run(update_sectors.save_validated_sector_map(sector_map))


def test_invalid_coingecko_category_schema_becomes_a_transient_failure(monkeypatch):
    async def detail(*_args):
        return {"name": "Bitcoin", "categories": [123]}

    monkeypatch.setattr(update_sectors, "get_coin_detail", detail)
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {})

    result = asyncio.run(
        update_sectors.tag_market(
            None,
            "btc",
            "KRW-BTC",
            {"btc": ["bitcoin"]},
            upbit_name="Bitcoin",
        )
    )

    assert result == ("KRW-BTC", ["Untagged", "Invalid_Category"])


@pytest.mark.parametrize("categories", [None, ""])
def test_falsy_invalid_coingecko_category_schema_is_not_treated_as_empty(
    monkeypatch, categories
):
    async def detail(*_args):
        return {"name": "Bitcoin", "categories": categories}

    monkeypatch.setattr(update_sectors, "get_coin_detail", detail)
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {})

    result = asyncio.run(
        update_sectors.tag_market(
            None,
            "btc",
            "KRW-BTC",
            {"btc": ["bitcoin"]},
            upbit_name="Bitcoin",
        )
    )

    assert result == ("KRW-BTC", ["Untagged", "Invalid_Category"])


def test_missing_coingecko_category_field_is_a_transient_schema_failure(monkeypatch):
    async def detail(*_args):
        return {"name": "Bitcoin"}

    monkeypatch.setattr(update_sectors, "get_coin_detail", detail)
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {})

    result = asyncio.run(
        update_sectors.tag_market(
            None,
            "btc",
            "KRW-BTC",
            {"btc": ["bitcoin"]},
            upbit_name="Bitcoin",
        )
    )

    assert result == ("KRW-BTC", ["Untagged", "Invalid_Category"])

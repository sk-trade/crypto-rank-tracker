import asyncio

import pytest

import update_sectors
from common import sector_loader
from common.models import (
    LEGACY_SECTOR_PLACEHOLDER_CATEGORIES,
    SectorMap,
    SectorTagResult,
    SectorTagStatus,
    TickerData,
    UNTAGGED_SECTOR_CATEGORY,
)
from common.signals.detector import calculate_sector_correlation
from common.storage_client import StateErrorCode, StateLoadError


def _tagged(market: str, *categories: str) -> SectorTagResult:
    return SectorTagResult(
        market=market,
        status=SectorTagStatus.TAGGED,
        categories=list(categories),
    )


def _failed(market: str, status: SectorTagStatus) -> SectorTagResult:
    return SectorTagResult(market=market, status=status)


def test_sector_categories_are_exact_identifiers_not_parenthesis_aliases():
    sectors, reverse = sector_loader.process_sector_data(
        SectorMap(
            {
                "KRW-A": ["Layer 1 (L1)"],
                "KRW-B": ["Layer 1"],
            }
        )
    )

    assert sectors == {"Layer 1 (L1)": ["KRW-A"], "Layer 1": ["KRW-B"]}
    assert reverse["KRW-A"] == ["Layer 1 (L1)"]


@pytest.mark.parametrize(
    "legacy_diagnostic",
    sorted(
        LEGACY_SECTOR_PLACEHOLDER_CATEGORIES - {UNTAGGED_SECTOR_CATEGORY}
    ),
)
def test_production_untagged_placeholders_do_not_form_runtime_sectors(
    legacy_diagnostic,
):
    sector_map = SectorMap(
        {
            market: [UNTAGGED_SECTOR_CATEGORY, legacy_diagnostic]
            for market in ["KRW-A", "KRW-B", "KRW-C", "KRW-D"]
        }
    )
    sectors, reverse = sector_loader.process_sector_data(sector_map)
    tickers = {
        market: TickerData(market=market, price_change_10m=2.0)
        for market in sector_map.root
    }

    assert sectors == {}
    assert all(tags == [] for tags in reverse.values())
    assert calculate_sector_correlation(
        "KRW-A", tickers, sectors, reverse
    ) == 0.0


def test_transient_sector_failure_canonicalizes_preserved_legacy_placeholders(
    monkeypatch,
):
    saved = []

    async def load(*_args):
        return {
            "KRW-A": [UNTAGGED_SECTOR_CATEGORY, "API_Error"],
            "KRW-B": ["DeFi"],
        }

    async def save(filename, value, *_args):
        saved.append((filename, value))

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", save)

    asyncio.run(
        update_sectors.save_validated_sector_map(
            [
                _failed("KRW-A", SectorTagStatus.LOOKUP_FAILED),
                _tagged("KRW-B", "DeFi"),
            ]
        )
    )

    canonical = {
        "KRW-A": [UNTAGGED_SECTOR_CATEGORY],
        "KRW-B": ["DeFi"],
    }
    assert saved == [
        (update_sectors.SECTOR_MAP_ROLLBACK_FILE_NAME, canonical),
        (update_sectors.config.SECTOR_MAP_FILE_NAME, canonical),
    ]


def test_sector_map_rejects_suspicious_bulk_change():
    with pytest.raises(update_sectors.SectorUpdateError) as error:
        update_sectors.validate_sector_map_change(
            {"KRW-A": ["A"], "KRW-B": ["B"]}, {"KRW-A": ["Changed"]}
        )
    assert (
        error.value.code
        is update_sectors.SectorUpdateErrorCode.SUSPICIOUS_MAP_CHANGE
    )


def test_sector_map_write_failure_does_not_restore_a_stale_canonical_snapshot(
    monkeypatch,
):
    saved = []

    async def load(*_args):
        return {"KRW-A": ["A"]}

    async def save(filename, value, *_args):
        saved.append((filename, value))
        if filename == update_sectors.config.SECTOR_MAP_FILE_NAME:
            raise RuntimeError("write failed")

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", save)

    with pytest.raises(RuntimeError):
        asyncio.run(update_sectors.save_validated_sector_map([_tagged("KRW-A", "A")]))
    assert saved == [
        (update_sectors.SECTOR_MAP_ROLLBACK_FILE_NAME, {"KRW-A": ["A"]}),
        (update_sectors.config.SECTOR_MAP_FILE_NAME, {"KRW-A": ["A"]}),
    ]


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
            [
                _failed("KRW-A", SectorTagStatus.LOOKUP_FAILED),
                _tagged("KRW-B", "DeFi"),
                _tagged("KRW-C", "Gaming"),
                _tagged("KRW-D", "Payments"),
            ]
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

    with pytest.raises(update_sectors.SectorUpdateError) as error:
        asyncio.run(
            update_sectors.save_validated_sector_map(
                [
                    _failed("KRW-BTC", SectorTagStatus.LOOKUP_FAILED),
                    _failed("KRW-ETH", SectorTagStatus.LOOKUP_NOT_FOUND),
                ]
            )
        )
    assert (
        error.value.code
        is update_sectors.SectorUpdateErrorCode.BOOTSTRAP_COVERAGE_INSUFFICIENT
    )


def test_sector_map_allows_fresh_bootstrap_with_a_usable_category(monkeypatch):
    saved = []

    async def load(*_args):
        return None

    async def save(filename, value, *_args):
        saved.append((filename, value))

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", save)

    results = [
        _tagged("KRW-BTC", "Layer 1"),
        _failed("KRW-ETH", SectorTagStatus.LOOKUP_FAILED),
    ]
    asyncio.run(update_sectors.save_validated_sector_map(results))

    assert saved == [
        (
            update_sectors.config.SECTOR_MAP_FILE_NAME,
            {
                "KRW-BTC": ["Layer 1"],
                "KRW-ETH": [UNTAGGED_SECTOR_CATEGORY],
            },
        )
    ]


def test_live_sized_unique_symbol_coverage_can_bootstrap_without_name_aliases():
    results = [_tagged(f"KRW-TAGGED-{index}", "Layer 1") for index in range(145)]
    results.extend(
        _failed(f"KRW-UNTAGGED-{index}", SectorTagStatus.SYMBOL_AMBIGUOUS)
        for index in range(124)
    )

    update_sectors.validate_sector_map_bootstrap({}, results)


def test_sector_map_rejects_near_total_fresh_bootstrap_failure(monkeypatch):
    async def load(*_args):
        return None

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", pytest.fail)
    results = [_tagged("KRW-VALID", "Layer 1")]
    results.extend(
        _failed(f"KRW-FAILED-{index}", SectorTagStatus.LOOKUP_FAILED)
        for index in range(9)
    )

    with pytest.raises(update_sectors.SectorUpdateError) as error:
        asyncio.run(update_sectors.save_validated_sector_map(results))
    assert (
        error.value.code
        is update_sectors.SectorUpdateErrorCode.BOOTSTRAP_COVERAGE_INSUFFICIENT
    )


@pytest.mark.parametrize(
    "results",
    [
        [],
        [{"market": "BTC", "status": "tagged", "categories": ["Layer 1"]}],
        [
            {"market": "KRW-BTC", "status": "tagged", "categories": ["Layer 1"]},
            {"market": "KRW-BTC", "status": "no_category"},
        ],
        [
            {
                "market": "KRW-BTC",
                "status": "lookup_failed",
                "categories": ["Layer 1"],
            }
        ],
    ],
)
def test_sector_update_rejects_invalid_tag_result_schema(monkeypatch, results):
    async def load(*_args):
        return None

    monkeypatch.setattr(update_sectors, "load_json", load)
    monkeypatch.setattr(update_sectors, "save_json", pytest.fail)

    with pytest.raises(update_sectors.SectorUpdateError) as error:
        asyncio.run(update_sectors.save_validated_sector_map(results))
    assert (
        error.value.code is update_sectors.SectorUpdateErrorCode.INVALID_TAG_RESULTS
    )


@pytest.mark.parametrize(
    "sector_map",
    [
        {"KRW-BTC": "Layer 1"},
        {"KRW-BTC": [123]},
        {"KRW-BTC": []},
        {123: ["Layer 1"]},
        {},
    ],
)
def test_sector_loader_rejects_invalid_canonical_schema(monkeypatch, sector_map):
    async def load(*_args, **_kwargs):
        return sector_map

    monkeypatch.setattr(sector_loader, "load_json", load)

    with pytest.raises(StateLoadError) as error:
        asyncio.run(sector_loader.load_and_process_sectors())
    assert error.value.code is StateErrorCode.INVALID_SCHEMA


def test_sector_loader_distinguishes_missing_file_from_explicit_null(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(sector_loader.config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(sector_loader.config, "LOCAL_STATE_DIR", str(tmp_path))

    assert asyncio.run(sector_loader.load_and_process_sectors()) == ({}, {})

    (tmp_path / sector_loader.config.SECTOR_MAP_FILE_NAME).write_text(
        "null", encoding="utf-8"
    )
    with pytest.raises(StateLoadError) as error:
        asyncio.run(sector_loader.load_and_process_sectors())
    assert error.value.code is StateErrorCode.NULL_DOCUMENT


def test_invalid_coingecko_category_schema_becomes_a_transient_failure(monkeypatch):
    async def detail(*_args):
        return update_sectors.CoinGeckoDetail(
            name="Bitcoin", categories=[123]
        )

    monkeypatch.setattr(update_sectors, "get_coin_detail", detail)
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {})

    result = asyncio.run(
        update_sectors.tag_market(
            None,
            "btc",
            "KRW-BTC",
            {"btc": ["bitcoin"]},
        )
    )

    assert result.market == "KRW-BTC"
    assert result.status is SectorTagStatus.INVALID_CATEGORY
    assert result.categories == []


@pytest.mark.parametrize("categories", [None, ""])
def test_falsy_invalid_coingecko_category_schema_is_not_treated_as_empty(
    monkeypatch, categories
):
    async def detail(*_args):
        return update_sectors.CoinGeckoDetail(
            name="Bitcoin", categories=categories
        )

    monkeypatch.setattr(update_sectors, "get_coin_detail", detail)
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {})

    result = asyncio.run(
        update_sectors.tag_market(
            None,
            "btc",
            "KRW-BTC",
            {"btc": ["bitcoin"]},
        )
    )

    assert result.market == "KRW-BTC"
    assert result.status is SectorTagStatus.INVALID_CATEGORY
    assert result.categories == []


def test_missing_coingecko_category_field_is_a_transient_schema_failure(monkeypatch):
    async def detail(*_args):
        return update_sectors.CoinGeckoDetail(name="Bitcoin")

    monkeypatch.setattr(update_sectors, "get_coin_detail", detail)
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {})

    result = asyncio.run(
        update_sectors.tag_market(
            None,
            "btc",
            "KRW-BTC",
            {"btc": ["bitcoin"]},
        )
    )

    assert result.market == "KRW-BTC"
    assert result.status is SectorTagStatus.INVALID_CATEGORY
    assert result.categories == []

import asyncio
from unittest.mock import AsyncMock

import update_sectors
from common.models import SectorTagStatus


def test_unique_symbol_identity_does_not_depend_on_provider_display_name():
    coin = update_sectors.CoinGeckoDetail(
        name="Bitcoin", platforms={"ethereum": "0x1"}
    )
    assert update_sectors.validate_coin_identity(coin)


def test_explicit_override_rejects_name_or_network_mismatch():
    coin = update_sectors.CoinGeckoDetail(
        name="Bitcoin", platforms={"ethereum": "0x1"}
    )
    assert not update_sectors.validate_coin_identity(
        coin,
        update_sectors.CoinGeckoOverride(
            id="bitcoin", name="Bitcoin Cash", network="ethereum"
        ),
    )
    assert not update_sectors.validate_coin_identity(
        coin,
        update_sectors.CoinGeckoOverride(id="bitcoin", network="solana"),
    )


def test_structured_override_requires_matching_name_and_network():
    coin = update_sectors.CoinGeckoDetail(
        name="Wrapped Bitcoin", platforms={"ethereum": "0x2"}
    )
    assert update_sectors.validate_coin_identity(
        coin,
        update_sectors.CoinGeckoOverride(
            id="wrapped-bitcoin", name="Wrapped Bitcoin", network="ethereum"
        ),
    )


def test_tag_market_accepts_unique_symbol_when_provider_names_differ(monkeypatch):
    monkeypatch.setattr(
        update_sectors,
        "get_coin_detail",
        AsyncMock(
            return_value=update_sectors.CoinGeckoDetail(
                name="Different Asset", categories=["Payments"]
            )
        ),
    )
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {})

    result = asyncio.run(
        update_sectors.tag_market(None, "pay", "KRW-PAY", {"pay": ["tenx"]})
    )

    assert result.market == "KRW-PAY"
    assert result.status is SectorTagStatus.TAGGED
    assert result.categories == ["Payments"]


def test_tag_market_rejects_explicit_override_identity_mismatch(monkeypatch):
    monkeypatch.setattr(
        update_sectors,
        "get_coin_detail",
        AsyncMock(
            return_value=update_sectors.CoinGeckoDetail(
                name="Different Asset", categories=["Payments"]
            )
        ),
    )
    monkeypatch.setattr(
        update_sectors,
        "CG_SYMBOL_OVERRIDES",
        {
            "pay": update_sectors.CoinGeckoOverride(
                id="tenx", name="TenX"
            )
        },
    )

    result = asyncio.run(
        update_sectors.tag_market(
            None, "pay", "KRW-PAY", {"pay": ["tenx", "tenx-pay"]}
        )
    )

    assert result.status is SectorTagStatus.IDENTITY_MISMATCH

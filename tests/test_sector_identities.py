import asyncio
from unittest.mock import AsyncMock

import update_sectors


def test_coin_identity_rejects_name_or_network_mismatch():
    coin = {"name": "Bitcoin", "platforms": {"ethereum": "0x1"}}
    assert update_sectors.validate_coin_identity("Bitcoin", coin)
    assert not update_sectors.validate_coin_identity("Bitcoin Cash", coin)
    assert not update_sectors.validate_coin_identity("Bitcoin", coin, {"network": "solana"})


def test_structured_override_requires_matching_name_and_network():
    coin = {"name": "Wrapped Bitcoin", "platforms": {"ethereum": "0x2"}}
    assert update_sectors.validate_coin_identity(None, coin, {"name": "Wrapped Bitcoin", "network": "ethereum"})


def test_tag_market_rejects_coingecko_name_mismatch(monkeypatch):
    monkeypatch.setattr(
        update_sectors,
        "get_coin_detail",
        AsyncMock(return_value={"name": "Different Asset", "platforms": {}, "categories": ["Payments"]}),
    )

    result = asyncio.run(
        update_sectors.tag_market(None, "pay", "KRW-PAY", {"pay": ["tenx"]}, upbit_name="TenX")
    )

    assert result == ("KRW-PAY", ["Untagged", "Identity_Mismatch"])

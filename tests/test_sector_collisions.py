import asyncio
from unittest.mock import AsyncMock

import pytest

import update_sectors


def test_empty_symbol_override_configuration_uses_an_empty_mapping():
    assert update_sectors.parse_symbol_overrides(None) == {}
    assert update_sectors.parse_symbol_overrides("") == {}
    assert update_sectors.parse_symbol_overrides("   ") == {}


def test_symbol_override_configuration_requires_a_json_object():
    with pytest.raises(RuntimeError, match="JSON object"):
        update_sectors.parse_symbol_overrides("[]")


def test_ambiguous_coingecko_symbol_is_rejected_without_override(monkeypatch):
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {})
    result = asyncio.run(update_sectors.tag_market(None, "pay", "KRW-PAY", {"pay": ["tenx", "tenx-pay"]}))
    assert result == ("KRW-PAY", ["Untagged", "CG_Symbol_Ambiguous"])


def test_valid_manual_override_selects_the_explicit_coingecko_id(monkeypatch):
    monkeypatch.setattr(update_sectors, "CG_SYMBOL_OVERRIDES", {"pay": "tenx"})
    monkeypatch.setattr(
        update_sectors,
        "get_coin_detail",
        AsyncMock(return_value={"name": "TenX", "platforms": {}, "categories": ["Payments"]}),
    )
    result = asyncio.run(update_sectors.tag_market(None, "pay", "KRW-PAY", {"pay": ["tenx", "tenx-pay"]}))
    assert result == ("KRW-PAY", ["Payments"])

import asyncio
from unittest.mock import AsyncMock

import aiohttp
import pytest

import update_sectors
from common.models import (
    MarketListing,
    SectorTagResult,
    SectorTagStatus,
)
from common.storage_client import StateErrorCode, StateSaveError


class DummyResponse:
    def __init__(self, payload=None):
        self._payload = payload if payload is not None else []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    async def json(self):
        return self._payload


class DummySession:
    def __init__(self, payload):
        self.payload = payload

    def get(self, *args, **kwargs):
        return DummyResponse(self.payload)


def test_upbit_market_collection_returns_typed_listings():
    result = asyncio.run(
        update_sectors.get_upbit_krw_markets(
            DummySession(
                [
                    {
                        "market": "KRW-BTC",
                        "english_name": "Bitcoin",
                    },
                    {"market": "BTC-USDT", "english_name": "Bitcoin"},
                ]
            )
        )
    )

    assert result == {
        "btc": MarketListing(
            market="KRW-BTC",
            english_name="Bitcoin",
        )
    }


def test_main_raises_when_upbit_markets_empty(monkeypatch):
    monkeypatch.setattr(update_sectors.config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(update_sectors, "get_upbit_krw_markets", AsyncMock(return_value={}))
    monkeypatch.setattr(
        update_sectors,
        "get_coingecko_coins_list",
        AsyncMock(return_value={"btc": ["bitcoin"]}),
    )
    monkeypatch.setattr(update_sectors, "save_json", pytest.fail)

    class DummyClientSession:
        async def __aenter__(self):
            return DummySession([])

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(aiohttp, "ClientSession", DummyClientSession)

    with pytest.raises(update_sectors.SectorUpdateError) as error:
        asyncio.run(update_sectors.main())
    assert (
        error.value.code
        is update_sectors.SectorUpdateErrorCode.UPBIT_MARKETS_UNAVAILABLE
    )


def test_main_raises_when_coingecko_symbols_empty(monkeypatch):
    monkeypatch.setattr(update_sectors.config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(
        update_sectors,
        "get_upbit_krw_markets",
        AsyncMock(return_value={"btc": MarketListing(market="KRW-BTC")}),
    )
    monkeypatch.setattr(update_sectors, "get_coingecko_coins_list", AsyncMock(return_value={}))
    monkeypatch.setattr(update_sectors, "save_json", pytest.fail)

    class DummyClientSession:
        async def __aenter__(self):
            return DummySession([])

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(aiohttp, "ClientSession", DummyClientSession)

    with pytest.raises(update_sectors.SectorUpdateError) as error:
        asyncio.run(update_sectors.main())
    assert (
        error.value.code
        is update_sectors.SectorUpdateErrorCode.COINGECKO_LIST_UNAVAILABLE
    )


def test_main_reraises_save_json_failure(monkeypatch):
    monkeypatch.setattr(update_sectors.config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(
        update_sectors,
        "get_upbit_krw_markets",
        AsyncMock(return_value={"btc": MarketListing(market="KRW-BTC")}),
    )
    monkeypatch.setattr(
        update_sectors,
        "get_coingecko_coins_list",
        AsyncMock(return_value={"btc": ["bitcoin"]}),
    )
    monkeypatch.setattr(
        update_sectors,
        "tag_market",
        AsyncMock(
            return_value=SectorTagResult(
                market="KRW-BTC",
                status=SectorTagStatus.TAGGED,
                categories=["Defi"],
            )
        ),
    )
    monkeypatch.setattr(update_sectors, "load_json", AsyncMock(return_value=None))

    async def raise_save(*args, **kwargs):
        raise StateSaveError(
            StateErrorCode.WRITE_FAILED, update_sectors.config.SECTOR_MAP_FILE_NAME
        )

    monkeypatch.setattr(update_sectors, "save_json", raise_save)

    class DummyClientSession:
        async def __aenter__(self):
            return DummySession([])

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(aiohttp, "ClientSession", DummyClientSession)

    with pytest.raises(StateSaveError) as error:
        asyncio.run(update_sectors.main())
    assert error.value.code is StateErrorCode.WRITE_FAILED

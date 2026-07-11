import asyncio
from unittest.mock import AsyncMock

import aiohttp
import pytest

import update_sectors


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


def test_main_raises_when_upbit_markets_empty(monkeypatch):
    monkeypatch.setattr(update_sectors.config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(update_sectors, "get_upbit_krw_markets", AsyncMock(return_value={}))
    monkeypatch.setattr(update_sectors, "get_coingecko_coins_list", AsyncMock(return_value={"btc": "bitcoin"}))
    monkeypatch.setattr(update_sectors, "save_json", pytest.fail)

    class DummyClientSession:
        async def __aenter__(self):
            return DummySession([])

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(aiohttp, "ClientSession", DummyClientSession)

    with pytest.raises(RuntimeError, match="Upbit KRW 마켓 목록 조회 결과가 비어 있습니다."):
        asyncio.run(update_sectors.main())


def test_main_raises_when_coingecko_symbols_empty(monkeypatch):
    monkeypatch.setattr(update_sectors.config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(update_sectors, "get_upbit_krw_markets", AsyncMock(return_value={"btc": "KRW-BTC"}))
    monkeypatch.setattr(update_sectors, "get_coingecko_coins_list", AsyncMock(return_value={}))
    monkeypatch.setattr(update_sectors, "save_json", pytest.fail)

    class DummyClientSession:
        async def __aenter__(self):
            return DummySession([])

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(aiohttp, "ClientSession", DummyClientSession)

    with pytest.raises(RuntimeError, match="CoinGecko 코인 목록 조회 결과가 비어 있습니다."):
        asyncio.run(update_sectors.main())


def test_main_reraises_save_json_failure(monkeypatch):
    monkeypatch.setattr(update_sectors.config, "STATE_STORAGE_METHOD", "LOCAL")
    monkeypatch.setattr(update_sectors, "get_upbit_krw_markets", AsyncMock(return_value={"btc": "KRW-BTC"}))
    monkeypatch.setattr(update_sectors, "get_coingecko_coins_list", AsyncMock(return_value={"btc": "bitcoin"}))
    monkeypatch.setattr(update_sectors, "tag_market", AsyncMock(return_value=("KRW-BTC", ["Defi"])))
    monkeypatch.setattr(update_sectors, "load_json", AsyncMock(return_value={}))

    async def raise_save(*args, **kwargs):
        raise RuntimeError("save failed")

    monkeypatch.setattr(update_sectors, "save_json", raise_save)

    class DummyClientSession:
        async def __aenter__(self):
            return DummySession([])

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(aiohttp, "ClientSession", DummyClientSession)

    with pytest.raises(RuntimeError, match="save failed"):
        asyncio.run(update_sectors.main())

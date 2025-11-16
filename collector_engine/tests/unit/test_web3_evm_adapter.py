import asyncio
import pytest
from typing import List

from collector_engine.app.shell.adapters.evm.web3_reader import Web3EvmReader

PROVIDER_URL = "http://localhost"


@pytest.mark.asyncio
async def test_latest_block_number__success(monkeypatch):
    reader = Web3EvmReader(PROVIDER_URL)

    class DummyBlock:
        def __init__(self, number: int):
            self.number = number

    async def fake_get_block(arg):
        assert arg == "latest"
        return DummyBlock(123456)

    monkeypatch.setattr(reader.w3.eth, "get_block", fake_get_block)

    blk_no = await reader.latest_block_number()

    assert blk_no == 123456


@pytest.mark.asyncio
async def test_get_logs__address_and_filter(monkeypatch):
    reader = Web3EvmReader(PROVIDER_URL)
    captured_filters: List[dict] = []

    async def fake_get_logs(filter_obj):
        captured_filters.append(filter_obj)
        return [
            {"address": filter_obj["address"], "data": "0x", "topics": []},
        ]

    monkeypatch.setattr(reader.w3.eth, "get_logs", fake_get_logs)
    address = b"\x11" * 20

    result = await reader.get_logs(address=address, from_block=100, to_block=110)

    assert isinstance(result, list)
    assert captured_filters, "get_logs was not called"
    f = captured_filters[0]
    assert f == {
        "fromBlock": 100,
        "toBlock": 110,
        "address": "0x" + address.hex(),
    }


@pytest.mark.asyncio
async def test_get_transactions__bytes_hash_conversion(monkeypatch):
    reader = Web3EvmReader(PROVIDER_URL)
    calls: List[str] = []

    async def fake_get_transaction(h: str):
        calls.append(h)
        return {"hash": h, "nonce": 1}

    monkeypatch.setattr(reader.w3.eth, "get_transaction", fake_get_transaction)
    hashes = [b"\xaa" * 32, b"\xbb" * 32]

    txs = await reader.get_transactions(hashes)

    assert len(txs) == 2
    assert calls == ["0x" + h.hex() for h in hashes]
    assert all("hash" in tx for tx in txs)


@pytest.mark.asyncio
async def test_get_receipts__bytes_hash_conversion(monkeypatch):
    reader = Web3EvmReader(PROVIDER_URL)
    calls: List[str] = []

    async def fake_get_receipt(h: str):
        calls.append(h)
        return {"transactionHash": h, "status": 1}

    monkeypatch.setattr(reader.w3.eth, "get_transaction_receipt", fake_get_receipt)
    hashes = [b"\xcc" * 32, b"\xdd" * 32]

    receipts = await reader.get_receipts(hashes)

    assert len(receipts) == 2
    assert calls == ["0x" + h.hex() for h in hashes]
    assert all("transactionHash" in rc for rc in receipts)


@pytest.mark.asyncio
async def test_get_transactions__empty_input(monkeypatch):
    reader = Web3EvmReader(PROVIDER_URL)

    async def fake_get_transaction(h: str):
        raise AssertionError("Should not be called for empty input")

    monkeypatch.setattr(reader.w3.eth, "get_transaction", fake_get_transaction)

    txs = await reader.get_transactions([])

    assert txs == []


@pytest.mark.asyncio
async def test_get_transactions__concurrency_limit(monkeypatch):
    reader = Web3EvmReader(PROVIDER_URL, max_concurrency=2)
    current = 0
    max_seen = 0
    lock = asyncio.Lock()

    async def fake_get_transaction(h: str):
        nonlocal current, max_seen
        async with lock:
            current += 1
            max_seen = max(max_seen, current)
        await asyncio.sleep(0.02)
        async with lock:
            current -= 1
        return {"hash": h}

    monkeypatch.setattr(reader.w3.eth, "get_transaction", fake_get_transaction)
    hashes = [b"\x01" * 32 for _ in range(6)]

    _ = await reader.get_transactions(hashes)

    assert max_seen <= 2
    assert max_seen >= 2

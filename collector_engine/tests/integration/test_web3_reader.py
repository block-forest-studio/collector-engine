import os
import inspect
import pytest

from collector_engine.app.infrastructure.adapters.evm.web3_reader import Web3EvmReader


def test_web3_reader_surface_no_network():
    reader = Web3EvmReader("http://example.invalid")

    assert hasattr(reader, "latest_block_number")
    assert hasattr(reader, "get_logs")
    assert hasattr(reader, "get_transactions")
    assert hasattr(reader, "get_receipts")

    assert inspect.iscoroutinefunction(reader.latest_block_number)
    assert inspect.iscoroutinefunction(reader.get_logs)
    assert inspect.iscoroutinefunction(reader.get_transactions)
    assert inspect.iscoroutinefunction(reader.get_receipts)


@pytest.mark.slow
@pytest.mark.asyncio
async def test_web3_reader_latest_block_optional_rpc():
    url = os.environ.get("TEST_RPC_URL")
    if not url:
        pytest.skip("TEST_RPC_URL not set; skipping real RPC test")

    reader = Web3EvmReader(url)
    blk = await reader.latest_block_number()
    assert isinstance(blk, int) and blk > 0

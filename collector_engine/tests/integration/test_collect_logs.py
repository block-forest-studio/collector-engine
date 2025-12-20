import pytest
import pyarrow as pa

from collector_engine.app.infrastructure.adapters.storage.parquet_store import ParquetDatasetStore
from collector_engine.app.application.services.collectors.collect_logs import collect_logs
from collector_engine.app.infrastructure.registry.schemas import ContractInfo


class FakeEvmReader:
    def __init__(self, latest: int):
        self._latest = latest

    async def latest_block_number(self) -> int:
        return self._latest

    async def get_logs(self, *, address: bytes, from_block: int, to_block: int):
        logs = []
        for i, blk in enumerate(range(from_block, to_block + 1)):
            logs.append(
                {
                    "blockNumber": blk,
                    "blockHash": "0x" + ("aa" * 32),
                    "transactionHash": "0x" + ("bb" * 32),
                    "logIndex": i,
                    "address": "0x" + address.hex(),
                    "topics": ["0x" + ("00" * 32)],
                    "data": "0x",
                    "removed": False,
                }
            )
        return logs

    async def get_transactions(self, hashes):
        return []

    async def get_receipts(self, hashes):
        return []


@pytest.mark.asyncio
async def test_collect_logs_idempotent(tmp_path):
    chain_id = 1
    latest = 105
    reader = FakeEvmReader(latest)

    logs_path = tmp_path / "logs"
    store = ParquetDatasetStore(logs_path)

    contract = ContractInfo(
        name="PoolManager",
        abi="",
        address=b"\x11" * 20,
        genesis_block=100,
    )

    await collect_logs(
        chain_id=chain_id,
        contract_info=contract,
        reader=reader,
        store=store,
        batch_size=10,
    )

    names_before = set(store.list_names())
    assert names_before, "No parquet files created"
    newest = sorted(names_before)[-1]
    table = store.read_table(newest)
    assert isinstance(table, pa.Table)
    # max block number should be latest
    assert table["block_number"].to_pylist()[-1] <= latest

    await collect_logs(
        chain_id=chain_id,
        contract_info=contract,
        reader=reader,
        store=store,
        batch_size=10,
    )
    names_after = set(store.list_names())
    assert names_after == names_before, "Collecting again should not create new files"

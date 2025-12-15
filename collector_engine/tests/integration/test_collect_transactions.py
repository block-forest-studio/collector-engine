import pytest
import pyarrow as pa

from collector_engine.app.infrastructure.adapters.storage.parquet_store import ParquetDatasetStore
from collector_engine.app.application.services.collect_transactions import collect_transactions
from collector_engine.app.infrastructure.registry.schemas import ContractInfo
from collector_engine.app.infrastructure.parquet.schema import LOG_SCHEMA, TX_SCHEMA


class FakeEvmReader:
    async def latest_block_number(self) -> int:
        return 0

    async def get_logs(self, *, address: bytes, from_block: int, to_block: int):
        return []

    async def get_transactions(self, hashes):
        txs = []
        for h in hashes:
            txs.append(
                {
                    "blockHash": "0x" + ("aa" * 32),
                    "blockNumber": 200,
                    "from": "0x" + ("11" * 20),
                    "gas": 21_000,
                    "gasPrice": 1_000_000_000,
                    "hash": "0x" + h.hex(),
                    "input": "0x",
                    "nonce": 1,
                    "to": "0x" + ("22" * 20),
                    "transactionIndex": 0,
                    "value": 0,
                    "type": 2,
                    "v": 27,
                    "r": b"\xcc" * 32,
                    "s": b"\xdd" * 32,
                }
            )
        return txs

    async def get_receipts(self, hashes):
        return []


def _write_logs_file(store: ParquetDatasetStore, hashes: list[bytes]):
    buf = {name: [] for name in LOG_SCHEMA.names}
    for i, h in enumerate(hashes):
        row = {
            "chain_id": 1,
            "block_number": 150,
            "block_hash": b"\xaa" * 32,
            "transaction_hash": h,
            "log_index": i,
            "address": b"\x11" * 20,
            "topic0": None,
            "topic1": None,
            "topic2": None,
            "topic3": None,
            "data": b"",
            "removed": False,
        }
        for k in LOG_SCHEMA.names:
            buf[k].append(row[k])
    store.write_buffer(
        buffer=buf,
        schema=LOG_SCHEMA,
        file_name="logs_150_150",
        rows_per_file=10,
        force=True,
    )


@pytest.mark.asyncio
async def test_collect_transactions_idempotent(tmp_path):
    chain_id = 1
    contract = ContractInfo(name="PoolManager", abi="", address=b"\x11" * 20, genesis_block=100)

    logs_store = ParquetDatasetStore(tmp_path / "logs")
    tx_store = ParquetDatasetStore(tmp_path / "txs")
    reader = FakeEvmReader()

    tx_hashes = [b"\xbb" * 32, b"\xcc" * 32]
    _write_logs_file(logs_store, tx_hashes)

    await collect_transactions(
        chain_id=chain_id,
        contract_info=contract,
        reader=reader,
        logs_store=logs_store,
        tx_store=tx_store,
        batch_size=10,
    )

    names_before = set(tx_store.list_names())
    assert names_before, "No tx parquet files created"
    newest = sorted(names_before)[-1]
    table = tx_store.read_table(newest)
    assert isinstance(table, pa.Table)
    assert set(table.column_names) == set(TX_SCHEMA.names)

    await collect_transactions(
        chain_id=chain_id,
        contract_info=contract,
        reader=reader,
        logs_store=logs_store,
        tx_store=tx_store,
        batch_size=10,
    )

    names_after = set(tx_store.list_names())
    assert names_after == names_before, "Second run should be idempotent"

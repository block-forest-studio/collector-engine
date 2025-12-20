import pytest
import pyarrow as pa

from collector_engine.app.infrastructure.adapters.storage.parquet_store import ParquetDatasetStore
from collector_engine.app.application.services.collectors.collect_receipts import collect_receipts
from collector_engine.app.infrastructure.registry.schemas import ContractInfo
from collector_engine.app.infrastructure.parquet.schema import TX_SCHEMA, RECEIPT_SCHEMA


class FakeEvmReader:
    async def latest_block_number(self) -> int:
        return 0

    async def get_logs(self, *, address: bytes, from_block: int, to_block: int):
        return []

    async def get_transactions(self, hashes):
        return []

    async def get_receipts(self, hashes):
        out = []
        for h in hashes:
            out.append(
                {
                    "blockHash": "0x" + ("aa" * 32),
                    "blockNumber": 250,
                    "transactionHash": "0x" + h.hex(),
                    "transactionIndex": 0,
                    "from": "0x" + ("11" * 20),
                    "to": "0x" + ("22" * 20),
                    "contractAddress": None,
                    "status": 1,
                    "type": 2,
                    "gasUsed": 21_000,
                    "cumulativeGasUsed": 21_000,
                    "effectiveGasPrice": 1_000_000_000,
                    "logsBloom": None,
                    "logs": [],
                }
            )
        return out


def _write_tx_file(store: ParquetDatasetStore, hashes: list[bytes]):
    buf = {name: [] for name in TX_SCHEMA.names}
    for i, h in enumerate(hashes):
        row = {
            "block_hash": b"\xaa" * 32,
            "block_number": 240,
            "from": b"\x11" * 20,
            "gas": 21_000,
            "gas_price": 1_000_000_000,
            "max_fee_per_gas": None,
            "max_priority_fee_per_gas": None,
            "hash": h,
            "input": b"",
            "nonce": 1,
            "to": b"\x22" * 20,
            "transaction_index": i,
            "value": 0,
            "type": 2,
            "chain_id": 1,
            "v": 27,
            "r": "0x" + ("cc" * 32),
            "s": "0x" + ("dd" * 32),
            "y_parity": None,
            "access_list": [],
        }
        for k in TX_SCHEMA.names:
            buf[k].append(row[k])
    store.write_buffer(
        buffer=buf,
        schema=TX_SCHEMA,
        file_name="txs_240_240",
        rows_per_file=10,
        force=True,
    )


@pytest.mark.asyncio
async def test_collect_receipts_idempotent(tmp_path):
    chain_id = 1
    contract = ContractInfo(name="PoolManager", abi="", address=b"\x11" * 20, genesis_block=100)

    tx_store = ParquetDatasetStore(tmp_path / "txs")
    receipts_store = ParquetDatasetStore(tmp_path / "receipts")
    reader = FakeEvmReader()

    tx_hashes = [b"\xbb" * 32, b"\xcc" * 32]
    _write_tx_file(tx_store, tx_hashes)

    await collect_receipts(
        chain_id=chain_id,
        contract_info=contract,
        reader=reader,
        tx_store=tx_store,
        receipts_store=receipts_store,
        batch_size=10,
    )

    names_before = set(receipts_store.list_names())
    assert names_before, "No receipts parquet files created"
    newest = sorted(names_before)[-1]
    table = receipts_store.read_table(newest)
    assert isinstance(table, pa.Table)
    assert set(table.column_names) == set(RECEIPT_SCHEMA.names)

    await collect_receipts(
        chain_id=chain_id,
        contract_info=contract,
        reader=reader,
        tx_store=tx_store,
        receipts_store=receipts_store,
        batch_size=10,
    )

    names_after = set(receipts_store.list_names())
    assert names_after == names_before, "Second run should be idempotent"

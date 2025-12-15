import pytest

from collector_engine.app.application.services.run_pipeline import (
    run_pipeline,
    PipelineDeps,
    PipelineConfig,
)
from collector_engine.app.infrastructure.adapters.storage.parquet_store import ParquetDatasetStore
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


@pytest.mark.asyncio
async def test_pipeline_end_to_end_idempotent(tmp_path):
    chain_id = 1
    latest = 105
    reader = FakeEvmReader(latest)

    logs_store = ParquetDatasetStore(tmp_path / "logs")
    tx_store = ParquetDatasetStore(tmp_path / "txs")
    receipts_store = ParquetDatasetStore(tmp_path / "receipts")

    contract = ContractInfo(
        name="PoolManager",
        abi="",
        address=b"\x11" * 20,
        genesis_block=100,
    )

    cfg = PipelineConfig(chain_id=chain_id, protocol="uniswap_v4", contract_info=contract)
    deps = PipelineDeps(
        reader=reader, logs_store=logs_store, tx_store=tx_store, receipts_store=receipts_store
    )

    await run_pipeline(cfg=cfg, deps=deps)

    logs_f = set(logs_store.list_names())
    txs_f = set(tx_store.list_names())
    rec_f = set(receipts_store.list_names())

    assert logs_f, "Logs parquet not created"
    assert txs_f, "Transactions parquet not created"
    assert rec_f, "Receipts parquet not created"

    await run_pipeline(cfg=cfg, deps=deps)

    assert set(logs_store.list_names()) == logs_f
    assert set(tx_store.list_names()) == txs_f
    assert set(receipts_store.list_names()) == rec_f

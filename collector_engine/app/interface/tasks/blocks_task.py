from pathlib import Path

from collector_engine.app.infrastructure.config.settings import app_config, web3_config
from collector_engine.app.domain.ports.out import EvmReader, DatasetStore
from collector_engine.app.infrastructure.factories.evm_reader_factory import evm_reader_factory
from collector_engine.app.infrastructure.factories.storage_factory import storage_factory
from collector_engine.app.application.services.collectors.collect_blocks import collect_blocks


async def blocks_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect block data (with timestamps) for a specific chain."""
    reader: EvmReader = evm_reader_factory("web3", web3_config.rpc_url(chain_id))

    base_path = Path(app_config.data_path) / "chain" / str(chain_id) / "blocks"
    store: DatasetStore = storage_factory("parquet", base_path)

    await collect_blocks(
        chain_id=chain_id,
        reader=reader,
        store=store,
    )

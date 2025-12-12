from pathlib import Path

from collector_engine.app.infrastructure.config.settings import app_config, web3_config
from collector_engine.app.domain.ports.out import EvmReader, DatasetStore
from collector_engine.app.infrastructure.factories.evm_reader_factory import evm_reader_factory
from collector_engine.app.infrastructure.factories.storage_factory import create_dataset_store
from collector_engine.app.infrastructure.registry.registry import get_protocol_info
from collector_engine.app.application.services.collect_logs import collect_logs
from collector_engine.app.application.services.collect_transactions import collect_transactions


async def logs_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect logs for a specific contract."""
    reader: EvmReader = evm_reader_factory("web3", web3_config.rpc_url(chain_id))

    base_path = Path(app_config.data_path) / protocol / contract_name / "logs"
    store: DatasetStore = create_dataset_store("parquet", base_path)

    protocol_info = get_protocol_info(chain_id, protocol)
    try:
        contract_info = next(c for c in protocol_info.contracts if c.name == contract_name)
    except StopIteration:
        raise ValueError(
            f"Contract {contract_name!r} not found in protocol {protocol!r} for chain {chain_id}"
        )

    await collect_logs(
        chain_id=chain_id,
        contract_info=contract_info,
        reader=reader,
        store=store,
    )


async def transactions_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect transactions for a specific contract."""
    reader: EvmReader = evm_reader_factory("web3", web3_config.rpc_url(chain_id))

    base_path = Path(app_config.data_path) / protocol / contract_name
    logs_store: DatasetStore = create_dataset_store("parquet", base_path / "logs")
    tx_store: DatasetStore = create_dataset_store("parquet", base_path / "transactions")

    protocol_info = get_protocol_info(chain_id, protocol)
    try:
        contract_info = next(c for c in protocol_info.contracts if c.name == contract_name)
    except StopIteration:
        raise ValueError(
            f"Contract {contract_name!r} not found in protocol {protocol!r} for chain {chain_id}"
        )

    await collect_transactions(
        chain_id=chain_id,
        contract_info=contract_info,
        reader=reader,
        logs_store=logs_store,
        tx_store=tx_store,
    )


async def receipts_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect receipts for a specific contract."""
    print("receipts_task called")

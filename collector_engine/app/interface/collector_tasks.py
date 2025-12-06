from pathlib import Path

from collector_engine.app.infrastructure.config.settings import app_config, web3_config
from collector_engine.app.domain.ports.out import EvmReader, DatasetStore
from collector_engine.app.infrastructure.factories.evm_reader_factory import evm_reader_factory
from collector_engine.app.infrastructure.factories.storage_factory import create_dataset_store
from collector_engine.app.infrastructure.registry.registry import get_protocol_info
from collector_engine.app.application.services.collect_logs import collect_logs


async def logs_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect logs for a specific contract."""
    client: EvmReader = evm_reader_factory("web3", web3_config.rpc_url(chain_id))  # noqa: F841

    base_path = Path(app_config.data_path) / protocol / contract_name
    store: DatasetStore = create_dataset_store("parquet", base_path)  # noqa: F841
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
        protocol=protocol,
        reader=client,
        store=store,
    )


async def transactions_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect transactions for a specific contract."""
    print("transactions_task called")


async def receipts_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect receipts for a specific contract."""
    print("receipts_task called")

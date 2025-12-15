from pathlib import Path

from collector_engine.app.infrastructure.config.settings import app_config, web3_config
from collector_engine.app.infrastructure.factories.evm_reader_factory import evm_reader_factory
from collector_engine.app.infrastructure.factories.storage_factory import create_dataset_store
from collector_engine.app.infrastructure.registry.registry import get_protocol_info

from collector_engine.app.application.services.run_pipeline import (
    PipelineConfig,
    PipelineDeps,
    run_pipeline,
)


async def pipeline_task(chain_id: int, protocol: str, contract_name: str) -> None:
    reader = evm_reader_factory("web3", web3_config.rpc_url(chain_id))

    base_path = Path(app_config.data_path) / protocol / contract_name
    logs_store = create_dataset_store("parquet", base_path / "logs")
    tx_store = create_dataset_store("parquet", base_path / "transactions")
    receipts_store = create_dataset_store("parquet", base_path / "receipts")

    protocol_info = get_protocol_info(chain_id, protocol)
    try:
        contract_info = next(c for c in protocol_info.contracts if c.name == contract_name)
    except StopIteration:
        raise ValueError(
            f"Contract {contract_name!r} not found in protocol {protocol!r} for chain {chain_id}"
        )

    cfg = PipelineConfig(chain_id=chain_id, protocol=protocol, contract_info=contract_info)
    deps = PipelineDeps(
        reader=reader,
        logs_store=logs_store,
        tx_store=tx_store,
        receipts_store=receipts_store,
    )

    await run_pipeline(cfg=cfg, deps=deps)

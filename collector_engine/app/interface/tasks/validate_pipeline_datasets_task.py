from __future__ import annotations

from pathlib import Path

from collector_engine.app.infrastructure.config.settings import app_config, web3_config
from collector_engine.app.infrastructure.factories.evm_reader_factory import evm_reader_factory
from collector_engine.app.infrastructure.factories.storage_factory import storage_factory
from collector_engine.app.infrastructure.registry.registry import get_protocol_info

from collector_engine.app.infrastructure.parquet.schema import LOG_SCHEMA, TX_SCHEMA, RECEIPT_SCHEMA

from collector_engine.app.application.services.validation.validate_pipeline_datasets import (
    validate_pipeline_datasets,
)


async def validate_pipeline_datasets_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """
    Validate logs/txs/receipts parquet sets for given (chain, protocol, contract).
    """
    # reader not required for validation, but kept consistent with other tasks:
    _ = evm_reader_factory("web3", web3_config.rpc_url(chain_id))

    base_path = Path(app_config.data_path) / protocol / contract_name
    logs_store = storage_factory("parquet", base_path / "logs")
    tx_store = storage_factory("parquet", base_path / "transactions")
    receipts_store = storage_factory("parquet", base_path / "receipts")

    protocol_info = get_protocol_info(chain_id, protocol)
    try:
        _ = next(c for c in protocol_info.contracts if c.name == contract_name)
    except StopIteration:
        raise ValueError(
            f"Contract {contract_name!r} not found in protocol {protocol!r} for chain {chain_id}"
        )

    report = await validate_pipeline_datasets(
        logs_store=logs_store,
        tx_store=tx_store,
        receipts_store=receipts_store,
        log_schema=LOG_SCHEMA,
        tx_schema=TX_SCHEMA,
        receipt_schema=RECEIPT_SCHEMA,
    )

    report.log_summary()

    # if not report.ok:
    #     # Non-zero failure semantics at task level (CLI can surface this)
    #     raise RuntimeError("Validation failed. See errors above.")

    # logger.info("Validation OK.")

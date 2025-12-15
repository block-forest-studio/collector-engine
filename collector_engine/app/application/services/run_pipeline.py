from dataclasses import dataclass
from loguru import logger

from collector_engine.app.infrastructure.registry.schemas import ContractInfo
from collector_engine.app.domain.ports.out import EvmReader, DatasetStore

from collector_engine.app.application.services.collect_logs import collect_logs
from collector_engine.app.application.services.collect_transactions import collect_transactions
from collector_engine.app.application.services.collect_receipts import collect_receipts


@dataclass(frozen=True)
class PipelineDeps:
    reader: EvmReader
    logs_store: DatasetStore
    tx_store: DatasetStore
    receipts_store: DatasetStore


@dataclass(frozen=True)
class PipelineConfig:
    chain_id: int
    protocol: str
    contract_info: ContractInfo


async def run_pipeline(*, cfg: PipelineConfig, deps: PipelineDeps) -> None:
    logger.info(
        "Pipeline start: chain_id={}, protocol={}, contract={}",
        cfg.chain_id,
        cfg.protocol,
        cfg.contract_info.name,
    )

    logger.info("Step 1/3: collect logs")
    await collect_logs(
        chain_id=cfg.chain_id,
        contract_info=cfg.contract_info,
        reader=deps.reader,
        store=deps.logs_store,
    )

    logger.info("Step 2/3: collect transactions")
    await collect_transactions(
        chain_id=cfg.chain_id,
        contract_info=cfg.contract_info,
        reader=deps.reader,
        logs_store=deps.logs_store,
        tx_store=deps.tx_store,
    )

    logger.info("Step 3/3: collect receipts")
    await collect_receipts(
        chain_id=cfg.chain_id,
        contract_info=cfg.contract_info,
        reader=deps.reader,
        tx_store=deps.tx_store,
        receipts_store=deps.receipts_store,
    )

    logger.info("Pipeline finished successfully.")

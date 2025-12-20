import pyarrow.compute as pc
from loguru import logger
from typing import Any
from collector_engine.app.infrastructure.registry.schemas import ContractInfo
from collector_engine.app.domain.ports.out import EvmReader, DatasetStore
from collector_engine.app.domain.pure.block_ranges import block_ranges
from collector_engine.app.domain.pure.logs import write_logs_to_buffer
from collector_engine.app.domain.pure.buffer_utils import rows_to_column_buffer
from collector_engine.app.infrastructure.parquet.schema import LOG_SCHEMA
from collector_engine.app.infrastructure.parquet.constants import ROWS_PER_FILE
from collector_engine.app.application.services.flush_buffer import flush_buffer


TO_BLOCK_IDX = 1


async def get_latest_block_from_store(store: DatasetStore) -> int | None:
    """Get latest block number from store."""
    f_names = store.list_names()

    if not f_names:
        return None

    sorted_names = sorted(
        f_names,
        key=lambda f: int(f.split("_")[TO_BLOCK_IDX].removesuffix(".parquet")),
    )
    table = store.read_table(sorted_names[-1])
    col = table["block_number"]
    max_scalar = pc.max(col)

    if max_scalar is None:
        return None

    return int(max_scalar.as_py())


async def collect_logs(
    *,
    chain_id: int,
    contract_info: ContractInfo,
    reader: EvmReader,
    store: DatasetStore,
    batch_size: int = 1000,
    rows_per_file: int = ROWS_PER_FILE,
) -> None:
    """Collect logs for a specific contract."""
    latest_stored_block = await get_latest_block_from_store(store)
    from_block = (  # noqa: F841
        contract_info.genesis_block if latest_stored_block is None else latest_stored_block + 1
    )
    to_block = await reader.latest_block_number()

    logger.info(
        "Starting logs collection for {} on chain {} (from_block={}, to_block={}, resume_from={})",
        contract_info.name,
        chain_id,
        from_block,
        to_block,
        latest_stored_block,
    )

    buffer: dict[str, list] = {name: [] for name in LOG_SCHEMA.names}

    for from_, to_ in block_ranges(from_block, to_block, batch_size):
        logs = await reader.get_logs(
            address=contract_info.address,
            from_block=from_,
            to_block=to_,
        )

        if not logs:
            logger.info("No logs in range [{} - {}], skipping", from_, to_)
            continue

        rows: list[dict[str, Any]] = write_logs_to_buffer(chain_id, list(logs))
        buffer = rows_to_column_buffer(rows, list(LOG_SCHEMA.names), buffer)

        buffer = flush_buffer(
            buffer=buffer,
            store=store,
            rows_per_file=rows_per_file,
            force=False,
            schema=LOG_SCHEMA,
            file_prefix="logs",
            block_field="block_number",
            index_field="log_index",
        )

    buffer = flush_buffer(
        buffer=buffer,
        store=store,
        rows_per_file=rows_per_file,
        force=True,
        schema=LOG_SCHEMA,
        file_prefix="logs",
        block_field="block_number",
        index_field="log_index",
    )

    logger.info(
        "Finished logs collection for {} on chain {}. Last block stored: {}",
        contract_info.name,
        chain_id,
        await get_latest_block_from_store(store),
    )

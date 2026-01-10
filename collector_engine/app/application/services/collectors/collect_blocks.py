import pyarrow.compute as pc
from loguru import logger
from typing import Any

from collector_engine.app.domain.ports.out import EvmReader, DatasetStore
from collector_engine.app.domain.pure.block_ranges import block_ranges
from collector_engine.app.domain.pure.blocks_timestamps import write_blocks_to_buffer
from collector_engine.app.domain.pure.buffer_utils import rows_to_column_buffer
from collector_engine.app.infrastructure.parquet.schema import BLOCK_SCHEMA
from collector_engine.app.infrastructure.parquet.constants import ROWS_PER_FILE
from collector_engine.app.application.services.flush_buffer import flush_buffer


TO_BLOCK_IDX = 1


async def get_latest_block_from_store(store: DatasetStore) -> int | None:
    """Get latest block number from Parquet store for blocks."""
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


async def collect_blocks(
    *,
    chain_id: int,
    reader: EvmReader,
    store: DatasetStore,
    batch_size: int = 1000,
    rows_per_file: int = ROWS_PER_FILE,
) -> None:
    """Collect blocks for a given chain into a Parquet dataset."""
    latest_stored_block = await get_latest_block_from_store(store)
    from_block = 0 if latest_stored_block is None else latest_stored_block + 1
    to_block = await reader.latest_block_number()

    logger.info(
        "Starting blocks collection for chain {} (from_block={}, to_block={}, resume_from={})",
        chain_id,
        from_block,
        to_block,
        latest_stored_block,
    )

    buffer: dict[str, list] = {name: [] for name in BLOCK_SCHEMA.names}

    for from_, to_ in block_ranges(from_block, to_block, batch_size):
        blocks = await reader.get_blocks_range(from_block=from_, to_block=to_)

        if not blocks:
            logger.info("No blocks in range [{} - {}], skipping", from_, to_)
            continue

        rows: list[dict[str, Any]] = write_blocks_to_buffer(chain_id, list(blocks))
        buffer = rows_to_column_buffer(rows, list(BLOCK_SCHEMA.names), buffer)
        logger.info("Collected blocks in range [{} - {}]", from_, to_)
        buffer = flush_buffer(
            buffer=buffer,
            store=store,
            rows_per_file=rows_per_file,
            force=False,
            schema=BLOCK_SCHEMA,
            file_prefix="blocks",
            block_field="block_number",
            index_field="block_number",
        )

    buffer = flush_buffer(
        buffer=buffer,
        store=store,
        rows_per_file=rows_per_file,
        force=True,
        schema=BLOCK_SCHEMA,
        file_prefix="blocks",
        block_field="block_number",
        index_field="block_number",
    )

    logger.info(
        "Finished blocks collection for chain {}. Last block stored: {}",
        chain_id,
        await get_latest_block_from_store(store),
    )

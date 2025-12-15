from typing import Any
from loguru import logger
import pyarrow.compute as pc

from collector_engine.app.infrastructure.registry.schemas import ContractInfo
from collector_engine.app.domain.ports.out import EvmReader, DatasetStore
from collector_engine.app.infrastructure.parquet.constants import ROWS_PER_FILE
from collector_engine.app.infrastructure.parquet.schema import RECEIPT_SCHEMA
from collector_engine.app.domain.pure.buffer_utils import rows_to_column_buffer
from collector_engine.app.domain.pure.receipts import write_receipts_to_buffer
from collector_engine.app.application.services.flush_buffer import flush_buffer


def _get_tx_files_to_process(
    tx_names: list[str],
    receipt_names: list[str],
) -> list[str]:
    """
    Returns a list of txs_*.parquet files for which there are no corresponding
    receipts_*.parquet.
    """
    processed_suffixes = {
        name.removeprefix("receipts_").removesuffix(".parquet")
        for name in receipt_names
        if name.endswith(".parquet")
    }

    return [
        name
        for name in tx_names
        if name.endswith(".parquet")
        and name.removeprefix("txs_").removesuffix(".parquet") not in processed_suffixes
    ]


async def collect_receipts(
    *,
    chain_id: int,
    contract_info: ContractInfo,
    reader: EvmReader,
    tx_store: DatasetStore,
    receipts_store: DatasetStore,
    batch_size: int = 100,
    rows_per_file: int = ROWS_PER_FILE,
) -> None:
    """
    For collected txs_*.parquet files, fetch corresponding receipts and store them.
    """
    logger.info(
        "Starting receipts collection for {} on chain {}",
        contract_info.name,
        chain_id,
    )

    tx_files = tx_store.list_names()
    receipt_files = receipts_store.list_names()

    tx_names = [n for n in tx_files if n.startswith("txs_") and n.endswith(".parquet")]
    receipt_names = [
        n for n in receipt_files if n.startswith("receipts_") and n.endswith(".parquet")
    ]

    if not tx_names:
        logger.warning(
            "No transaction parquet files found for contract {} on chain {}. "
            "Run transactions_task first.",
            contract_info.name,
            chain_id,
        )
        return

    files_to_process = _get_tx_files_to_process(tx_names, receipt_names)

    if not files_to_process:
        logger.info(
            "All receipts already collected for {} on chain {}. Nothing to do.",
            contract_info.name,
            chain_id,
        )
        return

    logger.info(
        "Found {} tx parquet files, {} receipts parquet files, {} files to process.",
        len(tx_names),
        len(receipt_names),
        len(files_to_process),
    )

    buffer: dict[str, list] = {name: [] for name in RECEIPT_SCHEMA.names}

    for name in sorted(files_to_process):
        logger.info("Processing tx parquet file: {}", name)

        table = tx_store.read_table(name)  # pyarrow.Table

        hashes_col = table["hash"]
        unique_hashes_arr = pc.unique(hashes_col)
        hashes: list[bytes] = [val.as_py() for val in unique_hashes_arr]

        logger.info(
            "File {} has {} unique transaction hashes, batch_size={}",
            name,
            len(hashes),
            batch_size,
        )

        _hashes_len = len(hashes)
        for i in range(0, len(hashes), batch_size):
            chunk_hashes = hashes[i : i + batch_size]
            receipts = await reader.get_receipts(chunk_hashes)

            if not receipts:
                continue

            rows: list[dict[str, Any]] = write_receipts_to_buffer(chain_id, list(receipts))
            buffer = rows_to_column_buffer(rows, list(RECEIPT_SCHEMA.names), buffer)

            _hashes_len = _hashes_len - len(chunk_hashes)
            logger.info("Chunk processed, left {} hashes for file {}.", _hashes_len, name)

            buffer = flush_buffer(
                buffer=buffer,
                store=receipts_store,
                rows_per_file=rows_per_file,
                force=False,
                schema=RECEIPT_SCHEMA,
                file_prefix="receipts",
                block_field="block_number",
                index_field="transaction_index",
            )

        buffer = flush_buffer(
            buffer=buffer,
            store=receipts_store,
            rows_per_file=rows_per_file,
            force=True,
            schema=RECEIPT_SCHEMA,
            file_prefix="receipts",
            block_field="block_number",
            index_field="transaction_index",
        )

    logger.info(
        "Finished receipts collection for {} on chain {}.",
        contract_info.name,
        chain_id,
    )

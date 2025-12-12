import pyarrow.compute as pc
import pyarrow as pa
from loguru import logger
from typing import Any
from collector_engine.app.infrastructure.registry.schemas import ContractInfo
from collector_engine.app.domain.ports.out import EvmReader, DatasetStore
from collector_engine.app.infrastructure.parquet.constants import ROWS_PER_FILE
from collector_engine.app.infrastructure.parquet.schema import TX_SCHEMA
from collector_engine.app.domain.pure.transactions import write_transactions_to_buffer
from collector_engine.app.domain.pure.buffer_utils import rows_to_column_buffer
from collector_engine.app.application.services.flush_buffer import flush_buffer


def _get_logs_files_to_processing(
    logs_names: list[str],
    tx_names: list[str],
) -> list[str]:
    """
    Return list of logs_*.parquet files for which there is not yet a corresponding txs_*.parquet file.
    We assume the names:
      - logs_FROM_TO.parquet
      - txs_FROM_TO.parquet
    """
    tx_suffixes = {name.removeprefix("txs_").removesuffix(".parquet") for name in tx_names}

    return [
        name
        for name in logs_names
        if name.removeprefix("logs_").removesuffix(".parquet") not in tx_suffixes
    ]


def _extract_unique_tx_hashes(table: pa.Table) -> list[bytes]:
    """
    Extracts unique transaction_hash as list[bytes] from logs_*.parquet table.
    We assume that 'transaction_hash' column is binary/fixed_size_binary.
    """

    col = table["transaction_hash"]
    unique = pc.unique(col)  # -> ChunkedArray
    return [bytes(v) for v in unique.to_pylist()]


async def collect_transactions(
    *,
    chain_id: int,
    contract_info: ContractInfo,
    reader: EvmReader,
    logs_store: DatasetStore,
    tx_store: DatasetStore,
    batch_size: int = 100,
    rows_per_file: int = ROWS_PER_FILE,
) -> None:
    """
    For collected logs_*.parquet files, fetch corresponding transactions and store them.
    """

    log_files = logs_store.list_names()
    tx_files = tx_store.list_names()

    if not log_files:
        logger.warning(
            "No logs found for contract {} on chain-id {}. "
            "Please run the logs collector first. Skipping transactions collection.",
            contract_info.name,
            chain_id,
        )
        return

    pq_names_for_processing = _get_logs_files_to_processing(log_files, tx_files)

    if not pq_names_for_processing:
        logger.info(
            "All logs for contract {} on chain-id {} "
            "have already been processed into transactions. Nothing to do.",
            contract_info.name,
            chain_id,
        )
        return

    logger.info(
        "Starting transactions collection for {} on chain {}. " "Files to process: {}",
        contract_info.name,
        chain_id,
        pq_names_for_processing,
    )

    buffer: dict[str, list] = {name: [] for name in TX_SCHEMA.names}

    for name in pq_names_for_processing:
        logger.info(
            "Processing transactions for logs file: {} (contract={}, chain_id={})",
            name,
            contract_info.name,
            chain_id,
        )

        table = logs_store.read_table(name)
        hashes = _extract_unique_tx_hashes(table)

        if not hashes:
            logger.info("No transaction hashes in logs file {}, skipping", name)
            continue

        _hashes_len = len(hashes)
        for i in range(0, len(hashes), batch_size):
            chunk_hashes = hashes[i : i + batch_size]

            txs = await reader.get_transactions(chunk_hashes)

            if not txs:
                continue

            rows: list[dict[str, Any]] = write_transactions_to_buffer(chain_id, list(txs))
            buffer = rows_to_column_buffer(rows, list(TX_SCHEMA.names), buffer)

            _hashes_len = _hashes_len - len(chunk_hashes)
            logger.info("Chunk processed, left {} hashes for file {}.", _hashes_len, name)

            buffer = flush_buffer(
                buffer=buffer,
                store=tx_store,
                rows_per_file=rows_per_file,
                force=False,
                schema=TX_SCHEMA,
                file_prefix="txs",
                block_field="block_number",
                index_field="transaction_index",
            )

        buffer = flush_buffer(
            buffer=buffer,
            store=tx_store,
            rows_per_file=rows_per_file,
            force=True,
            schema=TX_SCHEMA,
            file_prefix="txs",
            block_field="block_number",
            index_field="transaction_index",
        )

    logger.info(
        "Finished transactions collection for {} on chain {}.",
        contract_info.name,
        chain_id,
    )

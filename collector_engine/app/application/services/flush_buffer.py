from typing import Any, Optional
from loguru import logger

from collector_engine.app.domain.ports.out import DatasetStore


def flush_buffer(
    *,
    buffer: dict[str, list],
    store: DatasetStore,
    rows_per_file: int,
    force: bool,
    schema: Any,
    file_prefix: str,  # "logs" / "txs" / "receipts"
    block_field: str,
    index_field: Optional[str] = None,  # "log_index", "transaction_index" etc.
) -> dict[str, list]:
    """
    buffer flush:
    - if buffer is empty → does nothing,
    - if buffer has fewer than rows_per_file and force=False → does nothing,
    - if buffer has >= rows_per_file or force=True:
        - sorts by block_field (+ index_field if present),
        - writes to file: {file_prefix}_{min_block}_{max_block}.parquet
        - returns new buffer (from store adapter).
    """
    rows = len(buffer.get(block_field, []))
    if rows == 0:
        return buffer

    should_flush = force or rows >= rows_per_file
    if not should_flush:
        return buffer

    if index_field is not None:
        order = sorted(
            range(rows),
            key=lambda i: (buffer[block_field][i], buffer[index_field][i]),
        )
    else:
        order = sorted(range(rows), key=lambda i: buffer[block_field][i])

    buffer = {k: [buffer[k][i] for i in order] for k in buffer}

    file_name = f"{file_prefix}_{buffer[block_field][0]}_{buffer[block_field][-1]}"

    logger.info(
        "Writing {} parquet file: {} (rows: {}, force={})",
        file_prefix,
        file_name,
        rows,
        force,
    )

    return store.write_buffer(
        buffer=buffer,
        schema=schema,
        file_name=file_name,
        rows_per_file=rows_per_file,
        force=force,
    )

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from typing import List
from pathlib import Path


def write_parquet(file_path: str, rows: list, schema: pa.Schema) -> None:
    if not rows:
        return
    batch = pa.record_batch(rows, schema=schema)
    pq.write_table(
        pa.Table.from_batches([batch]),
        file_path,
        compression="zstd",
        data_page_size=1 << 20,  # 1MB pages
    )


def write_and_flush_if_needed(
    buffer: dict[str, list],
    schema: pa.Schema,
    pq_path: Path,
    file_name: str,
    rows_per_file: int,
    force: bool = False,
) -> dict[str, list]:
    rows_in_buffer = len(buffer["block_number"])
    if rows_in_buffer == 0:
        return buffer
    if rows_in_buffer >= rows_per_file or force:
        if not pq_path.exists():
            create_path_if_not_exist(pq_path)
        file_path = pq_path / f"{file_name}.parquet"
        rows = [pa.array(buffer[name], type=schema.field(name).type) for name in schema.names]
        write_parquet(str(file_path), rows, schema)
        return {name: [] for name in schema.names}
    return buffer


def merge_parquet_files(
    temp_files_path: Path,
    temp_files_lst: list[str],
    final_file_path: Path,
    final_file_name: str,
    batch_size: int = 100_000,
) -> None:
    """Merge multiple parquet files into a single parquet file."""
    writer = None
    try:
        for f in temp_files_lst:
            pf = pq.ParquetFile(temp_files_path / f"{f}.parquet")
            for batch in pf.iter_batches(batch_size=batch_size):
                if writer is None:
                    writer = pq.ParquetWriter(
                        final_file_path / f"{final_file_name}.parquet", batch.schema
                    )
                writer.write_batch(batch)
    finally:
        if writer is not None:
            writer.close()


def get_pq_names(pq_path: Path) -> List[str]:
    if not pq_path or not pq_path.exists() or not pq_path.is_dir():
        return []
    try:
        return os.listdir(pq_path)
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
        return []


def create_path_if_not_exist(path: Path) -> None:
    """Create directory if not exist."""
    path.mkdir(parents=True, exist_ok=True)


def read_parquet(pq_path: Path, file_name: str) -> pd.DataFrame:
    return pd.read_parquet(pq_path / file_name)

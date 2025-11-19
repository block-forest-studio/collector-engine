from __future__ import annotations

# shell/adapters/storage/parquet_store.py
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from collector_engine.app.shell.helpers.parquet import get_pq_names, write_and_flush_if_needed


class ParquetDatasetStore:
    """
    DatasetStore implementation backed by Parquet files.

    - list_names: list parquet files in a directory
    - read_table: read a parquet file as a pyarrow.Table (or pandas if you prefer)
    - write_buffer: use your existing buffered writer (write_and_flush_if_needed)
    """

    def __init__(self, base_path: str | Path):
        self.base_path = Path(base_path)

    def list_names(self) -> list[str]:
        names = get_pq_names(self.base_path)
        return [n for n in names if n.endswith(".parquet")]

    def read_table(self, name: str) -> pa.Table:
        return pq.read_table(self.base_path / name)

    def write_buffer(
        self,
        *,
        buffer: dict[str, list],
        schema: pa.Schema,
        file_name: str,
        rows_per_file: int,
        force: bool = False,
    ) -> dict[str, list]:
        return write_and_flush_if_needed(
            buffer=buffer,
            schema=schema,
            pq_path=self.base_path,
            file_name=file_name,
            rows_per_file=rows_per_file,
            force=force,
        )

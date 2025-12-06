from typing import Callable, Dict
from pathlib import Path

from collector_engine.app.domain.ports.out import DatasetStore
from collector_engine.app.infrastructure.adapters.storage.parquet_store import ParquetDatasetStore

DatasetStoreFactory = Callable[[str | Path], DatasetStore]

_STORAGE_REGISTRY: Dict[str, DatasetStoreFactory] = {
    "parquet": lambda base_path: ParquetDatasetStore(base_path),
    # "csv": lambda base_path: CsvDatasetStore(base_path),
    # "sql": lambda base_path: SqlDatasetStore(dsn, base_path)  # if needed
}


def create_dataset_store(backend: str, base_path: str | Path) -> DatasetStore:
    try:
        factory = _STORAGE_REGISTRY[backend]
    except KeyError:
        raise ValueError(f"Unsupported storage backend: {backend}")
    return factory(base_path)

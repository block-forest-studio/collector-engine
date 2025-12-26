from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from collector_engine.app.domain.ports.out import DatasetLoader


@dataclass(frozen=True)
class LoadToSqlConfig:
    base_path: Path  # .../data/<protocol>/<contract>/


def load_to_sql(*, cfg: LoadToSqlConfig, loader: DatasetLoader) -> None:
    loader.load_parquet_dir(
        parquet_dir=cfg.base_path / "logs",
        dataset="logs",
        file_prefix="logs_",
    )
    loader.load_parquet_dir(
        parquet_dir=cfg.base_path / "transactions",
        dataset="txs",
        file_prefix="txs_",
    )
    loader.load_parquet_dir(
        parquet_dir=cfg.base_path / "receipts",
        dataset="receipts",
        file_prefix="receipts_",
    )

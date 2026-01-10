from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from collector_engine.app.domain.ports.out import DatasetLoader


@dataclass(frozen=True)
class LoadContractScopedToSqlConfig:
    # .../data/<protocol>/<contract>/
    contract_base_path: Path


def load_contract_scoped_data_to_sql(
    *,
    cfg: LoadContractScopedToSqlConfig,
    loader: DatasetLoader,
) -> None:
    loader.load_parquet_dir(
        parquet_dir=cfg.contract_base_path / "logs",
        dataset="logs",
        file_prefix="logs_",
    )
    loader.load_parquet_dir(
        parquet_dir=cfg.contract_base_path / "transactions",
        dataset="txs",
        file_prefix="txs_",
    )
    loader.load_parquet_dir(
        parquet_dir=cfg.contract_base_path / "receipts",
        dataset="receipts",
        file_prefix="receipts_",
    )

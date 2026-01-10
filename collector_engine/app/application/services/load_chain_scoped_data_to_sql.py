from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from collector_engine.app.domain.ports.out import DatasetLoader


@dataclass(frozen=True)
class LoadChainScopedToSqlConfig:
    # .../data/chain/<chain_id>/
    base_path: Path
    chain_id: int


def load_chain_scoped_data_to_sql(
    *,
    cfg: LoadChainScopedToSqlConfig,
    loader: DatasetLoader,
) -> None:
    loader.load_parquet_dir(
        parquet_dir=cfg.base_path / "blocks",
        dataset="blocks",
        file_prefix="blocks_",
    )

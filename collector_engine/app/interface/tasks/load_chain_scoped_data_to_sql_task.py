from pathlib import Path

from collector_engine.app.application.services.load_chain_scoped_data_to_sql import (
    LoadChainScopedToSqlConfig,
    load_chain_scoped_data_to_sql,
)
from collector_engine.app.infrastructure.config.settings import app_config
from collector_engine.app.infrastructure.factories.loader_factory import loader_factory


async def load_chain_scoped_data_to_sql_task(
    chain_id: int, protocol: str, contract_name: str
) -> None:
    base_path = Path(app_config.data_path) / "chain" / str(chain_id)

    loader = loader_factory("postgres_copy")
    cfg = LoadChainScopedToSqlConfig(base_path=base_path, chain_id=chain_id)
    load_chain_scoped_data_to_sql(cfg=cfg, loader=loader)

from pathlib import Path

from collector_engine.app.infrastructure.config.settings import app_config
from collector_engine.app.infrastructure.factories.loader_factory import loader_factory
from collector_engine.app.application.services.load_to_sql import LoadToSqlConfig, load_to_sql


async def load_to_sql_task(chain_id: int, protocol: str, contract_name: str) -> None:
    base_path = Path(app_config.data_path) / protocol / contract_name

    loader = loader_factory("postgres_copy")
    cfg = LoadToSqlConfig(base_path=base_path)

    load_to_sql(cfg=cfg, loader=loader)

from collector_engine.app.domain.ports.out import DatasetLoader
from collector_engine.app.infrastructure.adapters.db.postgres_copy_loader import PostgresCopyLoader
from collector_engine.app.infrastructure.config.settings import app_config


def loader_factory(kind: str = "postgres_copy") -> DatasetLoader:
    if kind == "postgres_copy":
        return PostgresCopyLoader(app_config.postgres_dsn)
    raise ValueError(f"Unknown loader kind: {kind!r}")

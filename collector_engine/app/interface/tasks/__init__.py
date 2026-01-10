from __future__ import annotations

from collections.abc import Awaitable, Callable

from collector_engine.app.interface.tasks.load_chain_scoped_data_to_sql_task import (
    load_chain_scoped_data_to_sql_task,
)
from collector_engine.app.interface.tasks.load_contract_scoped_data_to_sql_task import (
    load_contract_scoped_data_to_sql_task,
)

from .logs_task import logs_task
from .transactions_task import transactions_task
from .receipts_task import receipts_task
from .pipeline_task import pipeline_task
from .validate_pipeline_datasets_task import validate_pipeline_datasets_task
from .blocks_task import blocks_task

TaskFn = Callable[[int, str, str], Awaitable[None]]

TASKS: dict[str, TaskFn] = {
    "logs_task": logs_task,
    "transactions_task": transactions_task,
    "receipts_task": receipts_task,
    "pipeline_task": pipeline_task,
    "validate_pipeline_datasets_task": validate_pipeline_datasets_task,
    "load_contract_scoped_data_to_sql_task": load_contract_scoped_data_to_sql_task,
    "load_chain_scoped_data_to_sql_task": load_chain_scoped_data_to_sql_task,
    "blocks_task": blocks_task,
}

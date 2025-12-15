from __future__ import annotations

from collections.abc import Awaitable, Callable

from .logs_task import logs_task
from .transactions_task import transactions_task
from .receipts_task import receipts_task
from .pipeline_task import pipeline_task

TaskFn = Callable[[int, str, str], Awaitable[None]]

TASKS: dict[str, TaskFn] = {
    "logs_task": logs_task,
    "transactions_task": transactions_task,
    "receipts_task": receipts_task,
    "pipeline_task": pipeline_task,
}

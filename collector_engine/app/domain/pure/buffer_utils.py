from collections.abc import Callable, Iterable
from typing import TypeVar, Any

T = TypeVar("T")
Row = dict[str, Any]


def to_buffer(chain_id: int, items: Iterable[T], to_row: Callable[[int, T], Row]) -> list[Row]:
    return [to_row(chain_id, item) for item in items]


def rows_to_column_buffer(
    rows: Iterable[dict[str, Any]],
    columns: list[str],
    buffer: dict[str, list] | None = None,
) -> dict[str, list]:
    if buffer is None:
        buffer = {c: [] for c in columns}
    for row in rows:
        for col in columns:
            buffer[col].append(row[col])
    return buffer

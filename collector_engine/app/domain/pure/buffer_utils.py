from collections.abc import Callable, Iterable
from typing import TypeVar, Any

T = TypeVar("T")
Row = dict[str, Any]


def to_buffer(chain_id: int, items: Iterable[T], to_row: Callable[[int, T], Row]) -> list[Row]:
    return [to_row(chain_id, item) for item in items]

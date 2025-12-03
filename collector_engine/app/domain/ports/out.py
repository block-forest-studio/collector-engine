# 🛳
from typing import Protocol, Iterable, Sequence, Mapping, Any


class EvmReader(Protocol):
    async def latest_block_number(self) -> int: ...
    async def get_logs(
        self, *, address: bytes, from_block: int, to_block: int
    ) -> Sequence[Mapping[str, Any]]: ...
    async def get_transactions(self, hashes: Iterable[bytes]) -> Sequence[Mapping[str, Any]]: ...
    async def get_receipts(self, hashes: Iterable[bytes]) -> Sequence[Mapping[str, Any]]: ...


class DatasetStore(Protocol):
    def list_names(self) -> list[str]: ...
    def read_table(self, name: str) -> Any: ...
    def write_buffer(
        self,
        *,
        buffer: dict[str, list],
        schema: Any,
        path: str,
        file_name: str,
        force: bool = False,
    ) -> dict[str, list]: ...

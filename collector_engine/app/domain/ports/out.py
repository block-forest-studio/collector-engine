# 🛳
from typing import Protocol, Iterable, Sequence, Mapping, Any
from web3.types import LogReceipt


class EvmReader(Protocol):
    async def latest_block_number(self) -> int: ...
    async def get_logs(
        self, *, address: bytes, from_block: int, to_block: int
    ) -> Sequence[LogReceipt]: ...
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
        file_name: str,
        rows_per_file: int,
        force: bool = False,
    ) -> dict[str, list]: ...

# 🛳
from __future__ import annotations

from pathlib import Path
from typing import Protocol, Iterable, Sequence, Any, Literal
from web3.types import LogReceipt, TxReceipt, TxData


class EvmReader(Protocol):
    async def latest_block_number(self) -> int: ...
    async def get_logs(
        self, *, address: bytes, from_block: int, to_block: int
    ) -> Sequence[LogReceipt]: ...
    async def get_transactions(self, hashes: Iterable[bytes]) -> Sequence[TxData]: ...
    async def get_receipts(self, hashes: Iterable[bytes]) -> Sequence[TxReceipt]: ...


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


DatasetName = Literal["logs", "txs", "receipts"]


class DatasetLoader(Protocol):
    def load_parquet_dir(
        self,
        *,
        parquet_dir: Path,
        dataset: DatasetName,
        file_prefix: str,
    ) -> None: ...

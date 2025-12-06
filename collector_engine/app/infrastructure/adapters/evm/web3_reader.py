from __future__ import annotations
from typing import Iterable, Sequence, Callable, Awaitable
import asyncio
from web3 import AsyncWeb3
from web3.types import LogReceipt


class Web3EvmReader:
    def __init__(
        self, provider_url: str, *, max_concurrency: int = 16, request_timeout: float = 30.0
    ):
        self.w3 = AsyncWeb3(
            AsyncWeb3.AsyncHTTPProvider(provider_url, request_kwargs={"timeout": request_timeout})
        )
        self._sem = asyncio.Semaphore(max_concurrency)

    async def _lim(self, coro: Awaitable[dict]) -> dict:
        async with self._sem:
            return await coro

    @staticmethod
    async def _one(h: bytes, func: Callable[[str], Awaitable[dict]]) -> dict:
        return await func("0x" + h.hex())

    async def latest_block_number(self) -> int:
        blk = await self.w3.eth.get_block("latest")
        return int(blk.number)  # type: ignore[attr-defined]

    async def get_logs(
        self, *, address: bytes, from_block: int, to_block: int
    ) -> Sequence[LogReceipt]:
        addr_hex = "0x" + address.hex()
        checksum_addr = self.w3.to_checksum_address(addr_hex)
        return await self.w3.eth.get_logs(
            {
                "fromBlock": from_block,
                "toBlock": to_block,
                "address": checksum_addr,
            }
        )

    async def get_transactions(self, hashes: Iterable[bytes]) -> Sequence[dict]:
        coros = [self._lim(self._one(h, self.w3.eth.get_transaction)) for h in hashes]  # type: ignore[arg-type]
        return await asyncio.gather(*coros)

    async def get_receipts(self, hashes: Iterable[bytes]) -> Sequence[dict]:
        coros = [self._lim(self._one(h, self.w3.eth.get_transaction_receipt)) for h in hashes]  # type: ignore[arg-type]
        return await asyncio.gather(*coros)

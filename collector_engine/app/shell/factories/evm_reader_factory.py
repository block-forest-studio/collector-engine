from __future__ import annotations

from typing import Callable, Dict

from collector_engine.app.core.ports import EvmReader
from collector_engine.app.shell.adapters.evm.web3_reader import Web3EvmReader
# from .jsonrpc_reader import JsonRpcEvmReader  # maybe later
# from .fake_reader import FakeEvmReader       # for tests

EvmReaderFactory = Callable[[str], EvmReader]

_EVM_READER_REGISTRY: Dict[str, EvmReaderFactory] = {
    "web3": lambda url: Web3EvmReader(url),
    # "jsonrpc": lambda url: JsonRpcEvmReader(url),
    # "fake": lambda url: FakeEvmReader(),
}


def create_evm_reader(backend: str, provider_url: str) -> EvmReader:
    try:
        factory = _EVM_READER_REGISTRY[backend]
    except KeyError:
        raise ValueError(f"Unsupported EVM reader backend: {backend!r}")
    return factory(provider_url)

from collector_engine.app.config.settings import web3_config
from collector_engine.app.core.ports import EvmReader
from collector_engine.app.shell.adapters.evm.web3_reader import Web3EvmReader


async def logs_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect logs for a specific contract."""
    client: EvmReader = Web3EvmReader(  # noqa: F841
        provider_url=web3_config.rpc_url(chain_id),
        max_concurrency=web3_config.client_max_concurrency,
        request_timeout=web3_config.client_request_timeout,
    )


async def transactions_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect transactions for a specific contract."""
    print("transactions_task called")


async def receipts_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect receipts for a specific contract."""
    print("receipts_task called")

async def logs_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect logs for a specific contract."""
    print("logs_task called")


async def transactions_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect transactions for a specific contract."""
    print("transactions_task called")


async def receipts_task(chain_id: int, protocol: str, contract_name: str) -> None:
    """Collect receipts for a specific contract."""
    print("receipts_task called")

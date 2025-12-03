from collector_engine.app.infrastructure.registry.schemas import ContractInfo
from collector_engine.app.domain.ports.out import EvmReader, DatasetStore


TO_BLOCK_IDX = 1


async def get_latest_block_from_store(store: DatasetStore) -> int | None:
    """Get latest block number from store."""
    f_names = store.list_names()

    if f_names is None:
        return None

    sorted_names = sorted(
        f_names, key=lambda f: f.split("_")[TO_BLOCK_IDX].removesuffix(".parquet")
    )
    df = store.read_table(sorted_names[-1])
    return int(df["block_number"].max())


async def collect_logs(
    *,
    chain_id: int,
    contract_info: ContractInfo,
    protocol: str,
    reader: EvmReader,
    store: DatasetStore,
    batch_size: int = 1000,
) -> None:
    """Collect logs for a specific contract."""
    latest_stored_block = await get_latest_block_from_store(store)
    from_block = (  # noqa: F841
        contract_info.genesis_block if latest_stored_block is None else latest_stored_block + 1
    )

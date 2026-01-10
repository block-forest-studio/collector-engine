# collector_engine/app/domain/pure/blocks.py
from __future__ import annotations

from typing import Any

from web3.types import BlockData

from collector_engine.app.domain.pure.buffer_utils import to_buffer
from collector_engine.app.domain.pure.bytes_utils import (
    b32_validate,
    to_bytes,
)


def block_to_row(chain_id: int, block: BlockData) -> dict[str, Any]:
    number = int(block["number"])
    timestamp = int(block["timestamp"])

    block_hash = b32_validate(
        to_bytes(block["hash"]),
        "block_hash",
    )
    parent_hash = b32_validate(
        to_bytes(block["parentHash"]),
        "parent_hash",
    )

    gas_used = int(block.get("gasUsed", 0))
    gas_limit = int(block.get("gasLimit", 0))

    base_fee_raw = block.get("baseFeePerGas")
    base_fee_per_gas = int(base_fee_raw) if base_fee_raw is not None else None

    txs = block.get("transactions", [])
    tx_count = len(txs)

    return {
        "chain_id": int(chain_id),
        "block_number": number,
        "block_hash": block_hash,
        "parent_hash": parent_hash,
        "timestamp": timestamp,
        "base_fee_per_gas": base_fee_per_gas,
        "gas_used": gas_used,
        "gas_limit": gas_limit,
        "tx_count": tx_count,
    }


def write_blocks_to_buffer(chain_id: int, blocks: list[BlockData]) -> list[dict[str, Any]]:
    return to_buffer(chain_id, blocks, block_to_row)

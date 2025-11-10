from typing import Any
from web3.types import TxData
from collector_engine.app.collectors.core.buffer_utils import to_buffer
from collector_engine.app.collectors.core.bytes_utils import (
    b20_validate,
    b32_validate,
    to_bytes,
)


def _is_unmined(tx: TxData) -> bool:
    return tx.get("blockHash") is None


def _normalize_access_list(access_list: list) -> list[dict[str, Any]]:
    if not access_list:
        return []
    out = []
    for entry in access_list:
        addr_bytes = b20_validate(to_bytes(entry["address"]), "access_list.address")
        storage_keys = entry.get("storageKeys") or []
        storage_keys_bytes = [
            b32_validate(to_bytes(k), "access_list.storage_keys") for k in storage_keys
        ]
        out.append(
            {
                "address": addr_bytes,
                "storage_keys": storage_keys_bytes,
            }
        )
    return out


def transaction_to_row(chain_id: int, tx: TxData, include_unmined: bool = False) -> dict[str, Any]:
    if _is_unmined(tx) and not include_unmined:
        raise ValueError(
            "Unmined (pending) transaction is not allowed when include_unmined is False"
        )

    raw_block_hash = tx.get("blockHash")
    raw_block_number = tx.get("blockNumber")
    raw_tx_index = tx.get("transactionIndex")

    return {
        "chain_id": chain_id,
        "block_hash": (
            b32_validate(to_bytes(raw_block_hash), "block_hash")
            if raw_block_hash is not None
            else None
        ),
        "block_number": int(raw_block_number) if raw_block_number is not None else None,
        "transaction_index": int(raw_tx_index) if raw_tx_index is not None else None,
        "from": b20_validate(to_bytes(tx["from"]), "from"),
        "gas": int(tx["gas"]),
        "gas_price": int(tx["gasPrice"])
        if "gasPrice" in tx and tx["gasPrice"] is not None
        else None,
        "max_fee_per_gas": int(tx["maxFeePerGas"]) if tx.get("maxFeePerGas") else None,
        "max_priority_fee_per_gas": int(tx["maxPriorityFeePerGas"])
        if tx.get("maxPriorityFeePerGas")
        else None,
        "hash": b32_validate(to_bytes(tx["hash"]), "hash"),
        "input": to_bytes(tx["input"]),
        "nonce": int(tx["nonce"]),
        "to": b20_validate(to_bytes(tx.get("to")), "to") if tx.get("to") else None,
        "value": int(tx["value"]),
        "type": int(tx["type"]) if "type" in tx and tx["type"] is not None else None,
        "v": int(tx["v"]),
        "r": b32_validate(to_bytes(tx["r"]), "r"),
        "s": b32_validate(to_bytes(tx["s"]), "s"),
        "y_parity": int(tx["yParity"]) if "yParity" in tx and tx["yParity"] is not None else None,
        "access_list": _normalize_access_list(tx.get("accessList", [])),
    }


def write_transactions_to_buffer(chain_id: int, transactions: list[TxData]) -> list[dict[str, Any]]:
    return to_buffer(chain_id, transactions, transaction_to_row)

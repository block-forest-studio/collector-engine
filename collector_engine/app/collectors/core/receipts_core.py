from typing import Any
from web3.types import TxReceipt, LogReceipt
from collector_engine.app.collectors.core.buffer_utils import to_buffer
from collector_engine.app.collectors.core.bytes_utils import (
    b20_validate,
    b32_validate,
    b256_validate,
    to_bytes,
)


def normalize_logs(logs: list[LogReceipt] | None) -> list[dict]:
    """
    Converts web3.py receipt['logs'] into a list of structures matching the schema:
    - struct<address: binary(20)
    - block_hash: binary(32)
    - block_number: int64
    - block_timestamp: int64? (optional)
    - data: binary
    - log_index: int32
    - removed: bool
    - topics: list<binary>
    - transaction_hash: binary(32)
    - transaction_index: int32>
    """
    result = []
    for lg in logs or []:
        # blockTimestamp sometimes is added by some providers (optional)
        block_ts = lg.get("blockTimestamp")
        if isinstance(block_ts, str) and block_ts.startswith("0x"):
            try:
                block_ts = int(block_ts, 16)
            except Exception:
                block_ts = None
        elif isinstance(block_ts, int):
            pass
        else:
            block_ts = None

        result.append(
            {
                "address": b20_validate(to_bytes(lg["address"]), "address"),
                "block_hash": b32_validate(to_bytes(lg["blockHash"]), "block_hash"),
                "block_number": int(lg["blockNumber"]),
                "block_timestamp": block_ts,  # nullable
                "data": to_bytes(lg["data"]),
                "log_index": int(lg["logIndex"]),
                "removed": bool(lg.get("removed", False)),
                "topics": [to_bytes(t) for t in (lg.get("topics") or [])],  # list<binary()>
                "transaction_hash": b32_validate(
                    to_bytes(lg["transactionHash"]), "transaction_hash"
                ),
                "transaction_index": int(lg["transactionIndex"]),
            }
        )
    return result


def receipt_to_row(chain_id: int, receipt: TxReceipt) -> dict[str, Any]:
    # type (0/1/2), can be string "0x2" — unify to int / None
    rtype = receipt.get("type")
    if isinstance(rtype, str) and rtype.startswith("0x"):
        rtype = int(rtype, 16)

    egp = receipt.get("effectiveGasPrice")
    if isinstance(egp, str) and egp.startswith("0x"):
        egp = int(egp, 16)

    lb = receipt.get("logsBloom")

    return {
        "chain_id": chain_id,
        "block_hash": b32_validate(to_bytes(receipt["blockHash"]), "block_hash"),
        "block_number": int(receipt["blockNumber"]),
        "transaction_hash": b32_validate(to_bytes(receipt["transactionHash"]), "transaction_hash"),
        "transaction_index": int(receipt["transactionIndex"]),
        "from": b20_validate(to_bytes(receipt["from"]), "from"),
        "to": b20_validate(to_bytes(receipt["to"]), "to") if receipt.get("to") else None,
        "contract_address": b20_validate(to_bytes(receipt["contractAddress"]), "contract_address")
        if receipt.get("contractAddress")
        else None,
        # status can be None (old clients) — schema has nullable=True
        "status": int(receipt.get("status")) if receipt.get("status") is not None else None,  # type: ignore
        "type": int(rtype) if rtype is not None else None,
        "gas_used": int(receipt["gasUsed"]),
        "cumulative_gas_used": int(receipt["cumulativeGasUsed"]),
        # effectiveGasPrice only for EIP-1559 / newer clients
        "effective_gas_price": int(egp) if egp is not None else None,
        # 256 bytes bloom (2048 bits)
        "logs_bloom": b256_validate(to_bytes(lb), "logs_bloom") if lb else None,
        # event logs (list of structs)
        "logs": normalize_logs(receipt.get("logs")),
    }


def write_receipts_to_buffer(chain_id: int, receipts: list[TxReceipt]) -> list[dict[str, Any]]:
    return to_buffer(chain_id, receipts, receipt_to_row)

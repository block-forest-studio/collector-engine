from typing import Any
from web3.types import LogReceipt
from collector_engine.app.collectors.core.buffer_utils import to_buffer
from collector_engine.app.collectors.core.bytes_utils import (
    b20_validate,
    b32_validate,
    to_bytes,
)


def log_to_row(chain_id: int, log: LogReceipt) -> dict[str, Any]:
    topics = list(log.get("topics", []))
    return {
        "chain_id": chain_id,
        "block_number": int(log["blockNumber"]),
        "block_hash": b32_validate(to_bytes(log["blockHash"]), "block_hash"),
        "transaction_hash": b32_validate(to_bytes(log["transactionHash"]), "transaction_hash"),
        "log_index": int(log["logIndex"]),
        "address": b20_validate(to_bytes(log["address"]), "address"),
        "topic0": b32_validate(to_bytes(topics[0] if len(topics) > 0 else None), "topic0"),
        "topic1": b32_validate(to_bytes(topics[1] if len(topics) > 1 else None), "topic1"),
        "topic2": b32_validate(to_bytes(topics[2] if len(topics) > 2 else None), "topic2"),
        "topic3": b32_validate(to_bytes(topics[3] if len(topics) > 3 else None), "topic3"),
        "data": to_bytes(log["data"]),
        "removed": bool(log.get("removed", False)),
    }


def write_logs_to_buffer(chain_id: int, logs: list[LogReceipt]) -> list[dict[str, Any]]:
    return to_buffer(chain_id, logs, log_to_row)

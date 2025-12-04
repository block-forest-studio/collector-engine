import copy
import pytest
from typing import Any
from hexbytes import HexBytes
from collector_engine.app.domain.pure.logs import write_logs_to_buffer, log_to_row


"""
Example log for clarity:
logs = [
    {
        "address": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "topics": [
            "40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f",
            "1dd3c9cf9d1734cc0d69505ce783372d1a9c1066a55e25b3f01026e71569bf83",
            "00000000000000000000000066a9893cc07d91d95644aedd05d03f95e1dba8af"
        ],
        "data": "ffffffffffffffffffffffffffffffffffffffffffffffffffe78c550000000000000000000000000000000000000000000000000000001c07f9f6bede3b1ac8000000000000000000000000000000000000010e922a8a7f2bb83e6ef7db61ef000000000000000000000000000000000000000000000002d497d48dc0afd246000000000000000000000000000000000000000000000000000000000001b5900000000000000000000000000000000000000000000000000000000000002710",
        "blockHash": "5ec538e3711b00b68d361c16783316bffa8fe549951803a61523e11134dd6345",
        "blockNumber": 23199654,
        "blockTimestamp": "0x68a8f147",
        "transactionHash": "9adcf6ca395fc263527a35d36e3d243a31ba29f3acad7c6c8bb4161e02e5578b",
        "transactionIndex": 3,
        "logIndex": 132,
        "removed": False
    },
    {...},
]

Fields description:
- address: 20 bytes. The contract address that emitted the event.
- topics: Array of 32-byte topic hashes. Used for indexed event paramaters.
  * the first topic (topics[0]) is always the Keccak-256 hash of the event signature, e.g.
    Transfer(address,address,uint256) -> 0xddf252ad...
  * the following topics (topics[1], topics[2], ...) store the indexed arguments of that event.
  * you can use topics to filter logs effeciently without decoding all data.
- data: 32-byte data field. A blob of unindexed event paramerers, ABI-encoded.
  This contains the remaining (non-indexed) arguments of the event.
  It's variable length - not necessarily exactly 32 bytes.
- blockHash: 32-byte block hash that includes this log.
- blockNumber: Block number that includes this log.
- blockTimestamp: Block timestamp of that block (seconds since Unix epoch).
  Not part of the standard eth_getLogs result - often added manually by apps for convenience.
- transactionHash: 32-byte transaction hash that triggered this event.
- transactionIndex: Transaction index. The position (0-based) of that transaction within the block.
- logIndex: Log index. The position (0-based) of this specific log within the blocks full list of logs.
- removed: Boolean indicating if the log was removed due to a chain reorganization (reorg).
"""

CHAIN_ID = 1


def make_log(
    *,
    address: str = "0x" + "11" * 20,
    blockHash: str = "0x" + "aa" * 32,
    transactionHash: str = "0x" + "bb" * 32,
    blockNumber: int = 123,
    logIndex: int = 0,
    topics: list[str] | None = None,
    data: str = "0x",
    removed: bool = False,
) -> dict[str, Any]:
    """
    Create a synthetic LogReceipt-like dict for testing.

    Only override fields you care about; others get valid defaults.
    """
    return {
        "address": address,
        "blockHash": blockHash,
        "transactionHash": transactionHash,
        "blockNumber": blockNumber,
        "logIndex": logIndex,
        "topics": topics or [],
        "data": data,
        "removed": removed,
    }


def make_logs(n: int) -> list[dict[str, Any]]:
    return [make_log(logIndex=i) for i in range(n)]


def test_write_logs_to_buffer__success():
    logs = [
        make_log(logIndex=1, topics=["0x" + "00" * 32, "0x" + "01" * 32], data="0xdeadbeef"),
        make_log(logIndex=2, topics=["0x" + "02" * 32], data="0x"),
    ]

    rows = write_logs_to_buffer(CHAIN_ID, logs)

    assert isinstance(rows, list)
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == {
            "chain_id",
            "block_number",
            "block_hash",
            "transaction_hash",
            "log_index",
            "address",
            "topic0",
            "topic1",
            "topic2",
            "topic3",
            "data",
            "removed",
        }
        assert isinstance(row["chain_id"], int)
        assert isinstance(row["block_number"], int)
        assert isinstance(row["block_hash"], bytes)
        assert isinstance(row["transaction_hash"], bytes)
        assert isinstance(row["log_index"], int)
        assert isinstance(row["address"], bytes)
        assert row["topic0"] is None or isinstance(row["topic0"], bytes)
        assert row["topic1"] is None or isinstance(row["topic1"], bytes)
        assert row["topic2"] is None or isinstance(row["topic2"], bytes)
        assert row["topic3"] is None or isinstance(row["topic3"], bytes)
        assert isinstance(row["data"], bytes)
        assert isinstance(row["removed"], bool)
    assert rows[0]["log_index"] == 1
    assert rows[1]["log_index"] == 2


@pytest.mark.parametrize(
    "topics, expected",
    [
        ([], [None, None, None, None]),
        (["0x" + "00" * 32], ["bytes", None, None, None]),
        (["0x" + "00" * 32, "0x" + "01" * 32], ["bytes", "bytes", None, None]),
        (["0x" + "00" * 32, "0x" + "01" * 32, "0x" + "02" * 32], ["bytes", "bytes", "bytes", None]),
        (
            ["0x" + "00" * 32, "0x" + "01" * 32, "0x" + "02" * 32, "0x" + "03" * 32],
            ["bytes", "bytes", "bytes", "bytes"],
        ),
    ],
)
def test_write_logs_to_buffer_topics_arity__success(topics, expected):
    log = make_log(topics=topics)

    row = log_to_row(CHAIN_ID, log)

    topics_row = [row["topic0"], row["topic1"], row["topic2"], row["topic3"]]
    for val, exp in zip(topics_row, expected):
        if exp == "bytes":
            assert isinstance(val, bytes)
        else:
            assert val is None


def test_write_logs_to_buffer_invalid_address__error():
    bad = make_log(address="0x" + "11" * 19)  # 19 bytes instead of 20 bytes

    with pytest.raises(ValueError) as exc:
        log_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "address: expected 20 bytes, got 19"


def test_write_logs_to_buffer_invalid_blockhash__error():
    bad = make_log(blockHash="0x" + "aa" * 31)  # 31 bytes instead of 32 bytes

    with pytest.raises(ValueError) as exc:
        log_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "block_hash: expected 32 bytes, got 31"


def test_write_logs_to_buffer_invalid_txhash__error():
    bad = make_log(transactionHash="0x" + "bb" * 33)  # 33 bytes instead of 32 bytes

    with pytest.raises(ValueError) as exc:
        log_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "transaction_hash: expected 32 bytes, got 33"


def test_write_logs_to_buffer_hex_in_data__error():
    bad = make_log(data="0xxyz123")

    with pytest.raises(ValueError):
        log_to_row(CHAIN_ID, bad)


def test_write_logs_to_buffer_missing_removed_defaults_false__success():
    log = make_log(removed=False)
    del log["removed"]
    row = log_to_row(1, log)

    assert row["removed"] is False


def test_write_logs_to_buffer_missing_topics_treated_as_success():
    log = make_log()
    row = log_to_row(1, log)

    assert row["topic0"] is None
    assert row["topic1"] is None
    assert row["topic2"] is None
    assert row["topic3"] is None


def test_write_logs_to_buffer_input_not_mutated_success():
    log = make_log(topics=["0x" + "00" * 32])
    original = copy.deepcopy(log)
    _ = log_to_row(1, log)

    assert log == original


def test_hexbytes_inputs_are_supported():
    log = make_log(
        blockHash=HexBytes(b"\xaa" * 32).hex(),
        transactionHash=HexBytes(b"\xbb" * 32).hex(),
        address="0x" + "11" * 20,
        topics=["0x" + "00" * 32],
        data="0x",
    )

    row = log_to_row(1, log)

    assert isinstance(row["block_hash"], bytes)
    assert isinstance(row["transaction_hash"], bytes)

import copy
import pytest
from hexbytes import HexBytes

from collector_engine.app.domain.pure.receipts import (
    receipt_to_row,
    write_receipts_to_buffer,
)


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
    removed: bool | None = None,
    blockTimestamp: int | str | None = None,
    transactionIndex: int = 0,
):
    lg: dict = {
        "address": address,
        "blockHash": blockHash,
        "transactionHash": transactionHash,
        "blockNumber": blockNumber,
        "logIndex": logIndex,
        "topics": topics or [],
        "data": data,
        "transactionIndex": transactionIndex,
    }
    if removed is not None:
        lg["removed"] = removed
    if blockTimestamp is not None:
        # accept int or already hex str (caller ensures format)
        if isinstance(blockTimestamp, int):
            lg["blockTimestamp"] = hex(blockTimestamp)
        else:
            lg["blockTimestamp"] = blockTimestamp
    return lg


def make_receipt(
    *,
    blockHash: str = "0x" + "aa" * 32,
    blockNumber: int = 123,
    transactionHash: str = "0x" + "cc" * 32,
    transactionIndex: int = 0,
    from_: str = "0x" + "11" * 20,
    to: str | None = "0x" + "22" * 20,
    contractAddress: str | None = None,
    status: int | None = 1,
    type_: int | str | None = 2,
    gasUsed: int = 21000,
    cumulativeGasUsed: int = 21000,
    effectiveGasPrice: int | str | None = 100,
    logsBloom: str | None = None,
    logs: list[dict] | None = None,
):
    rc: dict = {
        "blockHash": blockHash,
        "blockNumber": blockNumber,
        "transactionHash": transactionHash,
        "transactionIndex": transactionIndex,
        "from": from_,
        "gasUsed": gasUsed,
        "cumulativeGasUsed": cumulativeGasUsed,
        "logs": logs or [],
    }
    if to is not None:
        rc["to"] = to
    if contractAddress is not None:
        rc["contractAddress"] = contractAddress
    if status is not None:
        rc["status"] = status
    if type_ is not None:
        rc["type"] = type_
    if effectiveGasPrice is not None:
        rc["effectiveGasPrice"] = effectiveGasPrice
    if logsBloom is not None:
        rc["logsBloom"] = logsBloom
    return rc


def test_write_receipts_to_buffer__success():
    receipts = [
        make_receipt(transactionIndex=1, logs=[make_log(logIndex=5)]),
        make_receipt(transactionIndex=2, logs=[make_log(logIndex=6)]),
    ]

    rows = write_receipts_to_buffer(CHAIN_ID, receipts)

    assert isinstance(rows, list)
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == {
            "chain_id",
            "block_hash",
            "block_number",
            "transaction_hash",
            "transaction_index",
            "from",
            "to",
            "contract_address",
            "status",
            "type",
            "gas_used",
            "cumulative_gas_used",
            "effective_gas_price",
            "logs_bloom",
            "logs",
        }
        assert isinstance(row["chain_id"], int)
        assert isinstance(row["block_hash"], bytes)
        assert isinstance(row["block_number"], int)
        assert isinstance(row["transaction_hash"], bytes)
        assert isinstance(row["transaction_index"], int)
        assert isinstance(row["from"], bytes)
        assert row["to"] is None or isinstance(row["to"], bytes)
        assert row["contract_address"] is None or isinstance(row["contract_address"], bytes)
        assert row["status"] in (0, 1, None)
        assert row["type"] is None or isinstance(row["type"], int)
        assert isinstance(row["gas_used"], int)
        assert isinstance(row["cumulative_gas_used"], int)
        assert row["effective_gas_price"] is None or isinstance(row["effective_gas_price"], int)
        assert row["logs_bloom"] is None or isinstance(row["logs_bloom"], bytes)
        assert isinstance(row["logs"], list)

    assert rows[0]["transaction_index"] == 1
    assert rows[1]["transaction_index"] == 2
    assert rows[0]["logs"][0]["log_index"] == 5
    assert rows[1]["logs"][0]["log_index"] == 6


def test_receipt_to_row_hex_normalization__success():
    r = make_receipt(type_="0x2", effectiveGasPrice="0x64")

    row = receipt_to_row(CHAIN_ID, r)

    assert row["type"] == 2
    assert row["effective_gas_price"] == 0x64


def test_receipt_to_row_invalid_block_hash__error():
    bad = make_receipt(blockHash="0x" + "aa" * 31)

    with pytest.raises(ValueError) as exc:
        receipt_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "block_hash: expected 32 bytes, got 31"


def test_receipt_to_row_invalid_tx_hash__error():
    bad = make_receipt(transactionHash="0x" + "cc" * 31)

    with pytest.raises(ValueError) as exc:
        receipt_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "transaction_hash: expected 32 bytes, got 31"


def test_receipt_to_row_invalid_from_address__error():
    bad = make_receipt(from_="0x" + "11" * 19)

    with pytest.raises(ValueError) as exc:
        receipt_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "from: expected 20 bytes, got 19"


def test_receipt_to_row_invalid_to_address__error():
    bad = make_receipt(to="0x" + "22" * 19)

    with pytest.raises(ValueError) as exc:
        receipt_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "to: expected 20 bytes, got 19"


def test_receipt_to_row_invalid_contract_address__error():
    bad = make_receipt(contractAddress="0x" + "33" * 19)

    with pytest.raises(ValueError) as exc:
        receipt_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "contract_address: expected 20 bytes, got 19"


def test_receipt_to_row_invalid_logs_bloom__error():
    bad = make_receipt(logsBloom="0x" + "ff" * 255)  # 255 bytes instead of 256

    with pytest.raises(ValueError) as exc:
        receipt_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "logs_bloom: expected 256 bytes, got 255"


def test_receipt_to_row_logs_topics_and_removed_default__success():
    logs = [
        make_log(topics=["0x" + "00" * 32, "0x" + "01" * 32]),
        make_log(topics=[], removed=None),  # removed omitted -> False
    ]
    r = make_receipt(logs=logs)

    row = receipt_to_row(CHAIN_ID, r)

    l0 = row["logs"][0]
    assert isinstance(l0["topics"], list) and len(l0["topics"]) == 2
    assert all(isinstance(t, bytes) for t in l0["topics"])
    l1 = row["logs"][1]
    assert l1["removed"] is False


def test_receipt_to_row_input_not_mutated__success():
    r = make_receipt(logs=[make_log(topics=["0x" + "00" * 32])])
    original = copy.deepcopy(r)

    _ = receipt_to_row(CHAIN_ID, r)

    assert r == original


def test_receipt_to_row_hexbytes_inputs_supported__success():
    r = make_receipt(
        blockHash=HexBytes(b"\xaa" * 32),
        transactionHash=HexBytes(b"\xcc" * 32),
        from_="0x" + "11" * 20,
        to="0x" + "22" * 20,
        logs=[
            make_log(
                blockHash=HexBytes(b"\xaa" * 32).hex(),
                transactionHash=HexBytes(b"\xbb" * 32).hex(),
                address="0x" + "11" * 20,
                topics=["0x" + "00" * 32],
            )
        ],
    )

    row = receipt_to_row(CHAIN_ID, r)

    assert isinstance(row["block_hash"], bytes)
    assert isinstance(row["transaction_hash"], bytes)
    assert isinstance(row["from"], bytes)
    assert isinstance(row["to"], bytes)
    assert isinstance(row["logs"][0]["block_hash"], bytes)
    assert isinstance(row["logs"][0]["transaction_hash"], bytes)

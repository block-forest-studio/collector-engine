# tests/core/test_transactions_core.py

import copy
from typing import Any

import pytest
from hexbytes import HexBytes
from web3.types import TxData

from collector_engine.app.domain.pure.transactions import (
    transaction_to_row,
    write_transactions_to_buffer,
)

"""
Example transaction for clarity:

txs = [
    {
        "blockHash": "0x5ec538e3711b00b68d361c16783316bffa8fe549951803a61523e11134dd6345",
        "blockNumber": 23199654,
        "transactionIndex": 42,
        "hash": "0x9adcf6ca395fc263527a35d36e3d243a31ba29f3acad7c6c8bb4161e02e5578b",
        "from": "0x742d35Cc6634C0532925a3b844Bc454e4438f44e",
        "to": "0x000000000004444c5dc75cB358380D2e3dE08A90",
        "value": "0xde0b6b3a7640000",  # 1 ETH in wei
        "gas": 21000,
        "gasPrice": "0x4a817c800",       # 20 gwei
        "maxFeePerGas": None,
        "maxPriorityFeePerGas": None,
        "input": "0x",                   # empty -> simple ETH transfer
        "nonce": 12,
        "v": "0x25",
        "r": "0x3b4b3a4b...deadbeef",    # signature part 1
        "s": "0x7c5f1b6a...feedface",    # signature part 2
        "type": "0x0",                   # Legacy transaction
        "accessList": [],                # present for EIP-2930 / 1559 txs
        "yParity": None,
    },
    {...},
]


Fields description:
- blockHash: 32 bytes. The hash of the block containing this transaction.
  None if the transaction is pending (unmined).

- blockNumber: Integer. The block number that includes this transaction.
  None if pending.

- transactionIndex: Integer. The position (0-based) of this transaction within its block.
  None if pending.

- hash: 32 bytes. Unique transaction hash (identifier).

- from: 20 bytes. The sender’s Ethereum address (EOA) that signed and sent the transaction.

- to: 20 bytes. The recipient address — either a contract or EOA.
  None for contract-creation transactions.

- value: Integer. Amount of wei transferred with this transaction.

- gas: Integer. Gas limit provided by the sender.

- gasPrice: Integer. Gas price (wei per gas unit) for legacy transactions (type 0).
  For EIP-1559 transactions (type 2), this is replaced by:
    - maxFeePerGas: maximum total fee per gas unit.
    - maxPriorityFeePerGas: maximum tip per gas unit to the miner.

- input: Arbitrary bytes. Encoded calldata for contract calls.
  Empty (0x) for simple ETH transfers.

- nonce: Integer. Number of transactions sent from the sender’s address before this one.

- type: Integer. Transaction type:
    0 → Legacy
    1 → Access List (EIP-2930)
    2 → Dynamic Fee (EIP-1559)

- v, r, s: ECDSA signature components (ensure transaction authenticity).
  Together define who signed the transaction.

- yParity: Integer (0 or 1). Alternative to v for EIP-1559 typed transactions.

- accessList: Array of access entries (EIP-2930/EIP-1559).
  Each entry contains:
    - address: 20 bytes (contract address accessed)
    - storageKeys: Array of 32-byte storage keys touched by this transaction.
  This allows nodes to prefetch touched storage slots.

"""

CHAIN_ID = 1


def make_tx(
    *,
    from_: str = "0x" + "11" * 20,
    to: str | None = "0x" + "22" * 20,
    blockHash: str | None = "0x" + "aa" * 32,
    blockNumber: int | None = 123,
    transactionIndex: int | None = 0,
    gas: int = 21_000,
    gasPrice: int | None = 1_000_000_000,
    maxFeePerGas: int | None = None,
    maxPriorityFeePerGas: int | None = None,
    hash_: str = "0x" + "bb" * 32,
    input_: str = "0x",
    nonce: int = 0,
    value: int = 0,
    type_: int | None = 2,
    v: int = 27,
    r: str | int | bytes = b"\xcc" * 32,
    s: str | int | bytes = b"\xdd" * 32,
    yParity: int | None = None,
    accessList: list[dict[str, Any]] | None = None,
) -> TxData:
    """
    Create a synthetic TxData-like dict for testing.
    Only override fields you care about; others get valid defaults.
    """
    tx: dict[str, Any] = {
        "from": from_,
        "to": to,
        "blockHash": blockHash,
        "blockNumber": blockNumber,
        "transactionIndex": transactionIndex,
        "gas": gas,
        "hash": hash_,
        "input": input_,
        "nonce": nonce,
        "value": value,
        "v": v,
        "r": r,
        "s": s,
    }

    if gasPrice is not None:
        tx["gasPrice"] = gasPrice
    if maxFeePerGas is not None:
        tx["maxFeePerGas"] = maxFeePerGas
    if maxPriorityFeePerGas is not None:
        tx["maxPriorityFeePerGas"] = maxPriorityFeePerGas
    if type_ is not None:
        tx["type"] = type_
    if yParity is not None:
        tx["yParity"] = yParity
    if accessList is not None:
        tx["accessList"] = accessList

    return tx  # type: ignore[return-value]


def make_txs(n: int) -> list[TxData]:
    return [make_tx(transactionIndex=i, nonce=i) for i in range(n)]


def test_write_transactions_to_buffer__success():
    txs = [
        make_tx(transactionIndex=1, nonce=1, value=123),
        make_tx(transactionIndex=2, nonce=2, value=456),
    ]

    rows = write_transactions_to_buffer(CHAIN_ID, txs)

    assert isinstance(rows, list)
    assert len(rows) == 2
    for row in rows:
        assert set(row.keys()) == {
            "chain_id",
            "block_hash",
            "block_number",
            "transaction_index",
            "from",
            "gas",
            "gas_price",
            "max_fee_per_gas",
            "max_priority_fee_per_gas",
            "hash",
            "input",
            "nonce",
            "to",
            "value",
            "type",
            "v",
            "r",
            "s",
            "y_parity",
            "access_list",
        }
        assert isinstance(row["chain_id"], int)
        assert row["block_hash"] is None or isinstance(row["block_hash"], bytes)
        assert row["block_number"] is None or isinstance(row["block_number"], int)
        assert row["transaction_index"] is None or isinstance(row["transaction_index"], int)
        assert isinstance(row["from"], bytes)
        assert isinstance(row["gas"], int)
        assert row["gas_price"] is None or isinstance(row["gas_price"], int)
        assert row["max_fee_per_gas"] is None or isinstance(row["max_fee_per_gas"], int)
        assert row["max_priority_fee_per_gas"] is None or isinstance(
            row["max_priority_fee_per_gas"], int
        )
        assert isinstance(row["hash"], bytes)
        assert isinstance(row["input"], (bytes, type(None)))  # to_bytes may return b"" for "0x"
        assert isinstance(row["nonce"], int)
        assert row["to"] is None or isinstance(row["to"], bytes)
        assert isinstance(row["value"], int)
        assert row["type"] is None or isinstance(row["type"], int)
        assert isinstance(row["v"], int)
        assert isinstance(row["r"], str) and row["r"].startswith("0x") and len(row["r"]) == 66
        assert isinstance(row["s"], str) and row["s"].startswith("0x") and len(row["s"]) == 66
        assert row["y_parity"] is None or isinstance(row["y_parity"], int)
        assert isinstance(row["access_list"], list)

    assert rows[0]["transaction_index"] == 1
    assert rows[1]["transaction_index"] == 2
    assert rows[0]["value"] == 123
    assert rows[1]["value"] == 456


def test_transaction_to_row_unmined_disallowed__error():
    tx = make_tx(blockHash=None, blockNumber=None, transactionIndex=None)

    with pytest.raises(ValueError) as exc:
        transaction_to_row(CHAIN_ID, tx, include_unmined=False)

    assert "Unmined (pending) transaction is not allowed" in str(exc.value)


def test_transaction_to_row_unmined_allowed__success():
    tx = make_tx(blockHash=None, blockNumber=None, transactionIndex=None)

    row = transaction_to_row(CHAIN_ID, tx, include_unmined=True)

    assert row["block_hash"] is None
    assert row["block_number"] is None
    assert row["transaction_index"] is None


def test_transaction_to_row_invalid_from_address__error():
    bad = make_tx(from_="0x" + "11" * 19)  # 19 bytes instead of 20

    with pytest.raises(ValueError) as exc:
        transaction_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "from: expected 20 bytes, got 19"


def test_transaction_to_row_invalid_to_address__error():
    bad = make_tx(to="0x" + "22" * 19)  # 19 bytes instead of 20

    with pytest.raises(ValueError) as exc:
        transaction_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "to: expected 20 bytes, got 19"


def test_transaction_to_row_invalid_blockhash__error():
    bad = make_tx(blockHash="0x" + "aa" * 31)  # 31 bytes instead of 32

    with pytest.raises(ValueError) as exc:
        transaction_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "block_hash: expected 32 bytes, got 31"


def test_transaction_to_row_invalid_txhash__error():
    bad = make_tx(hash_="0x" + "bb" * 31)  # 31 bytes instead of 32

    with pytest.raises(ValueError) as exc:
        transaction_to_row(CHAIN_ID, bad)

    assert str(exc.value) == "hash: expected 32 bytes, got 31"


def test_transaction_to_row_r_length_reflected_in_hex__success():
    short = make_tx(r=b"\xcc" * 31)
    row = transaction_to_row(CHAIN_ID, short)
    assert isinstance(row["r"], str) and row["r"].startswith("0x")
    assert len(row["r"]) == 2 + 31 * 2


def test_transaction_to_row_s_length_reflected_in_hex__success():
    short = make_tx(s=b"\xdd" * 31)
    row = transaction_to_row(CHAIN_ID, short)
    assert isinstance(row["s"], str) and row["s"].startswith("0x")
    assert len(row["s"]) == 2 + 31 * 2


def test_transaction_to_row_access_list__success():
    access_list = [
        {
            "address": "0x" + "33" * 20,
            "storageKeys": [
                "0x" + "44" * 32,
                "0x" + "55" * 32,
            ],
        }
    ]

    tx = make_tx(accessList=access_list)
    row = transaction_to_row(CHAIN_ID, tx)

    assert isinstance(row["access_list"], list)
    assert len(row["access_list"]) == 1
    entry = row["access_list"][0]
    assert isinstance(entry["address"], bytes)
    assert len(entry["address"]) == 20
    assert isinstance(entry["storage_keys"], list)
    assert len(entry["storage_keys"]) == 2
    for sk in entry["storage_keys"]:
        assert isinstance(sk, bytes)
        assert len(sk) == 32


def test_transaction_to_row_input_not_mutated__success():
    tx = make_tx(
        accessList=[
            {
                "address": "0x" + "33" * 20,
                "storageKeys": ["0x" + "44" * 32],
            }
        ]
    )
    original = copy.deepcopy(tx)

    _ = transaction_to_row(CHAIN_ID, tx)

    assert tx == original


def test_transaction_to_row_hexbytes_inputs_supported__success():
    tx = make_tx(
        blockHash=HexBytes(b"\xaa" * 32),
        hash_=HexBytes(b"\xbb" * 32),
        from_="0x" + "11" * 20,
        to="0x" + "22" * 20,
        r=HexBytes(b"\xcc" * 32),
        s=HexBytes(b"\xdd" * 32),
    )

    row = transaction_to_row(CHAIN_ID, tx)

    assert isinstance(row["block_hash"], bytes)
    assert isinstance(row["hash"], bytes)
    assert isinstance(row["r"], str) and row["r"].startswith("0x")
    assert isinstance(row["s"], str) and row["s"].startswith("0x")

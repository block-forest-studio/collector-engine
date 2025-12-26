import pyarrow as pa

LOG_SCHEMA = pa.schema(
    [
        ("chain_id", pa.int32()),
        ("block_number", pa.int64()),
        ("block_hash", pa.binary(32)),
        ("transaction_hash", pa.binary(32)),
        ("log_index", pa.int32()),
        ("address", pa.binary(20)),
        ("topic0", pa.binary(32)),
        ("topic1", pa.binary(32)),
        ("topic2", pa.binary(32)),
        ("topic3", pa.binary(32)),
        ("data", pa.binary()),
        ("removed", pa.bool_()),
    ]
)

TX_SCHEMA = pa.schema(
    [
        ("block_hash", pa.binary(32)),
        ("block_number", pa.int64()),
        ("from", pa.binary(20)),
        ("gas", pa.int64()),
        ("gas_price", pa.decimal128(38, 0)),
        ("max_fee_per_gas", pa.decimal128(38, 0)),
        ("max_priority_fee_per_gas", pa.decimal128(38, 0)),
        ("hash", pa.binary(32)),
        ("input", pa.binary()),
        ("nonce", pa.int64()),
        ("to", pa.binary(20)),
        ("transaction_index", pa.int32()),
        ("value", pa.decimal128(38, 0)),
        ("type", pa.int32()),
        ("chain_id", pa.int64()),
        ("v", pa.int64()),
        ("r", pa.string()),
        ("s", pa.string()),
        ("y_parity", pa.int8()),
        (
            "access_list",
            pa.list_(
                pa.struct(
                    [
                        ("address", pa.binary(20)),
                        ("storage_keys", pa.list_(pa.binary())),
                    ]
                )
            ),
        ),
    ]
)

RECEIPT_SCHEMA = pa.schema(
    [
        pa.field("chain_id", pa.int64(), nullable=False),
        pa.field("block_hash", pa.binary(32), nullable=False),
        pa.field("block_number", pa.int64(), nullable=False),
        pa.field("transaction_hash", pa.binary(32), nullable=False),
        pa.field("transaction_index", pa.int32(), nullable=False),
        pa.field("from", pa.binary(20), nullable=False),
        pa.field("to", pa.binary(20), nullable=True),
        pa.field("contract_address", pa.binary(20), nullable=True),
        pa.field("status", pa.int8(), nullable=True),
        pa.field("type", pa.int32(), nullable=True),
        pa.field("gas_used", pa.int64(), nullable=False),
        pa.field("cumulative_gas_used", pa.int64(), nullable=False),
        pa.field("effective_gas_price", pa.decimal128(38, 0), nullable=True),
        pa.field("logs_bloom", pa.binary(256), nullable=True),
        pa.field(
            "logs",
            pa.list_(
                pa.struct(
                    [
                        pa.field("address", pa.binary(20), nullable=False),
                        pa.field("block_hash", pa.binary(32), nullable=False),
                        pa.field("block_number", pa.int64(), nullable=False),
                        pa.field("block_timestamp", pa.int64(), nullable=True),
                        pa.field("data", pa.binary(), nullable=False),
                        pa.field("log_index", pa.int32(), nullable=False),
                        pa.field("removed", pa.bool_(), nullable=False),
                        pa.field("topics", pa.list_(pa.binary()), nullable=False),
                        pa.field("transaction_hash", pa.binary(32), nullable=False),
                        pa.field("transaction_index", pa.int32(), nullable=False),
                    ]
                )
            ),
        ),
    ]
)

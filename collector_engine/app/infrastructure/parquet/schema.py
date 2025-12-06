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

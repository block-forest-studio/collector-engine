import pyarrow as pa

from collector_engine.app.infrastructure.adapters.storage.parquet_store import ParquetDatasetStore
from collector_engine.app.infrastructure.parquet.schema import LOG_SCHEMA


def test_parquet_store_write_and_read(tmp_path):
    store = ParquetDatasetStore(tmp_path)

    buffer = {name: [] for name in LOG_SCHEMA.names}

    rows = [
        {
            "chain_id": 1,
            "block_number": 100,
            "block_hash": b"\xaa" * 32,
            "transaction_hash": b"\xbb" * 32,
            "log_index": 0,
            "address": b"\x11" * 20,
            "topic0": b"\x00" * 32,
            "topic1": None,
            "topic2": None,
            "topic3": None,
            "data": b"",
            "removed": False,
        },
        {
            "chain_id": 1,
            "block_number": 101,
            "block_hash": b"\xaa" * 32,
            "transaction_hash": b"\xbb" * 32,
            "log_index": 1,
            "address": b"\x11" * 20,
            "topic0": b"\x00" * 32,
            "topic1": None,
            "topic2": None,
            "topic3": None,
            "data": b"",
            "removed": False,
        },
    ]

    # build column buffer
    for row in rows:
        for name in LOG_SCHEMA.names:
            buffer[name].append(row[name])

    # force flush
    out = store.write_buffer(
        buffer=buffer,
        schema=LOG_SCHEMA,
        file_name="logs_100_101",
        rows_per_file=10,
        force=True,
    )

    assert out == {name: [] for name in LOG_SCHEMA.names}

    names = store.list_names()
    assert any(n.endswith(".parquet") for n in names)

    # read back
    table = store.read_table(sorted(names)[-1])
    assert isinstance(table, pa.Table)
    assert set(table.column_names) == set(LOG_SCHEMA.names)
    assert table.num_rows == len(rows)

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg
from psycopg.types.json import Jsonb

from collector_engine.app.domain.ports.out import DatasetLoader, DatasetName


@dataclass(frozen=True)
class CopySpec:
    table: str
    columns: list[str]
    kinds: dict[str, str]


LOGS_COPY_SPEC = CopySpec(
    table="raw.logs",
    columns=[
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
    ],
    kinds={
        "chain_id": "int",
        "block_number": "int",
        "block_hash": "bytes",
        "transaction_hash": "bytes",
        "log_index": "int",
        "address": "bytes",
        "topic0": "bytes",
        "topic1": "bytes",
        "topic2": "bytes",
        "topic3": "bytes",
        "data": "bytes",
        "removed": "bool",
    },
)

TXS_COPY_SPEC = CopySpec(
    table="raw.transactions",
    columns=[
        "chain_id",
        "block_hash",
        "block_number",
        "transaction_index",
        "from",
        "to",
        "gas",
        "gas_price",
        "max_fee_per_gas",
        "max_priority_fee_per_gas",
        "hash",
        "input",
        "nonce",
        "value",
        "type",
        "v",
        "r",
        "s",
        "y_parity",
        "access_list",
    ],
    kinds={
        "chain_id": "int",
        "block_hash": "bytes",
        "block_number": "int",
        "transaction_index": "int",
        "from": "bytes",
        "to": "bytes",
        "gas": "int",
        "gas_price": "num",  # numeric
        "max_fee_per_gas": "num",
        "max_priority_fee_per_gas": "num",
        "hash": "bytes",
        "input": "bytes",
        "nonce": "int",
        "value": "num",  # numeric
        "type": "int",
        "v": "int",
        "r": "text",  # text
        "s": "text",  # text
        "y_parity": "int",
        "access_list": "json",  # jsonb
    },
)

RECEIPTS_COPY_SPEC = CopySpec(
    table="raw.receipts",
    columns=[
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
    ],
    kinds={
        "chain_id": "int",
        "block_hash": "bytes",
        "block_number": "int",
        "transaction_hash": "bytes",
        "transaction_index": "int",
        "from": "bytes",
        "to": "bytes",
        "contract_address": "bytes",
        "status": "int",
        "type": "int",
        "gas_used": "int",
        "cumulative_gas_used": "int",
        "effective_gas_price": "num",
        "logs_bloom": "bytes",
        "logs": "json",  # jsonb
    },
)


class PostgresCopyLoader(DatasetLoader):
    """
    DatasetLoader implementation using PostgreSQL COPY ... FORMAT binary
    + temporary table  and INSERT ... ON CONFLICT.
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def load_parquet_dir(
        self,
        *,
        parquet_dir: Path,
        dataset: DatasetName,
        file_prefix: str,
    ) -> None:
        spec = self._spec_for(dataset)
        self._copy_parquet_dir(
            parquet_dir=parquet_dir,
            file_prefix=file_prefix,
            spec=spec,
            on_conflict="DO NOTHING",
        )

    def _spec_for(self, dataset: DatasetName) -> CopySpec:
        if dataset == "logs":
            return LOGS_COPY_SPEC
        if dataset == "txs":
            return TXS_COPY_SPEC
        if dataset == "receipts":
            return RECEIPTS_COPY_SPEC
        raise ValueError(f"Unknown dataset: {dataset!r}")

    def _copy_parquet_dir(
        self,
        *,
        parquet_dir: Path,
        file_prefix: str,
        spec: CopySpec,
        on_conflict: str,
        batch_rows: int = 50_000,
    ) -> None:
        files = sorted(
            p
            for p in parquet_dir.iterdir()
            if p.is_file() and p.name.startswith(file_prefix) and p.suffix == ".parquet"
        )
        if not files:
            return

        with psycopg.connect(self._dsn) as conn:
            for fp in files:
                self._copy_one_file(
                    conn,
                    fp,
                    spec=spec,
                    on_conflict=on_conflict,
                    batch_rows=batch_rows,
                )
            conn.commit()

    def _copy_one_file(
        self,
        conn: psycopg.Connection,
        parquet_file: Path,
        *,
        spec: CopySpec,
        on_conflict: str,
        batch_rows: int,
    ) -> None:
        tmp = f"tmp_{spec.table.replace('.', '_')}"

        # cytation np. "from"
        cols_sql = ", ".join(f'"{c}"' for c in spec.columns)

        create_tmp = f"""
            DROP TABLE IF EXISTS {tmp};
            CREATE TEMP TABLE {tmp}
            (LIKE {spec.table} INCLUDING DEFAULTS)
            ON COMMIT DROP;
        """

        copy_sql = f"""
            COPY {tmp} ({cols_sql})
            FROM STDIN
            WITH (FORMAT text);
        """

        insert_sql = f"""
            INSERT INTO {spec.table} ({cols_sql})
            SELECT {cols_sql} FROM {tmp}
            ON CONFLICT {on_conflict};
        """

        with conn.cursor() as cur:
            cur.execute(create_tmp)

            pf = pq.ParquetFile(parquet_file)
            with cur.copy(copy_sql) as copy:
                for batch in pf.iter_batches(batch_size=batch_rows):
                    for row in self._iter_py_rows(batch, spec):
                        copy.write_row(row)

            cur.execute(insert_sql)

    def _iter_py_rows(self, batch: pa.RecordBatch, spec: CopySpec) -> Iterator[list[Any]]:
        """
        RecordBatch conversion -> list[Python values] compatible with Postgres types.
        No manual string building, no NUL characters.
        """

        def _json_safe(v: Any) -> Any:
            # Recursively convert bytes into hex strings for JSONB
            if v is None:
                return None
            if isinstance(v, (bytes, bytearray, memoryview)):
                b = bytes(v) if not isinstance(v, bytes) else v
                return "0x" + b.hex()
            if isinstance(v, dict):
                return {k: _json_safe(val) for k, val in v.items()}
            if isinstance(v, list):
                return [_json_safe(x) for x in v]
            # Arrow may return Decimal, int, bool, str directly which are JSON-safe
            return v

        cols: dict[str, pa.Array] = {}
        schema = batch.schema
        present = set(schema.names)
        for name in spec.columns:
            idx = schema.get_field_index(name)
            if idx == -1:
                raise KeyError(
                    f"Column {name!r} not found in parquet batch. Present: {sorted(present)}"
                )
            cols[name] = batch.column(idx)
        n = batch.num_rows

        for i in range(n):
            row: list[Any] = []
            for col_name in spec.columns:
                scalar = cols[col_name][i]
                val = scalar.as_py() if hasattr(scalar, "as_py") else scalar
                kind = spec.kinds[col_name]

                if val is None:
                    row.append(None)
                    continue

                if kind == "bytes":
                    # bytea: we pass bytes / memoryview
                    if isinstance(val, (bytes, bytearray, memoryview)):
                        row.append(memoryview(val))
                    else:
                        row.append(memoryview(bytes(val)))
                elif kind == "int":
                    row.append(int(val))
                elif kind == "num":
                    # for decimal128 Arrow usually returns Decimal -> OK
                    if isinstance(val, Decimal):
                        row.append(val)
                    else:
                        row.append(Decimal(str(val)))
                elif kind == "bool":
                    row.append(bool(val))
                elif kind == "text":
                    s = str(val).replace("\x00", "")
                    row.append(s)
                elif kind == "json":
                    # list/dict -> jsonb, ensure bytes are JSON-serializable
                    row.append(Jsonb(_json_safe(val)))
                else:
                    raise ValueError(f"Unknown kind {kind!r} for column {col_name!r}")

            yield row

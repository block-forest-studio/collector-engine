from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.compute as pc
from loguru import logger

from collector_engine.app.domain.ports.out import DatasetStore


@dataclass
class ValidationIssue:
    level: str  # "ERROR" | "WARN"
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationReport:
    issues: list[ValidationIssue] = field(default_factory=list)

    def error(self, code: str, message: str, **context: Any) -> None:
        self.issues.append(ValidationIssue("ERROR", code, message, dict(context)))

    def warn(self, code: str, message: str, **context: Any) -> None:
        self.issues.append(ValidationIssue("WARN", code, message, dict(context)))

    @property
    def ok(self) -> bool:
        return not any(i.level == "ERROR" for i in self.issues)

    def log_summary(self) -> None:
        errors = [i for i in self.issues if i.level == "ERROR"]
        warns = [i for i in self.issues if i.level == "WARN"]
        logger.info("Validation summary: {} errors, {} warnings", len(errors), len(warns))
        for i in errors[:50]:
            logger.error("[{}] {} | {}", i.code, i.message, i.context)
        for i in warns[:50]:
            logger.warning("[{}] {} | {}", i.code, i.message, i.context)


# -------------------------
# Helpers
# -------------------------


def _only_parquet(names: Iterable[str], prefix: str) -> list[str]:
    return [n for n in names if n.startswith(prefix) and n.endswith(".parquet")]


def _suffix(name: str, prefix: str) -> str:
    # logs_FROM_TO.parquet -> FROM_TO
    return name.removeprefix(prefix).removesuffix(".parquet")


def _names_by_suffix(names: list[str], prefix: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for n in names:
        suf = _suffix(n, prefix)
        out[suf] = n
    return out


def _schema_equal(actual: pa.Schema, expected: pa.Schema) -> tuple[bool, str]:
    # strict-ish: same names + types (metadata ignored)
    if actual.names != expected.names:
        return False, f"column names differ: actual={actual.names} expected={expected.names}"
    for f_a, f_e in zip(actual, expected, strict=True):
        if f_a.type != f_e.type:
            return False, f"type mismatch on '{f_a.name}': actual={f_a.type} expected={f_e.type}"
    return True, ""


def _count_distinct(arr: pa.Array | pa.ChunkedArray) -> int:
    # pc.count_distinct works on both Array and ChunkedArray
    v = pc.count_distinct(arr)
    return int(v.as_py()) if v is not None else 0


def _to_bytes_list(arr: pa.Array | pa.ChunkedArray) -> list[bytes]:
    # for fixed_size_binary/binary columns
    # Note: to_pylist() can be big; per-file is usually acceptable for your suffix-sized chunks.
    lst = arr.to_pylist()  # list[bytes | memoryview | None]
    out: list[bytes] = []
    for x in lst:
        if x is None:
            continue
        out.append(bytes(x))
    return out


def _unique_bytes(arr: pa.Array | pa.ChunkedArray) -> list[bytes]:
    u = pc.unique(arr)  # Array
    return _to_bytes_list(u)


def _validate_file_schema(
    report: ValidationReport,
    *,
    table: pa.Table,
    expected_schema: pa.Schema,
    file_name: str,
) -> None:
    ok, reason = _schema_equal(table.schema, expected_schema)
    if not ok:
        report.error(
            "SCHEMA_MISMATCH",
            f"Schema mismatch for {file_name}: {reason}",
            file=file_name,
        )


def _validate_uniqueness(
    report: ValidationReport,
    *,
    table: pa.Table,
    file_name: str,
    columns: list[str],
    key_name: str,
) -> None:
    if table.num_rows == 0:
        report.warn("EMPTY_FILE", f"Empty parquet file: {file_name}", file=file_name)
        return

    # Make operations predictable (avoid ChunkedArray surprises)
    table = table.combine_chunks()

    key_tbl = table.select(columns)

    # Group by the composite key, and count occurrences
    # (aggregate needs a column name; count any column, e.g. first key column)
    count_col = columns[0]
    grouped = key_tbl.group_by(columns).aggregate([(count_col, "count")])

    distinct = grouped.num_rows
    total = table.num_rows

    if distinct != total:
        # Find worst duplication count to make the error actionable
        cnt_arr = grouped[f"{count_col}_count"]
        max_count = pc.max(cnt_arr).as_py() if grouped.num_rows else None

        report.error(
            "DUPLICATE_KEY",
            (
                f"Duplicates detected in {file_name} for key {key_name}: "
                f"rows={total}, distinct={distinct}, max_dupe_count={max_count}"
            ),
            file=file_name,
            key=key_name,
            rows=total,
            distinct=distinct,
            max_dupe_count=max_count,
        )


def _validate_subset(
    report: ValidationReport,
    *,
    left_hashes: list[bytes],
    right_hashes: list[bytes],
    left_label: str,
    right_label: str,
    suffix: str,
) -> None:
    # left ⊆ right
    right_set = set(right_hashes)
    missing = [h for h in left_hashes if h not in right_set]
    if missing:
        report.error(
            "MISSING_REFERENCES",
            f"{left_label} has hashes missing in {right_label} (suffix={suffix}): missing={len(missing)}",
            suffix=suffix,
            missing_count=len(missing),
            example_missing="0x" + missing[0].hex(),
        )


def _validate_tx_receipt_block_consistency(
    report: ValidationReport,
    *,
    tx_table: pa.Table,
    rcpt_table: pa.Table,
    suffix: str,
) -> None:
    # Compare block_number for hashes that exist in both.
    tx_hashes = _to_bytes_list(tx_table["hash"])
    tx_blocks = tx_table["block_number"].to_pylist()  # list[int|None]
    tx_map: dict[bytes, int] = {}
    for h, b in zip(tx_hashes, tx_blocks, strict=False):
        if h is None or b is None:
            continue
        # last write wins; duplicates should already be caught by uniqueness
        tx_map[h] = int(b)

    rc_hashes = _to_bytes_list(rcpt_table["transaction_hash"])
    rc_blocks = rcpt_table["block_number"].to_pylist()
    mismatches = 0
    example: dict[str, Any] | None = None

    for h, b in zip(rc_hashes, rc_blocks, strict=False):
        if h is None or b is None:
            continue
        txb = tx_map.get(h)
        if txb is None:
            continue
        if int(b) != txb:
            mismatches += 1
            if example is None:
                example = {
                    "hash": "0x" + h.hex(),
                    "tx_block": txb,
                    "receipt_block": int(b),
                }

    if mismatches:
        report.error(
            "BLOCK_NUMBER_MISMATCH",
            f"Found {mismatches} tx/receipt block_number mismatches (suffix={suffix})",
            suffix=suffix,
            mismatches=mismatches,
            example=example,
        )


async def validate_pipeline_datasets(
    *,
    logs_store: DatasetStore,
    tx_store: DatasetStore,
    receipts_store: DatasetStore,
    log_schema: pa.Schema,
    tx_schema: pa.Schema,
    receipt_schema: pa.Schema,
) -> ValidationReport:
    """
    Validates:
      - per-file schema + uniqueness
      - suffix pairing: logs <-> txs <-> receipts
      - coverage: logs.transaction_hash ⊆ txs.hash, txs.hash ⊆ receipts.transaction_hash
      - consistency: tx.block_number == receipt.block_number for shared hashes
    """
    report = ValidationReport()

    log_files = _only_parquet(logs_store.list_names(), "logs_")
    tx_files = _only_parquet(tx_store.list_names(), "txs_")
    rc_files = _only_parquet(receipts_store.list_names(), "receipts_")

    # Logs are the root dataset. Transactions and receipts are derived datasets.
    # That is why only log_files are treated as a hard precondition.
    if not log_files:
        report.warn("NO_LOG_FILES", "No logs parquet files found.")
        return report

    logs_by = _names_by_suffix(log_files, "logs_")
    tx_by = _names_by_suffix(tx_files, "txs_")
    rc_by = _names_by_suffix(rc_files, "receipts_")

    # Per-file validation + cross-file validation per suffix
    for suf, log_name in sorted(logs_by.items(), key=lambda x: x[0]):
        tx_name = tx_by.get(suf)
        rc_name = rc_by.get(suf)

        log_table = logs_store.read_table(log_name)
        _validate_file_schema(
            report, table=log_table, expected_schema=log_schema, file_name=log_name
        )
        _validate_uniqueness(
            report,
            table=log_table,
            file_name=log_name,
            columns=["block_number", "log_index"],
            key_name="(block_number, log_index)",
        )

        if tx_name is None:
            report.error(
                "MISSING_TX_FILE",
                f"Missing txs file for logs suffix={suf}",
                suffix=suf,
                expected=f"txs_{suf}.parquet",
                logs_file=log_name,
            )
            continue

        tx_table = tx_store.read_table(tx_name)
        _validate_file_schema(report, table=tx_table, expected_schema=tx_schema, file_name=tx_name)
        _validate_uniqueness(
            report,
            table=tx_table,
            file_name=tx_name,
            columns=["hash"],
            key_name="hash",
        )

        # logs -> txs coverage
        log_tx_hashes = _unique_bytes(log_table["transaction_hash"])
        tx_hashes = _unique_bytes(tx_table["hash"])
        _validate_subset(
            report,
            left_hashes=log_tx_hashes,
            right_hashes=tx_hashes,
            left_label="logs.transaction_hash",
            right_label="txs.hash",
            suffix=suf,
        )

        # receipts checks if receipts file exists (optional if not collected yet)
        if rc_name is None:
            report.warn(
                "MISSING_RECEIPTS_FILE",
                f"Missing receipts file for suffix={suf} (ok if receipts not collected yet)",
                suffix=suf,
                expected=f"receipts_{suf}.parquet",
            )
            continue

        rc_table = receipts_store.read_table(rc_name)
        _validate_file_schema(
            report, table=rc_table, expected_schema=receipt_schema, file_name=rc_name
        )
        _validate_uniqueness(
            report,
            table=rc_table,
            file_name=rc_name,
            columns=["transaction_hash"],
            key_name="transaction_hash",
        )

        # txs -> receipts coverage
        rc_hashes = _unique_bytes(rc_table["transaction_hash"])
        _validate_subset(
            report,
            left_hashes=tx_hashes,
            right_hashes=rc_hashes,
            left_label="txs.hash",
            right_label="receipts.transaction_hash",
            suffix=suf,
        )

        # tx vs receipt block_number consistency
        _validate_tx_receipt_block_consistency(
            report, tx_table=tx_table, rcpt_table=rc_table, suffix=suf
        )

    return report

# ADR-004: Dataset Scope & Layout

## Status

Accepted

## Context

Different datasets (blocks, transactions, logs, traces, pools) have distinct access patterns. Poor partitioning leads to small files and slow scans. We need a standard layout to keep ingestion and downstream loading efficient.

## Decision

- Organize datasets under `data/<chain_id>/<dataset>/`.
- Partition by time-derived or block-range keys appropriate to the dataset (e.g., `block_date=YYYY-MM-DD` or `block_range_start=N`), avoiding over-fragmentation.
- Keep per-dataset schemas versioned; include schema version in metadata and optionally in partition path when breaking changes occur.
- Store companion metadata (checkpoints, schema, stats) alongside dataset root.

## Consequences

Positive
- Predictable file discovery for loaders and analytics jobs.
- Partition pruning for common queries; avoids small-file explosion.
- Easier multi-chain support via top-level chain id.

Negative
- Requires upfront partition strategy per dataset.
- Schema versioning adds management overhead.

## Alternatives Considered

- Flat per-block files with no partitioning: rejected (too many files, poor scan perf).
- Single monolithic Parquet per dataset: rejected (no pruning, update contention).

# ADR-002: Parquet-First Ingestion Pipeline

## Status

Accepted

## Context

Collector receives RPC data and must preserve raw fidelity while enabling cheap reprocessing. Writing directly to Parquet (columnar, compressed) gives durable, schema-friendly storage independent of downstream databases. We still need fast SQL access for analytics, but that can be loaded after durable landing.

## Decision

- Land all ingested data into partitioned Parquet datasets (per chain/dataset).
- Keep Parquet as the source of truth; treat SQL targets as derivatives.
- Maintain schemas explicitly (pyarrow schema files) and evolve via versioned schema definitions.
- Use append-only writes; forbid in-place mutation except controlled compaction.

## Consequences

Positive
- Cheap replay into any downstream warehouse (Postgres, DuckDB, BigQuery).
- Stable, self-describing storage; schema drift is controlled.
- Columnar compression reduces storage and speeds scans.

Negative
- Requires explicit load/transform steps to serve APIs.
- Partition planning matters to avoid small-file problems.

## Alternatives Considered

- Write straight to SQL and export to Parquet later: rejected; loses raw fidelity and couples ingestion to SQL availability.
- JSONL landing then Parquet conversion: adds latency and storage without benefit.

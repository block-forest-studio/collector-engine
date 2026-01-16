# ADR-0001: Parquet-first data ingestion before SQL loading

## Status

Accepted

## Context

Collector Engine ingests large volumes of on-chain data (logs, transactions, receipts, blocks) from EVM-compatible blockchains via RPC providers.

There were two possible ingestion strategies:

1. Direct-to-DB ingestion
Fetch data from RPC and insert it directly into PostgreSQL tables (raw / analytics).

2. Parquet-first ingestion (chosen)
Fetch data from RPC, persist immutable datasets as Parquet files, and load them into PostgreSQL using bulk COPY operations.

The system is designed to support:
- historical backfills,
- protocol additions,
- schema evolution,
- reprocessing with new logic,
- deterministic indexing.

## Decision

Collector Engine persists all collected blockchain data first into immutable Parquet datasets.
PostgreSQL tables are populated only by loading from these Parquet files using bulk COPY operations.

PostgreSQL is not treated as the primary system of record.
Parquet datasets are the canonical, replayable source of truth for downstream processing.

## Rationale

1. Deterministic replay and reprocessing

Parquet files preserve the original raw data exactly as retrieved from the blockchain, including:
- ordering (block_number, log_index),
- missing or NULL values,
- historical quirks of RPC responses.

This enables:
- deterministic reprocessing of historical data,
- safe re-running of indexers after logic changes,
- re-decoding events with updated ABIs,
- partial replays of selected block ranges.

With direct-to-DB ingestion, the original input stream is lost once data is normalized, deduplicated, or updated via ON CONFLICT logic.

2. Safe and precise backfills

Backfill scenarios include:
- adding support for new protocols,
- extending indexed history,
- correcting decoding or enrichment bugs,
- introducing new analytics tables.

Parquet-first ingestion allows:
- replaying only specific block ranges,
- isolating backfills from live ingestion,
- avoiding re-fetching data from RPC providers.

Databases typically store state, not events.
Parquet acts as an immutable event log.

3. Performance and scalability

Writing Parquet:
- is append-friendly,
- avoids transactional overhead,
- scales linearly with data volume.

Bulk loading via COPY:
- is significantly faster than batched INSERTs,
- minimizes WAL and index maintenance overhead,
- keeps database load predictable and controllable.

Direct inserts during ingestion would:
- couple ingestion speed to database performance,
- increase operational risk under high throughput,
- complicate retry and resume logic.

4. Clear separation of responsibilities

This architecture enforces a clean split:

| Layer | Responsibility |
|-------|----------------|
| Collector Engine | Data acquisition and normalization |
| Parquet datasets | Immutable system of record |
| PostgreSQL raw schema | Relational access and joins |
| Analytics schema | Derived projections and query optimization |

Collector Engine does not own analytics semantics and does not treat the database as authoritative storage.

---

5. Operational robustness

Parquet-first ingestion enables:
- offline processing,
- recovery after partial failures,
- inspection of raw datasets for debugging,
- repeatable pipelines independent of database state.

The database can be dropped and rebuilt from Parquet at any time.

## Consequences

Positive
- Deterministic, replayable pipelines
- Safer schema evolution
- Easier debugging and audits
- Better performance characteristics
- Decoupling of ingestion from analytics

Negative

- Increased storage usage (Parquet + SQL)
- Additional pipeline step (COPY)
- Slightly higher conceptual complexity

These trade-offs are acceptable given the system’s long-term analytics and indexing goals.

## Alternatives Considered

Direct-to-DB ingestion

Rejected due to:
- loss of immutable raw input,
- harder backfills,
- coupling ingestion to database performance,
- reduced determinism.

## Event streaming systems (Kafka, Pulsar)

Deferred:
- unnecessary operational complexity at current scale,
- Parquet provides sufficient replay and durability guarantees.

## Notes

This decision aligns Collector Engine with established data engineering patterns:
- event log + materialized views,
- lakehouse-style ingestion,
- batch-first processing for analytical systems.

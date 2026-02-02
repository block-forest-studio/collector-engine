# Collector Engine PRD

## Overview
Collector Engine ingests EVM chain data into durable Parquet datasets and canonical SQL tables. It is the source-of-truth data layer feeding indexer-engine and API surfaces. The service must be replayable, idempotent, and storage-agnostic while supporting multi-chain and protocol-scoped datasets.

## Goals
- Deterministically collect blocks, logs, transactions, and receipts for supported chains.
- Persist raw fidelity in Parquet (partitioned) and mirror canonical tables in `raw` (and `analytics.blocks`).
- Ensure resume/replay without duplication via checkpoints and idempotent writes.
- Keep ingestion protocol-neutral; protocol metadata handled via registry, not baked into pipelines.
- Provide CLI/tasks to run backfills and incremental catch-up.

## Non-Goals
- ABI decoding or protocol-specific projections (handled by indexer-engine/domain indexers).
- API serving; Collector is headless/batch oriented.
- Analytics transformations beyond block metadata.

## Users & Use Cases
- **Data platform engineers**: backfill and operate ingestion jobs across chains/contracts.
- **Indexer engineers**: rely on stable `raw.*` tables and Parquet datasets to build staging/analytics/domain projections.
- **Analysts**: may query Parquet for exploratory work; expect schema stability.

## Scope (What to Build)
- **Datasets**
  - Contract-scoped: logs, transactions, receipts → Parquet + `raw.logs`, `raw.transactions`, `raw.receipts`.
  - Chain-scoped: blocks → Parquet + `analytics.blocks`.
- **Pipelines**
  - Block-range collection tasks for each dataset (backfill + incremental).
  - Buffer → write Parquet → load SQL (if enabled) per dataset.
  - Checkpointing per dataset/chain to resume safely.
- **Configurability**
  - RPC endpoints, chain IDs, contract addresses, block ranges, batch sizes, output roots, partitions.
  - Feature flags for Parquet-only or Parquet+SQL writes.
- **Execution surfaces**
  - CLI/tasks for scheduled or ad-hoc runs.
  - Factories/registries for readers (RPC) and stores (Parquet, SQL loaders).

## Functional Requirements
- Support multi-chain ingestion with chain-specific RPC endpoints.
- Accept block range inputs (from/to) and infer next ranges from checkpoints for incremental runs.
- Enforce idempotent writes using deterministic file naming and `ON CONFLICT DO NOTHING` at SQL load stage.
- Partition Parquet by chain_id and time/block-range per dataset to avoid small-file explosion.
- Persist checkpoints (highest processed block/tx/log) alongside dataset metadata; restart resumes from next uncommitted offset.
- Validate binary fields (addresses, topics, hashes) and fail fast on malformed input.
- Expose progress/metrics (blocks processed, rows written, file counts, durations) per run.

## Data Contracts (canonical)
- `raw.logs`: `chain_id`, `block_number`, `transaction_hash`, `log_index`, `address`, `topic0..3`, `data`.
- `raw.transactions`: `chain_id`, `hash`, `block_number`, `transaction_index`, `from`, `to`, `value`, `type`, gas fields.
- `raw.receipts`: `chain_id`, `transaction_hash`, `status`, gas fields, cumulative gas, effective gas price.
- `analytics.blocks`: `chain_id`, `block_number`, `timestamp`, base fee, gas used/limit, parent hash, etc.
- Parquet schemas mirror these and are versioned (pyarrow schema files) with append-only semantics.

## Quality Attributes / NFRs
- **Reliability**: deterministic replay; no dupes/skips across restarts. Checkpoints stored durably.
- **Performance**: batch RPC requests; configurable batch size/parallelism; target throughput 1k–5k blocks/min on healthy RPC.
- **Scalability**: multiple chains/contract sets; partitioned storage; avoid tiny files via buffering thresholds.
- **Observability**: structured logs; per-run metrics; error surfaces with block range context.
- **Security**: secrets via env; no sensitive data persisted beyond blockchain payloads.

## Operational Requirements
- CLI supports: backfill range, tail/follow latest, resume from checkpoint, dry-run/validate mode.
- Config via env/flags; sane defaults in `.env.example`.
- Health: exit non-zero on partial failure; emit last safe checkpoint.
- Storage: Parquet path layout `data/<chain_id>/<dataset>/partition=*`; SQL connections via settings.

## Risks / Mitigations
- **RPC instability** → retry with backoff; small batch fallback; checkpoints minimize redo.
- **Schema drift** → explicit schema files + migrations; versioned schemas in metadata.
- **Small-file explosion** → buffering thresholds and compaction hooks.
- **Clock skew/late blocks** → prefer block-number sequencing over timestamps for ordering.

## Milestones (suggested)
1) MVP: logs/tx/receipts to Parquet with checkpoints; CLI backfill + resume.
2) SQL load: enable raw.* loading with idempotent inserts.
3) Blocks pipeline to `analytics.blocks`.
4) Hardening: metrics, retries/backoff tuning, compaction hooks, chain registry polish.

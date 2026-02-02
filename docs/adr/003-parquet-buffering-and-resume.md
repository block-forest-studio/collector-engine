# ADR-003: Parquet Buffering & Resume Semantics

## Status

Accepted

## Context

Collector streams blocks/logs; RPC hiccups and process restarts are expected. We need deterministic resume without duplicating or skipping records. Parquet writes are batch-oriented, so we must buffer, commit, and checkpoint carefully.

## Decision

- Buffer RPC results in memory/disk until reaching a target row count/size, then write a Parquet file per partition.
- Maintain checkpoint metadata (highest block/tx/log offset per dataset/chain) stored alongside dataset metadata.
- On restart, read checkpoints and resume from the next uncommitted offset; if a partial batch exists, rebuild buffer from RPC and rewrite.
- Keep idempotent file naming (e.g., partition + sequence) to allow safe reruns.

## Consequences

Positive
- Deterministic recovery without double-counting.
- Predictable file sizes and partition health.
- Clear operational surface for retries.

Negative
- Checkpoint management adds code paths and requires persistence.
- Slightly higher latency vs. streaming each record individually.

## Alternatives Considered

- Direct streaming writes per record: rejected due to tiny files and RPC coupling.
- Rely on RPC pagination alone: insufficient for durable guarantees.

# ADR-001: Ports & Adapters Layering for Collector Engine

## Status

Accepted

## Context

Collector Engine ingests on-chain data and must stay testable, replaceable, and storage-agnostic. A Clean Architecture / Ports & Adapters layout already exists (domain, application, infrastructure, interface). Formalizing it prevents accidental coupling (e.g., application importing concrete adapters) and keeps unit tests fast.

## Decision

Adopt strict layering with ports:
- **Domain**: pure functions and Protocol ports (`EvmReader`, `DatasetStore`, `DatasetLoader`), no I/O or third-party libs.
- **Application**: orchestration services (collect_*; run_pipeline; load_*_to_sql) depending only on domain ports/types.
- **Infrastructure**: concrete adapters (Web3 reader, Parquet store, Postgres COPY loader), protocol registry, config, schemas.
- **Interface**: CLI/tasks wiring config + adapters into application services.

Allowed deps: interface → infrastructure/application; application → domain; infrastructure → domain. Reverse edges are forbidden.

## Consequences

Positive
- High testability of domain/application without concrete infra.
- Swap/extend adapters (RPC provider, storage backend) without touching orchestration.
- Clear review guardrails for future contributions.

Negative
- Some code currently leaks infra types into application; requires ongoing discipline or refactor.

## Alternatives Considered

- Flat package with direct adapter imports in orchestration: rejected (tight coupling, harder testing).
- Heavy DI container: deferred; current scale doesn’t need it.

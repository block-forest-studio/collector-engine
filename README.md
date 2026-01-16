# 🪄 Collector Engine

Collector Engine is a modular on-chain data ingestion service for **EVM-based protocols**.

Its responsibility is to **extract raw blockchain data**, normalize it, and persist it in
**columnar Parquet datasets** and **canonical SQL tables**, forming the immutable foundation
for downstream indexing, analytics, and APIs.

Collector Engine writes:
- **contract-scoped immutable data** into the `raw` schema,
- **chain-scoped canonical block metadata** into `analytics.blocks`.

Collector Engine is **not an analytics engine**.
It is the **source-of-truth data ingestion layer**

---

## ⚙️ Architecture Overview

Collector Engine follows **Clean Architecture** and **Ports & Adapters** principles:

- strict separation of concerns
- deterministic, replayable pipelines
- replaceable infrastructure (RPC, storage, SQL loaders)

A detailed architecture description is available in [`architecture.md`](docs/architecture.md).

---

## 🧱 Layered Architecture

### 🔹 Domain (Core)

Pure, side-effect-free logic:

- **Ports (interfaces)**:
  - `EvmReader`
  - `DatasetStore`
  - `DatasetLoader`
- **Pure transformations**
- **No I/O**
- **Fully testable**

---

### 🔹 Application Layer (Use Cases)

Orchestrates domain logic:

- defines **collection workflows**
- enforces idempotency and resumability
- coordinates ports

---

### 🔹 Infrastructure Layer

Concrete adapters:

- Web3 adapters
- Parquet & PostgreSQL loaders
- Protocol registry
- Configuration & factories

---

### 🔹 Interface Layer

User-facing entry points:

- CLI
- Task runners

---

## 🔄 Data Lifecycle

1. Logs → Parquet + raw.logs
2. Transactions → Parquet + raw.transactions
3. Receipts → Parquet + raw.receipts
4. Blocks → Parquet + analytics.blocks

---

## 🗄️ Database Responsibility Model

| Schema      | Responsibility            |
|------------|---------------------------|
| raw        | Immutable blockchain data |
| staging    | Indexer responsibility   |
| analytics  | Partial (blocks only)     |

---

## 🧩 Database Bootstrap

Before loading data, initialize RAW schema:

```bash
make db-migrate
```

---

## 🚀 Installation

```bash
uv venv --python 3.13
source .venv/bin/activate
uv pip install -e ".[dev]"
```

---

## 🧰 CLI Usage

```bash
uv run python -m collector_engine.cli --help
```

---

## 🪪 License

Apache 2.0

# 🪄 Collector Engine

**Collector Engine** is a modular on-chain data indexer for **EVM-based DeFi protocols**.
It efficiently collects **logs, transactions, and receipts**, stores them in **Parquet**, and is designed to later export the processed data into **relational SQL tables** powering analytics and API services.

---

# ⚙️ Architecture Overview

Collector Engine is built using **Clean Architecture principles** with
**Ports & Adapters**, emphasizing:

- strict separation between domain logic and infrastructure
- replaceable Web3 and storage backends
- deterministic, testable data pipelines

A detailed architectural breakdown is available in [`ARCHITECTURE.md`](ARCHITECTURE.md).

### 🔹 Domain (Core)
Pure business logic:

- **Ports (interfaces)**: `EvmReader`, `DatasetStore`
- **Pure functions**: parsing, validation, mapping (e.g. `log_to_row`)
- **Domain models** (entities, if needed)

> No I/O. No Web3. No filesystem. Pure and testable.

---

### 🔹 Application Layer (Usecases)

Coordinates domain logic:

- Defines **what** happens during log/tx/receipt collection.
- Uses **ports**, not concrete implementations.
- No Web3 calls, no file access.

---

### 🔹 Infrastructure Layer

Concrete implementations of ports:

- **Web3 adapters** (`Web3EvmReader`)
- **Storage adapters** (Parquet, CSV, SQL)
- **Registry** loader (YAML protocol configs, ABI)
- **Config** (Pydantic settings)
- **Factories** (reader & storage selection)

> Replaceable at runtime — e.g., switch from Web3.py to another backend.

---

### 🔹 Interface Layer

User-facing entry points:

- **CLI** (`collector_engine.cli`)
- **Task runners** (`collector_tasks`)
- Future: REST API, schedulers, UI

> This layer wires together the adapters and usecases.

---

## 🔄 Data Lifecycle

Collector Engine processes on-chain data in three sequential stages:

1. **Logs**
   - Ethereum event logs are collected per contract and block range
   - Stored as `logs_*.parquet`

2. **Transactions**
   - Transactions are derived from collected logs
   - Stored as `txs_*.parquet`

3. **Receipts**
   - Transaction receipts are fetched and normalized
   - Stored as `receipts_*.parquet`

Each stage is resumable, idempotent, and can be re-run independently.

---

# 🚀 Installation

Requires **Python ≥ 3.13** and [uv](https://github.com/astral-sh/uv).

```bash
uv venv --python 3.13
source .venv/bin/activate
uv pip install -e ".[dev]"
```

# 🔧 Configuration

Collector uses environment variables to configure RPC providers and API keys.

Copy the example file:
```bash
cp .env.example .env
```
Fill in your RPC URLs (Alchemy, Infura, etc.) and API credentials.

# 🧰 Usage (CLI)

List available commands:
```bash
uv run python -m collector_engine.cli --help
```

Run the collector:
```bash
uv run python -m collector_engine.cli collector run
```

## 🧱 Makefile Commands

| Command          | Description                                               |
|------------------|-----------------------------------------------------------|
| `make run`       | Run the collector CLI                                     |
| `make lint`      | Run Ruff lint checks                                      |
| `make format`    | Auto-fix lint issues with Ruff                            |
| `make test`      | Run tests with pytest                                     |
| `make typecheck` | Run mypy static type checks                               |

All commands execute within the uv-managed environment.

---

## 🧪 Testing & Quality

Run tests:

```bash
pytest
```

Lint and type check:
```bash
pre-commit run --all-files
```

Tooling Stack

🧹 Ruff — linting & formatting
🆎 Mypy — static typing
🧪 Pytest — testing
🔄 Pre-commit — automated quality checks

# 🤝 Contributing

Contributions are welcome!
See CONTRIBUTING.md for guidelines.

We welcome:

- new DeFi protocol integrations
- new storage or Web3 adapters
- performance improvements
- documentation updates

# 🪪 License

Licensed under the Apache 2.0 License.

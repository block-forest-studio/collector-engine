# 🪄 Collector Engine

**Collector Engine** is a modular on-chain data collector for **EVM-based DeFi protocols**.
It efficiently fetches and processes **logs, transactions, and receipts**, stores them in **Parquet** format,
and will later map the data into **relational SQL tables** to power analytics and API services.

---

## ⚙️ Architecture

Collector Engine follows the **Functional Core, Procedural Shell** design pattern:

- **Functional Core** — pure, testable functions responsible for data parsing, transformation, and validation.
- **Procedural Shell** — I/O, CLI, configuration, logging, and orchestration.


---

## 🚀 Installation

Requires **Python ≥ 3.13** and [uv](https://github.com/astral-sh/uv).

```bash
# 1️⃣ Create a virtual environment
uv venv --python 3.13
source .venv/bin/activate    # Windows: .venv\Scripts\activate

# 2️⃣ Install dependencies (including dev tools)
uv pip install -e ".[dev]"
```

## 🔧 Configuration

Collector uses environment variables to connect to EVM RPC providers such as Alchemy, Infura, or Blast.

Create a .env file in the project root:

```
# .env
# Example: Ethereum Mainnet via Alchemy
```

## 🧰 Usage (CLI)

Collector exposes a CLI built with Typer.

List available commands:
```
...
```

## 🧪 Testing & Quality

Run all tests:
```
pytest
```

Lint and type check:
```
pre-commit run --all-files
```

Tooling stack

🧹 Ruff – linting and formatting

🆎 Mypy – static type checking

🧪 Pytest – testing framework

🔄 Pre-commit – automatic checks before commits

## 🤝 Contributing

Contributions are welcome!
See CONTRIBUTING.md
 for development guidelines.

Bug reports, feature requests (new DeFi protocols), and documentation improvements are encouraged 🙌

## 🪪 License

Licensed under the Apache 2.0 License.

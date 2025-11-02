# 🤝 Contributing to Collector Engine

Thanks for your interest in contributing to **Collector Engine**! 🎉
This project aims to build an open and extensible framework for collecting, processing, and transforming **on-chain DeFi data** from EVM-compatible protocols.

Your ideas, improvements, and bug fixes are very welcome.

---

## 🧱 Development Setup

### 1️⃣ Fork and clone the repository
```bash
git clone https://github.com/<your-username>/collector-engine.git
cd collector-engine
```

### 2️⃣ Create a virtual environment (with uv)

```
uv venv --python 3.13
source .venv/bin/activate    # or .venv\Scripts\activate on Windows
uv pip install -e ".[dev]"
```

### 3️⃣ Configure environment variables

Copy .env.example to .env and update your provider credentials:
```
...
```

## 🌿 Branching Model

We follow a simplified trunk-based workflow:

main — always stable and releasable

feature branches — for new features, fixes, or improvements

```
git checkout -b feature/add-new-contract
```

Commit small, focused changes, and use conventional commits
:

- feat: new feature
- fix: bug fix
- docs: documentation change
- refactor: code improvement

## 🧹 Code Style & Linting

We use several tools to ensure clean, consistent code:

| Tool | Purpose |
|-----------|-----------|
| Ruff   | Linting & auto-formatting   |
| Mypy   |  Static type checking   |
| Pre-commit   |  Runs quality checks before commits   |

## 🧪 Testing

Unit tests are written with pytest.

Run all tests locally:
```
pytest
```

Run a single test file:
```
pytest collector_engine/tests/unit/test_logs_core.py
```

Please ensure that all tests pass and new functionality is covered by appropriate tests.

## 🔍 Commit & Push

Commit your changes:
```
git add .
git commit -m "feat: add uniswap v4 collector"
```
Push to your fork:
```
git push origin feature/add-uniswap-v4
```

## 🧩 Pull Requests

When your feature or fix is ready:

1. Open a Pull Request (PR) to the main branch
2. Ensure the CI (tests + lint) passes
3. Provide a clear description of what the PR changes and why

Maintainers will review your PR — small, focused contributions get merged faster 🚀

## Tips for Contributors

- Keep PRs small and focused — easier to review and merge
- Use type hints everywhere possible
- Keep data-fetching logic separated from data-transformation logic
- Avoid secrets in commits — .env is already gitignored
- Feel free to open an Issue to discuss ideas before coding

## 🪪 License

By contributing to this repository, you agree that your contributions will be licensed under the Apache 2.0 License.

Thank you for helping improve Collector Engine! 💫
Let’s make on-chain data accessible and transparent for everyone.

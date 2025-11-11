run:
	python -m collector_engine.cli collector run

lint:
	uv run ruff check .

format:
	uv run ruff check --fix .

test:
	uv run pytest

typecheck:
	uv run mypy .

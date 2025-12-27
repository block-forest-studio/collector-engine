# Load variables from .env
ifneq (,$(wildcard .env))
	include .env
	export
endif

run:
	uv run python -m collector_engine.app.interface.cli collector run

lint:
	uv run ruff check .

format:
	uv run ruff check --fix .

test:
	uv run pytest

typecheck:
	uv run mypy .

db-ping:
	psql "$(POSTGRES_DSN)" -c "select 1;"

db-shell:
	psql "$(POSTGRES_DSN)"

db-migrate:
	psql "$(POSTGRES_DSN)" -f collector_engine/app/infrastructure/db/migrations/001_raw_schema.sql

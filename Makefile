# Thin wrapper over the Python CLI (D-010). Every target has an equivalent
# `python -m pipeline <command>` so the workflow also runs on Windows without make.

PYTHON ?= python
PRESET ?= small
PROFILE ?= conservative
SEED ?=

SEED_ARG := $(if $(SEED),--seed $(SEED),)

.PHONY: help install check-config docs generate ingest match-dbt match-spark compare-engines evaluate results test lint format

help:
	@echo "make install      Install pinned dependencies and the project (editable)"
	@echo "make check-config Validate config and reference data"
	@echo "make docs         Regenerate docs/DATA_GENERATION.md from config"
	@echo "make generate     Generate synthetic CDC loads    (PRESET=$(PRESET) SEED=...)"
	@echo "make ingest       Apply CDC events to current state"
	@echo "make match-dbt    Run matching with dbt on DuckDB (PROFILE=$(PROFILE))"
	@echo "make match-spark  Run matching with PySpark       (PROFILE=$(PROFILE))"
	@echo "make compare-engines  Check dbt and Spark outputs are identical (PROFILE=$(PROFILE))"
	@echo "make evaluate     Score results against ground truth (PROFILE=$(PROFILE))"
	@echo "make results      Match and evaluate every profile, then write docs/RESULTS.md"
	@echo "make test         Run the test suite"
	@echo "make lint         Lint and check formatting"

install:
	$(PYTHON) -m pip install -r requirements-lock.txt
	$(PYTHON) -m pip install --no-deps -e .

generate:
	$(PYTHON) -m pipeline generate --preset $(PRESET) $(SEED_ARG)

ingest:
	$(PYTHON) -m pipeline ingest

match-dbt:
	$(PYTHON) -m pipeline match-dbt --profile $(PROFILE)

match-spark:
	$(PYTHON) -m pipeline match-spark --profile $(PROFILE)

compare-engines:
	$(PYTHON) -m pipeline compare-engines --profile $(PROFILE)

evaluate:
	$(PYTHON) -m pipeline evaluate --profile $(PROFILE)

results:
	$(foreach p,conservative balanced aggressive,$(PYTHON) -m pipeline match-dbt --profile $(p) && $(PYTHON) -m pipeline evaluate --profile $(p) &&) true
	$(PYTHON) -m pipeline results

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

check-config:
	$(PYTHON) -m pipeline check-config

docs:
	$(PYTHON) -m pipeline docs

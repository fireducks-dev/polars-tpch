.DEFAULT_GOAL := help

PYTHONPATH=
SHELL=/bin/bash
VENV=.venv
VENV_BIN=$(VENV)/bin
VENV_FIREDUCKS = .venv-fireducks

.venv:  ## Set up Python virtual environment and install dependencies
	python3 -m venv $(VENV)
	$(MAKE) install-deps

.venv-fireducks:  ## Set up minimum virtual environment for running with FireDucks
	python3 -m venv $(VENV_FIREDUCKS)
	$(VENV_FIREDUCKS)/bin/pip install fireducks linetimer pydantic pydantic_settings

.PHONY: install-deps
install-deps: .venv  ## Install Python project dependencies
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip install --compile -r requirements.txt
	$(VENV_BIN)/uv pip install --compile -r requirements-dev.txt

.PHONY: bump-deps
bump-deps: .venv  ## Bump Python project dependencies
	$(VENV_BIN)/python -m pip install --upgrade uv
	$(VENV_BIN)/uv pip compile requirements.in > requirements.txt
	$(VENV_BIN)/uv pip compile requirements-dev.in > requirements-dev.txt

.PHONY: fmt
fmt:  ## Run autoformatting and linting
	$(VENV_BIN)/ruff check
	$(VENV_BIN)/ruff format
	$(VENV_BIN)/mypy

.PHONY: pre-commit
pre-commit: fmt  ## Run all code quality checks

.PHONY: tables
tables: .venv  ## Generate data tables
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -vf -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables/scale-$(SCALE_FACTOR)/
	$(VENV_BIN)/python -m scripts.prepare_data
	rm -rf data/tables/scale-$(SCALE_FACTOR)/*.tbl

.PHONY: tables-pyarrow
tables-pyarrow: .venv  ## Generate data tables using pyarrow
	$(MAKE) -C tpch-dbgen dbgen
	cd tpch-dbgen && ./dbgen -vf -s $(SCALE_FACTOR) && cd ..
	mkdir -p "data/tables_pyarrow/scale-$(SCALE_FACTOR)"
	mv tpch-dbgen/*.tbl data/tables_pyarrow/scale-$(SCALE_FACTOR)/
	PATH_TABLES=data/tables_pyarrow $(VENV_BIN)/python -m scripts.prepare_data_pyarrow
	rm -rf data/tables_pyarrow/scale-$(SCALE_FACTOR)/*.tbl

.PHONY: run-polars
run-polars: .venv  ## Run Polars benchmarks
	$(VENV_BIN)/python -m queries.polars

.PHONY: run-duckdb
run-duckdb: .venv  ## Run DuckDB benchmarks
	$(VENV_BIN)/python -m queries.duckdb

.PHONY: run-pandas
run-pandas: .venv  ## Run pandas benchmarks
	$(VENV_BIN)/python -m queries.pandas

.PHONY: run-pyspark
run-pyspark: .venv  ## Run PySpark benchmarks
	$(VENV_BIN)/python -m queries.pyspark

.PHONY: run-dask
run-dask: .venv  ## Run Dask benchmarks
	$(VENV_BIN)/python -m queries.dask

.PHONY: run-modin
run-modin: .venv  ## Run Modin benchmarks
	$(VENV_BIN)/python -m queries.modin

.PHONY: run-fireducks
run-fireducks: .venv-fireducks  ## Run FireDucks benchmarks
	PATH_TABLES=data/tables_pyarrow $(VENV_FIREDUCKS)/bin/python -m queries.fireducks

.PHONY: run-all
run-all: run-polars run-duckdb run-pandas run-pyspark run-dask run-modin  ## Run all benchmarks

.PHONY: plot
plot: .venv  ## Plot results
	$(VENV_BIN)/python -m scripts.plot_bars

.PHONY: clean
clean:  clean-tpch-dbgen clean-tables  ## Clean up everything
	$(VENV_BIN)/ruff clean
	@rm -rf .mypy_cache/
	@rm -rf .venv/
	@rm -rf output/
	@rm -rf spark-warehouse/

.PHONY: clean-tpch-dbgen
clean-tpch-dbgen:  ## Clean up TPC-H folder
	@$(MAKE) -C tpch-dbgen clean
	@rm -rf tpch-dbgen/*.tbl

.PHONY: clean-tables
clean-tables:  ## Clean up data tables
	@rm -rf data/tables/

.PHONY: help
help:  ## Display this help screen
	@echo -e "\033[1mAvailable commands:\033[0m"
	@grep -E '^[a-z.A-Z_0-9-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' | sort

PYTHON  ?= python
SRC      = src tests

.PHONY: help install format lint docs-check typecheck test build clean all

help:           ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:        ## install the package in editable mode with dev extras
	$(PYTHON) -m pip install -e ".[dev]"

format:         ## auto-format and fix lint issues
	ruff format $(SRC)
	ruff check --fix $(SRC)

lint:           ## check formatting and lint without mutations
	ruff format --check $(SRC)
	ruff check $(SRC)

docs-check:     ## ensure fetcher docs blocks exist for every fetcher
	PYDANTIC_DISABLE_PLUGINS=__all__ $(PYTHON) scripts/check_fetcher_docs.py
	PYDANTIC_DISABLE_PLUGINS=__all__ $(PYTHON) scripts/check_sink_docs.py

typecheck:      ## run mypy static analysis
	mypy src

test:           ## run the test suite with coverage
	PYDANTIC_DISABLE_PLUGINS=__all__ PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest -p pytest_cov --cov=data_ingestion --cov-report=term-missing

build:          ## build distribution packages
	$(PYTHON) -m build

clean:          ## remove build artefacts and caches
	rm -rf dist build site htmlcov .coverage .coverage.*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info"  -exec rm -rf {} + 2>/dev/null || true

all: format lint docs-check typecheck test  ## run the full quality pipeline

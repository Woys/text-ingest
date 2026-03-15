PYTHON  ?= python
SRC      = src tests examples

.PHONY: help install format lint typecheck test build clean all

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

typecheck:      ## run mypy static analysis
	mypy src

test:           ## run the test suite with coverage
	pytest --cov=data_ingestion --cov-report=term-missing

build:          ## build distribution packages
	$(PYTHON) -m build

clean:          ## remove build artefacts and caches
	rm -rf dist build site htmlcov .coverage .coverage.*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info"  -exec rm -rf {} + 2>/dev/null || true

all: format lint typecheck test  ## run the full quality pipeline

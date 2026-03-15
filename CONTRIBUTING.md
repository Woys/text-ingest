# Contributing

Thanks for contributing to `massive-data-ingestion`.

This guide explains how to develop, test, document, and submit changes with library-quality standards.

## Principles

1. Preserve public contracts unless explicitly planning a breaking change.
2. Prefer clear, typed, testable code over clever shortcuts.
3. Keep documentation and behavior aligned in the same PR.
4. Treat contributors and users with respect (`CODE_OF_CONDUCT.md`).

## Development Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

## Local Quality Gate

Run before opening a pull request:

```bash
make all
```

This executes:

- formatting and lint checks
- mypy static typing checks
- test suite with coverage

## Project Structure

- `src/data_ingestion/`: library source
- `tests/`: automated tests
- `examples/`: runnable usage examples and notebooks
- `docs/`: architecture, API contracts, operations, release, standards

## Contribution Types

Common contribution categories:

- bug fixes
- new source fetchers
- transform engine enhancements
- performance and reliability improvements
- documentation and examples

## Coding Standards

- Follow existing style and typing conventions.
- Add type hints for new/changed APIs.
- Keep functions focused and composable.
- Prefer explicit errors over silent failure.

## Testing Expectations

For non-trivial changes, add or update tests in `tests/`.

You should cover:

- expected behavior
- edge cases
- failure modes
- compatibility behavior (when API/contracts are touched)

## Documentation Expectations

Update docs whenever behavior changes.

At minimum, review:

- `README.md`
- `docs/API_CONTRACT.md` (if contract semantics changed)
- `docs/OPERATIONS.md` (if deployment/runtime behavior changed)
- `CHANGELOG.md`

## Changelog Policy

Add an entry under `[Unreleased]` in `CHANGELOG.md`.

Use Keep a Changelog categories (`Added`, `Changed`, `Fixed`, etc.).

## Pull Request Checklist

Before requesting review:

1. code builds and tests pass locally
2. docs are updated where needed
3. changelog entry is added
4. PR description explains motivation, approach, and compatibility impact

## Review Criteria

Maintainers prioritize:

- correctness and safety
- API contract stability
- test coverage adequacy
- operational clarity
- readability and maintainability

## Adding a New Fetcher

1. Add source config model in `config.py`.
2. Implement fetcher in `fetchers/` with `fetch_pages` and `normalize`.
3. Register fetcher in factory/registry import chain.
4. Add tests for config, fetching, normalization.
5. Update docs/examples if user-visible.

## Security and Responsible Disclosure

Do not open public issues for vulnerabilities.

Follow `SECURITY.md` for private reporting instructions.

## Need Help?

Open an issue with:

- what you are trying to do
- what you expected
- what happened instead
- reproducible snippet/logs where possible

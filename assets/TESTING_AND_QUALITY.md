# Testing and Quality Strategy

This document describes how quality is enforced for the library.

## Quality Objectives

The testing strategy aims to ensure:

- correctness of ingestion and normalization behavior
- stability of public API contracts
- reliability under error and resume scenarios
- confidence for incremental refactors

## Quality Gate

The canonical local/CI gate is:

```bash
make all
```

This runs:

- `ruff format` + `ruff check`
- `mypy src`
- `pytest --cov=data_ingestion`

## Test Layers

### 1) Unit Tests

Scope:

- model validation and serialization
- config validation rules
- transform engine operations and compatibility behavior
- sink serialization behavior

### 2) Pipeline Behavior Tests

Scope:

- aggregation across sources
- topic filtering and transform drops
- fail-fast vs continue-on-error semantics
- checkpoint/resume behavior and summary metrics

### 3) Adapter-Level Tests

Scope:

- Spark and Airflow helper semantics
- adapter integration with stream/pipeline behavior

### 4) Analysis/CLI Tests

Scope:

- CLI argument parsing and execution
- analysis helper behavior

## What to Test for New Changes

When submitting changes, include tests for:

- happy path behavior
- edge cases and malformed inputs
- failure modes and error surfaces
- backward compatibility when contracts are touched

## Coverage Guidance

Coverage is a signal, not the only goal.

Prioritize high-value assertions in critical paths:

- fetcher normalization
- transform semantics
- pipeline summary metrics
- checkpoint/resume safety

## Flakiness Prevention

Recommended practices:

- avoid live external network calls in tests
- use deterministic fixtures/mocks
- avoid time-sensitive assertions where possible
- keep tests independent and isolated

## Type Safety

`mypy` runs in strict mode for `src`.

Expectations:

- new code should be fully typed
- avoid suppressing type errors unless unavoidable
- justify any ignores in code review

## Linting and Formatting

Ruff enforces style and static checks.

If a rule is intentionally relaxed, document rationale in `pyproject.toml` and PR notes.

## Documentation as Quality

Any behavior change should update relevant docs in the same pull request.

Documentation drift is treated as a quality defect.

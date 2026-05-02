# Architecture

This document describes the technical architecture, contracts, and tradeoffs of `massive-data-ingestion`.

## Goals

The library is designed around four core goals:

1. Provide a consistent ingestion interface across heterogeneous APIs.
2. Keep runtime paths explicit: raw streaming, normalized pipeline, and declarative transforms.
3. Maintain predictable output contracts (`NormalizedRecord`) for downstream consumers.
4. Support production operation with observability and resumability.

## System Context

The project sits between external data providers and downstream data consumers.

- Inputs: HTTP APIs (OpenAlex, Crossref, NewsAPI, Hacker News, Federal Register, SEC EDGAR).
- Processing: fetch, normalize, filter, transform, dedupe, optional full-text enrichment.
- Outputs: JSONL/CSV/Parquet through sinks or user-managed orchestration tools.

## Layered Design

The codebase is intentionally layered to reduce coupling.

1. `fetchers/` (provider-specific):
- Owns source-specific API behavior and normalization logic.
- Implements `BaseFetcher` contract.

2. `pipeline.py` (orchestration core):
- Builds and executes fetchers.
- Applies topic filtering and declarative transforms.
- Handles sink writes, batch flush, errors, and checkpoints.

3. `sinks/` (output concerns):
- Encapsulates destination format and serialization details.
- Keeps I/O semantics outside pipeline orchestration.

4. `adapters/` (orchestration integration):
- Airflow/Spark helpers for framework-specific entry points.

5. `models.py` / `config.py` (contracts):
- Pydantic schemas define runtime data and configuration boundaries.

## Primary Data Paths

### 1) Raw Streaming Path

`stream_records(..., raw=True)`

- Fetchers yield provider-native payloads with minimal processing.
- Best for teams that need full control in Spark/Airflow/custom jobs.
- No transform spec is applied in this mode.

### 2) Normalized Streaming Path

`stream_records(..., raw=False)` or `stream_transformed_records(...)`

- Fetchers normalize each item into `NormalizedRecord`.
- Optional `transform_spec` is compiled and applied by `TransformationEngine`.
- Suitable when users want consistent schema without writing transform functions.

### 3) Managed Pipeline Path

`run_to_jsonl(...)` / `run_to_jsonl_with_full_text(...)`

- Full managed flow: build fetchers, run pipeline, write sink, return `PipelineSummary`.
- Optional checkpoint/resume support for source-level resumability.

## Normalization Contract

Each fetcher must normalize source payloads into `NormalizedRecord`.

Key invariants:

- `source` is always present and non-empty.
- Field names are stable across all providers.
- `raw_payload` keeps source-native details for audit/debug use.
- `record_type` is explicit (`article`, `news`, `preprint`).

This contract allows downstream systems to use a single schema regardless of input source.

## Transform Engine Design

The transform engine is declarative and library-executed.

Supported operations:

- `require_fields`
- `include_terms`
- `exclude_terms`
- `assign_topic_from_terms`
- `set_field`
- `dedupe`

Design constraints:

- No arbitrary user code execution in transforms.
- Stateful dedupe is scoped to one engine instance (run-local behavior).
- Spec versioning exists to support future DSL evolution.

Backward compatibility:

- Missing `version` defaults to `1`.
- Legacy `steps` alias is accepted for `transforms`.

## Pipeline Execution Model

Per-source processing loop:

1. Iterate fetchers in order.
2. For each record: topic filter -> transform engine -> batch buffer.
3. Flush batches to sink based on configured batch size.
4. Record per-source metrics (`seen`, `kept`, dropped counts).

Error handling:

- `fail_fast=True`: first source failure aborts run.
- `fail_fast=False`: source error is recorded; pipeline continues.

## Checkpoint/Resume Model

Checkpointing is source-granular.

- Checkpoint file stores a sorted list of completed source names.
- After each successful source, checkpoint is persisted.
- `resume=True` skips any source already marked completed.

Tradeoff:

- Source-level checkpoints are simple and robust.
- Fine-grained record offsets are intentionally not implemented yet.

## Observability Model

`PipelineSummary` returns run-level and source-level metrics:

- `total_records`, `by_source`, `failed_sources`
- `by_source_stats` with seen/kept/drop/skipped counters
- checkpoint metadata (`resumed_from_checkpoint`, `checkpoint_entries`)

Structured logs provide detailed run traces (start/finish/flush/fail events).

## Extension Points

### Add a New Fetcher

1. Define source config in `config.py`.
2. Implement fetcher class in `fetchers/`.
3. Register fetcher and add factory imports.
4. Add tests for fetching, normalization, and config validation.

### Add a New Sink

1. Implement `BaseSink` contract in `sinks/`.
2. Add config model and sink tests.
3. Ensure deterministic serialization semantics.

### Add a New Transform Operation

1. Extend transform spec model.
2. Implement execution behavior in `TransformationEngine.apply`.
3. Add migration/compatibility tests.

## Performance and Scalability

Current scaling model:

- Streaming iteration minimizes in-memory retention.
- Batched sink writes reduce I/O overhead.
- Full-text enrichment is optional and worker-limited.

Known limits:

- Pipeline processing is currently single-process orchestration.
- Dedupe state is in-memory for the run.
- Checkpoints are source-level, not record-level.

## Architectural Principles

1. Explicit contracts over implicit behavior.
2. Backward compatibility where practical.
3. User control at boundaries, library guarantees in core paths.
4. Operational safety (resumability, clear failure modes, deterministic outputs).

## Non-Goals (Current Scope)

- Distributed execution engine.
- Exactly-once semantics across multi-process workers.
- Arbitrary user code execution inside transform DSL.

These can be layered in future versions without breaking current contracts.

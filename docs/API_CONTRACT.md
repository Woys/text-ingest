# API Contract and Compatibility

This document defines the public API contract and compatibility policy for `massive-data-ingestion`.

## Compatibility Policy

The project follows Semantic Versioning.

- Patch (`x.y.Z`): bug fixes, docs, internal refactors with no public API break.
- Minor (`x.Y.z`): backward-compatible new capabilities.
- Major (`X.y.z`): intentional breaking changes.

Public API break means any incompatible change to:

- exported Python symbols
- function signatures
- model fields used by consumers
- expected output schema or semantics

## Public Python API Surface

The canonical public API is the package export list in `src/data_ingestion/__init__.py`.

Consumers should import from `data_ingestion` instead of deep internal modules when possible.

Examples:

- `run_to_jsonl`
- `stream_records`
- `stream_transformed_records`
- `TransformationEngine`
- `PipelineSummary`

## `NormalizedRecord` Contract

`NormalizedRecord` is the core schema contract used by normalized paths.

Stability expectations:

- Existing fields are not removed in minor releases.
- Existing field names and types remain stable.
- New optional fields may be added in minor releases.

Key fields:

- identity: `source`, `external_id`
- content: `title`, `abstract`, `full_text`
- metadata: `authors`, `published_date`, `url`, `topic`, `record_type`
- lineage: `raw_payload`, `fetched_at`

## `PipelineSummary` Contract

`PipelineSummary` contains run results and operational metrics.

Stable fields:

- `total_records`
- `by_source`
- `failed_sources`
- `by_source_stats`
- `output_target`
- `resumed_from_checkpoint`
- `checkpoint_path`
- `checkpoint_entries`

`by_source_stats` entries (`SourceRunStats`):

- `seen`
- `kept`
- `dropped_by_topic`
- `dropped_by_transform`
- `checkpoint_skipped`

## Streaming API Semantics

### `stream_records`

- `raw=True` yields `(source, raw_payload_dict)`
- `raw=False` yields `(source, NormalizedRecord)`
- `transform_spec` is only valid when `raw=False`

### `stream_transformed_records`

- always yields normalized records
- always applies declarative transform spec

## Declarative Transform Spec Contract

Supported structure:

```json
{
  "version": 1,
  "transforms": [
    {"op": "require_fields", "fields": ["title", "url"]},
    {"op": "dedupe", "keys": ["source", "external_id", "url"]}
  ]
}
```

Backward compatibility:

- if `version` is omitted, `1` is assumed
- legacy `steps` alias is accepted for `transforms`

Forward compatibility:

- unknown future versions are rejected explicitly

## Checkpoint Contract

Checkpoint files are JSON documents with completed source names.

Example:

```json
{
  "version": 1,
  "updated_at": "2026-03-16T00:00:00+00:00",
  "completed_sources": ["crossref", "openalex"]
}
```

Compatibility rules:

- additional fields may be added in future versions
- existing keys maintain meaning for version `1`

## Deprecation Policy

When a feature must be changed incompatibly:

1. mark behavior as deprecated in docs/changelog
2. provide migration path and timeline
3. remove only in the next major release

Exceptions are limited to security-critical emergency fixes.

## Error Contract

Common error families:

- configuration errors (`ConfigurationError` / validation errors)
- source failures (`FetcherError` and wrapped runtime exceptions)
- pipeline failures (`PipelineError`)
- sink failures (`SinkError`)

Behavior:

- with `fail_fast=True`, source failure aborts run
- with `fail_fast=False`, failures are recorded in `failed_sources`

## Contract Testing Expectations

Any API-affecting change should include tests for:

- backward compatibility of function signatures
- output schema invariants
- transform spec compatibility/migration behavior
- summary metrics and checkpoint semantics

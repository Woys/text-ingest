# Operations and Deployment Guide

This guide covers production operation, reliability, and monitoring practices.

## Runtime Modes

Supported operation modes:

1. CLI (`mdi-run`) for scheduled jobs and automation.
2. Python API for embedded or orchestrated use.
3. Adapter-based integration with Airflow/Spark.

## Deployment Topologies

Common deployment patterns:

- single scheduled container (cron/Kubernetes CronJob)
- Airflow DAG task execution
- Spark pre-ingestion stage feeding DataFrame workflows

## Production Checklist

Before deploying:

1. pin package version
2. run `make all`
3. validate spec file against staging APIs
4. verify output/checkpoint path permissions
5. configure alerting on failures and low-volume anomalies

## CLI Operation

Minimal run:

```bash
mdi-run run \
  --spec-file deploy/spec.json \
  --output-file /var/data/mdi/output.jsonl \
  --overwrite \
  --log-level INFO
```

Date-bounded run:

```bash
mdi-run run \
  --spec-file deploy/spec.json \
  --output-file /var/data/mdi/output.jsonl \
  --start-date 2026-03-01 \
  --end-date 2026-03-15
```

## Python API Operation

Use the API path when you need transform and checkpoint controls.

```python
from data_ingestion.pipeline import run_to_jsonl

summary = run_to_jsonl(
    fetcher_specs=specs,
    output_file="/var/data/mdi/output.jsonl",
    transform_spec=transform_spec,
    checkpoint_path="/var/data/mdi/checkpoint.json",
    resume=True,
    fail_fast=False,
)
```

## Checkpoint and Resume Practices

Recommendations:

- use stable checkpoint paths per pipeline job
- do not share one checkpoint file across unrelated pipelines
- rotate or archive checkpoints when source roster changes significantly

Operational caveat:

- source-level checkpoints skip completed sources, not partial source progress

## Logging and Observability

Set log level via CLI or `MDI_LOG_LEVEL`.

Suggested ingestion metrics to track externally:

- run success/failure counts
- records processed per source
- drop ratios (`dropped_by_topic`, `dropped_by_transform`)
- resume skip count (`checkpoint_skipped`)
- run duration

`PipelineSummary` should be persisted or emitted to your monitoring layer.

## Capacity and Performance

Tuning knobs:

- `sink_write_batch_size`
- source page size (`per_page`, `rows`, `hits_per_page`)
- full-text settings (`max_chars`, worker count in resolver config)

Recommended approach:

1. start with conservative page sizes
2. profile bottlenecks (API latency vs sink I/O)
3. increase batch sizes incrementally
4. monitor failure rates and timeout behavior

## Data Management Practices

For output storage:

- separate raw and curated output directories
- include run date partitions in file paths
- configure retention policies
- back up critical outputs before destructive overwrites

## Failure Handling and Recovery

When a run fails:

1. inspect `failed_sources` in summary
2. verify source credentials and API quotas
3. rerun with `resume=True` and same checkpoint
4. if schema-level changes occurred, archive old checkpoint and restart cleanly

## Security Operations Notes

- never commit API keys
- prefer environment variables / secret managers
- run jobs with least-privilege file permissions
- avoid writing secrets to logs or output payloads

Refer to `SECURITY.md` for disclosure and vulnerability handling.

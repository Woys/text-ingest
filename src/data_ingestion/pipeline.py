from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from data_ingestion.config import (
    JsonlSinkConfig,
    PipelineConfig,
    RuntimeOptimizationConfig,
)
from data_ingestion.exceptions import PipelineError
from data_ingestion.factories import build_fetchers
from data_ingestion.full_text import FullTextResolver
from data_ingestion.logging_utils import get_logger
from data_ingestion.models import NormalizedRecord, PipelineSummary, SourceRunStats
from data_ingestion.sinks.jsonl import JsonlSink
from data_ingestion.transforms import TransformationEngine

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Iterator

    from data_ingestion.fetchers.base import BaseFetcher
    from data_ingestion.sinks.base import BaseSink

logger = get_logger(__name__)

_ASYNC_PROGRESS_EVERY = 100


class _AsyncDone:
    pass


_ASYNC_DONE = _AsyncDone()


class DataDumperPipeline:
    def __init__(
        self,
        sink: BaseSink,
        config: PipelineConfig | None = None,
        transform_engine: TransformationEngine | None = None,
        checkpoint_path: str | None = None,
        resume: bool = False,
    ) -> None:
        if resume and checkpoint_path is None:
            raise ValueError("resume=True requires checkpoint_path")

        self.sink = sink
        self.config = config or PipelineConfig()
        self.transform_engine = transform_engine
        self.checkpoint_path = checkpoint_path
        self.resume = resume
        self._preserve_raw_payload = (
            self.config.runtime.write_raw_payload
            or not self.config.runtime.drop_raw_payload_after_transform
            or (
                self.transform_engine is not None
                and self.transform_engine.uses_raw_payload()
            )
        )
        self.full_text_resolver = (
            FullTextResolver() if self.config.runtime.enrich_full_text else None
        )

        # Cache to prevent redundant parsing and casting of topic configs per record
        self._fetcher_topic_configs: dict[
            int, tuple[list[str], list[str], str | None]
        ] = {}

        logger.info(
            "Pipeline initialized fail_fast=%s enrich_full_text=%s "
            "sink_write_batch_size=%d transforms_enabled=%s resume=%s checkpoint=%s",
            self.config.fail_fast,
            self.config.runtime.enrich_full_text,
            self.config.runtime.sink_write_batch_size,
            self.transform_engine is not None,
            self.resume,
            self.checkpoint_path,
        )

    def _prepare_for_batch(self, record: NormalizedRecord) -> NormalizedRecord:
        if self._preserve_raw_payload:
            return record
        record.raw_payload = {}
        return record

    def _load_checkpoint_sources(self) -> set[str]:
        if self.checkpoint_path is None:
            return set()

        path = Path(self.checkpoint_path)
        if not path.exists():
            return set()

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(f"Cannot read checkpoint file '{path}': {exc}") from exc

        if not isinstance(payload, dict):
            raise PipelineError(f"Checkpoint file '{path}' must contain a JSON object")

        completed = payload.get("completed_sources", [])
        if not isinstance(completed, list) or not all(
            isinstance(item, str) for item in completed
        ):
            raise PipelineError(
                f"Checkpoint file '{path}' has invalid 'completed_sources' format"
            )

        return set(completed)

    def _write_checkpoint_sources(self, completed_sources: set[str]) -> None:
        if self.checkpoint_path is None:
            return

        path = Path(self.checkpoint_path)
        payload = {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "completed_sources": sorted(completed_sources),
        }

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        except OSError as exc:
            raise PipelineError(
                f"Cannot write checkpoint file '{path}': {exc}"
            ) from exc

    def _process_batch(self, batch: list[NormalizedRecord]) -> None:
        if not batch:
            return

        logger.debug("Processing batch size=%d", len(batch))

        if self.full_text_resolver is not None:
            logger.debug("Enriching batch with full text size=%d", len(batch))
            batch = self.full_text_resolver.enrich_many(batch)

        self.sink.write_many(batch)
        logger.debug("Batch written size=%d", len(batch))

    @staticmethod
    def _record_text(record: NormalizedRecord) -> str:
        parts: list[str] = []
        for value in (
            record.title,
            record.abstract,
            record.full_text,
            record.url,
            record.full_text_url,
            record.external_id,
        ):
            if value:
                parts.append(value)
        parts.extend(author for author in record.authors if author)
        return " ".join(parts).lower()

    def _topic_decision(
        self,
        fetcher: BaseFetcher,
        record: NormalizedRecord,
    ) -> tuple[bool, str | None]:
        # Fetch cached config to avoid repeated list() casts and getattr on every record
        fetcher_id = id(fetcher)
        if fetcher_id not in self._fetcher_topic_configs:
            fetcher_config = getattr(fetcher, "config", None)
            inc = list(getattr(fetcher_config, "topic_include", []))
            exc = list(getattr(fetcher_config, "topic_exclude", []))

            query = getattr(fetcher_config, "query", None)
            q_val = None
            if isinstance(query, str):
                stripped_query = query.strip()
                if stripped_query:
                    q_val = stripped_query.lower()

            self._fetcher_topic_configs[fetcher_id] = (inc, exc, q_val)

        include_terms, exclude_terms, q_val = self._fetcher_topic_configs[fetcher_id]

        matched_topic: str | None = None

        if include_terms or exclude_terms:
            haystack = self._record_text(record)

            if include_terms:
                for term in include_terms:
                    if term in haystack:
                        matched_topic = term
                        break
                if matched_topic is None:
                    logger.debug(
                        "Record filtered by topic_include source=%s external_id=%s",
                        fetcher.source_name,
                        record.external_id,
                    )
                    return False, None

            if exclude_terms and any(term in haystack for term in exclude_terms):
                logger.debug(
                    "Record filtered by topic_exclude source=%s external_id=%s",
                    fetcher.source_name,
                    record.external_id,
                )
                return False, None

        if matched_topic is not None:
            return True, matched_topic

        # Prefer topic inferred by fetchers over query fallback.
        if isinstance(record.topic, str):
            stripped_topic = record.topic.strip()
            if stripped_topic:
                return True, stripped_topic

        if q_val:
            return True, q_val

        return True, None

    def run(self, fetchers: Iterable[BaseFetcher]) -> PipelineSummary:
        logger.info("Pipeline run started")
        total_records = 0
        by_source: dict[str, int] = {}
        by_source_stats: dict[str, SourceRunStats] = {}
        failed_sources: dict[str, str] = {}
        batch_size = self.config.runtime.sink_write_batch_size

        completed_sources = self._load_checkpoint_sources() if self.resume else set()
        checkpoint_entries = len(completed_sources)

        try:
            for fetcher in fetchers:
                source_name = fetcher.source_name

                if source_name in completed_sources:
                    logger.info(
                        "Skipping source=%s due to checkpoint resume", source_name
                    )
                    by_source.setdefault(source_name, 0)
                    by_source_stats[source_name] = SourceRunStats(checkpoint_skipped=1)
                    continue

                logger.info("Starting source=%s", source_name)
                source_count = 0
                seen_count = 0
                dropped_by_topic = 0
                dropped_by_transform = 0
                batch: list[NormalizedRecord] = []
                source_failed = False

                try:
                    for record in fetcher.fetch_all():
                        seen_count += 1
                        keep, topic = self._topic_decision(fetcher, record)
                        if not keep:
                            dropped_by_topic += 1
                            continue

                        if topic is not None:
                            record.topic = topic

                        if self.transform_engine is not None:
                            transformed = self.transform_engine.apply(record)
                            if transformed is None:
                                dropped_by_transform += 1
                                continue
                            record = transformed

                        record = self._prepare_for_batch(record)
                        batch.append(record)
                        source_count += 1
                        total_records += 1

                        if len(batch) >= batch_size:
                            logger.debug(
                                "Flushing batch source=%s size=%d",
                                source_name,
                                len(batch),
                            )
                            self._process_batch(batch)
                            batch = []

                    if batch:
                        logger.debug(
                            "Flushing final batch source=%s size=%d",
                            source_name,
                            len(batch),
                        )
                        self._process_batch(batch)

                except Exception as exc:
                    source_failed = True
                    failed_sources[source_name] = str(exc)
                    logger.exception("Fetcher failed for source=%s", source_name)
                    if self.config.fail_fast:
                        raise PipelineError(
                            f"Pipeline aborted: fetcher '{source_name}' failed"
                        ) from exc

                by_source[source_name] = by_source.get(source_name, 0) + source_count
                by_source_stats[source_name] = SourceRunStats(
                    seen=seen_count,
                    kept=source_count,
                    dropped_by_topic=dropped_by_topic,
                    dropped_by_transform=dropped_by_transform,
                    checkpoint_skipped=0,
                )

                if self.checkpoint_path is not None and not source_failed:
                    completed_sources.add(source_name)
                    self._write_checkpoint_sources(completed_sources)

                logger.info(
                    "Finished source=%s seen=%d kept=%d dropped_by_topic=%d "
                    "dropped_by_transform=%d",
                    source_name,
                    seen_count,
                    source_count,
                    dropped_by_topic,
                    dropped_by_transform,
                )
        finally:
            logger.debug("Closing sink")
            self.sink.close()

        output_target: str | None = getattr(
            getattr(self.sink, "config", None), "output_file", None
        )

        summary = PipelineSummary(
            total_records=total_records,
            by_source=by_source,
            failed_sources=failed_sources,
            by_source_stats=by_source_stats,
            output_target=output_target,
            resumed_from_checkpoint=self.resume,
            checkpoint_path=self.checkpoint_path,
            checkpoint_entries=checkpoint_entries,
        )
        logger.info(
            "Pipeline run completed total_records=%d failed_sources=%d output=%s",
            summary.total_records,
            len(summary.failed_sources),
            summary.output_target,
        )
        return summary


def _apply_date_overrides(
    fetcher_specs: list[dict[str, Any]],
    start_date: str | None,
    end_date: str | None,
) -> list[dict[str, Any]]:
    if not start_date and not end_date:
        return fetcher_specs

    logger.info(
        "Applying date overrides start_date=%s end_date=%s specs=%d",
        start_date,
        end_date,
        len(fetcher_specs),
    )

    result: list[dict[str, Any]] = []
    for spec in fetcher_specs:
        raw_config = spec.get("config")
        config = dict(raw_config) if isinstance(raw_config, dict) else {}

        if start_date:
            config.setdefault("start_date", start_date)
        if end_date:
            config.setdefault("end_date", end_date)
        result.append({**spec, "config": config})
    return result


def stream_records(
    fetcher_specs: list[dict[str, Any]],
    *,
    raw: bool = True,
    transform_spec: dict[str, Any] | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Iterator[tuple[str, dict[str, Any] | NormalizedRecord]]:
    """Stream records with minimal library-side processing.

    `raw=True` yields raw source payload dicts.
    `raw=False` yields normalized `NormalizedRecord` objects.
    """
    logger.info(
        "stream_records started raw=%s specs=%d start_date=%s end_date=%s "
        "transforms_enabled=%s",
        raw,
        len(fetcher_specs),
        start_date,
        end_date,
        transform_spec is not None,
    )

    if raw and transform_spec is not None:
        raise ValueError("transform_spec is only supported when raw=False")

    specs = _apply_date_overrides(fetcher_specs, start_date, end_date)
    fetchers = build_fetchers(specs)
    transform_engine = (
        TransformationEngine(transform_spec) if transform_spec is not None else None
    )

    for fetcher in fetchers:
        yielded = 0
        if raw:
            for item in fetcher.fetch_raw():
                yielded += 1
                yield fetcher.source_name, item
            logger.info(
                "stream_records source=%s yielded_raw=%d", fetcher.source_name, yielded
            )
            continue

        for record in fetcher.fetch_all():
            if transform_engine is not None:
                transformed = transform_engine.apply(record)
                if transformed is None:
                    continue
                record = transformed

            yielded += 1
            yield fetcher.source_name, record

        logger.info(
            "stream_records source=%s yielded_normalized=%d",
            fetcher.source_name,
            yielded,
        )


def stream_transformed_records(
    fetcher_specs: list[dict[str, Any]],
    *,
    transform_spec: dict[str, Any] | str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Iterator[tuple[str, NormalizedRecord]]:
    for source, record in stream_records(
        fetcher_specs,
        raw=False,
        transform_spec=transform_spec,
        start_date=start_date,
        end_date=end_date,
    ):
        assert isinstance(record, NormalizedRecord)
        yield source, record


async def _async_iter_fetcher_items(
    fetcher: BaseFetcher,
    *,
    raw: bool,
) -> AsyncIterator[tuple[str, dict[str, Any] | NormalizedRecord]]:
    if raw:
        async for item in fetcher.async_fetch_raw():
            yield fetcher.source_name, item
        return

    async for record in fetcher.async_fetch_all():
        yield fetcher.source_name, record


async def _async_stream_records_sequential(
    fetchers: list[BaseFetcher],
    *,
    raw: bool,
    transform_engine: TransformationEngine | None,
) -> AsyncIterator[tuple[str, dict[str, Any] | NormalizedRecord]]:
    for fetcher in fetchers:
        yielded = 0
        started = time.monotonic()
        logger.info(
            "async_stream_records source=%s started raw=%s",
            fetcher.source_name,
            raw,
        )
        async for source, item in _async_iter_fetcher_items(fetcher, raw=raw):
            if not raw and transform_engine is not None:
                assert isinstance(item, NormalizedRecord)
                transformed = transform_engine.apply(item)
                if transformed is None:
                    continue
                item = transformed

            yielded += 1
            if yielded % _ASYNC_PROGRESS_EVERY == 0:
                logger.info(
                    "async_stream_records source=%s yielded=%d elapsed_seconds=%.1f",
                    fetcher.source_name,
                    yielded,
                    time.monotonic() - started,
                )
            yield source, item

        logger.info(
            "async_stream_records source=%s completed yielded=%d elapsed_seconds=%.1f",
            fetcher.source_name,
            yielded,
            time.monotonic() - started,
        )


async def _async_stream_records_concurrent(
    fetchers: list[BaseFetcher],
    *,
    raw: bool,
    transform_engine: TransformationEngine | None,
    max_source_concurrency: int | None,
    max_async_queue_size: int,
) -> AsyncIterator[tuple[str, dict[str, Any] | NormalizedRecord]]:
    queue: asyncio.Queue[
        tuple[str, dict[str, Any] | NormalizedRecord | BaseException | _AsyncDone]
    ] = asyncio.Queue(maxsize=max_async_queue_size)
    concurrency = max_source_concurrency or len(fetchers) or 1
    semaphore = asyncio.Semaphore(concurrency)

    async def produce(fetcher: BaseFetcher) -> None:
        async with semaphore:
            yielded = 0
            started = time.monotonic()
            logger.info(
                "async_stream_records source=%s producer_started raw=%s",
                fetcher.source_name,
                raw,
            )
            try:
                async for source, item in _async_iter_fetcher_items(fetcher, raw=raw):
                    await queue.put((source, item))
                    yielded += 1
                    if yielded % _ASYNC_PROGRESS_EVERY == 0:
                        logger.info(
                            "async_stream_records source=%s queued=%d queue_size=%d "
                            "elapsed_seconds=%.1f",
                            fetcher.source_name,
                            yielded,
                            queue.qsize(),
                            time.monotonic() - started,
                        )
            except BaseException as exc:
                logger.exception(
                    "async_stream_records source=%s producer_failed queued=%d",
                    fetcher.source_name,
                    yielded,
                )
                await queue.put((fetcher.source_name, exc))
            finally:
                logger.info(
                    "async_stream_records source=%s producer_completed queued=%d "
                    "elapsed_seconds=%.1f",
                    fetcher.source_name,
                    yielded,
                    time.monotonic() - started,
                )
                await queue.put((fetcher.source_name, _ASYNC_DONE))

    tasks = [asyncio.create_task(produce(fetcher)) for fetcher in fetchers]
    remaining = len(tasks)

    try:
        while remaining:
            source, item = await queue.get()
            if isinstance(item, _AsyncDone):
                remaining -= 1
                continue

            if isinstance(item, BaseException):
                raise item

            if not raw and transform_engine is not None:
                assert isinstance(item, NormalizedRecord)
                transformed = transform_engine.apply(item)
                if transformed is None:
                    continue
                item = transformed

            yield source, item
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def async_stream_records(
    fetcher_specs: list[dict[str, Any]],
    *,
    raw: bool = True,
    transform_spec: dict[str, Any] | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    concurrent_sources: bool = False,
    max_source_concurrency: int | None = None,
    max_async_queue_size: int = 100,
) -> AsyncIterator[tuple[str, dict[str, Any] | NormalizedRecord]]:
    """Async sibling of `stream_records`.

    Fetching is delegated to fetcher async wrappers, which run existing sync
    source iterators in worker threads while preserving current quota controls.
    """
    logger.info(
        "async_stream_records started raw=%s specs=%d start_date=%s end_date=%s "
        "transforms_enabled=%s concurrent_sources=%s",
        raw,
        len(fetcher_specs),
        start_date,
        end_date,
        transform_spec is not None,
        concurrent_sources,
    )

    if raw and transform_spec is not None:
        raise ValueError("transform_spec is only supported when raw=False")
    if max_source_concurrency is not None and max_source_concurrency < 1:
        raise ValueError("max_source_concurrency must be >= 1")
    if max_async_queue_size < 1:
        raise ValueError("max_async_queue_size must be >= 1")

    specs = _apply_date_overrides(fetcher_specs, start_date, end_date)
    fetchers = build_fetchers(specs)
    transform_engine = (
        TransformationEngine(transform_spec) if transform_spec is not None else None
    )

    if concurrent_sources:
        async for item in _async_stream_records_concurrent(
            fetchers,
            raw=raw,
            transform_engine=transform_engine,
            max_source_concurrency=max_source_concurrency,
            max_async_queue_size=max_async_queue_size,
        ):
            yield item
        return

    async for item in _async_stream_records_sequential(
        fetchers,
        raw=raw,
        transform_engine=transform_engine,
    ):
        yield item


async def async_stream_transformed_records(
    fetcher_specs: list[dict[str, Any]],
    *,
    transform_spec: dict[str, Any] | str,
    start_date: str | None = None,
    end_date: str | None = None,
    concurrent_sources: bool = False,
    max_source_concurrency: int | None = None,
    max_async_queue_size: int = 100,
) -> AsyncIterator[tuple[str, NormalizedRecord]]:
    async for source, record in async_stream_records(
        fetcher_specs,
        raw=False,
        transform_spec=transform_spec,
        start_date=start_date,
        end_date=end_date,
        concurrent_sources=concurrent_sources,
        max_source_concurrency=max_source_concurrency,
        max_async_queue_size=max_async_queue_size,
    ):
        assert isinstance(record, NormalizedRecord)
        yield source, record


def run_to_jsonl(
    fetcher_specs: list[dict[str, Any]],
    output_file: str,
    *,
    append: bool = True,
    fail_fast: bool = True,
    enrich_full_text: bool = False,
    include_raw_payload: bool = False,
    sink_write_batch_size: int = 500,
    transform_spec: dict[str, Any] | str | None = None,
    checkpoint_path: str | None = None,
    resume: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
) -> PipelineSummary:
    logger.info(
        "run_to_jsonl called output=%s specs=%d append=%s fail_fast=%s",
        output_file,
        len(fetcher_specs),
        append,
        fail_fast,
    )

    specs = _apply_date_overrides(fetcher_specs, start_date, end_date)
    fetchers = build_fetchers(specs)

    pipeline_config = PipelineConfig(
        fail_fast=fail_fast,
        runtime=RuntimeOptimizationConfig(
            enrich_full_text=enrich_full_text,
            write_raw_payload=include_raw_payload,
            sink_write_batch_size=sink_write_batch_size,
        ),
    )

    sink = JsonlSink(
        JsonlSinkConfig(output_file=output_file, append=append),
        include_raw_payload=include_raw_payload,
    )

    transform_engine = (
        TransformationEngine(transform_spec) if transform_spec is not None else None
    )
    pipeline = DataDumperPipeline(
        sink=sink,
        config=pipeline_config,
        transform_engine=transform_engine,
        checkpoint_path=checkpoint_path,
        resume=resume,
    )
    summary = pipeline.run(fetchers)
    logger.info(
        "run_to_jsonl finished total_records=%d failed_sources=%d output=%s",
        summary.total_records,
        len(summary.failed_sources),
        summary.output_target,
    )
    return summary


def run_to_jsonl_with_full_text(
    fetcher_specs: list[dict[str, Any]],
    output_file: str,
    full_text_output_file: str,
    *,
    append: bool = True,
    fail_fast: bool = True,
    full_text_max_chars: int = 200_000,
    transform_spec: dict[str, Any] | str | None = None,
    checkpoint_path: str | None = None,
    resume: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
) -> PipelineSummary:
    from data_ingestion.config import FullTextResolutionConfig
    from data_ingestion.full_text import FullTextResolver

    logger.info(
        "run_to_jsonl_with_full_text called output=%s full_text_output=%s "
        "specs=%d append=%s fail_fast=%s full_text_max_chars=%d",
        output_file,
        full_text_output_file,
        len(fetcher_specs),
        append,
        fail_fast,
        full_text_max_chars,
    )

    specs = _apply_date_overrides(fetcher_specs, start_date, end_date)
    fetchers = build_fetchers(specs)

    pipeline_config = PipelineConfig(
        fail_fast=fail_fast,
        runtime=RuntimeOptimizationConfig(enrich_full_text=True),
    )

    sink = JsonlSink(
        JsonlSinkConfig(output_file=output_file, append=append),
        include_raw_payload=False,
    )

    transform_engine = (
        TransformationEngine(transform_spec) if transform_spec is not None else None
    )
    pipeline = DataDumperPipeline(
        sink=sink,
        config=pipeline_config,
        transform_engine=transform_engine,
        checkpoint_path=checkpoint_path,
        resume=resume,
    )
    pipeline.full_text_resolver = FullTextResolver(
        FullTextResolutionConfig(max_chars=full_text_max_chars)
    )

    summary = pipeline.run(fetchers)
    logger.info(
        "run_to_jsonl_with_full_text finished total_records=%d "
        "failed_sources=%d output=%s",
        summary.total_records,
        len(summary.failed_sources),
        summary.output_target,
    )
    return summary

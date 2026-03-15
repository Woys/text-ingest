from __future__ import annotations

import pytest

from data_ingestion.models import NormalizedRecord
from data_ingestion.transforms import TransformationEngine


def test_transformation_engine_filters_by_include_terms() -> None:
    engine = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "include_terms",
                    "terms": ["ai"],
                    "fields": ["title"],
                }
            ]
        }
    )

    keep = engine.apply(
        NormalizedRecord(source="openalex", title="AI systems", raw_payload={})
    )
    drop = engine.apply(
        NormalizedRecord(source="openalex", title="Databases", raw_payload={})
    )

    assert keep is not None
    assert drop is None


def test_transformation_engine_dedupe_drops_second_duplicate() -> None:
    engine = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "dedupe",
                    "keys": ["source", "external_id"],
                }
            ]
        }
    )

    first = engine.apply(
        NormalizedRecord(source="openalex", external_id="x-1", raw_payload={})
    )
    second = engine.apply(
        NormalizedRecord(source="openalex", external_id="x-1", raw_payload={})
    )

    assert first is not None
    assert second is None


def test_transformation_engine_assigns_topic_from_terms() -> None:
    engine = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "assign_topic_from_terms",
                    "terms": ["machine learning", "ai"],
                    "fields": ["title"],
                }
            ]
        }
    )

    transformed = engine.apply(
        NormalizedRecord(
            source="openalex",
            title="Machine learning for retrieval",
            raw_payload={},
        )
    )

    assert transformed is not None
    assert transformed.topic == "machine learning"


def test_transformation_engine_accepts_legacy_spec_without_version() -> None:
    engine = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "set_field",
                    "field": "topic",
                    "value": "legacy",
                }
            ]
        }
    )

    transformed = engine.apply(
        NormalizedRecord(source="openalex", title="x", raw_payload={})
    )
    assert transformed is not None
    assert transformed.topic == "legacy"


def test_transformation_engine_migrates_legacy_steps_key() -> None:
    engine = TransformationEngine(
        {
            "steps": [
                {
                    "op": "set_field",
                    "field": "topic",
                    "value": "migrated",
                }
            ]
        }
    )

    transformed = engine.apply(
        NormalizedRecord(source="openalex", title="x", raw_payload={})
    )
    assert transformed is not None
    assert transformed.topic == "migrated"


def test_transformation_engine_rejects_future_version() -> None:
    with pytest.raises(ValueError, match="Unsupported transform spec version"):
        TransformationEngine({"version": 999, "transforms": []})

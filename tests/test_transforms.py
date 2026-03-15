from __future__ import annotations

import json

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


def test_transformation_engine_require_fields_and_exclude_terms() -> None:
    engine = TransformationEngine(
        {
            "transforms": [
                {"op": "require_fields", "fields": ["title"]},
                {"op": "exclude_terms", "terms": ["forbidden"], "fields": ["title"]},
            ]
        }
    )

    dropped_missing = engine.apply(
        NormalizedRecord(source="openalex", title=None, raw_payload={})
    )
    dropped_excluded = engine.apply(
        NormalizedRecord(source="openalex", title="forbidden content", raw_payload={})
    )
    kept = engine.apply(
        NormalizedRecord(source="openalex", title="allowed content", raw_payload={})
    )

    assert dropped_missing is None
    assert dropped_excluded is None
    assert kept is not None


def test_transformation_engine_assign_topic_respects_overwrite_flag() -> None:
    keep_existing = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "assign_topic_from_terms",
                    "terms": ["ai"],
                    "fields": ["title"],
                    "overwrite": False,
                }
            ]
        }
    )
    overwrite_existing = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "assign_topic_from_terms",
                    "terms": ["ai"],
                    "fields": ["title"],
                    "overwrite": True,
                }
            ]
        }
    )

    rec1 = NormalizedRecord(
        source="openalex",
        title="AI trend",
        topic="preset",
        raw_payload={},
    )
    rec2 = NormalizedRecord(
        source="openalex",
        title="AI trend",
        topic="preset",
        raw_payload={},
    )

    out1 = keep_existing.apply(rec1)
    out2 = overwrite_existing.apply(rec2)

    assert out1 is not None and out1.topic == "preset"
    assert out2 is not None and out2.topic == "ai"


def test_transformation_engine_set_field_validation_error() -> None:
    import pytest

    with pytest.raises(ValueError, match="field must be one of"):
        TransformationEngine(
            {"transforms": [{"op": "set_field", "field": "source", "value": "x"}]}
        )


def test_transformation_engine_load_spec_from_file(tmp_path) -> None:
    path = tmp_path / "spec.json"
    payload = json.dumps(
        {
            "version": 1,
            "transforms": [{"op": "set_field", "field": "topic", "value": "x"}],
        }
    )
    path.write_text(payload, encoding="utf-8")

    engine = TransformationEngine(str(path))
    rec = engine.apply(NormalizedRecord(source="openalex", title="t", raw_payload={}))

    assert rec is not None
    assert rec.topic == "x"


def test_transformation_engine_rejects_non_object_spec_file(tmp_path) -> None:
    import pytest

    path = tmp_path / "spec.json"
    path.write_text("[]", encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        TransformationEngine(str(path))


def test_transformation_engine_single_op_top_level_legacy_form() -> None:
    engine = TransformationEngine(
        {
            "op": "set_field",
            "field": "topic",
            "value": "legacy-single-op",
        }
    )

    rec = engine.apply(NormalizedRecord(source="openalex", raw_payload={}))
    assert rec is not None
    assert rec.topic == "legacy-single-op"


def test_transformation_engine_uses_raw_payload_path_and_hashable() -> None:
    engine = TransformationEngine(
        {
            "transforms": [
                {
                    "op": "dedupe",
                    "keys": ["raw_payload.meta.id", "raw_payload.tags"],
                }
            ]
        }
    )

    rec1 = NormalizedRecord(
        source="openalex",
        raw_payload={"meta": {"id": "1"}, "tags": ["a", "b"]},
    )
    rec2 = NormalizedRecord(
        source="openalex",
        raw_payload={"meta": {"id": "1"}, "tags": ["a", "b"]},
    )

    assert engine.apply(rec1) is not None
    assert engine.apply(rec2) is None


def test_transformation_engine_term_validators_reject_blank_terms() -> None:
    import pytest

    bad_specs = [
        {"op": "include_terms", "terms": ["   "]},
        {"op": "exclude_terms", "terms": ["   "]},
        {"op": "assign_topic_from_terms", "terms": ["   "]},
    ]

    for transform in bad_specs:
        with pytest.raises(ValueError, match="non-blank"):
            TransformationEngine({"transforms": [transform]})

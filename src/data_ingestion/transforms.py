from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field, field_validator

if TYPE_CHECKING:
    from data_ingestion.models import NormalizedRecord

_CURRENT_SPEC_VERSION = 1
_DEFAULT_TEXT_FIELDS = ["title", "abstract", "full_text", "url", "topic"]
_MUTABLE_FIELDS = {
    "external_id",
    "title",
    "authors",
    "published_date",
    "url",
    "abstract",
    "full_text",
    "full_text_url",
    "topic",
    "record_type",
    "raw_payload",
}


class _IncludeTermsTransform(BaseModel):
    op: Literal["include_terms"]
    terms: list[str] = Field(..., min_length=1)
    fields: list[str] = Field(default_factory=lambda: list(_DEFAULT_TEXT_FIELDS))

    @field_validator("terms", mode="after")
    @classmethod
    def _normalize_terms(cls, value: list[str]) -> list[str]:
        cleaned = [term.strip().lower() for term in value if term.strip()]
        if not cleaned:
            raise ValueError("terms must contain at least one non-blank value")
        return cleaned


class _ExcludeTermsTransform(BaseModel):
    op: Literal["exclude_terms"]
    terms: list[str] = Field(..., min_length=1)
    fields: list[str] = Field(default_factory=lambda: list(_DEFAULT_TEXT_FIELDS))

    @field_validator("terms", mode="after")
    @classmethod
    def _normalize_terms(cls, value: list[str]) -> list[str]:
        cleaned = [term.strip().lower() for term in value if term.strip()]
        if not cleaned:
            raise ValueError("terms must contain at least one non-blank value")
        return cleaned


class _RequireFieldsTransform(BaseModel):
    op: Literal["require_fields"]
    fields: list[str] = Field(..., min_length=1)


class _DedupeTransform(BaseModel):
    op: Literal["dedupe"]
    keys: list[str] = Field(default_factory=lambda: ["source", "external_id", "url"])


class _SetFieldTransform(BaseModel):
    op: Literal["set_field"]
    field: str
    value: Any

    @field_validator("field")
    @classmethod
    def _validate_field(cls, value: str) -> str:
        field_name = value.strip()
        if field_name not in _MUTABLE_FIELDS:
            allowed = ", ".join(sorted(_MUTABLE_FIELDS))
            raise ValueError(f"field must be one of: {allowed}")
        return field_name


class _AssignTopicFromTermsTransform(BaseModel):
    op: Literal["assign_topic_from_terms"]
    terms: list[str] = Field(..., min_length=1)
    fields: list[str] = Field(default_factory=lambda: list(_DEFAULT_TEXT_FIELDS))
    overwrite: bool = False

    @field_validator("terms", mode="after")
    @classmethod
    def _normalize_terms(cls, value: list[str]) -> list[str]:
        cleaned = [term.strip().lower() for term in value if term.strip()]
        if not cleaned:
            raise ValueError("terms must contain at least one non-blank value")
        return cleaned


class TransformationSpec(BaseModel):
    version: int = Field(default=_CURRENT_SPEC_VERSION, ge=1)
    transforms: list[
        _IncludeTermsTransform
        | _ExcludeTermsTransform
        | _RequireFieldsTransform
        | _DedupeTransform
        | _SetFieldTransform
        | _AssignTopicFromTermsTransform
    ] = Field(default_factory=list)


class TransformationEngine:
    """Compiled transformation engine applied to normalized records."""

    def __init__(self, spec: TransformationSpec | dict[str, Any] | str | Path) -> None:
        if isinstance(spec, (str, Path)):
            spec = self.load_spec(spec)

        if isinstance(spec, dict):
            normalized = self._normalize_spec_dict(spec)
            self.spec = TransformationSpec.model_validate(normalized)
        else:
            self.spec = spec

        if self.spec.version > _CURRENT_SPEC_VERSION:
            raise ValueError(
                "Unsupported transform spec version "
                f"{self.spec.version}; max supported is {_CURRENT_SPEC_VERSION}"
            )

        self._seen_keys: set[tuple[Any, ...]] = set()

    @staticmethod
    def load_spec(path: str | Path) -> dict[str, Any]:
        spec_path = Path(path)
        raw = spec_path.read_text(encoding="utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("Transformation spec must be a JSON object")
        return parsed

    @staticmethod
    def _normalize_spec_dict(spec: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(spec)

        # Legacy compatibility: allow `steps` as alias for `transforms`.
        if "transforms" not in normalized and "steps" in normalized:
            normalized["transforms"] = normalized.pop("steps")

        # Legacy compatibility: allow a single-op object at top-level.
        if "transforms" not in normalized and "op" in normalized:
            normalized = {"transforms": [normalized]}

        normalized.setdefault("version", _CURRENT_SPEC_VERSION)
        return normalized

    @staticmethod
    def _extract_value(record: NormalizedRecord, field_path: str) -> Any:
        if field_path.startswith("raw_payload."):
            current: Any = record.raw_payload
            for part in field_path.split(".")[1:]:
                if not isinstance(current, dict):
                    return None
                current = current.get(part)
            return current
        return getattr(record, field_path, None)

    @staticmethod
    def _is_present(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, (list, tuple, dict, set)):
            return bool(value)
        return True

    @classmethod
    def _record_text(cls, record: NormalizedRecord, fields: list[str]) -> str:
        parts: list[str] = []
        for field in fields:
            value = cls._extract_value(record, field)
            if value is None:
                continue
            if isinstance(value, str):
                parts.append(value)
            elif isinstance(value, list):
                parts.extend(str(item) for item in value if item is not None)
            else:
                parts.append(str(value))
        return " ".join(parts).lower()

    def uses_raw_payload(self) -> bool:
        for transform in self.spec.transforms:
            fields = getattr(transform, "fields", [])
            keys = getattr(transform, "keys", [])
            field = getattr(transform, "field", None)
            if any(str(item).startswith("raw_payload") for item in [*fields, *keys]):
                return True
            if field == "raw_payload":
                return True
        return False

    @staticmethod
    def _hashable(value: Any) -> Any:
        if isinstance(value, dict):
            return json.dumps(value, sort_keys=True, default=str)
        if isinstance(value, list):
            return tuple(TransformationEngine._hashable(item) for item in value)
        return value

    def apply(self, record: NormalizedRecord) -> NormalizedRecord | None:
        current = record
        for transform in self.spec.transforms:
            if isinstance(transform, _RequireFieldsTransform):
                if any(
                    not self._is_present(self._extract_value(current, field))
                    for field in transform.fields
                ):
                    return None
                continue

            if isinstance(transform, _IncludeTermsTransform):
                text = self._record_text(current, transform.fields)
                if not any(term in text for term in transform.terms):
                    return None
                continue

            if isinstance(transform, _ExcludeTermsTransform):
                text = self._record_text(current, transform.fields)
                if any(term in text for term in transform.terms):
                    return None
                continue

            if isinstance(transform, _AssignTopicFromTermsTransform):
                if current.topic and not transform.overwrite:
                    continue
                text = self._record_text(current, transform.fields)
                for term in transform.terms:
                    if term in text:
                        current.topic = term
                        break
                continue

            if isinstance(transform, _SetFieldTransform):
                setattr(current, transform.field, transform.value)
                continue

            if isinstance(transform, _DedupeTransform):
                dedupe_key = tuple(
                    self._hashable(self._extract_value(current, key))
                    for key in transform.keys
                )
                if dedupe_key in self._seen_keys:
                    return None
                self._seen_keys.add(dedupe_key)

        return current

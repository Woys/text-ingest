from __future__ import annotations

from difflib import SequenceMatcher
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from data_ingestion.models import NormalizedRecord


class _FuzzModule(Protocol):
    def partial_ratio(self, left: str, right: str) -> float | int: ...

    def token_set_ratio(self, left: str, right: str) -> float | int: ...


_fuzz: _FuzzModule | None
try:
    from rapidfuzz import fuzz as _fuzz_impl

    _fuzz = _fuzz_impl
except ModuleNotFoundError:
    _fuzz = None


def _ratio(left: str, right: str) -> float:
    return SequenceMatcher(None, left, right).ratio() * 100.0


def _partial_ratio(left: str, right: str) -> float:
    if _fuzz is not None:
        return float(_fuzz.partial_ratio(left, right))
    return _ratio(left, right)


def _token_set_ratio(left: str, right: str) -> float:
    if _fuzz is not None:
        return float(_fuzz.token_set_ratio(left, right))

    left_tokens = " ".join(sorted(set(left.split())))
    right_tokens = " ".join(sorted(set(right.split())))
    return _ratio(left_tokens, right_tokens)


def build_search_text(record: NormalizedRecord) -> str:
    return " ".join(
        part.strip()
        for part in [
            record.title or "",
            record.abstract or "",
            record.full_text or "",
        ]
        if part and part.strip()
    ).lower()


def fuzzy_match_record(
    record: NormalizedRecord,
    *,
    query: str | None = None,
    fuzzy_terms: list[str] | None = None,
    threshold: int = 85,
) -> bool:
    text = build_search_text(record)
    if not text:
        return False

    candidates: list[str] = []
    if query:
        candidates.append(query.lower())
    if fuzzy_terms:
        candidates.extend(term.lower() for term in fuzzy_terms)

    if not candidates:
        return True

    for candidate in candidates:
        if candidate in text:
            return True
        if _partial_ratio(candidate, text) >= threshold:
            return True
        if _token_set_ratio(candidate, text) >= threshold:
            return True

    return False

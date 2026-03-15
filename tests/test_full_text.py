from __future__ import annotations

from tempfile import SpooledTemporaryFile
from types import SimpleNamespace

import pytest
import requests

from data_ingestion.config import FullTextResolutionConfig
from data_ingestion.full_text import FullTextResolver
from data_ingestion.models import NormalizedRecord


class _CtxResponse:
    def __init__(
        self,
        *,
        content_type: str = "text/plain",
        url: str = "https://example.com/doc",
        body: bytes = b"hello world",
        encoding: str | None = "utf-8",
        raise_exc: Exception | None = None,
    ) -> None:
        self.headers = {"Content-Type": content_type}
        self.url = url
        self.encoding = encoding
        self._body = body
        self._raise_exc = raise_exc
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def raise_for_status(self) -> None:
        if self._raise_exc is not None:
            raise self._raise_exc

    def iter_content(self, chunk_size: int):
        _ = chunk_size
        yield self._body

    def close(self) -> None:
        self.closed = True


class _Session:
    def __init__(self, response_or_exc):
        self._response_or_exc = response_or_exc

    def get(self, *args, **kwargs):
        _ = args, kwargs
        if isinstance(self._response_or_exc, Exception):
            raise self._response_or_exc
        return self._response_or_exc


def _record(**kwargs) -> NormalizedRecord:
    base = {
        "source": "openalex",
        "external_id": "1",
        "title": "T",
        "raw_payload": {},
    }
    base.update(kwargs)
    return NormalizedRecord(**base)


def _spooled(data: bytes) -> SpooledTemporaryFile[bytes]:
    handle: SpooledTemporaryFile[bytes] = SpooledTemporaryFile(  # noqa: SIM115
        max_size=1024, mode="w+b"
    )
    handle.write(data)
    handle.seek(0)
    return handle


def test_enrich_record_noop_when_full_text_present() -> None:
    resolver = FullTextResolver(FullTextResolutionConfig(max_chars=20))
    rec = _record(full_text="   has text   ")

    out = resolver.enrich_record(rec)
    assert out.full_text == "has text"


def test_enrich_record_noop_without_url() -> None:
    resolver = FullTextResolver()
    rec = _record(full_text=None, full_text_url=None)

    out = resolver.enrich_record(rec)
    assert out.full_text is None


def test_enrich_record_fetches_and_sets_text(monkeypatch) -> None:
    resolver = FullTextResolver()
    response = _CtxResponse(content_type="text/plain", body=b"  hello   world ")
    monkeypatch.setattr(resolver, "_get_session", lambda: _Session(response))

    rec = _record(full_text=None, full_text_url="https://example.com/doc")
    out = resolver.enrich_record(rec)

    assert out.full_text == "hello world"


def test_enrich_record_handles_request_exception(monkeypatch) -> None:
    resolver = FullTextResolver()
    monkeypatch.setattr(
        resolver,
        "_get_session",
        lambda: _Session(requests.RequestException("boom")),
    )

    rec = _record(full_text=None, full_text_url="https://example.com/doc")
    out = resolver.enrich_record(rec)
    assert out.full_text is None


def test_enrich_record_handles_generic_extraction_exception(monkeypatch) -> None:
    resolver = FullTextResolver()
    response = _CtxResponse(content_type="text/plain", body=b"abc")
    monkeypatch.setattr(resolver, "_get_session", lambda: _Session(response))
    monkeypatch.setattr(
        resolver,
        "_extract_text_from_response",
        lambda _: (_ for _ in ()).throw(RuntimeError("bad extract")),
    )

    rec = _record(full_text=None, full_text_url="https://example.com/doc")
    out = resolver.enrich_record(rec)
    assert out.full_text is None


def test_enrich_many_empty_and_failure_fallback(monkeypatch) -> None:
    resolver = FullTextResolver()
    assert resolver.enrich_many([]) == []

    r1 = _record(external_id="1", full_text_url="u1")
    r2 = _record(external_id="2", full_text_url="u2")

    def enrich(rec):
        if rec.external_id == "2":
            raise RuntimeError("x")
        rec.full_text = "ok"
        return rec

    monkeypatch.setattr(resolver, "enrich_record", enrich)
    out = resolver.enrich_many([r1, r2])

    assert out[0].full_text == "ok"
    assert out[1].external_id == "2"


def test_to_full_text_document_and_clean_text() -> None:
    resolver = FullTextResolver(FullTextResolutionConfig(max_chars=5))

    rec_none = _record(full_text=None)
    assert resolver.to_full_text_document(rec_none) is None

    rec = _record(full_text="  abc   def  ", full_text_url="https://example.com")
    doc = resolver.to_full_text_document(rec)
    assert doc is not None
    assert doc.full_text == "abc d"


def test_extract_text_from_response_branches(monkeypatch) -> None:
    resolver = FullTextResolver()

    # PDF branch
    monkeypatch.setattr(
        resolver, "_download_to_spooled_file", lambda _: _spooled(b"pdf")
    )
    monkeypatch.setattr(resolver, "_pdf_to_text", lambda _: "pdf-text")
    resp_pdf = _CtxResponse(content_type="application/pdf")
    assert resolver._extract_text_from_response(resp_pdf) == "pdf-text"

    # JSON branch
    monkeypatch.setattr(
        resolver,
        "_download_to_spooled_file",
        lambda _: _spooled(b'{"content":"json text"}'),
    )
    resp_json = _CtxResponse(content_type="application/json")
    assert resolver._extract_text_from_response(resp_json) == "json text"

    # HTML branch
    monkeypatch.setattr(
        resolver, "_download_to_spooled_file", lambda _: _spooled(b"<p>hi</p>")
    )
    resp_html = _CtxResponse(content_type="text/html")
    assert resolver._extract_text_from_response(resp_html) == "hi"

    # Plain text / fallback branch
    monkeypatch.setattr(
        resolver,
        "_download_to_spooled_file",
        lambda _: _spooled(b" plain  text "),
    )
    resp_plain = _CtxResponse(content_type="text/plain")
    assert resolver._extract_text_from_response(resp_plain) == "plain text"


def test_download_to_spooled_file_and_limit_error() -> None:
    cfg = FullTextResolutionConfig(max_download_bytes=5, download_chunk_size=1024)
    resolver = FullTextResolver(cfg)

    class _Resp:
        def iter_content(self, chunk_size: int):
            _ = chunk_size
            yield b"ab"
            yield b""
            yield b"cd"

    handle = resolver._download_to_spooled_file(_Resp())
    assert handle.read() == b"abcd"
    handle.close()

    class _BigResp:
        def iter_content(self, chunk_size: int):
            _ = chunk_size
            yield b"abcdef"

    with pytest.raises(ValueError, match="max_download_bytes"):
        resolver._download_to_spooled_file(_BigResp())


def test_read_text_file_pdf_html_json_helpers(monkeypatch) -> None:
    resolver = FullTextResolver(FullTextResolutionConfig(max_chars=100))

    handle = _spooled("caf\xe9".encode("latin-1"))
    assert resolver._read_text_file(handle, "latin-1") == "café"

    assert (
        resolver._html_to_text("<style>x</style><p>Hello &amp; bye</p>")
        == "Hello & bye"
    )
    assert resolver._json_to_text('{"body":"A", "nested":[{"summary":"B"}]}') == "A B"
    assert resolver._json_to_text('{"not_text":"x"}') is None
    assert resolver._json_to_text("not-json") == "not-json"

    class _Page:
        def __init__(self, text=None, exc: Exception | None = None):
            self._text = text
            self._exc = exc

        def extract_text(self):
            if self._exc is not None:
                raise self._exc
            return self._text

    class _Reader:
        def __init__(self, handle):
            _ = handle
            self.pages = [
                _Page("first"),
                _Page(exc=RuntimeError("bad")),
                _Page("second"),
            ]

    monkeypatch.setattr("data_ingestion.full_text.PdfReader", _Reader)
    assert resolver._pdf_to_text(_spooled(b"x")) == "first second"

    class _BadReader:
        def __init__(self, handle):
            _ = handle
            raise RuntimeError("bad pdf")

    monkeypatch.setattr("data_ingestion.full_text.PdfReader", _BadReader)
    assert resolver._pdf_to_text(_spooled(b"x")) is None


def test_get_session_reuses_thread_local_session(monkeypatch) -> None:
    resolver = FullTextResolver()
    fake_session = SimpleNamespace()
    monkeypatch.setattr(
        "data_ingestion.full_text.build_retry_session", lambda _: fake_session
    )

    first = resolver._get_session()
    second = resolver._get_session()

    assert first is second

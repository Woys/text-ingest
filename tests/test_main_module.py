from __future__ import annotations

import runpy


def test_module_main_invokes_cli_main(monkeypatch) -> None:
    called = {"count": 0}

    def fake_main() -> None:
        called["count"] += 1

    monkeypatch.setattr("data_ingestion.cli.main", fake_main)
    runpy.run_module("data_ingestion.__main__", run_name="__main__")

    assert called["count"] == 1

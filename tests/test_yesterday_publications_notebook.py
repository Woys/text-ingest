from __future__ import annotations

import ast
import json
from pathlib import Path


def test_yesterday_publications_notebook_smoke() -> None:
    path = Path("examples/yesterday_publications_summary.ipynb")
    notebook = json.loads(path.read_text(encoding="utf-8"))

    source = "\n".join(
        "".join(cell.get("source", [])) for cell in notebook.get("cells", [])
    )

    assert "import asyncio" in source
    assert "async_stream_transformed_records" in source
    assert "await asyncio.gather" in source
    assert "LOW_MEMORY_EXPORT = True" in source
    assert "LOAD_EXPORTED_FOR_ANALYSIS = False" in source
    assert "writer.writerow(row)" in source
    low_memory_columns_block = source[
        source.index("LOW_MEMORY_COLUMNS") : source.index("def first_text_for_export")
    ]
    main_export_block = source[
        source.index("LOW_MEMORY_COLUMNS") : source.index("records_by_source")
    ]
    assert "raw_payload" not in low_memory_columns_block
    assert "rows = []" not in main_export_block

    for idx, cell in enumerate(notebook.get("cells", [])):
        if cell.get("cell_type") == "code":
            ast.parse("".join(cell.get("source", [])), filename=f"{path}:cell{idx}")

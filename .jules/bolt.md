## 2024-05-03 - Avoid full-text extraction for out-of-window records
**Learning:** In the text-ingest pipeline, retrieving full historical datasets and filtering after expensive text search via `_row_search_text()` causes significant bottleneck. Full text/abstract/payload searches are very expensive.
**Action:** Always pre-calculate and tightly bound `start_date` and `end_date` *before* delegating to the `search_industry_export()` function. Pushing boundaries down avoids iterating strings that we will throw away later anyway.

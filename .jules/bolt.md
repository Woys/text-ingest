
## 2026-05-04 - Cache topic rules resolution per fetcher
**Learning:** Checking the `topic_include`, `topic_exclude`, and `query` variables using `getattr` and constructing lists repetitively in the inner loop (called per record) of `_topic_decision` wastes a significant amount of CPU time since the config does not change per record. Also string matching and list creation with conditions can have heavy overheads when iterating 100k+ times.
**Action:** When filtering records by topic, I must retrieve the topic configuration only once per fetcher source, caching the parsed configuration inside the pipeline, and reuse it across all records from the same fetcher. Also avoid using `strip()` repeatedly for the same variable lookup or checking against a property that remains None.

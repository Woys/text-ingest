## 2026-05-10 - Pydantic v2 Serialization Bottleneck
**Learning:** In a codebase aggressively creating output dictionaries via dictionary mutation and `json.dumps()` on Pydantic v2 models, the intermediate dictionary allocation combined with string serialization creates a massive overhead.
**Action:** Always prefer Pydantic v2's native `.model_dump_json()` and `.model_dump()` with `exclude` sets when preparing models for sink output, rather than extracting standard dictionaries and mutating them. This reduces allocation and is significantly faster (~3.5x improvement).

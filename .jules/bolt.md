## 2024-05-17 - [Pydantic v2 Serialization Speedup]
**Learning:** Pydantic v2's native `model_dump_json()` is significantly faster (~4x) than calling `model_dump()` to get a dict and then using the standard library's `json.dumps()`. This is because Pydantic v2's serialization is implemented in Rust.
**Action:** When serializing Pydantic models to JSON strings (especially for things like JSONL outputs), use `model_dump_json()` directly rather than standard `json.dumps()`. Use the `exclude` parameter in `model_dump_json()` to handle field exclusions efficiently instead of dict manipulation.

## 2024-05-18 - [Pydantic v2 JSON DateTime Compatibility]
**Learning:** While `model_dump_json()` is much faster, it defaults to ISO 8601 for datetime types, which changes behavior compared to `json.dumps(default=str)`. Changing formats can silently break downstream data pipeline consumers expecting `str()` format.
**Action:** When optimizing existing pipelines with `model_dump_json()`, explicitly use `@field_serializer` with `mode="plain"` and `when_used="json"` to strictly preserve legacy formatting rules.

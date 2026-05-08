## 2024-05-08 - Fast Pydantic JSON Serialization
**Learning:** `json.dumps(self.model_dump())` is much slower than Pydantic's built in `self.model_dump_json()`. Pydantic implements custom fast serialization directly to string, which we can take advantage of inside Pydantic model methods.
**Action:** When adding `.to_json()` or `.to_json_line()` methods to Pydantic models, prefer `self.model_dump_json()` over manual dict conversion and `json.dumps()`.

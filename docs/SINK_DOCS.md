# Sink Docs Contract

- `file_name_py` (required, must end with `.py`)
- `name` (required, non-empty)
- `required_config` (required, non-empty list)
- `optional_config` (required, non-empty list)
- `notes` (optional)

```json
[
  {
    "file_name_py": "csv.py",
    "name": "CSV Sink",
    "required_config": [
      "output_file"
    ],
    "optional_config": [
      "append"
    ],
    "notes": "Writes normalized records to CSV for spreadsheet and tabular workflows."
  },
  {
    "file_name_py": "full_text_jsonl.py",
    "name": "Full Text JSONL Sink",
    "required_config": [
      "output_file"
    ],
    "optional_config": [
      "append",
      "encoding"
    ],
    "notes": "Writes resolved full-text documents to line-delimited JSON."
  },
  {
    "file_name_py": "jsonl.py",
    "name": "JSONL Sink",
    "required_config": [
      "output_file"
    ],
    "optional_config": [
      "append",
      "encoding"
    ],
    "notes": "Writes normalized records as JSON lines with optional append mode."
  },
  {
    "file_name_py": "parquet.py",
    "name": "Parquet Sink",
    "required_config": [
      "output_file"
    ],
    "optional_config": [
      "batch_size",
      "compression"
    ],
    "notes": "Writes normalized records to Parquet for analytics and columnar processing."
  }
]
```

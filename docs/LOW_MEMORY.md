# Low-Memory Usage

Use the streaming APIs and keep batches small when memory is more important than
throughput.

Recommended settings:

- `sink_write_batch_size=1` to `25`
- `include_raw_payload=False`
- `enrich_full_text=False`
- `max_source_concurrency=1` or `2`
- `max_async_queue_size=25` to `100`

The pipeline drops `record.raw_payload` after transforms by default unless raw
payload output is enabled or a transform references `raw_payload.*`.

For notebooks, stream records to CSV or JSONL first, then load a filtered export
into pandas only when interactive analysis is needed.

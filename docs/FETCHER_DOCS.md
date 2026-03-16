# Fetcher Docs Contract

Every concrete fetcher in `src/data_ingestion/fetchers/` must have exactly one docs block:

```text
\{% docs fetcher.py %\}
Description in plain words, required/optional config, notes.
\{% enddocs %\}
```

The block name must match the fetcher file name exactly (for example `openalex.py`).

{% docs crossref.py %}
Fetches scholarly metadata records from the Crossref Works API.

Required config:
- `query` (unless date-only mode/range is used)

Optional config:
- `rows`, `max_pages`, `start_date`, `end_date`, topic filters, `http`

Notes:
- Response records are normalized to `NormalizedRecord`.
- Request and parse failures are raised as `FetcherError`.
{% enddocs %}

{% docs federal_register.py %}
Fetches U.S. Federal Register documents and notices.

Required config:
- `query` (unless date-only mode/range is used)

Optional config:
- `per_page`, `max_pages`, `start_date`, `end_date`, topic filters, `http`

Notes:
- Uses publication date fields from Federal Register payloads.
- Produces `RecordType.NEWS`-style normalized output.
{% enddocs %}

{% docs hackernews.py %}
Fetches stories/comments from Hacker News through the Algolia API.

Required config:
- `query` (unless date-only mode/range is used)

Optional config:
- `hits_per_page`, `hn_item_type`, `use_date_sort`, `max_pages`, dates, `http`

Notes:
- Falls back to Hacker News item URL when source URL is missing.
- Tag metadata is used for topic extraction when available.
{% enddocs %}

{% docs newsapi.py %}
Fetches news articles from NewsAPI `/v2/everything`.

Required config:
- `query`
- `api_key` (or `NEWSAPI_KEY` environment variable)

Optional config:
- `language`, `page_size`, `max_pages`, dates, topic filters, `http`

Notes:
- API status errors are surfaced as `FetcherError`.
- Emits `RecordType.NEWS`.
{% enddocs %}

{% docs openalex.py %}
Fetches academic works from OpenAlex.

Required config:
- `query` (unless date-only mode/range is used)

Optional config:
- `per_page`, `max_pages`, dates, topic filters, `http`

Notes:
- Includes abstract reconstruction from OpenAlex index fields.
- Uses OpenAlex concept/topic metadata when present.
{% enddocs %}

{% docs website.py %}
Fetches website updates from RSS/Atom feeds.

Required config:
- At least one of `feed_url` or `site_url`

Optional config:
- `query`, `target_date`, `start_date`, `end_date`, `max_items`, `http`

Notes:
- Supports feed autodiscovery via `<link rel="alternate" ...>` in site HTML.
- Intended for feed-based sources only.
{% enddocs %}

{% docs website_html.py %}
Fetches website updates directly from HTML pages when RSS/Atom is not available.

Required config:
- `site_url`

Optional config:
- `list_page_urls`, `link_include_patterns`, `link_exclude_patterns`
- `include_list_pages_as_items`, `max_candidate_links`, `max_items`
- `query`, `start_date`, `end_date`, `http`

Notes:
- Extracts article URLs from list pages and parses article HTML metadata/content.
- Can ingest list pages directly for changelog/release-notes style pages.
{% enddocs %}

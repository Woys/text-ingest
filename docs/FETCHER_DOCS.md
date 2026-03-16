# Fetcher Docs Contract

- `file_name_py` (required, must end with `.py`)
- `name` (required, non-empty)
- `required_config` (required, non-empty list)
- `optional_config` (required, non-empty list)
- `notes` (optional)

```json
[
  {
    "file_name_py": "crossref.py",
    "name": "Crossref Works Fetcher",
    "required_config": [
      "query (unless using date-only mode)"
    ],
    "optional_config": [
      "rows",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "topic_include/topic_exclude",
      "http"
    ],
    "notes": "Fetches scholarly metadata from Crossref and normalizes to NormalizedRecord."
  },
  {
    "file_name_py": "federal_register.py",
    "name": "Federal Register Fetcher",
    "required_config": [
      "query (unless using date-only mode)"
    ],
    "optional_config": [
      "per_page",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "topic_include/topic_exclude",
      "http"
    ],
    "notes": "Fetches U.S. Federal Register notices/documents and maps publication fields."
  },
  {
    "file_name_py": "hackernews.py",
    "name": "Hacker News Fetcher",
    "required_config": [
      "query (unless using date-only mode)"
    ],
    "optional_config": [
      "hits_per_page",
      "hn_item_type",
      "use_date_sort",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "Uses Algolia API, with fallback Hacker News item URL when external URL is missing."
  },
  {
    "file_name_py": "newsapi.py",
    "name": "NewsAPI Fetcher",
    "required_config": [
      "query",
      "api_key (or NEWSAPI_KEY env var)"
    ],
    "optional_config": [
      "page_size",
      "language",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "topic_include/topic_exclude",
      "http"
    ],
    "notes": "Uses NewsAPI everything endpoint and emits RecordType.NEWS."
  },
  {
    "file_name_py": "openalex.py",
    "name": "OpenAlex Fetcher",
    "required_config": [
      "query (unless using date-only mode)"
    ],
    "optional_config": [
      "per_page",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "topic_include/topic_exclude",
      "http"
    ],
    "notes": "Fetches OpenAlex works and reconstructs abstract text when possible."
  },
  {
    "file_name_py": "website.py",
    "name": "Website Feed Fetcher",
    "required_config": [
      "feed_url or site_url"
    ],
    "optional_config": [
      "query",
      "target_date",
      "start_date",
      "end_date",
      "languages",
      "max_items",
      "http"
    ],
    "notes": "RSS/Atom fetcher with feed autodiscovery from site HTML alternate links."
  },
  {
    "file_name_py": "website_html.py",
    "name": "Website HTML Fetcher",
    "required_config": [
      "site_url"
    ],
    "optional_config": [
      "list_page_urls",
      "link_include_patterns",
      "link_exclude_patterns",
      "include_list_pages_as_items",
      "max_candidate_links",
      "max_items",
      "query",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "HTML fallback fetcher for non-RSS sites, with article-link extraction and page parsing."
  }
]
```

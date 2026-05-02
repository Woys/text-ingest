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
    "file_name_py": "edgar.py",
    "name": "SEC EDGAR Fetcher",
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
    "notes": "Fetches SEC EDGAR submissions using the EFTS API and constructs full-text URLs."
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
    "file_name_py": "github.py",
    "name": "GitHub Repository Fetcher",
    "required_config": [
      "query"
    ],
    "optional_config": [
      "per_page",
      "sort",
      "github_token",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "Fetches public repository metadata from the GitHub Search API."
  },
  {
    "file_name_py": "googlenews.py",
    "name": "Google News RSS Fetcher",
    "required_config": [
      "query"
    ],
    "optional_config": [
      "hl",
      "gl",
      "ceid",
      "page_size",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "Fetches Google News RSS search items and maps them to normalized news records."
  },
  {
    "file_name_py": "guardian.py",
    "name": "Guardian Content API Fetcher",
    "required_config": [
      "query"
    ],
    "optional_config": [
      "api_key",
      "page_size",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "Fetches article metadata from The Guardian Content API."
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
    "file_name_py": "openlibrary.py",
    "name": "Open Library Fetcher",
    "required_config": [
      "query"
    ],
    "optional_config": [
      "page_size",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "Fetches book metadata and publication signals from Open Library search."
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
    "file_name_py": "reddit.py",
    "name": "Reddit Search Fetcher",
    "required_config": [
      "query"
    ],
    "optional_config": [
      "subreddit",
      "sort",
      "page_size",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "Fetches Reddit post discussions via public JSON search endpoints."
  },
  {
    "file_name_py": "stackexchange.py",
    "name": "Stack Exchange Fetcher",
    "required_config": [
      "query"
    ],
    "optional_config": [
      "site",
      "sort",
      "page_size",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "Fetches Q&A metadata from Stack Exchange APIs (for example Stack Overflow)."
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
  },
  {
    "file_name_py": "wikipedia.py",
    "name": "Wikipedia Fetcher",
    "required_config": [
      "query"
    ],
    "optional_config": [
      "wiki_language",
      "page_size",
      "max_pages",
      "start_date",
      "end_date",
      "languages",
      "http"
    ],
    "notes": "Searches Wikipedia and enriches results with summary extracts."
  }
]
```

## 2024-05-01 - Caching regex and date parsing in WebsiteHtmlFetcher
**Learning:** In `src/data_ingestion/fetchers/website_html.py`, date parsing and regex compilation are repeatedly executed with the exact same inputs/patterns across multiple HTML strings.
**Action:** Adding `@functools.lru_cache` significantly speeds up repetitive parsing tasks. Ensure pure functions like date parsers are isolated or static methods to make caching effective.

# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-03-16

### Added
- Comprehensive architecture documentation (`docs/ARCHITECTURE.md`)
- Public API and compatibility contract guide (`docs/API_CONTRACT.md`)
- Operations and deployment guide (`docs/OPERATIONS.md`)
- Release process and maintainer checklist (`docs/RELEASE_PROCESS.md`)
- Open source standards policy (`docs/OPEN_SOURCE_STANDARDS.md`)
- Governance model (`docs/GOVERNANCE.md`)
- Testing and quality strategy (`docs/TESTING_AND_QUALITY.md`)
- Documentation index (`docs/DOCUMENTATION.md`)
- Website feed fetcher (`source: "website"`) with RSS/Atom parsing, feed autodiscovery,
  date/query filtering, and `max_items` controls
- Website HTML fetcher (`source: "website_html"`) for non-RSS sources with link extraction,
  page parsing, and normalized output
- New website ingestion examples and tests for feed + HTML fetchers
- Fetcher docs contract (`docs/FETCHER_DOCS.md`) using `{% docs <fetcher.py> %}` blocks
- Fetcher docs validator script (`scripts/check_fetcher_docs.py`)

### Changed
- Expanded `README.md` with docs hub and standards-oriented guidance
- Expanded `CONTRIBUTING.md` with contributor workflow and quality expectations
- Expanded `SECURITY.md` with disclosure process and response expectations
- `FetcherSpec` now supports `website` and `website_html`
- `make all` now includes `docs-check` and test runs disable external plugin autoload noise
- CI now enforces fetcher docs coverage

### Fixed
- Runtime package version lookup now uses the correct distribution name (`text-ingest`)

## [0.2.0] - 2026-03-14

### Added
- `NormalizedRecord` — a shared output schema that every fetcher must produce
- `BaseFetcher.normalize()` abstract method
- NewsAPI fetcher (`source: "newsapi"`)

## [0.1.0] - 2026-03-14

### Added
- src layout and package metadata
- streaming ingestion pipeline
- OpenAlex and Crossref fetchers
- JSONL sink
- Airflow and Spark adapters
- pytest suite with coverage
- OSS repository standards and CI

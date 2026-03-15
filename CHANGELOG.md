# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive architecture documentation (`docs/ARCHITECTURE.md`)
- Public API and compatibility contract guide (`docs/API_CONTRACT.md`)
- Operations and deployment guide (`docs/OPERATIONS.md`)
- Release process and maintainer checklist (`docs/RELEASE_PROCESS.md`)
- Open source standards policy (`docs/OPEN_SOURCE_STANDARDS.md`)
- Governance model (`docs/GOVERNANCE.md`)
- Testing and quality strategy (`docs/TESTING_AND_QUALITY.md`)
- Documentation index (`docs/DOCUMENTATION.md`)

### Changed
- Expanded `README.md` with docs hub and standards-oriented guidance
- Expanded `CONTRIBUTING.md` with contributor workflow and quality expectations
- Expanded `SECURITY.md` with disclosure process and response expectations

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

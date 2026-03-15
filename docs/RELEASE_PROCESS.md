# Release Process

This document defines the release workflow for maintainers.

## Versioning Rules

The project uses Semantic Versioning and Keep a Changelog.

- patch: bug fixes and docs improvements
- minor: backward-compatible features
- major: breaking contract changes

## Pre-Release Checklist

Before cutting a release:

1. ensure `CHANGELOG.md` has release notes under `[Unreleased]`
2. run full quality gate: `make all`
3. confirm docs and examples reflect current API
4. verify package metadata/version in `pyproject.toml`
5. verify security-sensitive dependencies and advisories

## Release Steps

1. move changelog entries from `[Unreleased]` to a dated release section
2. tag release version in VCS (`vX.Y.Z`)
3. build artifacts:

```bash
python -m build
```

4. validate wheel/sdist install in clean environment
5. publish artifacts to package index
6. publish release notes and migration guidance (if needed)

## Post-Release Steps

1. create fresh `[Unreleased]` section in changelog
2. monitor early adopter issues and regressions
3. patch quickly if release-critical issues are discovered

## Breaking Change Protocol

If a breaking change is required:

1. document rationale in architecture/API docs
2. add migration examples before release
3. communicate timeline and upgrade path in changelog/release notes
4. bump major version

## Documentation Requirements per Release

Each release should keep these artifacts consistent:

- `README.md`
- `docs/API_CONTRACT.md`
- `docs/ARCHITECTURE.md`
- `docs/OPERATIONS.md`
- `CHANGELOG.md`

## Maintainer Sign-Off Template

Release sign-off should include:

- version: `X.Y.Z`
- compatibility scope: patch/minor/major
- test status: pass/fail
- docs status: updated/not-updated
- known issues: list

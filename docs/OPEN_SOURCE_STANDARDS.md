# Open Source Library Standards

This document defines the operating standards for this project as an open source library.

## Standard Categories

The project standards are grouped into nine categories:

1. API Stability
2. Documentation Quality
3. Testing and Quality Gates
4. Security
5. Release and Versioning
6. Contributor Experience
7. Governance and Decision-Making
8. Operational Reliability
9. Community Support

## 1) API Stability

Required practices:

- maintain explicit public API surface
- document compatibility behavior for every contract type
- avoid breaking changes outside major versions
- provide deprecation and migration paths

Evidence artifacts:

- `docs/API_CONTRACT.md`
- changelog entries for API-affecting changes

## 2) Documentation Quality

Required practices:

- keep docs task-oriented and architecture-oriented
- keep examples runnable and version-aligned
- document both quick-start and production operations

Minimum docs set:

- README
- architecture
- API contract
- operations/deployment
- contributing/security policies

## 3) Testing and Quality Gates

Required practices:

- unit tests for models, transforms, and pipeline behavior
- integration-like tests for runtime summaries and failure handling
- linting and typing checks in CI/release gate

Quality gate:

```bash
make all
```

## 4) Security

Required practices:

- documented vulnerability reporting process
- no secrets in repository
- principle of least privilege for runtime paths
- timely dependency maintenance

Evidence artifacts:

- `SECURITY.md`
- secret handling guidance in operations docs

## 5) Release and Versioning

Required practices:

- semantic versioning discipline
- keep-a-changelog release notes
- release checklist with docs/test verification

Evidence artifacts:

- `CHANGELOG.md`
- `docs/RELEASE_PROCESS.md`

## 6) Contributor Experience

Required practices:

- clear setup instructions
- clear coding/testing expectations
- clear PR checklist and review criteria

Evidence artifacts:

- `CONTRIBUTING.md`
- examples and tests for extension workflows

## 7) Governance and Decision-Making

Required practices:

- maintainers make transparent technical decisions
- decisions should be documented when they affect API/contracts
- community feedback is welcomed and addressed respectfully

Suggested lightweight process:

1. capture proposal in issue/PR
2. discuss tradeoffs
3. merge with rationale recorded in docs/changelog

## 8) Operational Reliability

Required practices:

- observable run summaries
- clear failure semantics
- resumability for long-running ingestion jobs

Current implementation highlights:

- source-level checkpoint/resume
- per-source run stats in pipeline summary

## 9) Community Support

Required practices:

- define issue and PR response expectations
- label issues by type and priority
- close stale issues with clear reasoning (not silently)

## Documentation Review Policy

Every non-trivial code change should answer:

1. Does this alter behavior users rely on?
2. Does this change API/contract semantics?
3. Does this impact operations, deployment, or security?

If yes, docs must be updated in the same PR.

## Definition of Done (Library Change)

A change is complete when:

- code compiles/tests pass
- API impact is assessed
- docs are updated
- changelog entry exists
- migration notes are included when needed

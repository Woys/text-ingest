# Governance

This document describes how technical and project decisions are made.

## Maintainer Responsibilities

Maintainers are responsible for:

- preserving API and data contract stability
- reviewing and merging contributions
- maintaining release hygiene and changelog quality
- handling security reports privately and responsibly
- ensuring respectful community conduct

## Decision-Making Process

For non-trivial technical changes:

1. proposal is opened via issue or pull request
2. maintainers and contributors discuss alternatives and tradeoffs
3. final decision is documented in PR discussion and relevant docs
4. changelog is updated for user-visible impact

## Decision Criteria

Decisions are prioritized by:

1. correctness and safety
2. compatibility and migration cost
3. operational reliability
4. maintainability and clarity
5. ecosystem/community impact

## Contract-Affecting Changes

If a change affects API/schema/runtime semantics:

- update `docs/API_CONTRACT.md`
- provide migration guidance
- choose version bump according to semver policy

## Release Authority

Maintainers own release approval and publication.

A release should not proceed unless:

- quality gate passes
- docs are updated
- changelog is complete

## Community Participation

Contributors are encouraged to:

- propose enhancements via issues/PRs
- challenge design tradeoffs with evidence
- help improve docs/examples/tests

## Conflict Resolution

When disagreements occur:

1. restate goals and constraints
2. compare options with explicit tradeoffs
3. prefer reversible decisions when uncertain
4. maintainers make final call if consensus is not reached

Behavior standards are governed by `CODE_OF_CONDUCT.md`.

## Long-Term Stewardship

The project favors conservative evolution:

- stable contracts
- explicit deprecations
- incremental improvements over abrupt rewrites

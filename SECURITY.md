# Security Policy

This project takes security reports seriously and aims for responsible, private handling.

## Reporting a Vulnerability

Please do not open public issues for security vulnerabilities.

Report privately to project maintainers with:

- affected component/version
- impact and attack scenario
- reproduction steps or proof of concept
- suggested mitigation (if available)

## Response Expectations

Target maintainer response windows:

- initial acknowledgement: within 3 business days
- triage status update: within 7 business days
- remediation timeline: shared after severity assessment

These are best-effort targets, not legal SLAs.

## Disclosure Process

1. Reporter submits vulnerability details privately.
2. Maintainers reproduce and assess severity.
3. Fix is prepared and validated.
4. Patch release is published.
5. Public disclosure is coordinated after fix availability.

## Supported Versions

Security fixes are prioritized for the latest released minor line.

Older versions may receive best-effort guidance, but patch backports are not guaranteed.

## Secrets and Credentials

Never commit secrets to version control.

- Use environment variables or external secret managers.
- NewsAPI key should be provided as `NEWSAPI_KEY` or config value at runtime.
- Avoid logging secrets or embedding them in sample configs.

## Operational Hardening Recommendations

- run with least-privilege file permissions
- isolate output and checkpoint paths per job
- review dependency updates regularly
- monitor unusual error spikes and ingestion anomalies

## Supply Chain Notes

The project depends on third-party libraries. Keep dependencies updated and run security scans in CI where possible.

## Scope

This policy covers the library source and official release artifacts.

Integrations and deployment environments remain the operator's responsibility.

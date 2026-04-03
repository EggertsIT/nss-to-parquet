# Security Policy

## Reporting a Vulnerability

Please report security issues privately to your internal security contact or repository maintainers.
Do not open public issues for active vulnerabilities.

When reporting, include:
- affected version/commit
- impact and attack scenario
- reproduction steps or proof of concept
- suggested fix (if available)

## Security Baseline

- Run with least privilege (dedicated service account).
- Keep `metrics.bind_addr = "127.0.0.1:9090"` unless protected by network controls.
- Keep `schema.strict_type_validation = true`.
- Keep `durability.enabled = true` for at-least-once ingestion behavior.
- Restrict inbound source IPs to trusted NSS senders.

## Current Assessment

See [pentest.md](./pentest.md) for the latest internal penetration test summary and residual risks.

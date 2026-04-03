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

## Dependency Audit Allowlist Policy

`cargo audit` is enforced in CI with warnings denied.

Temporary advisory exceptions are managed in [`audit-allowlist.txt`](./audit-allowlist.txt) and must include:
- a documented business/technical reason,
- an owner responsible for follow-up,
- a review/removal plan with regular reassessment.

Current exception:
- `RUSTSEC-2024-0436` (`paste`, transitive via `parquet`)

## Release Artifact Verification

Verify release binaries before deployment.

1. Download assets from the GitHub release tag (example: `v0.1.0`):
   - `nss-ingestor-linux-x86_64`
   - `checksums.txt`
   - `checksums.txt.sig`
   - `checksums.txt.pem`
   - `sbom.cdx.json`
2. Validate the SHA-256 checksum:

```bash
mkdir -p dist
cp nss-ingestor-linux-x86_64 dist/nss-ingestor-linux-x86_64
sha256sum -c checksums.txt
```

3. Verify checksum signature and certificate provenance (requires `cosign`):

```bash
cosign verify-blob \
  --certificate checksums.txt.pem \
  --signature checksums.txt.sig \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp "https://github.com/.+/.+/.github/workflows/release-artifacts.yml@refs/tags/.+" \
  checksums.txt
```

4. Optionally validate SBOM structure:

```bash
jq -r '.bomFormat, .specVersion, (.components | length)' sbom.cdx.json
```

# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in rk, please report it responsibly.

**Do not open a public issue.**

Instead, email the maintainers directly at the address listed in the
[Cargo.toml](Cargo.toml) or use GitHub's
[private vulnerability reporting](https://github.com/gustaveiffel/rktvs/security/advisories/new).

We will acknowledge receipt within 48 hours and aim to provide a fix or
mitigation plan within 7 days.

## Scope

The following are in scope:

- Authentication and authorization bypasses
- Data integrity issues (chunk hash verification, catalog corruption)
- Denial of service against the hub server
- TLS/QUIC transport vulnerabilities
- Path traversal or injection in tar ingest/export
- SQL injection in the catalog layer

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | Yes       |
| < 0.2   | No        |

## Disclosure Policy

We follow coordinated disclosure. We ask that you give us reasonable time to
address the issue before public disclosure (typically 90 days).

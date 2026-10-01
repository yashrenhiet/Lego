# Security Policy

## Supported versions

Lego is pre-1.0. Security fixes are made on `main` and released in the next version.

| Version | Supported |
|---|---|
| `main` / latest release | Yes |
| older releases | No |

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Use GitHub's private vulnerability reporting instead:
**[Report a vulnerability](https://github.com/yashrenhiet/Lego/security/advisories/new)**.

Please include:

- what the issue is and its potential impact,
- steps to reproduce or a proof of concept,
- the affected version or commit.

You can expect an acknowledgement within a few days. Once a fix is ready we'll coordinate the
disclosure with you and credit you in the release notes, unless you'd rather stay anonymous.

## Deployment hardening

Lego has no built-in authentication. When running it:

- Keep the relay's HTTP port (`8080`) and management port (`8081`) on a private network.
- Leave `lego.admin.write-enabled` at its default (`false`) unless replay/discard sit behind an
  authenticating gateway.
- Give the relay's database user only the privileges it needs on the `lego_*` tables.
- Supply secrets (database password, HTTP `Authorization` headers) through environment variables
  or your secret manager, never in committed configuration.

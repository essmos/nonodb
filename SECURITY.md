# Security policy

## Supported versions

nonodb is pre-1.0. Security fixes are applied to the latest published release
line. At the time of writing that is `0.26.x`.

Older releases may receive a fix only when the issue is critical and the fix can
be applied safely. There is no long-term support commitment before 1.0.

## Reporting a vulnerability

Please report suspected vulnerabilities privately through GitHub's private
vulnerability reporting:

1. Open the repository's **Security** tab.
2. Choose **Report a vulnerability**.
3. Include the affected version, deployment mode, reproduction steps, and the
   expected impact.

Do not open a public GitHub issue, discussion, or pull request for a suspected
vulnerability. Do not include real tokens, certificates, private keys, vault
keys, customer documents, or production audit logs.

If GitHub private reporting is unavailable, contact the maintainer through the
repository owner account and ask for a private channel. Do not send secrets in
the first message.

## What to include

- nonodb version or commit.
- Deployment: Unix socket, TCP, TLS/mTLS, Docker, replica, or standalone.
- Relevant configuration with secrets removed.
- Minimal reproduction or proof of concept.
- Whether the issue requires authentication or local access.
- Observed and expected behavior.
- Any suspected data exposure, corruption, or privilege escalation.

## Scope

Examples of in-scope reports:

- Authentication or authorization bypass.
- Cross-database, cross-collection, or tenant/vault isolation failures.
- Token, certificate, lockout, or scope-handling flaws.
- TLS or mTLS verification mistakes.
- Path traversal, unsafe import/restore behavior, or command injection.
- MongoDB wire authentication or authorization bypass.
- Backup, snapshot, or export exposing protected data.
- Remote code execution, memory safety, or denial of service with a realistic
  attack path.
- Data corruption caused by a security-relevant logic error.

Out of scope unless they demonstrate a concrete security impact:

- Missing hardening headers already documented in `docs/security-check.md`.
- Reports that require root, physical access, or a compromised host.
- Social engineering, spam, or generic dependency-version warnings without a
  reachable exploit path.
- Denial of service caused solely by intentionally unbounded queries from an
  already authorized administrator.
- Vulnerabilities in third-party services or infrastructure outside this
  repository.

## Process

- We aim to acknowledge a report within a few business days.
- We will validate the issue, prepare a fix, and coordinate disclosure.
- Please give us reasonable time to release a fix before publishing details.
- Credit is offered when requested and when it does not expose sensitive
  information.
- There is no bug bounty program.

## Safe harbor

Good-faith research that follows this policy, avoids privacy violations,
avoids destructive testing, and uses test data will not be treated as an attack
on the project. Stop testing and report immediately if you encounter real user
data or a production system.

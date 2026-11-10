# Security policy

## Supported versions

squad is before 1.0. Security fixes are made on the latest release and
on `main`; there are no maintained older branches yet.

## Reporting a vulnerability

Please do **not** open a public issue for a security problem. Report it
privately through GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
("Security" tab of the repository → "Report a vulnerability").

Include what you can: the affected version or commit, a description of
the problem and its impact, and steps or a test case that reproduce it.
You will get an answer within a week. Once a fix is available we publish
an advisory and credit the reporter, unless you prefer otherwise.

## Scope

In scope are the crates in this repository: the storage engine, the SQL
engine, the object-store clients and the wire-protocol server. Of
particular interest are:

- loss or corruption of acknowledged commits, and anything that defeats
  writer fencing in remote storage or replication;
- authentication bypass, TLS downgrade, or resource exhaustion of the
  server before authentication;
- reads of data outside a session's snapshot or transaction.

## Deployment notes

- `squad serve` accepts unauthenticated connections only on loopback
  addresses unless trust is enabled explicitly. Anything reachable over
  a network should use passwords (SCRAM-SHA-256) and TLS; see
  [docs/server.md](docs/server.md).
- Object-store credentials are read from the URL or from the standard
  `AWS_*` environment variables. Prefer the environment (or your
  orchestrator's secrets) over URLs in command lines, which other users
  of the machine can see.

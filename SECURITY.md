# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for a security problem. Report it privately through GitHub's *Report a vulnerability* (the Security tab of this repository), or by email to the maintainer address on the repository profile.

Include what you found, how to reproduce it, and what you think the impact is. You will get an acknowledgement within 3 working days and a first assessment within 10. We will agree a disclosure date with you, normally within 90 days, and credit you in the advisory unless you prefer not to be named.

Things we count as vulnerabilities, for example:

- a statement that tenantdb attributes to the wrong tenant, or to fewer tenants than it really touches (the attribution being *unsound*), because that hides a data leak. A reproduction is a schema, a statement and the tenants it actually reads;
- a way around `-strict=block` (a statement that should be refused is forwarded);
- a way to crash, hang or exhaust the memory of the proxy from a client connection;
- leaking data values or credentials into logs, metrics or error messages;
- anything that weakens TLS or authentication relay.

## Supported versions

Security fixes are made on the latest minor release.

## What tenantdb is, and is not, a defence against

tenantdb **observes and gates the SQL that passes through it**. It is a control for finding and preventing *application mistakes* (a forgotten tenant filter). It is not, on its own, a defence against a client that already has database credentials and is willing to bypass it by connecting to the database directly. Keep the database reachable only from tenantdb (and your admin tooling), and use the database's own controls (roles, row-level security) as the enforcement layer; tenantdb tells you where that enforcement is missing.

In block mode it fails closed (see [Failure behaviour](docs/OPERATIONS.md#failure-behaviour)). Its attribution is checked continuously against PostgreSQL itself: a differential fuzzer runs random statements through the analysis and through a real server and fails if the tenants the server returns are not inside what tenantdb claimed; row-level security, `COPY`, triggers, views and other features have their own comparisons against the server.

## Operating it safely

- **Credentials.** Pass the schema role's connection string through `TENANTDB_SCHEMA_DSN`, not a flag (flags appear in process lists) or a world-readable file. That role should be read-only: `SELECT` on the application tables and nothing else. tenantdb never writes to the database through it.
- **Client and upstream TLS.** Use `-tls-cert/-tls-key` and `-require-tls` for clients, and `-upstream-tls` to the database. Authentication (password, SCRAM) is relayed untouched; tenantdb never sees or stores a password it could not already see on the wire inside its own TLS termination. SCRAM channel binding (`SCRAM-SHA-256-PLUS`) cannot be relayed through any TLS-terminating proxy; clients that *require* it should connect without it or use `channel_binding=prefer`.
- **Logs.** By default statements are logged with every literal replaced by a placeholder. Do not switch to `-log-sql full` in production: statements contain the data your users sent.
- **Admin endpoints** (`-admin-addr`) have no authentication. Bind them to loopback or an internal interface.
- **Credential lookups.** For queries like `WHERE token = $1`, tenantdb asks the database which tenant the token belongs to. The token is hashed before it is cached, and cached entries expire after at most a minute.
- **Dependencies.** The binary has a small dependency set (`pgx`, `pg_query_go`, `protobuf`); CI scans them (`govulncheck`) and updates are automated.

## Verifying a release

Releases publish SHA-256 checksums, signed with [cosign](https://github.com/sigstore/cosign) (keyless, through the GitHub Actions identity of this repository), and an SBOM. See the release notes for the exact commands.

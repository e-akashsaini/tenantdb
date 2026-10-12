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

**What block mode checks, and what it takes on trust.** Block mode refuses a statement unless it can show the statement is confined to some tenant. The things it relies on without being able to prove them for every row:

- *The connection tag is declared by the client.* A statement that names no tenant is attributed to the tag; block mode therefore enforces "confined to some tenant", not "to the right one". Treat the tag as a convenience for apps that also use row-level security, not as authentication of the tenant.
- *Relationships between tables tie rows to one tenant.* A foreign key or learned join (`orders.customer_id -> customers.id`) is used to carry a tenant from one table to another once a sample of the data shows the rows agree, and is re-checked every ten minutes. A row written through tenantdb that contradicts it is refused and the relationship is distrusted; a row written some other way is caught at the next re-check. Composite keys that include the tenant column, or row-level security, make the database enforce this.
- *A table with no tenant column and no foreign key to a tenant table is shared data.* A table that has a foreign key to a tenant table (line items that reach their tenant through an order) is taken to belong, row by row, to its parent's tenant; list a table that is shared despite such a key in `-shared-tables`. A child that reaches its tenant only through another child (two hops) is still treated as shared; give it a tenant column or a direct key.
- *Trusted roles are not held to the policy* (`-strict-exempt`).

In block mode it fails closed (see [Failure behaviour](docs/OPERATIONS.md#failure-behaviour)). Its attribution is checked continuously against PostgreSQL itself: a differential fuzzer runs random statements through the analysis and through a real server and fails if the tenants the server returns are not inside what tenantdb claimed; row-level security, `COPY`, triggers, views and other features have their own comparisons against the server.

## Operating it safely

- **Credentials.** Pass the schema role's connection string through `TENANTDB_SCHEMA_DSN`, not a flag (flags appear in process lists) or a world-readable file. That role should be read-only: `SELECT` on the application tables and nothing else. tenantdb never writes to the database through it.
- **Client and upstream TLS.** Use `-tls-cert/-tls-key` and `-require-tls` for clients, and `-upstream-tls` to the database. Authentication (password, SCRAM) is relayed untouched; tenantdb never sees or stores a password it could not already see on the wire inside its own TLS termination. SCRAM channel binding (`SCRAM-SHA-256-PLUS`) is designed to detect a TLS-terminating middlebox, so it only works through tenantdb if the proxy presents the database's own certificate; otherwise set `channel_binding=disable` on the client. A client that *requires* channel binding is refused rather than silently downgraded. See [TLS, client certificates and SCRAM channel binding](docs/OPERATIONS.md#tls-client-certificates-and-scram-channel-binding). `-tls-client-ca` adds mutual TLS in front of the proxy.
- **Logs.** By default statements are logged with every literal replaced by a placeholder. Do not switch to `-log-sql full` in production: statements contain the data your users sent.
- **Admin endpoints** (`-admin-addr`) have no authentication. Bind them to loopback or an internal interface.
- **Credential lookups.** For queries like `WHERE token = $1`, tenantdb asks the database which tenant the token belongs to. The token is hashed before it is cached, and cached entries expire after at most a minute.
- **Dependencies.** The binary has a small dependency set (`pgx`, `pg_query_go`, `protobuf`); CI scans them (`govulncheck`) and updates are automated.

## Verifying a release

Releases publish SHA-256 checksums, signed with [cosign](https://github.com/sigstore/cosign) (keyless, through the GitHub Actions identity of this repository), and an SBOM. See the release notes for the exact commands.

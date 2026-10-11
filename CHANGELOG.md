# Changelog

All notable changes. The project follows [semantic versioning](https://semver.org); before 1.0 a minor version may change command-line flags or log fields (always listed here).

## Unreleased

### Attribution
- **Credential lookups** (`WHERE token = $1` joined to the user): attributed through the row the credential points at, with a hashed, short-lived cache.
- **`IN (SELECT ...)` / `= ANY (SELECT ...)`**: the subquery's confinement carries to the outer table when the columns are provably related. A correlated `EXISTS (SELECT ... WHERE s.x = o.y ...)` is read the same way.
- **Views** are read through their definitions: simple views are merged into the query (renamed tenant columns, per-tenant views, `current_setting` views), others are analysed as subqueries.
- **Learned relationships**: a join the application runs (`a.x = b.y`, `b.y` unique) becomes a foreign key candidate, verified against the data before it is used.
- **Row-level security**: policies are credited as an extra condition for roles that are subject to them (superuser, `BYPASSRLS`, owner and `FORCE` are handled; `SET ROLE` is tracked).
- **Triggers** are analysed with `NEW`/`OLD` bound to the row's tenant; writes to tables with rules are flagged.
- **`COPY ... FROM STDIN`** streams (text and CSV) are read, and the load is attributed to the tenants its rows name. In `block` mode the load is judged when the data ends and aborted with `CopyFail` (rolled back by PostgreSQL) if it violates the policy; binary streams are still refused up front.
- **`EXECUTE`** of a literal or `format()` string inside functions is read.
- PL/pgSQL bodies that `RETURN` a value, `RETURN NEXT/QUERY`, or assign variables are now readable (they used to be opaque).
- Two occurrences of the same table joined on a unique column are the same row, and so the same tenant.
- Events leave a session in the order its statements were sent, even when an earlier one waits on a row lookup.

### Operations
- Prometheus metrics, `/healthz`, `/readyz`, `/version` (`-admin-addr`).
- JSON logs (`-log-format json`), log levels, and logs without data values by default (`-log-sql normalized`).
- Graceful shutdown: idle clients are sent away, running queries finish (`-shutdown-timeout`); `SIGHUP` reloads the schema.
- Config file (`-config`).
- `-version`.
- Mutual TLS (`-tls-client-ca`): client certificates are verified in the handshake and recorded on the session.
- `-scram-channel-binding`: see below.
- A `Dockerfile` (distroless, non-root, with a `healthcheck` subcommand), CI (tests with `-race`, differential fuzzing, end-to-end checks, `govulncheck`), and a release workflow that publishes signed binaries, an image and an SBOM.
- `SECURITY.md`, `docs/OPERATIONS.md`.

### Fixed
- **SCRAM authentication through the proxy.** With TLS to the database, libpq-based clients (psql, psycopg, Rails' `pg`) failed to log in: with TLS to the proxy because channel binding cannot match across two TLS connections, without TLS because the database offered `SCRAM-SHA-256-PLUS` on what the client saw as a plain connection. The proxy now hides `-PLUS` from clients that cannot use it, and explains the remaining case (see `docs/OPERATIONS.md`).
- Updated Go to 1.27.2 and the dependencies: `govulncheck` reported standard-library and `golang.org/x/text` vulnerabilities in the previous toolchain.
- Draining treated a session with a query in flight as idle when no gate was installed.

## Earlier

Structural soundness model (every tenant-table occurrence must be confined), differential fuzzing against PostgreSQL, parser crash guard, protocol limits, pooler compatibility, and the DocuSeal case study. See the git history.

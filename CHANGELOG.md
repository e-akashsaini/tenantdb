# Changelog

All notable changes. The project follows [semantic versioning](https://semver.org); before 1.0 a minor version may change command-line flags or log fields (always listed here).

## Unreleased

### Attribution
- **Credential lookups** (`WHERE token = $1` joined to the user): attributed through the row the credential points at, with a hashed, short-lived cache.
- **`IN (SELECT ...)` / `= ANY (SELECT ...)`**: the subquery's confinement carries to the outer table when the columns are provably related.
- **Views** are read through their definitions: simple views are merged into the query (renamed tenant columns, per-tenant views, `current_setting` views), others are analysed as subqueries.
- **Learned relationships**: a join the application runs (`a.x = b.y`, `b.y` unique) becomes a foreign key candidate, verified against the data before it is used.
- **Row-level security**: policies are credited as an extra condition for roles that are subject to them (superuser, `BYPASSRLS`, owner and `FORCE` are handled; `SET ROLE` is tracked).
- **Triggers** are analysed with `NEW`/`OLD` bound to the row's tenant; writes to tables with rules are flagged.
- **`COPY ... FROM STDIN`** streams (text and CSV) are read, and the load is attributed to the tenants its rows name.
- **`EXECUTE`** of a literal or `format()` string inside functions is read.
- PL/pgSQL bodies that `RETURN` a value, `RETURN NEXT/QUERY`, or assign variables are now readable (they used to be opaque).
- Two occurrences of the same table joined on a unique column are the same row, and so the same tenant.

### Operations
- Prometheus metrics, `/healthz`, `/readyz`, `/version` (`-admin-addr`).
- JSON logs (`-log-format json`), log levels, and logs without data values by default (`-log-sql normalized`).
- Graceful shutdown: idle clients are sent away, running queries finish (`-shutdown-timeout`); `SIGHUP` reloads the schema.
- Config file (`-config`).
- `-version`.
- `SECURITY.md`, `docs/OPERATIONS.md`.

### Fixed
- Draining treated a session with a query in flight as idle when no gate was installed.

## Earlier

Structural soundness model (every tenant-table occurrence must be confined), differential fuzzing against PostgreSQL, parser crash guard, protocol limits, pooler compatibility, and the DocuSeal case study. See the git history.

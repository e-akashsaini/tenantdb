# tenantdb

A PostgreSQL proxy that works out **which tenant every SQL statement belongs to**, and can refuse the ones it can't pin down.

It sits between your application and Postgres, speaks the real wire protocol, and needs **no changes to the application**. You tell it which column holds the tenant (`tenant_id`, `org_id`, ...) and it parses each statement to find the tenant, with the actual Postgres parser (`pg_query_go`).

```
app ──TLS──▶ tenantdb :6432 ──▶ PostgreSQL :5432
               │  parses every statement, attributes it to a tenant,
               │  logs it, and (optionally) blocks what it can't attribute
               └─ read-only connection ──▶ schema, function bodies, row ownership, cache warm-up
```

## Why

Multi-tenant apps on a shared database depend on every query remembering `WHERE tenant_id = ...`. One forgotten filter leaks another customer's data. tenantdb makes that visible, and optionally impossible, without touching application code.

Authentication, TLS to the upstream, and query cancel are relayed untouched. Clients can encrypt their connection to the proxy too (`-tls-cert`).

## How a tenant is found

Each statement gets a **source** saying how its tenant was determined, tried roughly in this order:

| Source | Meaning |
|---|---|
| `explicit` | The statement names it: `tenant_id = 'acme'`, `IN (...)`, `INSERT` values, bound parameters (`$1`), `UPDATE`/`DELETE` filters, or a function call whose body filters on the tenant you pass it. |
| `untenanted` | Every table touched has no tenant column (shared reference data, `SELECT 1`). Nothing to attribute. |
| `derived` | Reads a temporary table that was filled from one tenant's data, so that tenant is whose data it is. |
| `cache` | The statement names a row (`WHERE id = 42`) whose owner was learned earlier from an `INSERT`, from result rows, or from start-up pre-warming. |
| `lookup` | Not cached, so the proxy asked the database (`SELECT tenant_id FROM t WHERE id = $1`) over its read-only connection. |
| `nomatch` | The statement names a row that doesn't exist. |
| `session` | The connection declared its tenant (see [Connection tag](#connection-tag)). |
| `transaction` | Nothing in this statement, but an earlier one in the same transaction named the tenant. |
| `scan` | Tenant data read with **no tenant or key filter at all**. It spans every tenant. Definite, not a guess. |
| `unknown` | Tenant data, and no tenant could be determined. This is the one to worry about. |

Every event also carries `scan` (reads across tenants, even if a tag attributed it) and `conflict` (disagrees with the connection's tag or the transaction's tenant).

### Things it understands
- **Prepared statements**, both protocol-level (`$n` mapped at `Parse`, read at `Bind`; text and binary UUID/integer values) and SQL-level (`PREPARE`/`EXECUTE`/`DEALLOCATE`).
- **Schemas.** Tables are resolved the way Postgres does, through `search_path`: `SET`, `SET LOCAL`, `RESET`, `DISCARD ALL`, startup `options`, and the role/database defaults from `ALTER ROLE/DATABASE ... SET search_path`.
- **Functions and procedures.** A call to an application-defined function is analysed by reading its body (SQL and PL/pgSQL). Each statement in the body is attributed with the call's arguments bound to the function's parameters, so `SELECT * FROM orders_of('acme')` is attributed to `acme` if the function filters on that argument. Arguments pass through nested calls. A function that reads tenant data without saying whose is `unknown` (or `scan` if it reads everything). `DO` blocks are read the same way.
- **Statements that run a query without being a plain `SELECT`**: `CREATE TABLE ... AS`, `SELECT ... INTO`, `EXPLAIN ANALYZE`, cursors, `COPY`, `CALL`, `EXECUTE`. They are attributed like the query they run; otherwise they would be a way around the policy. A plain `EXPLAIN` executes nothing and isn't attributed.
- **Temporary tables.** A temp table with its own tenant column is filtered like any tenant table. One filled from a query inherits the tenants of that query (`derived`); one filled from unattributed data stays `unknown`. They are per-session, shadow real tables the way Postgres does, and never enter the shared cache.
- **`INSERT` without a column list**, using the real column order from the catalog.
- **Rows by any unique column, and through foreign keys.** A statement that names a row by a unique column (`id`, `slug`, `uuid`: anything with a single-column unique index) is attributed to that row's tenant. A lookup by a foreign key (`WHERE submission_id = 5`) is attributed through the parent row. Foreign keys are the declared ones plus columns named `<thing>_id` next to a table called `<thing>` or `<thing>s`; each is checked against a sample of real rows (the child must always have its parent's tenant) and discarded if it isn't, or if there are no rows to check yet.
- **Polymorphic associations (Rails).** `WHERE record_type = 'Template' AND record_id = 5` is resolved through the table the class name maps to (`ActiveStorage::Blob` is `active_storage_blobs`), after the mapping has been checked against real rows. A type with no rows yet, or ids that come from a subquery, stay unknown.
- **Joins and aliases.** `... FROM templates JOIN submissions ... WHERE submissions.id = $1` is read through the qualifier. An unqualified column in a join could belong to either table, so it names none.
- **`OR`.** A WHERE clause confines a statement to a tenant only if every alternative does: `tenant_id = 'a' AND (x = 1 OR y = 2)` is `a`; `tenant_id = 'a' OR amount > 5` is not attributed to `a`.
- **Cache hygiene.** Rows are learned only from successful `INSERT`s and from result columns that come straight from one table. `DELETE` evicts, an `UPDATE` that changes a unique or tenant column drops that table's entries, and `TRUNCATE`/`DROP TABLE` forget the table. Entries also expire after `-cache-ttl`.

### It fails closed
These are `unknown`, never `untenanted`:
- a table the schema snapshot doesn't know,
- a view that hides its tenant column (it may wrap tenant tables),
- a temp table filled from data that couldn't be attributed,
- a call to a function whose code can't be read: dynamic SQL (`EXECUTE` of a computed string), `plpython`, C, and so on.

Built-in and extension functions are ignored.

## Quick start

Requires Go and, for the parser, a C toolchain (`pg_query_go` uses cgo).

```bash
cd proxy
go build -o tenantdb ./cmd/tenantdb

# Observe only: log which tenant every statement belongs to.
./tenantdb -listen :6432 -upstream 127.0.0.1:5432

# Point your app at :6432 instead of :5432. Nothing else changes.
```

For the full feature set, give it a **read-only role** to read the schema, pre-warm the cache and look up rows:

```sql
CREATE ROLE tenantdb_ro LOGIN PASSWORD '...' BYPASSRLS;       -- BYPASSRLS only lets it *read* every row for lookups
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tenantdb_ro;   -- needed to pre-warm and look up rows
```
```bash
export TENANTDB_SCHEMA_DSN='postgres://tenantdb_ro:...@127.0.0.1:5432/mydb'
./tenantdb -listen :6432 -upstream 127.0.0.1:5432
```

Prefer the environment variable over `-schema-dsn`: command-line flags show up in process lists. The role needs `SELECT` only to pre-warm and look up rows; it sees the full schema through `pg_catalog` either way.

**Row-level security.** A role subject to RLS can't see all rows, so lookups and pre-warming through it would silently miss some. The proxy detects this at start-up, warns, and skips those tables (their statements fall back to `unknown`). Give the schema role `BYPASSRLS` (it stays read-only) to fix it.

### Next to a connection pooler
Put tenantdb **in front of** a pooler such as PgBouncer (app -> tenantdb -> PgBouncer -> Postgres). Each client connection is then seen on its own, so connection tags, transactions and `search_path` follow the client the way they should. [`bench/compat`](bench/compat) runs the same workload through psycopg2, psycopg3, asyncpg, SQLAlchemy and node-postgres in every arrangement, PgBouncer in either order included.

### Encrypting client connections
```bash
./tenantdb -tls-cert server.crt -tls-key server.key            # clients may use TLS
./tenantdb -tls-cert server.crt -tls-key server.key -require-tls  # clients must
```
The certificate is re-read when the file changes (checked every 30 s), so renewals need no restart. TLS to the upstream is separate: `-upstream-tls`.

## Auditing a recorded workload: `tenantdb analyze`

The same attribution, run over a recording instead of live traffic. Nothing sits in the path of a database; you need only read access to one with the same schema.

```bash
# Record what the app runs (staging, a canary, or CI), in postgresql.conf:
#   log_statement = 'all'      log_line_prefix = '%m [%p] %u@%d '
export TENANTDB_SCHEMA_DSN='postgres://tenantdb_ro:...@staging/mydb'
tenantdb analyze -tenant-columns account_id postgres.log

# or from pg_stat_statements:
#   \copy (SELECT query, calls FROM pg_stat_statements) TO 'q.csv' CSV HEADER
tenantdb analyze -tenant-columns account_id q.csv
```

Formats: `.log` (PostgreSQL log, including bound parameters), `.csv` (a `pg_stat_statements` export; statements are weighted by `calls`), `.sql` (plain statements); `-format` overrides. `-json` writes the full report for scripts.

```
tenantdb analysis
=================
5747 statements read; 4947 attributed, 800 need no attribution (SET, BEGIN, DDL, ...)

How tenants were determined
  explicit          1065   21.5%
  cache             1484   30.0%
  untenanted        1985   40.1%
  scan               213    4.3%
  unknown            200    4.0%

What strict mode would flag: 413 of 4947 attributed statements (8.3%)

Worst query shapes (statements that differ only in their literals are grouped)
   1. [unknown] x152  public.access_tokens, public.users
      SELECT "users".* FROM "users" INNER JOIN "access_tokens" ...
```

By default it warms the row cache from the database and looks up the rows a statement names (`-warm`, `-lookup`), so a statement-by-id in a log with literal values is attributed. A `pg_stat_statements` export has only `$1` placeholders, so statements by id stay `unknown` there: the report still tells you which queries have no tenant filter, which is the main thing an audit wants.

### On a real Rails app

[`bench/docuseal/`](bench/docuseal/README.md) runs DocuSeal (Rails 8, `account_id` on its tables) behind the proxy and drives it. 4,947 statements: 8.3% would be flagged, almost all of them authentication, cross-account lookups, global configuration reads and bootstrap checks; and in `block` mode the unmodified app stops at login. Read the caveats there before quoting any number: it is one app, one account, scripted traffic.

### Running your own test suite through it

Point the test database connection at tenantdb (alert mode, `-schema-dsn` at a database with the same schema) and run the suite. The tests exercise your real ORM and driver, so the log shows the SQL shapes you actually produce. Or skip the proxy: enable `log_statement = 'all'` on the test database, run the suite, and `tenantdb analyze` the log. Nothing is at risk either way, and it covers whatever your tests cover and no more.

## Strict mode

`-strict` decides what happens to statements that violate the policy.

| Mode | Behaviour |
|---|---|
| `off` (default) | Report sources only. |
| `alert` | Flag violations in the log (`msg=violation`, WARN). Everything goes through. |
| `block` | Refuse to forward violations. Needs `-schema-dsn`. |

A statement **violates** when it is:
- `unknown`: tenant data, no tenant determined,
- `scan`: tenant data with no tenant or key filter,
- a `conflict`: a different tenant than the connection's tag or the transaction's.

`untenanted` statements and `nomatch` rows never violate. Things that can't be inspected are `unknown`: `COPY ... FROM STDIN` (tenants in the data stream), and legacy fast-path function calls (large-object calls such as `\lo_import`).

Two role lists relax the policy:
- `-strict-exempt=migrator,admin`: never held to the policy (migrations, admin tools, bulk `COPY` loads).
- `-strict-allow-scan=reports`: may read across all tenants, but must still be attributable.

### What blocking does to the client
- **Simple query:** dropped before the database sees it. The client gets `ERROR 42501 tenantdb: statement blocked (scan): ...` and the connection stays usable. A multi-statement query is refused as a whole.
- **Extended protocol (prepared statements):** the proxy holds `Parse/Bind/Describe/Execute` until `Sync`, so a refused batch never reaches the database. The client gets the error and a normal `ReadyForQuery`.
- **Inside a transaction (or any case where the database may already hold work):** the proxy sends a `FATAL` error and **closes the connection**, so Postgres rolls everything back. It can't fake an aborted transaction faithfully, and it won't leave one half-done. Clients will see a dropped connection.
- **Lookups** are finished before deciding, so a first-time row lookup can add latency in `block` mode. If the lookup fails, the statement is blocked (nothing is known, so nothing is allowed).
- **If the check itself fails** (a bug in the gate), the statement is refused with `XX000` by default (`-strict-on-error=block`). Use `-strict-on-error=allow` to let such statements through instead.

### Rolling it out
1. Run with `-strict=off` and read the logs.
2. Switch to `-strict=alert` against real traffic. Fix what legitimate traffic gets flagged (add roles to `-strict-exempt`/`-strict-allow-scan`, tag connections, or fix the queries).
3. Only then switch to `-strict=block`.

`alert` and `block` refuse to start without `-schema-dsn`, and `block` also refuses to start if the schema can't be loaded: failing open would silently allow everything, failing closed would block everything.

## Connection tag

A connection can **declare its tenant**, which attributes every query on it without parsing. This is the most reliable layer for apps that set the tenant per connection or per request:

```sql
SET tenantdb.tenant = 'acme';                      -- session
SET LOCAL tenantdb.tenant = 'acme';                -- until the transaction ends
SELECT set_config('tenantdb.tenant', 'acme', true); -- what most ORMs/drivers emit
```
```bash
PGOPTIONS="-c tenantdb.tenant=acme" psql ...       # startup option
```

A statement whose rows belong to a different tenant is flagged `conflict`; an unfiltered read on a tagged connection is still flagged `scan`. The variable name is configurable with `-tag-var`.

## Flags

Each flag also has an environment variable where listed.

| Flag | Env | Default | |
|---|---|---|---|
| `-listen` | `TENANTDB_LISTEN` | `:6432` | Address to accept clients on |
| `-upstream` | `TENANTDB_UPSTREAM` | `127.0.0.1:5432` | PostgreSQL `host:port` |
| `-upstream-tls` | `TENANTDB_UPSTREAM_TLS=1` | off | Dial the upstream with TLS |
| `-upstream-tls-insecure` | | off | Skip upstream cert verification (testing only) |
| `-tls-cert`, `-tls-key` | `TENANTDB_TLS_CERT`, `TENANTDB_TLS_KEY` | | Let clients encrypt their connection to the proxy |
| `-require-tls` | `TENANTDB_REQUIRE_TLS=1` | off | Refuse clients that don't use TLS |
| `-tenant-columns` | `TENANTDB_TENANT_COLUMNS` | `tenant_id` | Column(s) holding the tenant, comma-separated |
| `-key-columns` | `TENANTDB_KEY_COLUMNS` | `id` | Columns that identify a row when there is no schema to consult. With `-schema-dsn`, every provably unique column identifies a row. Empty disables the row cache |
| `-cache-size` | | `100000` | Max rows in the row-to-tenant cache |
| `-cache-ttl` | | `1h` | How long a cached owner is trusted (`0`: until evicted) |
| `-schema-dsn` | `TENANTDB_SCHEMA_DSN` | | Read-only role connection string (schema, function bodies, pre-warm, lookups) |
| `-schema-refresh` | | `5m` | How often to re-read the schema (it also reloads after DDL) |
| `-prewarm` | | on | Fill the cache from the database at start-up (background) |
| `-lookup` | | on | Ask the database about rows missing from the cache |
| `-tag-var` | `TENANTDB_TAG_VAR` | `tenantdb.tenant` | Session variable for the connection tag |
| `-strict` | `TENANTDB_STRICT` | `off` | `off`, `alert` or `block` |
| `-strict-exempt` | `TENANTDB_STRICT_EXEMPT` | | Roles never held to the policy |
| `-strict-allow-scan` | `TENANTDB_STRICT_ALLOW_SCAN` | | Roles allowed to scan |
| `-strict-on-error` | `TENANTDB_STRICT_ON_ERROR` | `block` | In block mode, what to do with a statement the check itself failed on: `block` or `allow` |
| `-debug` | | off | Verbose logging |

Pre-warming gives each table an equal share of the `-cache-size` budget, so one huge table can't starve the rest.

## Log output

```
level=INFO msg=query     session=7 user=app db=shop tenants=[acme] source=explicit scan=false conflict=false sql="SELECT ..."
level=WARN msg=violation session=9 ... source=unknown violation=unknown sql="SELECT dyn_fn()"
level=WARN msg=blocked   session=9 ... source=scan scan=true violation=scan sql="UPDATE orders SET amount = -1"
```

## Development

```bash
cd proxy
go vet ./... && go test -race ./...
```

`-race` and the parser need cgo (a C compiler and libc headers). On Windows, run the tests in WSL: Windows application-control policies may block the freshly built, unsigned test binary.

Some tests need a live Postgres and are skipped unless you set `TENANTDB_TEST_DSN`:

```bash
docker run -d --name pgtest -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16
TENANTDB_TEST_DSN=postgres://postgres:pw@127.0.0.1:5432/postgres go test -race ./...
```

### Layout

```
proxy/
  cmd/tenantdb/            flags, wiring, certificate reloading; `analyze` subcommand
  internal/analyze/        read a SQL file / pg_stat_statements / Postgres log, replay it, report
  internal/proxy/          wire-protocol relay: TLS, Observer (watch) and Gate (veto)
  internal/attrib/
    extract.go shape.go    parse a statement: tenants, keys, tables, calls, SET
    utility.go             CREATE TABLE AS, EXPLAIN ANALYZE, COPY, PREPARE/EXECUTE, temp tables
    funcs.go               read function and DO-block bodies (SQL and PL/pgSQL)
    attributor.go          per-connection state, sources, analysis of calls and temp tables
    cache.go               shared, bounded, expiring row -> tenant cache (LRU)
    catalog.go             schema snapshot: tables, uniqueness, RLS, functions, search_path
    lookup.go              ask the database who owns a row (de-duplicated)
    warm.go                background cache pre-warming
    strict.go              the policy and the blocking gate
bench/docuseal/            run a real Rails app (DocuSeal) behind the proxy and drive it
```

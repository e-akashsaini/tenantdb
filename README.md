# tenantdb

A PostgreSQL proxy that works out **which tenant every SQL statement belongs to**, and can refuse the ones it can't pin down.

It sits between your application and Postgres, speaks the real wire protocol, and needs **no changes to the application**. You tell it which column holds the tenant (`tenant_id`, `org_id`, ...) and it parses each statement to find the tenant, with the actual Postgres parser (`pg_query_go`).

```
app ──▶ tenantdb :6432 ──▶ PostgreSQL :5432
          │  parses every statement, attributes it to a tenant,
          │  logs it, and (optionally) blocks what it can't attribute
          └─ read-only connection ──▶ schema, row ownership, cache warm-up
```

## Why

Multi-tenant apps on a shared database depend on every query remembering `WHERE tenant_id = ...`. One forgotten filter leaks another customer's data. tenantdb makes that visible, and optionally impossible, without touching application code.

Authentication, TLS to the upstream, and query cancel are relayed untouched. The proxy is transparent to clients and fails open: if its own attribution code panics, the statement still goes through.

## How a tenant is found

Each statement gets a **source** saying how its tenant was determined, tried roughly in this order:

| Source | Meaning |
|---|---|
| `explicit` | The statement names it: `tenant_id = 'acme'`, `IN (...)`, `INSERT` values, bound parameters (`$1`), also `UPDATE`/`DELETE` filters. |
| `untenanted` | Every table touched has no tenant column (shared reference data, `SELECT 1`). Nothing to attribute. |
| `cache` | The statement names a row (`WHERE id = 42`) whose owner was learned earlier from an `INSERT`, from result rows, or from start-up pre-warming. |
| `lookup` | Not cached, so the proxy asked the database (`SELECT tenant_id FROM t WHERE id = $1`) over its read-only connection. |
| `nomatch` | The statement names a row that doesn't exist. |
| `session` | The connection declared its tenant (see [Connection tag](#connection-tag)). |
| `transaction` | Nothing in this statement, but an earlier one in the same transaction named the tenant. |
| `scan` | Tenant data read with **no tenant or key filter at all**. It spans every tenant. Definite, not a guess. |
| `unknown` | Tenant data, and no tenant could be determined. This is the one to worry about. |

Every event also carries `scan` (reads across tenants, even if a tag attributed it) and `conflict` (disagrees with the connection's tag or the transaction's tenant).

### Things it understands
- Prepared statements: `$n` is mapped to the tenant column at `Parse` time and read at `Bind` time (text and binary UUID/integer values).
- Schemas: tables are resolved the way Postgres does, through `search_path` (`SET`, `SET LOCAL`, `RESET`, `DISCARD ALL`, startup `options`). The same table name in two schemas is not ambiguous.
- `INSERT` without a column list, using the real column order from the catalog.
- Cache hygiene: rows are learned only from successful `INSERT`s and from result columns that come straight from one table. `DELETE` evicts, and an `UPDATE` that changes a key or tenant column drops that table's entries.

### It fails closed
These are `unknown`, never `untenanted`:
- a table the schema snapshot doesn't know (e.g. a temp table),
- a view that hides its tenant column (it may wrap tenant tables),
- a call to an **application-defined** function (its body may read tenant tables). Built-in and extension functions are ignored.

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
CREATE ROLE tenantdb_ro LOGIN PASSWORD '...';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tenantdb_ro;   -- needed to pre-warm and look up rows
```
```bash
export TENANTDB_SCHEMA_DSN='postgres://tenantdb_ro:...@127.0.0.1:5432/mydb'
./tenantdb -listen :6432 -upstream 127.0.0.1:5432
```

Prefer the environment variable over `-schema-dsn`: command-line flags show up in process lists. The role needs `SELECT` only to pre-warm and look up rows; it sees the full schema through `pg_catalog` either way.

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

`untenanted` statements and `nomatch` rows never violate.

Two role lists relax the policy:
- `-strict-exempt=migrator,admin`: never held to the policy (migrations, admin tools).
- `-strict-allow-scan=reports`: may read across all tenants, but must still be attributable.

### What blocking does to the client
- **Simple query:** dropped before the database sees it. The client gets `ERROR 42501 tenantdb: statement blocked (scan): ...` and the connection stays usable. A multi-statement query is refused as a whole.
- **Extended protocol (prepared statements):** the proxy holds `Parse/Bind/Describe/Execute` until `Sync`, so a refused batch never reaches the database. The client gets the error and a normal `ReadyForQuery`.
- **Inside a transaction (or any case where the database may already hold work):** the proxy sends a `FATAL` error and **closes the connection**, so Postgres rolls everything back. It can't fake an aborted transaction faithfully, and it won't leave one half-done. Clients will see a dropped connection.
- **Lookups** are finished before deciding, so a first-time row lookup can add latency in `block` mode. If the lookup fails, the statement is blocked (nothing is known, so nothing is allowed).

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
| `-tenant-columns` | `TENANTDB_TENANT_COLUMNS` | `tenant_id` | Column(s) holding the tenant, comma-separated |
| `-key-columns` | `TENANTDB_KEY_COLUMNS` | `id` | Columns identifying a row (row cache). Empty disables the cache |
| `-cache-size` | | `100000` | Max rows in the row-to-tenant cache |
| `-schema-dsn` | `TENANTDB_SCHEMA_DSN` | | Read-only role connection string (schema, pre-warm, lookups) |
| `-schema-refresh` | | `5m` | How often to re-read the schema (it also reloads after DDL) |
| `-prewarm` | | on | Fill the cache from the database at start-up (background) |
| `-lookup` | | on | Ask the database about rows missing from the cache |
| `-tag-var` | `TENANTDB_TAG_VAR` | `tenantdb.tenant` | Session variable for the connection tag |
| `-strict` | `TENANTDB_STRICT` | `off` | `off`, `alert` or `block` |
| `-strict-exempt` | `TENANTDB_STRICT_EXEMPT` | | Roles never held to the policy |
| `-strict-allow-scan` | `TENANTDB_STRICT_ALLOW_SCAN` | | Roles allowed to scan |
| `-debug` | | off | Verbose logging |

Pre-warming gives each table an equal share of the `-cache-size` budget, so one huge table can't starve the rest.

## Log output

```
level=INFO msg=query     session=7 user=app db=shop tenants=[acme] source=explicit scan=false conflict=false sql="SELECT ..."
level=WARN msg=violation session=9 ... source=unknown violation=unknown sql="SELECT my_fn()"
level=WARN msg=blocked   session=9 ... source=scan scan=true violation=scan sql="UPDATE orders SET amount = -1"
```

## Known limits

- **Plaintext on the client side.** The proxy answers a client's TLS request with "no" and the client falls back to plaintext, so run it next to the app (same host/pod/private network). TLS **to the upstream** is supported (`-upstream-tls`).
- **No connection pooling.** It's one upstream connection per client connection.
- **Application-defined functions are opaque.** They are classed `unknown`, never `untenanted`, but the proxy can't see which tenants they read.
- **Tables are matched by schema and name.** A table the catalog doesn't know (temp tables) is `unknown`. `ALTER ROLE ... SET search_path` isn't visible to the proxy; only `SET` statements and startup options are.
- **Key columns are assumed unique** (default `id`). A non-unique key makes the row cache wrong.
- **A cold or stale cache** costs a database lookup (`lookup` source), not correctness, as long as `-lookup` is on and the read-only role can `SELECT` the table.
- `COPY` data and function-call messages aren't gated in `block` mode.
- If the gate itself panics, the statement is let through and the panic is logged.

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
  cmd/tenantdb/            flags, wiring
  internal/proxy/          wire-protocol relay: Observer (watch) and Gate (veto)
  internal/attrib/
    extract.go shape.go    parse a statement: tenants, keys, tables, functions, SET
    attributor.go          per-connection state, sources, prepared statements
    cache.go               shared, bounded row -> tenant cache (LRU)
    catalog.go             schema snapshot: tables, columns, views, user functions
    lookup.go              ask the database who owns a row (de-duplicated)
    warm.go                background cache pre-warming
    strict.go              the policy and the blocking gate
```

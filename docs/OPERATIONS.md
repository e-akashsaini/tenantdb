# Running tenantdb

tenantdb is a single static-ish binary with no state on disk. Everything it knows (the schema snapshot, the row-to-tenant cache) is rebuilt from the database at start-up, so you can stop, replace and start it at will.

## A minimal production setup

```
app ─▶ (pgbouncer) ─▶ tenantdb :6432 ─▶ PostgreSQL
```

Put tenantdb **in front of** a pooler if you use one, so each client connection is seen (and tagged) individually. See the README.

```yaml
# /etc/tenantdb/tenantdb.conf
listen: ":6432"
upstream: db.internal:5432
upstream-tls: true
tls-cert: /etc/tenantdb/tls.crt
tls-key: /etc/tenantdb/tls.key
require-tls: true
tenant-columns: tenant_id
strict: alert
log-format: json
admin-addr: 127.0.0.1:9187
```

```bash
export TENANTDB_SCHEMA_DSN='postgres://tenantdb_ro:...@db.internal/app?sslmode=verify-full'
tenantdb -config /etc/tenantdb/tenantdb.conf
```

The schema role is read-only. It needs `SELECT` on the tables (for row lookups and cache pre-warming) and read access to the catalogs, which every role has. Give it `BYPASSRLS` if you use row-level security, so policies do not hide rows from it.

### Configuration

A setting can come from the command line, a config file, or an environment variable. Highest precedence first: **command line, config file, environment, built-in default.**

The config file has one `name: value` per line, using the flag names (`strict`, `upstream-tls`, `cache-size`, ...; `_` works for `-`). `#` starts a comment. Values may be quoted. A name that is not a flag is an error, so a typo in a security setting is never silently ignored. Keep the database connection string out of the file (use `TENANTDB_SCHEMA_DSN`); tenantdb warns if a world-readable config file holds one.

## Failure behaviour

What happens when something goes wrong depends on the mode, and is chosen to be the safe thing for each:

| Situation | `off` / `alert` | `block` |
|---|---|---|
| Statement analysis panics (a bug) | The statement is forwarded; the panic is logged and counted (`tenantdb_observer_panics_total`). | The statement is **refused** (`-strict-on-error block`, the default). `-strict-on-error allow` forwards it instead. |
| A statement cannot be parsed, or is absurdly large or deep | Forwarded, flagged `unknown` / `unparsed`. | Refused. |
| The schema cannot be read at start-up | Starts without it; everything that needs the schema is `unknown`. | **Refuses to start.** Silently allowing or silently blocking everything is not a decision tenantdb makes for you. |
| A schema refresh fails later | Keeps the last good snapshot; `tenantdb_schema_load_failures_total` rises. | Same. |
| The database cannot be reached | The client gets `08006`; `tenantdb_upstream_dial_failures_total` rises. `/readyz` fails. | Same. |
| A row-owner lookup fails or times out | The statement is `unknown`. | The statement is refused unless exempt. |
| tenantdb itself is down | Clients cannot connect: it is in the data path. Run more than one instance behind a load balancer and use `/readyz`. | Same. |

Alert mode never makes a statement fail that would otherwise have succeeded. Block mode refuses what it cannot attribute; roll it out in the order below.

### Rolling out

1. `strict: off`: watch `tenantdb_statements_total{source=...}`. See how much of your traffic is attributable.
2. `strict: alert`: `tenantdb_violations_total` and the `violation` log lines list what block mode would refuse. Fix the application, or list service roles in `strict-exempt` (migrations, admin tools) and `strict-allow-scan` (reporting).
3. `strict: block`, at first on one instance or one canary service.

## Endpoints

Set `admin-addr` to serve these. **They have no authentication**: bind a loopback or internal address.

| Path | |
|---|---|
| `/metrics` | Prometheus metrics |
| `/healthz` | `200 ok` while the process runs (liveness) |
| `/readyz` | `200` when it can serve clients; `503` with the failing checks otherwise (readiness): shutting down, schema required but not loaded, database unreachable |
| `/version` | version, commit, Go version as JSON |
| `/debug/pprof/` | profiling, only with `-admin-pprof` |

### Metrics

| Metric | Type | |
|---|---|---|
| `tenantdb_statements_total{source}` | counter | statements by how their tenant was found |
| `tenantdb_violations_total{violation,action}` | counter | policy objections (`unknown`, `scan`, `conflict`) and whether they were `alerted` or `blocked` |
| `tenantdb_conflicts_total`, `tenantdb_scans_total`, `tenantdb_unparsed_statements_total` | counter | |
| `tenantdb_analysis_seconds` | histogram | time spent analysing a client message before forwarding it (the latency tenantdb adds) |
| `tenantdb_connections_active`, `tenantdb_connections_total` | gauge, counter | |
| `tenantdb_connections_drained_total` | counter | idle clients sent away at shutdown |
| `tenantdb_rejected_statements_soft_total`, `..._hard_total` | counter | refusals that kept the connection / closed it |
| `tenantdb_upstream_dial_failures_total` | counter | |
| `tenantdb_tls_handshake_failures_total`, `tenantdb_startup_failures_total` | counter | |
| `tenantdb_observer_panics_total` | counter | should stay at 0: please report any |
| `tenantdb_draining` | gauge | 1 while stopping |
| `tenantdb_cache_entries`, `tenantdb_cache_hits_total`, `tenantdb_cache_misses_total` | gauge, counters | the row-to-tenant cache |
| `tenantdb_lookups_total`, `tenantdb_lookup_failures_total` | counter | row owners asked of the database |
| `tenantdb_schema_loaded`, `tenantdb_schema_loads_total`, `tenantdb_schema_load_failures_total`, `tenantdb_schema_last_load_timestamp_seconds`, `tenantdb_schema_tables`, `tenantdb_schema_views` | | schema snapshot health |
| `tenantdb_build_info{version,commit,go,strict}` | gauge | |
| `go_goroutines`, `go_memstats_heap_inuse_bytes` | gauge | |

Useful alerts: `increase(tenantdb_observer_panics_total[5m]) > 0`, `increase(tenantdb_schema_load_failures_total[15m]) > 3`, `time() - tenantdb_schema_last_load_timestamp_seconds > 3600`, `rate(tenantdb_upstream_dial_failures_total[5m]) > 0`, and a rise in `tenantdb_violations_total{violation="unknown"}` after a deploy.

## Logs

One line per attributed statement, as text or JSON (`-log-format json`), levels via `-log-level`. Warnings are policy violations and blocks; errors are operational problems.

Statements carry the values the application sends (emails, names, tokens). By default (`-log-sql normalized`) every literal is replaced by a placeholder (`WHERE email = $1`) so **the log holds no data values**; the tenants tenantdb found are logged separately. `-log-sql full` logs statements as sent (use it while debugging, not in production); `none` leaves them out.

## Stopping and restarting

`SIGTERM` or `SIGINT` starts a graceful stop:

1. the listener closes, so new connections go to another instance;
2. idle clients get `57P01 terminating connection due to administrator command` (what Postgres itself sends), and reconnect;
3. clients in the middle of a query or transaction have up to `-shutdown-timeout` (default 30s) to finish;
4. then whatever is left is closed.

A second signal closes everything at once. `SIGHUP` re-reads the schema now. TLS certificates are re-read when their files change; no signal is needed.

## Upgrading

tenantdb keeps no state on disk and the wire protocol is relayed unchanged, so an upgrade is a replacement. Behind a load balancer:

1. start the new version next to the old one and wait for `/readyz`;
2. send the old one `SIGTERM`; it drains as above while the balancer sends new connections to the new one;
3. check `tenantdb_violations_total` and `tenantdb_statements_total{source="unknown"}` for a change in what is attributable. Releases that improve attribution (see the [changelog](../CHANGELOG.md)) can turn `unknown` into attributed, never the reverse for a statement that was attributed correctly.

Without a balancer, a restart drops the connections open at that moment; clients with a retry policy (or a pooler in front) reconnect.

## Sizing

The relay adds one parse per statement (microseconds to a fraction of a millisecond; see `tenantdb_analysis_seconds` for your own numbers). Memory is the row cache (`-cache-size` entries, roughly 100 bytes each) plus per-connection state. Limits that protect it: `MaxMessageBytes` (128 MiB per client message), `MaxHeldBytes` (64 MiB a gated session may leave unsynced), a 30 s startup timeout, and a cap on statement size and nesting that the parser is given.

## Troubleshooting

| You see | Likely cause |
|---|---|
| Many `unknown` for a table | the table is not in the schema snapshot (new table: wait for the refresh or `SIGHUP`), or row-level security hides it from the schema role (give it `BYPASSRLS`; the start-up log lists such tables) |
| `scan` on a statement you think is filtered | the filter is inside an `OR` with an unfiltered alternative, or an outer join's `ON` clause, or `NOT`; none of those confine a statement to a tenant |
| `/readyz` is 503 `schema` | `-strict` needs the schema and it has not loaded: check `-schema-dsn` and `tenantdb_schema_load_failures_total` |
| Clients get `08006` | the database is unreachable from tenantdb |
| `tenantdb_observer_panics_total` rises | a bug: please open an issue with the statement shape (not the data) |

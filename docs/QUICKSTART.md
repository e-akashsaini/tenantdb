# Quick start: five minutes, only Docker

You will run a throwaway Postgres with two tenants' invoices in it, put tenantdb in front of it, and watch it name the tenant of each statement, then refuse the one that reads everybody's data.

Everything below was run exactly as written. You need Docker; nothing else is installed.

## 1. A database with two tenants

```bash
docker network create tdb-demo
docker run -d --name tdb-pg --network tdb-demo -e POSTGRES_PASSWORD=pw postgres:16
sleep 5   # let it start

docker exec -i tdb-pg psql -U postgres <<'SQL'
CREATE TABLE invoices (id bigint PRIMARY KEY, tenant_id text NOT NULL, amount int);
INSERT INTO invoices VALUES (1, 'acme', 100), (2, 'acme', 40), (3, 'globex', 250);
SQL
```

The tenant is the `tenant_id` column. That is the default name tenantdb looks for; use `-tenant-columns account_id,org_id` for others.

## 2. Put tenantdb in front of it (alert mode)

```bash
docker run -d --name tdb --network tdb-demo -p 6432:6432 \
  -e TENANTDB_UPSTREAM=tdb-pg:5432 \
  -e TENANTDB_SCHEMA_DSN='postgres://postgres:pw@tdb-pg:5432/postgres?sslmode=disable' \
  ghcr.io/e-akashsaini/tenantdb:0.1.0 -strict alert
```

- `TENANTDB_UPSTREAM` is the real database.
- `TENANTDB_SCHEMA_DSN` is a read-only connection it uses to read the table layout and to find out who owns a row. In production give it a dedicated read-only role (see the README); the superuser here is only for the demo.
- `-strict alert` lets every statement through and reports the risky ones.

## 3. Use the database through it

Your application would connect to port **6432** instead of 5432. Nothing else changes. Here `psql` plays the application:

```bash
q() { docker run --rm --network tdb-demo -e PGPASSWORD=pw postgres:16 \
        psql -h tdb -p 6432 -U postgres -t -c "$1"; }

q "SELECT sum(amount) FROM invoices WHERE tenant_id = 'acme'"   # names the tenant
q "SELECT * FROM invoices WHERE id = 3"                         # a row, no tenant named
q "SELECT * FROM invoices"                                      # everybody's rows
```

All three return their rows normally. Now read what tenantdb made of them:

```bash
docker logs tdb 2>&1 | grep -E "msg=(query|violation)"
```

```
level=INFO msg=query     tenants=[acme]   source=explicit scan=false  sql="SELECT sum(amount) FROM invoices WHERE tenant_id = $1"
level=INFO msg=query     tenants=[globex] source=cache    scan=false  sql="SELECT * FROM invoices WHERE id = $1"
level=WARN msg=violation tenants=[]       source=scan     scan=true   sql="SELECT * FROM invoices" violation=scan
```

(Trimmed to the interesting fields. The SQL is logged with values replaced by `$1`, so the log holds no customer data.)

What happened:

| Statement | Tenant | Why |
|---|---|---|
| `... WHERE tenant_id = 'acme'` | acme | The statement says so (`explicit`). |
| `... WHERE id = 3` | globex | It names no tenant, but row 3 belongs to globex: tenantdb remembers (or looks up) who owns each row (`cache`). |
| `SELECT * FROM invoices` | none | Nothing restricts it, so it reads every tenant's rows (`scan`). **Flagged.** |

That last line is the point. In a real application it is a query somebody forgot to filter.

## 4. Turn on enforcement (block mode)

```bash
docker rm -f tdb
docker run -d --name tdb --network tdb-demo -p 6432:6432 \
  -e TENANTDB_UPSTREAM=tdb-pg:5432 \
  -e TENANTDB_SCHEMA_DSN='postgres://postgres:pw@tdb-pg:5432/postgres?sslmode=disable' \
  ghcr.io/e-akashsaini/tenantdb:0.1.0 -strict block

q "SELECT sum(amount) FROM invoices WHERE tenant_id = 'acme'"   # still works
q "SELECT * FROM invoices"
```

```
 140

ERROR:  tenantdb: statement blocked (scan): it reads tenant data with no tenant or key filter, so it spans every tenant
```

The risky statement never reaches Postgres; the connection stays usable. Statements on tables with no tenant column (`SELECT 1`, shared lookup tables) pass.

## 5. Let a trusted role through

Migrations, admin tools and bulk loaders legitimately touch every tenant. Exempt their database roles:

```bash
docker exec tdb-pg psql -U postgres -c "CREATE ROLE migrator LOGIN PASSWORD 'pw' SUPERUSER"
docker rm -f tdb
docker run -d --name tdb --network tdb-demo -p 6432:6432 \
  -e TENANTDB_UPSTREAM=tdb-pg:5432 \
  -e TENANTDB_SCHEMA_DSN='postgres://postgres:pw@tdb-pg:5432/postgres?sslmode=disable' \
  ghcr.io/e-akashsaini/tenantdb:0.1.0 -strict block -strict-exempt migrator

docker run --rm --network tdb-demo -e PGPASSWORD=pw postgres:16 \
  psql -h tdb -p 6432 -U migrator -d postgres -t -c "SELECT count(*) FROM invoices"
```

```
     3
```

## Clean up

```bash
docker rm -f tdb tdb-pg && docker network rm tdb-demo
```

## Doing this to a real application

1. **Start in alert mode**, pointing the application's database port at tenantdb. Leave it for a few days of real traffic.
2. **Read the violations** (`level=WARN msg=violation`, or `tenantdb_violations_total` on `/metrics`). Each is either a bug to fix or a legitimate cross-tenant query to exempt. Expect some of both: on an unmodified Rails app (DocuSeal) about 5% of statements were flagged, mostly global settings and cross-account lookups. See [the write-up](WRITEUP.md).
3. **Switch to block mode** once the list is something you understand. Put exempt roles in `-strict-exempt`, or roles that may legitimately run reports in `-strict-allow-scan`.
4. **No traffic to point at it yet?** `tenantdb analyze` reads a recorded query log offline and gives the same report without putting anything in the path of your database. See the README.
5. **Put it in front of PgBouncer, not behind it.** Run more than one copy for production, and read [docs/OPERATIONS.md](OPERATIONS.md) for what it does when the database or schema lookup fails.

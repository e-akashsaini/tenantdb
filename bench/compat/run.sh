#!/usr/bin/env bash
# Runs one small multi-tenant workload through client libraries (psycopg2, psycopg3
# incl. pipeline mode, asyncpg incl. binary COPY, SQLAlchemy, node-postgres) against:
# direct Postgres, tenantdb in each strict mode, PgBouncer alone, and PgBouncer in
# front of and behind tenantdb. Anything that fails only when tenantdb is in the
# path is a compatibility bug.
#
# Needs: a Postgres 16 on 127.0.0.1:5432 (user postgres, password pw), go, python3
# with venv, node + npm, pgbouncer. Run it as root in WSL/Linux.
S="$(cd "$(dirname "$0")" && pwd)"
docker start pgtest >/dev/null 2>&1 || true
for i in $(seq 30); do pg_isready -h 127.0.0.1 -q && break; sleep 2; done
export PGPASSWORD=pw
cd "$S/../../proxy"
go build -o /tmp/tenantdb ./cmd/tenantdb || exit 1
pkill -x tenantdb 2>/dev/null; pkill -x pgbouncer 2>/dev/null; sleep 1

A="psql -h 127.0.0.1 -p 5432 -U postgres -X -q -t"
$A -d postgres -c "DROP DATABASE IF EXISTS compat WITH (FORCE)" -c "CREATE DATABASE compat" >/dev/null
$A -d compat <<'SQL' >/dev/null
CREATE TABLE compat_orders (id bigint PRIMARY KEY, tenant_id text, amount int);
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='tenantdb_ro') THEN CREATE ROLE tenantdb_ro LOGIN PASSWORD 'ro' BYPASSRLS; END IF; END $$;
GRANT CONNECT ON DATABASE compat TO tenantdb_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tenantdb_ro;
SQL

# client libraries
if [ ! -d /tmp/venv ]; then
  python3 -m venv /tmp/venv >/dev/null 2>&1
  /tmp/venv/bin/pip install -q "psycopg[binary]" psycopg2-binary asyncpg sqlalchemy >/tmp/pip.log 2>&1 || { tail -5 /tmp/pip.log; exit 1; }
fi
if [ ! -d /tmp/nodepg/node_modules/pg ]; then
  mkdir -p /tmp/nodepg && cd /tmp/nodepg && npm init -y >/dev/null 2>&1 && npm install pg >/tmp/npm.log 2>&1 || { tail -5 /tmp/npm.log; exit 1; }
  cd "$S/../../proxy"
fi

export TENANTDB_SCHEMA_DSN="postgres://tenantdb_ro:ro@127.0.0.1:5432/compat"
TD="/tmp/tenantdb -tenant-columns tenant_id"
($TD -listen 127.0.0.1:6511 -upstream 127.0.0.1:5432 >/tmp/c1.log 2>&1 &)
($TD -listen 127.0.0.1:6512 -upstream 127.0.0.1:5432 -strict alert >/tmp/c2.log 2>&1 &)
($TD -listen 127.0.0.1:6513 -upstream 127.0.0.1:5432 -strict block -strict-exempt postgres >/tmp/c3.log 2>&1 &)

rm -f /tmp/userlist.txt /tmp/pgb_a.ini /tmp/pgb_b.ini
# PgBouncer, transaction pooling with protocol-level prepared statements
cat > /tmp/userlist.txt <<'E'
"postgres" "pw"
E
cat > /tmp/pgb_a.ini <<'E'
[databases]
compat = host=127.0.0.1 port=5432 dbname=compat
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6600
auth_type = plain
auth_file = /tmp/userlist.txt
pool_mode = transaction
max_prepared_statements = 100
pidfile = /tmp/pgb_a.pid
logfile = /tmp/pgb_a.log
E
# chain B: pgbouncer in front of tenantdb
cat > /tmp/pgb_b.ini <<'E'
[databases]
compat = host=127.0.0.1 port=6522 dbname=compat
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6601
auth_type = plain
auth_file = /tmp/userlist.txt
pool_mode = transaction
max_prepared_statements = 100
pidfile = /tmp/pgb_b.pid
logfile = /tmp/pgb_b.log
E
# chain A: tenantdb in front of pgbouncer (upstream = pgbouncer :6600)
($TD -listen 127.0.0.1:6521 -upstream 127.0.0.1:6600 -strict alert >/tmp/c4.log 2>&1 &)
($TD -listen 127.0.0.1:6522 -upstream 127.0.0.1:5432 -strict alert >/tmp/c5.log 2>&1 &)
rm -f /tmp/pgb_a.log /tmp/pgb_b.log /tmp/pgb_a.pid /tmp/pgb_b.pid
chown postgres /tmp/userlist.txt /tmp/pgb_a.ini /tmp/pgb_b.ini 2>/dev/null; chmod 644 /tmp/userlist.txt
pgbouncer -d -u postgres /tmp/pgb_a.ini; pgbouncer -d -u postgres /tmp/pgb_b.ini
sleep 3
for f in /tmp/pgb_a.log /tmp/pgb_b.log; do grep -iE "error|fatal|refus" $f | tail -2; done

run() { # label host port
  out=$( { /tmp/venv/bin/python $S/clients.py $2 $3 postgres compat 2>&1 | grep -E "^(PASS|FAIL)"; (cd /tmp/nodepg && NODE_PATH=/tmp/nodepg/node_modules node $S/clients.js $2 $3 postgres compat 2>&1 | grep -E "^(PASS|FAIL)|Error"); } )
  pass=$(echo "$out" | grep -c "^PASS"); fail=$(echo "$out" | grep -c -v "^PASS")
  printf "%-62s %2d pass, %d fail\n" "$1" $pass $fail
  echo "$out" | grep -v "^PASS" | sed 's/^/      /' | cut -c1-200
}
run "direct Postgres"                          127.0.0.1 5432
run "tenantdb, strict=off"                     127.0.0.1 6511
run "tenantdb, strict=alert"                   127.0.0.1 6512
run "tenantdb, strict=block (role exempt, gate active)" 127.0.0.1 6513
run "PgBouncer alone (baseline for the chains)" 127.0.0.1 6600
run "chain A: client -> tenantdb -> PgBouncer -> Postgres" 127.0.0.1 6521
run "chain B: client -> PgBouncer -> tenantdb -> Postgres" 127.0.0.1 6601
pkill -x tenantdb; pkill -x pgbouncer

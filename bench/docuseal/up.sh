#!/usr/bin/env bash
# Runs DocuSeal (an open-source Rails app: ActiveRecord, an account_id column on
# its tables) with its database traffic going through tenantdb in alert mode.
#
#   bench/docuseal/up.sh        start Postgres (if needed), tenantdb, DocuSeal
#   bench/docuseal/traffic.sh   drive the app
#   bench/docuseal/down.sh      stop it
#
# Needs: docker, psql, go, openssl. Postgres 16 with password "pw" on 127.0.0.1:5432
# (docker run -d --name pgtest -e POSTGRES_PASSWORD=pw -p 5432:5432 postgres:16).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export PGPASSWORD="${PGPASSWORD:-pw}"
PSQL="psql -h 127.0.0.1 -p 5432 -U postgres -X -q"
PROXY_PORT="${PROXY_PORT:-6432}"
APP_PORT="${APP_PORT:-3000}"
LOG="${TENANTDB_LOG:-/tmp/tenantdb-docuseal.log}"

docker start pgtest >/dev/null 2>&1 || true
for _ in $(seq 30); do pg_isready -h 127.0.0.1 -q && break; sleep 2; done

echo "== stop any previous run, then a fresh database"
docker rm -f docuseal >/dev/null 2>&1 || true
pkill -x tenantdb 2>/dev/null || true
sleep 1
$PSQL -d postgres -c "DROP DATABASE IF EXISTS docuseal WITH (FORCE)" -c "CREATE DATABASE docuseal"
$PSQL -d postgres <<'SQL'
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tenantdb_ro') THEN
    CREATE ROLE tenantdb_ro LOGIN PASSWORD 'ro' BYPASSRLS;
  END IF;
END $$;
SQL
$PSQL -d docuseal <<'SQL'
GRANT CONNECT ON DATABASE docuseal TO tenantdb_ro;
GRANT USAGE ON SCHEMA public TO tenantdb_ro;
-- the app creates its tables as "postgres"; let the read-only role see them as they appear
ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES TO tenantdb_ro;
-- log what the app runs (not what tenantdb itself asks), so `tenantdb analyze` can replay it offline
ALTER DATABASE docuseal SET log_statement = 'all';
ALTER ROLE tenantdb_ro IN DATABASE docuseal SET log_statement = 'none';
SQL
$PSQL -d postgres -c "ALTER SYSTEM SET log_line_prefix = '%m [%p] %u@%d '" -c "SELECT pg_reload_conf()" >/dev/null

echo "== tenantdb (alert mode) on :$PROXY_PORT"
(cd "$ROOT/proxy" && go build -o /tmp/tenantdb ./cmd/tenantdb)
pkill -x tenantdb 2>/dev/null || true
sleep 1
TENANTDB_SCHEMA_DSN="postgres://tenantdb_ro:ro@127.0.0.1:5432/docuseal" \
  nohup /tmp/tenantdb -listen 127.0.0.1:$PROXY_PORT -upstream 127.0.0.1:5432 \
    -tenant-columns account_id -key-columns id -strict alert -schema-refresh 20s \
    >"$LOG" 2>&1 &
sleep 2

echo "== DocuSeal on :$APP_PORT (database traffic via tenantdb)"
docker rm -f docuseal >/dev/null 2>&1 || true
docker run -d --name docuseal --network host \
  -e DATABASE_URL="postgresql://postgres:pw@127.0.0.1:$PROXY_PORT/docuseal?sslmode=disable" \
  -e SECRET_KEY_BASE="$(openssl rand -hex 32)" \
  -e HOST="localhost:$APP_PORT" -e PORT="$APP_PORT" \
  docuseal/docuseal:latest >/dev/null

for i in $(seq 90); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$APP_PORT/"; then echo "up after ~$((i*2))s"; exit 0; fi
  sleep 2
done
echo "DocuSeal did not come up; container log:" >&2
docker logs --tail 40 docuseal >&2
exit 1

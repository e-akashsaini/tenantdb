#!/usr/bin/env bash
# End-to-end check of the operational features against a real PostgreSQL:
# config file, JSON logs without data values, /metrics, /readyz, graceful drain.
#
#   PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres PGPASSWORD=pw bench/ops/e2e.sh
#
# Needs psql, curl, go (to build) and a PostgreSQL the user can create a database in.
set -u
cd "${TENANTDB_PROXY_DIR:-$(dirname "$0")/../../proxy}"
export PGHOST=${PGHOST:-127.0.0.1} PGPORT=${PGPORT:-5432} PGUSER=${PGUSER:-postgres} PGPASSWORD=${PGPASSWORD:-pw}
DB=tenantdb_ops_e2e
LISTEN=127.0.0.1:6439
ADMIN=127.0.0.1:9199
WORK=$(mktemp -d)
BIN=$WORK/tenantdb
fail=0
ok()   { echo "  ok   $*"; }
bad()  { echo "  FAIL $*"; fail=1; }
check() { if eval "$2"; then ok "$1"; else bad "$1"; fi; }

cleanup() {
  [ -n "${PROXY_PID:-}" ] && kill -9 "$PROXY_PID" 2>/dev/null
  psql -d postgres -qc "DROP DATABASE IF EXISTS $DB" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

go build -ldflags "-X main.version=e2e" -o "$BIN" ./cmd/tenantdb || exit 1
psql -d postgres -qc "DROP DATABASE IF EXISTS $DB" -c "CREATE DATABASE $DB" >/dev/null || exit 1
psql -d $DB -q <<'SQL' || exit 1
CREATE TABLE orders (id bigint PRIMARY KEY, tenant_id text NOT NULL, email text);
INSERT INTO orders VALUES (1, 'acme', 'a@x'), (2, 'globex', 'g@x');
SQL

cat > "$WORK/tenantdb.conf" <<CONF
# settings for the e2e run
listen: "$LISTEN"
upstream: $PGHOST:$PGPORT
strict: alert
log-format: json
log-sql: normalized
admin-addr: $ADMIN
shutdown-timeout: 10s
CONF

export TENANTDB_SCHEMA_DSN="postgres://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$DB?sslmode=disable"
"$BIN" -config "$WORK/tenantdb.conf" 2> "$WORK/log" &
PROXY_PID=$!

for i in $(seq 50); do curl -fs "http://$ADMIN/readyz" >/dev/null 2>&1 && break; sleep 0.2; done
echo "== endpoints"
check "readyz is 200" "[ \"\$(curl -s -o /dev/null -w '%{http_code}' http://$ADMIN/readyz)\" = 200 ]"
check "healthz answers" "curl -fs http://$ADMIN/healthz | grep -q ok"
check "version says e2e" "curl -fs http://$ADMIN/version | grep -q e2e"
"$BIN" -version | grep -q e2e && ok "-version prints the version" || bad "-version"

PX="psql -h ${LISTEN%:*} -p ${LISTEN#*:} -d $DB -U $PGUSER -tA"
echo "== traffic"
$PX -c "SELECT id FROM orders WHERE tenant_id = 'acme' AND email = 'secret-token-12345'" >/dev/null
$PX -c "SELECT id FROM orders WHERE id = 2" >/dev/null
$PX -c "SELECT * FROM orders" >/dev/null
sleep 0.5
M=$(curl -fs http://$ADMIN/metrics)
check "statements are counted by source" "echo '$M' | grep -q 'tenantdb_statements_total{source=\"explicit\"}'"
check "a scan is counted as a violation" "echo '$M' | grep -q 'tenantdb_violations_total{violation=\"scan\",action=\"alerted\"} 1'"
check "connections were counted" "echo '$M' | grep -Eq 'tenantdb_connections_total [3-9]'"
check "schema loaded gauge is 1" "echo '$M' | grep -q 'tenantdb_schema_loaded 1'"
check "analysis latency is measured" "echo '$M' | grep -Eq 'tenantdb_analysis_seconds_count [1-9]'"
echo "== logs"
check "log lines are JSON" "head -c1 $WORK/log | grep -q '{'"
check "the data value is not in the log" "! grep -q 'secret-token-12345' $WORK/log"
check "the tenants are" "grep -q '\"tenants\":\[\"acme\"\]' $WORK/log"

echo "== graceful shutdown"
# an idle client (in the background, holds its connection) and a busy one
( $PX -c "SELECT pg_sleep(3), 'busy-done'" > "$WORK/busy" 2>&1 ) &
BUSY=$!
# psql only looks at its socket when it has something to send, so it sends a second statement after the shutdown
( { echo "SELECT 'idle-ready';"; sleep 4; echo "SELECT 'after';"; } | $PX > "$WORK/idle" 2>&1 ) &
IDLEJOB=$!
sleep 1
kill -TERM "$PROXY_PID"
START=$(date +%s)
wait "$PROXY_PID"; RC=$?
END=$(date +%s)
wait $BUSY 2>/dev/null
check "exit status is 0" "[ $RC -eq 0 ]"
check "the busy query finished" "grep -q busy-done $WORK/busy"
check "it waited for the busy query, not the whole timeout" "[ $((END-START)) -ge 1 ] && [ $((END-START)) -lt 9 ]"
wait $IDLEJOB 2>/dev/null
check "the idle client was told it was an administrator shutdown" "grep -qi 'administrator command' $WORK/idle"
check "the log says it is stopping" "grep -q 'stopping' $WORK/log"
check "the log says it stopped" "grep -q '\"msg\":\"stopped\"' $WORK/log"
check "new connections are refused" "! $PX -c 'select 1' >/dev/null 2>&1"
PROXY_PID=

echo
[ $fail -eq 0 ] && echo "all checks passed" || { echo "FAILED"; echo "--- log"; tail -20 "$WORK/log"; }
exit $fail

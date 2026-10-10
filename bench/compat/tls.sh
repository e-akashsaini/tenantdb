#!/usr/bin/env bash
# TLS and SCRAM against a real PostgreSQL with TLS and scram-sha-256: what works
# through the proxy, and what cannot. Needs docker, openssl, psql (libpq 13+), go.
#
#   bench/compat/tls.sh
#
# Channel binding (SCRAM-SHA-256-PLUS) ties authentication to one TLS connection.
# A proxy that ends TLS gives the client one connection and the database another,
# so the binding only matches if both present the same certificate.
set -u
cd "$(dirname "$0")/../../proxy"
WORK=$(mktemp -d)
chmod 755 "$WORK" # the database container reads its certificates from here
fail=0
PORT_PG=5433
cleanup() {
  for p in "$WORK"/*.pid; do [ -f "$p" ] && kill "$(cat "$p")" 2>/dev/null; done
  docker rm -f tenantdb-pgssl >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

go build -o "$WORK/tenantdb" ./cmd/tenantdb || exit 1
cd "$WORK"
openssl req -new -x509 -days 2 -nodes -out server.crt -keyout server.key -subj "/CN=localhost" >/dev/null 2>&1
openssl req -new -x509 -days 2 -nodes -out proxy.crt -keyout proxy.key -subj "/CN=localhost" >/dev/null 2>&1
openssl req -new -x509 -days 2 -nodes -out ca.crt -keyout ca.key -subj "/CN=client-ca" >/dev/null 2>&1
openssl req -new -nodes -out client.csr -keyout client.key -subj "/CN=billing" >/dev/null 2>&1
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 2 >/dev/null 2>&1
openssl req -new -x509 -days 2 -nodes -out rogue.crt -keyout rogue.key -subj "/CN=rogue" >/dev/null 2>&1
chmod 600 server.key; chown 999:999 server.crt server.key 2>/dev/null || chmod 644 server.key
docker rm -f tenantdb-pgssl >/dev/null 2>&1
docker run -d --name tenantdb-pgssl -p $PORT_PG:5432 -e POSTGRES_PASSWORD=pw -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
  -e POSTGRES_INITDB_ARGS="--auth-host=scram-sha-256" -v "$WORK":/certs postgres:16 \
  -c ssl=on -c ssl_cert_file=/certs/server.crt -c ssl_key_file=/certs/server.key -c password_encryption=scram-sha-256 >/dev/null
export PGPASSWORD=pw PGCONNECT_TIMEOUT=10
for i in $(seq 40); do psql "host=127.0.0.1 port=$PORT_PG user=postgres sslmode=require" -tAc "select 1" >/dev/null 2>&1 && break; sleep 2; done

start() { # name, port, flags...
  local name=$1 port=$2; shift 2
  ./tenantdb -listen 127.0.0.1:$port -upstream 127.0.0.1:$PORT_PG -upstream-tls -upstream-tls-insecure "$@" >"$WORK/$name.log" 2>&1 &
  echo $! > "$WORK/$name.pid"
  sleep 1
}
stop() { kill "$(cat "$WORK/$1.pid")" 2>/dev/null; wait "$(cat "$WORK/$1.pid")" 2>/dev/null; rm -f "$WORK/$1.pid"; }
# expect <ok|fail> <label> <conninfo> [grep pattern for the error]
expect() {
  local want=$1 label=$2 conn=$3 pat=${4:-}
  out=$(psql "$conn" -tAc "select 'ok'" 2>&1 | tr '\n' ' ')
  if [ "$want" = ok ]; then
    case "$out" in *ok*) printf "  ok    %s\n" "$label";; *) printf "  FAIL  %s: %s\n" "$label" "${out:0:150}"; fail=1;; esac
  else
    case "$out" in *ok*) printf "  FAIL  %s: it should have been refused\n" "$label"; fail=1;;
      *) if [ -z "$pat" ] || echo "$out" | grep -qi "$pat"; then printf "  ok    %s (refused: %s)\n" "$label" "$(echo "${out:0:90}" | sed 's/^psql: error: //')"; else printf "  FAIL  %s: wrong error: %s\n" "$label" "${out:0:150}"; fail=1; fi;; esac
  fi
}

C=127.0.0.1
echo "== distinct certificates: -scram-channel-binding=auto (default)"
start auto 6450 -tls-cert proxy.crt -tls-key proxy.key
expect ok   "client TLS, channel_binding=disable"      "host=$C port=6450 user=postgres sslmode=require channel_binding=disable"
expect ok   "client without TLS (the database offers -PLUS; the proxy hides it)" "host=$C port=6450 user=postgres sslmode=disable"
expect fail "client TLS, channel_binding=prefer (inherent: two different TLS connections)" "host=$C port=6450 user=postgres sslmode=require" "channel binding"
expect fail "client TLS, channel_binding=require" "host=$C port=6450 user=postgres sslmode=require channel_binding=require" "channel binding"
stop auto

echo "== the proxy presents the database's own certificate: channel binding works end to end"
start shared 6451 -tls-cert server.crt -tls-key server.key
expect ok   "client TLS, channel_binding=prefer"  "host=$C port=6451 user=postgres sslmode=require"
expect ok   "client TLS, channel_binding=require" "host=$C port=6451 user=postgres sslmode=require channel_binding=require"
expect ok   "client TLS, channel_binding=disable" "host=$C port=6451 user=postgres sslmode=require channel_binding=disable"
stop shared

echo "== -scram-channel-binding=strip"
start strip 6452 -tls-cert proxy.crt -tls-key proxy.key -scram-channel-binding=strip
expect ok   "client TLS, channel_binding=disable"  "host=$C port=6452 user=postgres sslmode=require channel_binding=disable"
expect ok   "client without TLS"                   "host=$C port=6452 user=postgres sslmode=disable"
expect fail "client TLS, channel_binding=require (clear client-side error)" "host=$C port=6452 user=postgres sslmode=require channel_binding=require" "channel binding is required"
stop strip

echo "== mutual TLS"
start mtls 6453 -tls-cert proxy.crt -tls-key proxy.key -tls-client-ca ca.crt -require-tls
expect ok   "client certificate signed by the CA"  "host=$C port=6453 user=postgres sslmode=require sslcert=client.crt sslkey=client.key channel_binding=disable"
expect fail "no client certificate"                "host=$C port=6453 user=postgres sslmode=require channel_binding=disable" "certificate\|handshake\|closed\|SSL"
expect fail "a certificate from another CA"        "host=$C port=6453 user=postgres sslmode=require sslcert=rogue.crt sslkey=rogue.key channel_binding=disable" "certificate\|handshake\|closed\|SSL"
expect fail "no TLS at all (-require-tls)"         "host=$C port=6453 user=postgres sslmode=disable" "TLS\|encrypt\|SSL"
grep -q 'client_cert' "$WORK/mtls.log" || true
stop mtls

echo
[ $fail -eq 0 ] && echo "all checks passed" || echo "FAILED"
exit $fail

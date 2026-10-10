#!/usr/bin/env bash
# Drives DocuSeal the way its users and integrations do: first-run setup, login,
# browsing the web UI, the JSON API (templates, submissions) and the public
# signing links. Every request is real Rails/ActiveRecord traffic that reaches
# Postgres through tenantdb.
#
#   ROUNDS=5 bench/docuseal/traffic.sh
set -uo pipefail

BASE="${BASE:-http://127.0.0.1:3000}"
ROUNDS="${ROUNDS:-3}"
JAR="$(mktemp)"
PSQL="psql -h 127.0.0.1 -p 5432 -U postgres -d docuseal -X -q -t -A"
export PGPASSWORD="${PGPASSWORD:-pw}"
ok=0; bad=0
code() { if [[ "$1" =~ ^[23] ]]; then ok=$((ok+1)); else bad=$((bad+1)); echo "  HTTP $1 for $2" >&2; fi; }
get()  { code "$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -c "$JAR" "$BASE$1")" "GET $1"; }
# The page's <meta name="csrf-token"> is the global token Rails accepts for any form.
token_from() { sed -n 's/.*name="csrf-token" content="\([^"]*\)".*/\1/p' | head -1; }

echo "== first-run setup and login"
tok="$(curl -s -b "$JAR" -c "$JAR" "$BASE/setup" | token_from)"
if [ -n "$tok" ]; then
  code "$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -c "$JAR" "$BASE/setup" -H "X-CSRF-Token: $tok" \
    --data-urlencode "authenticity_token=$tok" \
    --data-urlencode "user[first_name]=Ada" --data-urlencode "user[last_name]=Admin" \
    --data-urlencode "user[email]=admin@example.com" --data-urlencode "user[password]=Sup3rSecret!pass" \
    --data-urlencode "account[name]=Acme Inc" --data-urlencode "account[timezone]=UTC" \
    --data-urlencode "account[locale]=en-US" --data-urlencode "encrypted_config[value]=$BASE")" "POST /setup"
fi
tok="$(curl -s -b "$JAR" -c "$JAR" "$BASE/sign_in" | token_from)"
code "$(curl -s -o /dev/null -w '%{http_code}' -b "$JAR" -c "$JAR" "$BASE/sign_in" -H "X-CSRF-Token: $tok" \
  --data-urlencode "authenticity_token=$tok" --data-urlencode "user[email]=admin@example.com" \
  --data-urlencode "user[password]=Sup3rSecret!pass")" "POST /sign_in"

# The token is encrypted at rest and masked on the settings page, so ask the app.
get /settings/api
API_TOKEN="$(docker exec docuseal /app/bin/rails runner 'puts User.first.access_token.token' 2>/dev/null | tail -1)"
if [ -z "$API_TOKEN" ]; then echo "no API token yet (setup failed?)" >&2; exit 1; fi
api() { # method path [json]
  local m="$1" p="$2" d="${3:-}"
  if [ -n "$d" ]; then
    curl -s -w '\n%{http_code}' -X "$m" -H "X-Auth-Token: $API_TOKEN" -H 'Content-Type: application/json' -d "$d" "$BASE$p"
  else
    curl -s -w '\n%{http_code}' -X "$m" -H "X-Auth-Token: $API_TOKEN" "$BASE$p"
  fi
}
# jget KEY: a top-level key of the JSON in the body of an api() reply (first element if a list).
jget() { python3 -c '
import sys, json
raw = sys.stdin.read().rsplit("\n", 1)[0]
try:
    d = json.loads(raw)
except Exception:
    sys.exit(0)
if isinstance(d, list):
    d = d[0] if d else {}
v = d.get(sys.argv[1], "") if isinstance(d, dict) else ""
print("" if isinstance(v, (dict, list)) or v is None else v)
' "$1"; }

# A minimal valid one-page PDF (the open-source build creates templates from
# uploads in the web UI; its API has no template-creation endpoint).
PDF="$(mktemp --suffix=.pdf)"
python3 - "$PDF" <<'PY'
import sys
objs = [b"<</Type/Catalog/Pages 2 0 R>>", b"<</Type/Pages/Kids[3 0 R]/Count 1>>",
        b"<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]/Contents 4 0 R/Resources<</Font<</F1 5 0 R>>>>>>",
        None, b"<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>"]
stream = b"BT /F1 24 Tf 72 700 Td (Service agreement) Tj ET"
objs[3] = b"<</Length %d>>\nstream\n%s\nendstream" % (len(stream), stream)
out = b"%PDF-1.4\n"; offs = []
for i, o in enumerate(objs, 1):
    offs.append(len(out)); out += b"%d 0 obj\n%s\nendobj\n" % (i, o)
x = len(out)
out += b"xref\n0 %d\n0000000000 65535 f \n" % (len(objs) + 1)
for o in offs: out += b"%010d 00000 n \n" % o
out += b"trailer<</Size %d/Root 1 0 R>>\nstartxref\n%d\n%%%%EOF\n" % (len(objs) + 1, x)
open(sys.argv[1], "wb").write(out)
PY

csrf() { curl -s -b "$JAR" -c "$JAR" "$BASE/templates" | token_from; }

echo "== $ROUNDS rounds of browsing, template upload, submissions and signing"
for r in $(seq "$ROUNDS"); do
  for path in / /templates /submissions /settings/profile /settings/account /settings/users /settings/personalization /settings/notifications /settings/webhooks; do get "$path"; done

  for n in 1 2; do
    if [ "$n" = 1 ]; then
      hdr="$(mktemp)"
      curl -s -o /dev/null -D "$hdr" -b "$JAR" -c "$JAR" -H "X-CSRF-Token: $(csrf)" \
        -F "files[]=@$PDF;type=application/pdf" "$BASE/templates_upload"
      tid="$(sed -n 's/^[Ll]ocation: .*\/templates\/\([0-9]*\).*/\1/p' "$hdr" | head -1)"
      code "$(head -1 "$hdr" | awk '{print $2}')" "POST /templates_upload"; rm -f "$hdr"
      [ -z "$tid" ] && tid="$($PSQL -c 'SELECT max(id) FROM templates')"
      [ -z "$tid" ] && continue
      # The template editor's job: place a field on the page (done here by the app itself).
      docker exec docuseal /app/bin/rails runner "
        t = Template.find($tid); att = t.schema.first['attachment_uuid']
        t.fields = [{'uuid'=>SecureRandom.uuid, 'submitter_uuid'=>t.submitters.first['uuid'], 'name'=>'Full name',
                     'type'=>'text', 'required'=>true, 'preferences'=>{},
                     'areas'=>[{'x'=>0.1,'y'=>0.1,'w'=>0.3,'h'=>0.03,'page'=>0,'attachment_uuid'=>att}]}]
        t.save!" >/dev/null 2>&1
      base_tid="$tid"
    else
      out="$(api POST "/api/templates/$base_tid/clone" "{\"name\":\"Clone of $base_tid round $r\"}")"
      code "$(printf '%s' "$out" | tail -1)" "POST /api/templates/:id/clone"
      tid="$(printf '%s' "$out" | jget id)"; [ -z "$tid" ] && continue
    fi

    get "/templates/$tid/edit"; get "/templates/$tid"; get "/templates/$tid/preview"
    api GET "/api/templates/$tid" >/dev/null
    out="$(api POST /api/submissions "{\"template_id\":$tid,\"send_email\":false,\"submitters\":[{\"email\":\"signer$r$n@example.com\"}]}")"
    code "$(printf '%s' "$out" | tail -1)" "POST /api/submissions"
    sid="$(printf '%s' "$out" | jget submission_id)"; [ -z "$sid" ] && sid="$(printf '%s' "$out" | jget id)"
    slug="$(printf '%s' "$out" | jget slug)"
    submitter="$(printf '%s' "$out" | jget id)"

    [ -n "$slug" ] && { get "/s/$slug"; }   # public signing page: no login, found by slug
    [ -n "$sid" ] && { api GET "/api/submissions/$sid" >/dev/null; get "/submissions/$sid"; }
    [ -n "$submitter" ] && { api GET "/api/submitters/$submitter" >/dev/null; api PUT "/api/submitters/$submitter" '{"completed":true}' >/dev/null; }
    [ -n "$sid" ] && api GET "/api/submissions/$sid/documents" >/dev/null
  done

  for path in /api/templates /api/submissions /api/submitters "/api/templates?limit=5" "/api/submissions?limit=5&status=completed" /api/user; do
    code "$(api GET "$path" | tail -1)" "GET $path"
  done
done
rm -f "$PDF"
echo "done: $ok ok, $bad not ok"

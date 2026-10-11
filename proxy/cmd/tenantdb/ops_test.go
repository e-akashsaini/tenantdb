package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/e-akashsaini/tenantdb/internal/attrib"
	"github.com/e-akashsaini/tenantdb/internal/proxy"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "tenantdb.conf")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestConfigFileSettingsAndPrecedence(t *testing.T) {
	p := writeTemp(t, `
# a comment
listen: ":7000"
upstream = "db.internal:5432"   # trailing comment
strict: alert
log_format: json
--cache-size: 42
tag-var: 'my.tenant'
`)
	vals, order, err := parseConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"listen": ":7000", "upstream": "db.internal:5432", "strict": "alert", "log-format": "json", "cache-size": "42", "tag-var": "my.tenant"}
	for k, v := range want {
		if vals[k] != v {
			t.Errorf("%s = %q, want %q", k, vals[k], v)
		}
	}
	if len(order) != len(want) {
		t.Errorf("order = %v", order)
	}

	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	listen := fs.String("listen", ":6432", "")
	upstream := fs.String("upstream", "x", "")
	strict := fs.String("strict", "off", "")
	_ = fs.String("log-format", "text", "")
	size := fs.Int("cache-size", 1, "")
	_ = fs.String("tag-var", "", "")
	if err := fs.Parse([]string{"-listen", ":9999"}); err != nil {
		t.Fatal(err)
	}
	if err := applyConfig(fs, vals, order); err != nil {
		t.Fatal(err)
	}
	if *listen != ":9999" {
		t.Errorf("the command line must win over the file: %s", *listen)
	}
	if *upstream != "db.internal:5432" || *strict != "alert" || *size != 42 {
		t.Errorf("file values not applied: %s %s %d", *upstream, *strict, *size)
	}
}

func TestConfigFileErrorsAreLoud(t *testing.T) {
	for name, c := range map[string]string{
		"a typo in a setting":       "strikt: block\n",
		"a line with no value":      "strict\n",
		"the same name twice":       "strict: alert\nstrict: block\n",
		"a value of the wrong type": "cache-size: lots\n",
		"nesting the config file":   "config: other.conf\n",
	} {
		vals, order, err := parseConfig(writeTemp(t, c))
		if err == nil {
			fs := flag.NewFlagSet("t", flag.ContinueOnError)
			fs.String("strict", "off", "")
			fs.Int("cache-size", 1, "")
			fs.String("config", "", "")
			err = applyConfig(fs, vals, order)
		}
		if err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, _, err := parseConfig(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("a missing file is an error")
	}
}

func TestLogsCanBeJSONAndHideDataValues(t *testing.T) {
	var buf bytes.Buffer
	log, err := newLogger(&buf, "json", "info", false)
	if err != nil {
		t.Fatal(err)
	}
	log.Info("query", "tenants", []string{"acme"}, "sql", sqlForLog("normalized", "SELECT * FROM users WHERE email = 'ceo@example.com' AND token = 'sk-live-123' AND age > 41"))
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("not JSON: %v\n%s", err, buf.String())
	}
	sql, _ := rec["sql"].(string)
	for _, secret := range []string{"ceo@example.com", "sk-live-123", "41"} {
		if strings.Contains(sql, secret) {
			t.Errorf("normalized SQL still holds %q: %s", secret, sql)
		}
	}
	if !strings.Contains(sql, "$1") || !strings.Contains(sql, "users") {
		t.Errorf("normalized SQL: %s", sql)
	}
	if got := sqlForLog("full", "SELECT 'x'"); got != "SELECT 'x'" {
		t.Errorf("full: %s", got)
	}
	if got := sqlForLog("none", "SELECT 'x'"); got != "" {
		t.Errorf("none: %s", got)
	}
	if got := sqlForLog("normalized", "SELEKT ((("); !strings.Contains(got, "could not") {
		t.Errorf("unparseable SQL must not be logged verbatim by the normalizing mode: %q", got)
	}
	if got := sqlForLog("full", strings.Repeat("x", 20000)); len(got) > maxLoggedSQL+10 {
		t.Errorf("a huge statement fills the log: %d bytes", len(got))
	}
	if _, err := newLogger(&buf, "xml", "info", false); err == nil {
		t.Error("an unknown format should be refused")
	}
	if _, err := newLogger(&buf, "text", "loud", false); err == nil {
		t.Error("an unknown level should be refused")
	}
}

func adminFixture(t *testing.T, upstream string, drain bool, cat *attrib.Catalog, needs bool) (*httptest.Server, *proxy.Server, func()) {
	t.Helper()
	srv := proxy.New(proxy.Config{Listen: "127.0.0.1:0", Upstream: upstream, DrainTimeout: 100 * time.Millisecond})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	o := newObservability("v1.2.3", "abc123", "go1.x", "alert")
	a := attrib.New(attrib.Options{TenantColumns: []string{"tenant_id"}}, o.event)
	o.bind(srv, a, cat)
	adm := &adminServer{obs: o, srv: srv, catalog: cat, upstream: upstream, needs: needs, ver: map[string]string{"version": "v1.2.3"}}
	ts := httptest.NewServer(adm.handler(false))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	stop := func() { cancel(); <-done; ts.Close() }
	if drain {
		cancel()
		<-done
	}
	t.Cleanup(stop)
	return ts, srv, stop
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	b.ReadFrom(resp.Body)
	return resp.StatusCode, b.String()
}

func TestAdminEndpoints(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	ts, _, _ := adminFixture(t, up.Addr().String(), false, nil, false)

	if code, body := get(t, ts.URL+"/healthz"); code != 200 || !strings.HasPrefix(body, "ok") {
		t.Errorf("healthz: %d %q", code, body)
	}
	if code, body := get(t, ts.URL+"/readyz"); code != 200 || !strings.Contains(body, `"ready":true`) {
		t.Errorf("readyz: %d %q", code, body)
	}
	if code, body := get(t, ts.URL+"/version"); code != 200 || !strings.Contains(body, "v1.2.3") {
		t.Errorf("version: %d %q", code, body)
	}
	code, body := get(t, ts.URL+"/metrics")
	if code != 200 {
		t.Fatalf("metrics: %d", code)
	}
	for _, want := range []string{
		`tenantdb_build_info{version="v1.2.3",commit="abc123",go="go1.x",strict="alert"} 1`,
		"tenantdb_connections_active 0", "tenantdb_connections_total 0", "tenantdb_draining 0",
		"tenantdb_cache_entries 0", "tenantdb_unparsed_statements_total 0", "tenantdb_analysis_seconds_count 0",
		"go_goroutines ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics lacks %q", want)
		}
	}
}

func TestReadinessReflectsTheUpstreamTheSchemaAndShutdown(t *testing.T) {
	// the database is not there
	ts, _, _ := adminFixture(t, "127.0.0.1:1", false, nil, false)
	if code, body := get(t, ts.URL+"/readyz"); code != 503 || !strings.Contains(body, `"upstream"`) {
		t.Errorf("an unreachable database: %d %s", code, body)
	}
	// strict mode needs the schema, which has not been loaded
	up, _ := net.Listen("tcp", "127.0.0.1:0")
	defer up.Close()
	cat := attrib.NewCatalog("")
	ts2, _, _ := adminFixture(t, up.Addr().String(), false, cat, true)
	if code, body := get(t, ts2.URL+"/readyz"); code != 503 || !strings.Contains(body, "schema") {
		t.Errorf("schema required but missing: %d %s", code, body)
	}
	// ... without strict mode the proxy still serves
	ts3, _, _ := adminFixture(t, up.Addr().String(), false, attrib.NewCatalog(""), false)
	if code, _ := get(t, ts3.URL+"/readyz"); code != 200 {
		t.Errorf("schema optional: %d", code)
	}
	// and a draining server asks to be taken out of rotation
	ts4, srv4, stop := adminFixture(t, up.Addr().String(), false, nil, false)
	stop()
	_ = ts4
	if !srv4.Draining() {
		t.Error("expected the server to have drained")
	}
}

func TestEventsAreCounted(t *testing.T) {
	o := newObservability("v", "c", "g", "block")
	o.event(attrib.Event{Source: attrib.SourceExplicit})
	o.event(attrib.Event{Source: attrib.SourceExplicit})
	o.event(attrib.Event{Source: attrib.SourceUnknown, Violation: "unknown", Blocked: true, Unparsed: true})
	o.event(attrib.Event{Source: attrib.SourceScan, Scan: true, Violation: "scan"})
	o.event(attrib.Event{Source: attrib.SourceExplicit, Conflict: true, Violation: "conflict"})
	var b bytes.Buffer
	o.reg.Write(&b)
	out := b.String()
	for _, want := range []string{
		`tenantdb_statements_total{source="explicit"} 3`,
		`tenantdb_statements_total{source="unknown"} 1`,
		`tenantdb_violations_total{violation="unknown",action="blocked"} 1`,
		`tenantdb_violations_total{violation="scan",action="alerted"} 1`,
		"tenantdb_conflicts_total 1", "tenantdb_unparsed_statements_total 1", "tenantdb_scans_total 1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in\n%s", want, out)
		}
	}
}

func TestHealthcheckSubcommand(t *testing.T) {
	up, _ := net.Listen("tcp", "127.0.0.1:0")
	defer up.Close()
	ts, _, _ := adminFixture(t, up.Addr().String(), false, nil, false)
	if rc := runHealthcheck([]string{"-url", ts.URL + "/readyz"}); rc != 0 {
		t.Errorf("a ready instance: exit %d", rc)
	}
	if rc := runHealthcheck([]string{"-url", ts.URL + "/nope", "-timeout", "1s"}); rc != 1 {
		t.Errorf("a 404: exit %d", rc)
	}
	bad, _, _ := adminFixture(t, "127.0.0.1:1", false, nil, false)
	if rc := runHealthcheck([]string{"-url", bad.URL + "/readyz"}); rc != 1 {
		t.Errorf("an instance that cannot reach its database: exit %d", rc)
	}
	if rc := runHealthcheck([]string{"-url", "http://127.0.0.1:1/readyz", "-timeout", "1s"}); rc != 1 {
		t.Errorf("nothing listening: exit %d", rc)
	}
	t.Setenv("TENANTDB_ADMIN_ADDR", "0.0.0.0:1")
	if rc := runHealthcheck([]string{"-timeout", "1s"}); rc != 1 {
		t.Errorf("the default URL comes from TENANTDB_ADMIN_ADDR: exit %d", rc)
	}
}

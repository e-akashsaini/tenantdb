package attrib

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
)

func tbl(schema, name string, kind byte, cols ...string) []colRow {
	var out []colRow
	for _, c := range cols {
		out = append(out, colRow{schema: schema, table: name, kind: kind, col: c})
	}
	return out
}

func testCatalog() *Catalog {
	var rows []colRow
	add := func(r []colRow) { rows = append(rows, r...) }
	add(tbl("public", "orders", 'r', "id", "tenant_id", "amount"))
	add(tbl("public", "countries", 'r', "code", "name"))
	add(tbl("public", "notes", 'r', "tenant_id", "id", "body"))
	add(tbl("public", "orders_v", 'v', "id", "tenant_id")) // view exposing the tenant column
	add(tbl("public", "summary_v", 'v', "total"))          // view that hides it
	add(tbl("t1", "docs", 'r', "id", "tenant_id", "body")) // same name, tenant table...
	add(tbl("t2", "docs", 'r', "id", "title"))             // ...and shared table
	add(tbl("pg_catalog", "pg_tables", 'v', "schemaname", "tablename"))
	c := NewCatalog("")
	c.set(rows, map[string]bool{"public.my_func": true})
	return c
}

func catalogHarness() *harness {
	h := &harness{s: &proxy.Session{ID: 1, User: "u", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: testCatalog()}, h.record)
	return h
}

func TestSharedTablesAreUntenantedNotUnknown(t *testing.T) {
	h := catalogHarness()
	for _, q := range []string{
		`SELECT * FROM countries WHERE code = 'IE'`,
		`SELECT 1`,
		`SELECT now()`,
		`SELECT * FROM generate_series(1, 3)`,
		`INSERT INTO countries (code, name) VALUES ('IE', 'Ireland')`,
		`SELECT * FROM pg_tables`,
		`SELECT * FROM pg_catalog.pg_tables`,
		`WITH c AS (SELECT code FROM countries) SELECT * FROM c`,
	} {
		h.query(q)
		if e := h.last(t); e.Source != SourceUntenanted || e.Scan {
			t.Errorf("%s: %+v, want untenanted", q, e)
		}
	}
}

func TestUnfilteredReadOfTenantTableIsAScan(t *testing.T) {
	h := catalogHarness()
	for _, q := range []string{
		`SELECT * FROM orders`,
		`SELECT * FROM orders WHERE amount > 5`,
		`SELECT count(*) FROM orders o JOIN countries c ON true`,
		`UPDATE orders SET amount = 0 WHERE amount > 5`,
		`DELETE FROM orders`,
		`SELECT * FROM orders_v`,
	} {
		h.query(q)
		if e := h.last(t); e.Source != SourceScan || !e.Scan {
			t.Errorf("%s: %+v, want scan", q, e)
		}
	}
}

func TestFilteredButUnresolvedIsUnknownNotScan(t *testing.T) {
	h := catalogHarness()
	for _, q := range []string{
		`SELECT * FROM orders WHERE id = 42`,                 // key given, row never seen, no lookup configured
		`SELECT * FROM orders WHERE tenant_id <> 'acme'`,     // mentions the tenant but selects none
		`SELECT * FROM orders WHERE tenant_id IN (SELECT 1)`, // tenant filter we cannot read
		`INSERT INTO orders (id, amount) VALUES (1, 2)`,      // a write that names no tenant
	} {
		h.query(q)
		if e := h.last(t); e.Source != SourceUnknown || e.Scan {
			t.Errorf("%s: %+v, want unknown and not a scan", q, e)
		}
	}
}

func TestFailsClosedOnWhatItCannotSee(t *testing.T) {
	h := catalogHarness()
	for _, q := range []string{
		`SELECT * FROM mystery`,        // not in the catalog
		`SELECT my_func()`,             // application code may read tenant tables
		`SELECT public.my_func(1)`,     // same, schema-qualified
		`SELECT * FROM summary_v`,      // a view hiding its tenant column may wrap tenant tables
		`SELECT * FROM my_func() AS f`, // function in FROM
	} {
		h.query(q)
		if e := h.last(t); e.Source != SourceUnknown {
			t.Errorf("%s: %+v, want unknown", q, e)
		}
	}
}

func TestExplicitTenantStillWins(t *testing.T) {
	h := catalogHarness()
	h.query(`SELECT * FROM orders WHERE tenant_id = 'acme'`)
	if e := h.last(t); e.Source != SourceExplicit || e.Scan {
		t.Errorf("%+v", e)
	}
}

func TestWithoutCatalogNothingIsUntenantedOrScan(t *testing.T) {
	h := newHarness()
	h.query(`SELECT 1`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("source = %s", e.Source)
	}
}

func TestSchemaQualifiedAndSearchPath(t *testing.T) {
	h := catalogHarness()
	h.query(`SELECT * FROM t1.docs`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("t1.docs: %+v", e)
	}
	h.query(`SELECT * FROM t2.docs`)
	if e := h.last(t); e.Source != SourceUntenanted {
		t.Errorf("t2.docs: %+v", e)
	}
	// Unqualified "docs" is in neither schema of the default path.
	h.query(`SELECT * FROM docs`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("docs on default path: %+v", e)
	}
	h.query(`SET search_path = t2, public`)
	h.query(`SELECT * FROM docs`)
	if e := h.last(t); e.Source != SourceUntenanted {
		t.Errorf("docs with t2 first: %+v", e)
	}
	h.query(`SET search_path TO "t1"`)
	h.query(`SELECT * FROM docs`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("docs with t1: %+v", e)
	}
	h.query(`RESET search_path`)
	h.query(`SELECT * FROM docs`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("after reset: %+v", e)
	}
}

func TestSearchPathLocalEndsWithTransaction(t *testing.T) {
	h := catalogHarness()
	h.query(`BEGIN`)
	h.query(`SET LOCAL search_path = t2`)
	h.query(`SELECT * FROM docs`)
	if e := h.last(t); e.Source != SourceUntenanted {
		t.Errorf("inside txn: %+v", e)
	}
	h.query(`COMMIT`)
	h.query(`SELECT * FROM docs`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("after commit: %+v", e)
	}
}

func TestSearchPathFromStartupOptions(t *testing.T) {
	h := catalogHarness()
	h.s.Params = map[string]string{"options": "-c search_path=t1 -c other=1"}
	h.query(`SELECT * FROM docs`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("%+v", e)
	}
}

func TestCacheIsKeyedBySchema(t *testing.T) {
	h := catalogHarness()
	h.query(`INSERT INTO t1.docs (id, tenant_id) VALUES (1, 'acme')`)
	h.reply(nil, nil, "INSERT 0 1")
	h.rfq('I')
	h.query(`SELECT * FROM t1.docs WHERE id = 1`)
	if e := h.last(t); e.Source != SourceCache {
		t.Errorf("same schema: %+v", e)
	}
	if _, ok := h.a.Cache().Get("public.docs", "id", "1"); ok {
		t.Error("leaked into another schema")
	}
}

func TestConnectionTag(t *testing.T) {
	h := catalogHarness()
	h.query(`SELECT * FROM orders`)
	if e := h.last(t); e.Source != SourceScan {
		t.Fatalf("baseline: %+v", e)
	}
	h.query(`SET tenantdb.tenant = 'acme'`)
	h.query(`SELECT * FROM orders WHERE id = 5`)
	if e := h.last(t); e.Source != SourceSession || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("tagged: %+v", e)
	}
	h.query(`SELECT * FROM orders`)
	if e := h.last(t); e.Source != SourceSession || !e.Scan {
		t.Errorf("a tagged scan must still be flagged as a scan: %+v", e)
	}
	h.query(`SELECT * FROM orders WHERE tenant_id = 'globex'`)
	if e := h.last(t); e.Source != SourceExplicit || !e.Conflict {
		t.Errorf("explicit tenant contradicting the tag: %+v", e)
	}
	h.query(`SELECT * FROM countries`)
	if e := h.last(t); e.Source != SourceUntenanted {
		t.Errorf("shared data is not tagged: %+v", e)
	}
	h.query(`SELECT set_config('tenantdb.tenant', 'initech', false)`)
	h.query(`SELECT * FROM orders WHERE id = 5`)
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"initech"}) {
		t.Errorf("set_config: %+v", e)
	}
	h.query(`RESET tenantdb.tenant`)
	h.query(`SELECT * FROM orders WHERE id = 5`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("after reset: %+v", e)
	}
	h.query(`SET tenantdb.tenant = 'acme'`)
	h.query(`DISCARD ALL`)
	h.query(`SELECT * FROM orders WHERE id = 5`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("after discard all: %+v", e)
	}
}

func TestConnectionTagLocalAndStartup(t *testing.T) {
	h := catalogHarness()
	h.s.Params = map[string]string{"options": "-c tenantdb.tenant=startup"}
	h.query(`SELECT * FROM orders WHERE id = 1`)
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"startup"}) {
		t.Errorf("startup option: %+v", e)
	}
	h.query(`BEGIN`)
	h.query(`SELECT set_config('tenantdb.tenant', 'req', true)`)
	h.query(`SELECT * FROM orders WHERE id = 1`)
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"req"}) {
		t.Errorf("local set_config: %+v", e)
	}
	h.query(`COMMIT`)
	h.query(`SELECT * FROM orders WHERE id = 1`)
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"startup"}) {
		t.Errorf("back to the session value: %+v", e)
	}
}

func TestColumnlessInsertUsesSchemaOrder(t *testing.T) {
	h := catalogHarness()
	// orders is (id, tenant_id, amount)
	h.query(`INSERT INTO orders VALUES (1, 'acme', 10), (2, 'globex', 20)`)
	e := h.last(t)
	if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme", "globex"}) {
		t.Errorf("event = %+v", e)
	}
	h.reply(nil, nil, "INSERT 0 2")
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE id = 2`)
	if e := h.last(t); e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, []string{"globex"}) {
		t.Errorf("cache after column-less insert: %+v", e)
	}
	// notes is (tenant_id, id, body): a different order must be honoured.
	h.query(`INSERT INTO notes VALUES ('initech', 9, 'hi')`)
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"initech"}) {
		t.Errorf("notes: %+v", e)
	}
}

func TestColumnlessInsertWithoutCatalogIsUnknown(t *testing.T) {
	h := newHarness()
	h.query(`INSERT INTO orders VALUES (1, 'acme', 10)`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("event = %+v", e)
	}
}

// fakeLookup stands in for the database.
type fakeLookup struct {
	rows  map[string][]string // "table|col|val" -> tenants
	err   error
	calls atomic.Int32
}

func (f *fakeLookup) tenants(_ context.Context, ti *TableInfo, _ string, col, val string) ([]string, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}
	return f.rows[ti.Key()+"|"+col+"|"+val], nil
}

func TestLookupResolvesCacheMiss(t *testing.T) {
	h := catalogHarness()
	fl := &fakeLookup{rows: map[string][]string{"public.orders|id|42": {"acme"}}}
	h.a.lk = fl
	h.query(`SELECT * FROM orders WHERE id = 42`)
	h.waitFor(t, 1)
	if e := h.last(t); e.Source != SourceLookup || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("event = %+v", e)
	}
	// Now cached: no second database call.
	h.query(`SELECT * FROM orders WHERE id = 42`)
	if e := h.last(t); e.Source != SourceCache || fl.calls.Load() != 1 {
		t.Errorf("second lookup: %+v, calls=%d", e, fl.calls.Load())
	}
}

func TestLookupPartialAndMissingRows(t *testing.T) {
	h := catalogHarness()
	h.a.lk = &fakeLookup{rows: map[string][]string{"public.orders|id|1": {"acme"}, "public.orders|id|2": {"globex"}}}
	h.a.Cache().Put("public.orders", "id", "1", "acme")
	h.query(`SELECT * FROM orders WHERE id IN (1, 2)`)
	h.waitFor(t, 1)
	if e := h.last(t); e.Source != SourceLookup || !reflect.DeepEqual(e.Tenants, []string{"acme", "globex"}) {
		t.Errorf("partial hit: %+v", e)
	}
	h.query(`SELECT * FROM orders WHERE id = 999`)
	h.waitFor(t, 2)
	if e := h.last(t); e.Source != SourceNoMatch || e.Tenants != nil {
		t.Errorf("missing row: %+v", e)
	}
}

func TestLookupFailureFallsBack(t *testing.T) {
	h := catalogHarness()
	h.a.lk = &fakeLookup{err: errors.New("db down")}
	h.query(`SELECT * FROM orders WHERE id = 7`)
	h.waitFor(t, 1)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("must not claim nomatch when the lookup failed: %+v", e)
	}
	h.query(`SET tenantdb.tenant = 'acme'`)
	h.query(`SELECT * FROM orders WHERE id = 7`)
	h.waitFor(t, 2)
	if e := h.last(t); e.Source != SourceSession {
		t.Errorf("tag still applies when lookup fails: %+v", e)
	}
}

func TestLookupResultDoesNotLeakIntoNextTransaction(t *testing.T) {
	h := catalogHarness()
	gate := make(chan struct{})
	h.a.lk = gatedLookup{gate: gate, tenant: "acme"}
	h.query(`BEGIN`)
	h.query(`SELECT * FROM orders WHERE id = 3`) // lookup parks on the gate
	h.query(`COMMIT`)
	h.rfq('I')
	close(gate)
	h.waitFor(t, 1)
	h.query(`BEGIN`)
	h.query(`SELECT * FROM orders WHERE id = 99`) // no cache, lookup not gated now
	h.rfq('T')
	st := h.a.st(h.s)
	st.mu.Lock()
	leaked := st.txnTenant
	st.mu.Unlock()
	if len(leaked) != 0 {
		t.Errorf("tenant from a finished transaction leaked into the next: %v", leaked)
	}
}

type gatedLookup struct {
	gate   chan struct{}
	tenant string
}

func (g gatedLookup) tenants(ctx context.Context, _ *TableInfo, _ string, _, val string) ([]string, error) {
	if val == "3" {
		<-g.gate
		return []string{g.tenant}, nil
	}
	return nil, nil
}

func TestIsDDL(t *testing.T) {
	for tag, want := range map[string]bool{"CREATE TABLE": true, "ALTER TABLE": true, "DROP INDEX": true, "SELECT 1": false, "INSERT 0 1": false} {
		if isDDL(tag) != want {
			t.Errorf("isDDL(%q) != %v", tag, want)
		}
	}
}

func TestParseStartupOptions(t *testing.T) {
	got := parseStartupOptions("-c search_path=a,b -ctenantdb.tenant=x --work_mem=64MB -c statement_timeout=5s")
	want := map[string]string{"search_path": "a,b", "tenantdb.tenant": "x", "work_mem": "64MB", "statement_timeout": "5s"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}

// Needs a live Postgres: TENANTDB_TEST_DSN=postgres://user:pw@host:5432/db
func TestCatalogWarmAndLookupAgainstPostgres(t *testing.T) {
	dsn := os.Getenv("TENANTDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TENANTDB_TEST_DSN not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	cleanup := `DROP VIEW IF EXISTS wt_v; DROP TABLE IF EXISTS wt_orders, wt_refs; DROP FUNCTION IF EXISTS wt_fn()`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE wt_orders (id int PRIMARY KEY, "Tenant_Id" text, dropped int, amount int)`,
		`ALTER TABLE wt_orders DROP COLUMN dropped`,
		`CREATE TABLE wt_refs (code text)`,
		`CREATE VIEW wt_v AS SELECT amount FROM wt_orders`,
		`CREATE FUNCTION wt_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'`,
		`INSERT INTO wt_orders VALUES (1,'acme',5),(2,'globex',6),(3,NULL,7)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer conn.Exec(ctx, cleanup)

	cat := NewCatalog(dsn)
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	ti, ok := cat.Table(TableRef{Name: "wt_orders"}, []string{"public"}, "")
	if !ok || !reflect.DeepEqual(ti.Cols, []string{"id", "tenant_id", "amount"}) {
		t.Fatalf("wt_orders = %+v (dropped column must not appear)", ti)
	}
	if v, ok := cat.Table(TableRef{Name: "wt_v"}, []string{"public"}, ""); !ok || v.Kind != 'v' {
		t.Errorf("view not seen as a view: %+v", v)
	}
	if !cat.UserFunc(TableRef{Name: "wt_fn"}, []string{"public"}, "") || cat.UserFunc(TableRef{Name: "now"}, []string{"public"}, "") {
		t.Error("user function detection wrong")
	}
	if _, ok := cat.Table(TableRef{Name: "pg_tables"}, []string{"public"}, ""); !ok {
		t.Error("system catalogs must be known")
	}

	lk, err := NewLookup(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer lk.Close()
	a := New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat, Lookup: lk}, func(Event) {})
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := a.Warm(ctx, dsn, 1000, log); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"1": "acme", "2": "globex"} {
		if got, ok := a.Cache().Get("public.wt_orders", "id", id); !ok || got != want {
			t.Errorf("id %s = %q, %v; want %q", id, got, ok, want)
		}
	}
	if _, ok := a.Cache().Get("public.wt_orders", "id", "3"); ok {
		t.Error("row with NULL tenant must not be cached")
	}

	// The lookup itself, including a string bound to an integer key column.
	got, err := lk.tenants(ctx, ti, "tenant_id", "id", "2")
	if err != nil || !reflect.DeepEqual(got, []string{"globex"}) {
		t.Errorf("lookup = %v, %v", got, err)
	}
	if got, err = lk.tenants(ctx, ti, "tenant_id", "id", "999"); err != nil || len(got) != 0 {
		t.Errorf("missing row = %v, %v", got, err)
	}
}

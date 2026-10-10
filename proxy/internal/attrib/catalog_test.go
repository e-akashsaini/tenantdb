package attrib

import (
	"context"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
)

func testCatalog() *Catalog {
	c := NewCatalog("")
	c.set([]colRow{
		{"public", "orders", "id"}, {"public", "orders", "tenant_id"}, {"public", "orders", "amount"},
		{"public", "countries", "code"}, {"public", "countries", "name"},
		{"public", "notes", "tenant_id"}, {"public", "notes", "id"}, {"public", "notes", "body"},
		{"a", "dup", "x"}, {"b", "dup", "x"},
	})
	return c
}

func catalogHarness() *harness {
	h := &harness{s: &proxy.Session{ID: 1, User: "u", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: testCatalog()},
		func(e Event) { h.events = append(h.events, e) })
	return h
}

func TestSharedTablesAreUntenantedNotUnknown(t *testing.T) {
	h := catalogHarness()
	for _, q := range []string{
		`SELECT * FROM countries WHERE code = 'IE'`,
		`SELECT 1`,
		`SELECT now()`,
		`INSERT INTO countries (code, name) VALUES ('IE', 'Ireland')`,
	} {
		h.query(q)
		if e := h.last(t); e.Source != SourceUntenanted {
			t.Errorf("%s: source = %s, want untenanted", q, e.Source)
		}
	}
}

func TestTenantTableWithoutTenantStaysUnknown(t *testing.T) {
	h := catalogHarness()
	for _, q := range []string{
		`SELECT * FROM orders WHERE amount > 5`,
		`SELECT * FROM orders o JOIN countries c ON true`, // one tenant table is enough
		`SELECT * FROM mystery`,                           // not in the catalog: cannot vouch for it
		`SELECT * FROM dup`,                               // ambiguous across schemas
		`SELECT * FROM generate_series(1, 3)`,             // function in FROM is opaque
	} {
		h.query(q)
		if e := h.last(t); e.Source != SourceUnknown {
			t.Errorf("%s: source = %s, want unknown", q, e.Source)
		}
	}
}

func TestCTENameIsNotATable(t *testing.T) {
	h := catalogHarness()
	h.query(`WITH c AS (SELECT code FROM countries) SELECT * FROM c`)
	if e := h.last(t); e.Source != SourceUntenanted {
		t.Errorf("source = %s, want untenanted", e.Source)
	}
}

func TestWithoutCatalogNothingIsUntenanted(t *testing.T) {
	h := newHarness()
	h.query(`SELECT 1`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("source = %s", e.Source)
	}
}

func TestExplicitTenantStillWinsOverUntenanted(t *testing.T) {
	h := catalogHarness()
	h.query(`SELECT * FROM orders WHERE tenant_id = 'acme'`)
	if e := h.last(t); e.Source != SourceExplicit {
		t.Errorf("source = %s", e.Source)
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

func TestIsDDL(t *testing.T) {
	for tag, want := range map[string]bool{"CREATE TABLE": true, "ALTER TABLE": true, "DROP INDEX": true, "SELECT 1": false, "INSERT 0 1": false} {
		if isDDL(tag) != want {
			t.Errorf("isDDL(%q) != %v", tag, want)
		}
	}
}

// Needs a live Postgres: TENANTDB_TEST_DSN=postgres://user:pw@host:5432/db
func TestCatalogLoadAndWarmAgainstPostgres(t *testing.T) {
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
	for _, q := range []string{
		`DROP TABLE IF EXISTS wt_orders, wt_refs`,
		`CREATE TABLE wt_orders (id int PRIMARY KEY, "Tenant_Id" text, dropped int, amount int)`,
		`ALTER TABLE wt_orders DROP COLUMN dropped`,
		`CREATE TABLE wt_refs (code text)`,
		`INSERT INTO wt_orders VALUES (1,'acme',5),(2,'globex',6),(3,NULL,7)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer conn.Exec(ctx, `DROP TABLE IF EXISTS wt_orders, wt_refs`)

	cat := NewCatalog(dsn)
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	ti, ok := cat.Table("wt_orders")
	if !ok || !reflect.DeepEqual(ti.Cols, []string{"id", "tenant_id", "amount"}) {
		t.Fatalf("wt_orders = %+v (dropped column must not appear)", ti)
	}
	a := New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat}, func(Event) {})
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := a.Warm(ctx, dsn, 1000, log); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"1": "acme", "2": "globex"} {
		if got, ok := a.Cache().Get("wt_orders", "id", id); !ok || got != want {
			t.Errorf("id %s = %q, %v; want %q", id, got, ok, want)
		}
	}
	if _, ok := a.Cache().Get("wt_orders", "id", "3"); ok {
		t.Error("row with NULL tenant must not be cached")
	}
	if a.untenanted(Stmt{Tables: []string{"wt_refs"}}) != true || a.untenanted(Stmt{Tables: []string{"wt_orders"}}) {
		t.Error("untenanted classification wrong against live schema")
	}
}

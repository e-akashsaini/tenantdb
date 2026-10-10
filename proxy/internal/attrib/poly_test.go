package attrib

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestRailsTableNames(t *testing.T) {
	for class, want := range map[string]string{
		"Template":                  "templates",
		"Submission":                "submissions",
		"ActiveStorage::Blob":       "active_storage_blobs",
		"ActiveStorage::Attachment": "active_storage_attachments",
		"Company":                   "companies",
		"Address":                   "addresses",
		"Box":                       "boxes",
		"HTMLPage":                  "html_pages",
		"UserProfile":               "user_profiles",
		"Day":                       "days",
	} {
		got := railsTableNames(class)
		if len(got) == 0 || got[0] != want {
			t.Errorf("%s -> %v, want first candidate %s", class, got, want)
		}
	}
	if railsTableNames("") != nil {
		t.Error("empty class")
	}
}

func TestHasPolyID(t *testing.T) {
	ti := &TableInfo{Cols: []string{"id", "record_type", "record_id", "owner_id", "kind"}}
	for col, want := range map[string]bool{"record_id": true, "owner_id": false, "id": false, "kind": false, "record_type": false} {
		if ti.HasPolyID(col) != want {
			t.Errorf("HasPolyID(%q) != %v", col, want)
		}
	}
}

func polySpec() catSpec {
	var rows []colRow
	add := func(r []colRow) { rows = append(rows, r...) }
	add(tbl("public", "templates", 'r', "id", "account_id", "name"))
	add(tbl("public", "submissions", 'r', "id", "account_id"))
	add(tbl("public", "search_entries", 'r', "id", "account_id", "record_type", "record_id", "body"))
	return catSpec{
		rows:   rows,
		unique: map[string][]string{"public.templates": {"id"}, "public.submissions": {"id"}, "public.search_entries": {"id"}},
		polys: map[string]map[string]map[string]FKRef{
			"public.search_entries": {"record": {
				"Template":   {ParentKey: "public.templates", ParentCol: "id"},
				"Submission": {ParentKey: "public.submissions", ParentCol: "id"},
			}},
		},
	}
}

func polyHarness() *harness {
	h := specHarness(polySpec(), StrictOptions{})
	h.a = New(Options{TenantColumns: []string{"account_id"}, KeyColumns: []string{"id"}, Catalog: polySpec().build()}, h.record)
	return h
}

func TestPolymorphicLookupResolvesThroughTheNamedParent(t *testing.T) {
	h := polyHarness()
	h.a.Cache().Put("public.templates", "id", "5", "acme")
	h.a.Cache().Put("public.submissions", "id", "5", "globex") // the same id in another table is another row

	for sql, want := range map[string][]string{
		`SELECT * FROM search_entries WHERE record_type = 'Template' AND record_id = 5 LIMIT 1`:   {"acme"},
		`SELECT * FROM search_entries WHERE record_id = 5 AND record_type = 'Submission' LIMIT 1`: {"globex"}, // the order ActiveRecord emits varies
		`SELECT * FROM search_entries WHERE record_type = 'Template' AND record_id IN (5, 5)`:     {"acme"},
	} {
		h.query(sql)
		if e := h.last(t); e.Source != SourceCache || e.Scan || !reflect.DeepEqual(e.Tenants, want) {
			t.Errorf("%s: source=%s scan=%v tenants=%v, want cache %v", sql, e.Source, e.Scan, e.Tenants, want)
		}
		h.rfq('I')
	}
}

func TestPolymorphicLookupWithBoundParametersAndLookups(t *testing.T) {
	h := polyHarness()
	fl := &fakeLookup{rows: map[string][]string{"public.templates|id|9": {"initech"}}}
	h.a.lk = fl
	h.front(&pgproto3.Parse{Name: "p", Query: `SELECT * FROM search_entries WHERE record_type = $1 AND record_id = $2 LIMIT $3`})
	h.front(&pgproto3.Bind{PreparedStatement: "p", Parameters: [][]byte{[]byte("Template"), []byte("9"), []byte("1")}})
	h.front(&pgproto3.Execute{})
	h.waitFor(t, 1)
	if e := h.last(t); e.Source != SourceLookup || !reflect.DeepEqual(e.Tenants, []string{"initech"}) {
		t.Errorf("%+v", e)
	}
}

func TestPolymorphicLookupThatCannotBeResolvedIsUnknownNotAScan(t *testing.T) {
	h := polyHarness()
	h.a.Cache().Put("public.templates", "id", "5", "acme")
	for _, sql := range []string{
		`SELECT * FROM search_entries WHERE record_type = 'Mystery' AND record_id = 5`,                // a class with no verified table
		`SELECT * FROM search_entries WHERE record_id = 5`,                                            // no type to say which table
		`SELECT * FROM search_entries WHERE record_type = 'Template' AND record_id = 77`,              // a parent nobody has seen, no lookup configured
		`SELECT * FROM search_entries WHERE record_type = 'Template' AND record_id = 5 OR body = 'x'`, // an alternative the lookup cannot vouch for
	} {
		h.query(sql)
		if e := h.last(t); e.Source != SourceUnknown || e.Scan {
			t.Errorf("%s: %+v", sql, e)
		}
		h.rfq('I')
	}
	// A type with no id names no row at all: that is a scan.
	h.query(`SELECT * FROM search_entries WHERE record_type = 'Template'`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("%+v", e)
	}
}

// Needs a live Postgres: TENANTDB_TEST_DSN=postgres://user:pw@host:5432/db
func TestPolymorphicPairsAreVerifiedAgainstData(t *testing.T) {
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
	cleanup := `DROP TABLE IF EXISTS poly_notes, poly_widgets, poly_gadgets, poly_gizmos CASCADE`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE poly_widgets (id bigint PRIMARY KEY, tenant_id text)`,
		`CREATE TABLE poly_gadgets (id bigint PRIMARY KEY, tenant_id text)`,
		`CREATE TABLE poly_gizmos (id bigint PRIMARY KEY, tenant_id text)`,
		`CREATE TABLE poly_notes (id bigint PRIMARY KEY, tenant_id text, owner_type text, owner_id bigint)`,
		`INSERT INTO poly_widgets VALUES (1, 'acme'), (2, 'globex')`,
		`INSERT INTO poly_gadgets VALUES (1, 'acme'), (2, 'globex')`,
		`INSERT INTO poly_gizmos VALUES (1, 'acme'), (2, 'acme')`,
		// widgets and gadgets: every note has its owner's tenant. Gizmos: a note under another tenant.
		`INSERT INTO poly_notes VALUES (1, 'acme', 'PolyWidget', 1), (2, 'globex', 'PolyWidget', 2), (3, 'acme', 'PolyGadget', 1)`,
		`INSERT INTO poly_notes VALUES (4, 'globex', 'PolyGizmo', 1), (5, 'acme', 'PolyGizmo', 2)`, // wrong on purpose
		`INSERT INTO poly_notes VALUES (6, 'acme', 'NoSuchThing', 1)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer conn.Exec(ctx, cleanup)

	cat := NewCatalog(dsn)
	cat.TenantColumns = []string{"tenant_id"}
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	notes, _ := cat.Table(TableRef{Name: "poly_notes"}, []string{"public"}, "")
	if ref, ok := cat.Poly(notes, "owner", "PolyWidget"); !ok || ref.ParentKey != "public.poly_widgets" {
		t.Errorf("PolyWidget: %+v %v", ref, ok)
	}
	if ref, ok := cat.Poly(notes, "owner", "PolyGadget"); !ok || ref.ParentKey != "public.poly_gadgets" {
		t.Errorf("PolyGadget: %+v %v", ref, ok)
	}
	if _, ok := cat.Poly(notes, "owner", "PolyGizmo"); ok {
		t.Error("a mapping whose rows disagree with the parent's tenant must be rejected")
	}
	if _, ok := cat.Poly(notes, "owner", "NoSuchThing"); ok {
		t.Error("a class with no table")
	}
}

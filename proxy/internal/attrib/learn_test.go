package attrib

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/akashsaini/tenantdb/internal/proxy"

	"github.com/jackc/pgx/v5"
)

// Needs a live Postgres: TENANTDB_TEST_DSN=postgres://user:pw@host:5432/db
func TestRelationshipsAreLearnedFromJoinsAndVerified(t *testing.T) {
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
	cleanup := `DROP TABLE IF EXISTS ln_boxes, ln_folders, ln_noise, ln_empty_kids, ln_empty_parents CASCADE`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE ln_folders (id bigint PRIMARY KEY, tenant_id text)`,
		// folder points at ln_folders but nothing in its name says so
		`CREATE TABLE ln_boxes (id bigint PRIMARY KEY, tenant_id text, folder bigint)`,
		// ln_noise.n happens to equal folder ids, across tenants: not a relationship
		`CREATE TABLE ln_noise (id bigint PRIMARY KEY, tenant_id text, n bigint)`,
		`CREATE TABLE ln_empty_parents (id bigint PRIMARY KEY, tenant_id text)`,
		`CREATE TABLE ln_empty_kids (id bigint PRIMARY KEY, tenant_id text, up bigint)`,
		`INSERT INTO ln_folders SELECT g, 't' || (g % 3) FROM generate_series(1, 30) g`,
		`INSERT INTO ln_boxes SELECT g, 't' || ((g % 30 + 1) % 3), (g % 30) + 1 FROM generate_series(1, 90) g`,
		`INSERT INTO ln_noise SELECT g, 't' || (g % 3), (g % 5) + 1 FROM generate_series(1, 60) g`,
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
	boxes, _ := cat.Table(TableRef{Name: "ln_boxes"}, []string{"public"}, "")
	if _, ok := cat.FK(boxes, "folder"); ok {
		t.Fatal("the naming rule must not find this one")
	}

	h := &harness{s: &proxy.Session{ID: 1, User: "app", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat}, h.record)
	// Before: the join is not understood, so the box side stays unconfined.
	sql := `SELECT b.* FROM ln_boxes b JOIN ln_folders f ON b.folder = f.id WHERE f.tenant_id = 't1'`
	if e := h.attributed(t, sql); e.Source == SourceExplicit {
		t.Fatalf("claimed a tenant for a join it cannot yet vouch for: %+v", e)
	}
	// The application also joins the noise table to folders; both are only candidates.
	h.attributed(t, `SELECT n.* FROM ln_noise n JOIN ln_folders f ON n.n = f.id WHERE f.tenant_id = 't1'`)
	h.attributed(t, `SELECT 1 FROM ln_empty_kids k JOIN ln_empty_parents p ON k.up = p.id WHERE p.tenant_id = 't1'`)

	cat.DrainSuggestions(15 * time.Second)

	if ref, ok := cat.FK(boxes, "folder"); !ok || ref.ParentKey != "public.ln_folders" || ref.ParentCol != "id" {
		t.Errorf("a consistent relationship seen in a join should be learned: %+v %v", ref, ok)
	}
	noise, _ := cat.Table(TableRef{Name: "ln_noise"}, []string{"public"}, "")
	if _, ok := cat.FK(noise, "n"); ok {
		t.Error("equal values across tenants are not a relationship and must be rejected")
	}
	kids, _ := cat.Table(TableRef{Name: "ln_empty_kids"}, []string{"public"}, "")
	if _, ok := cat.FK(kids, "up"); ok {
		t.Error("with no rows there is nothing to verify against")
	}

	// After: the same statement is now confined through the folder.
	e := h.attributed(t, sql)
	if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"t1"}) {
		t.Errorf("after learning: %+v", e)
	}
	// ... and the learned relationship survives a schema reload.
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	boxes, _ = cat.Table(TableRef{Name: "ln_boxes"}, []string{"public"}, "")
	if _, ok := cat.FK(boxes, "folder"); !ok {
		t.Error("a reload forgot what had been learned")
	}
}

// A table that points at itself (folders, comments, categories): the join of two
// occurrences of it reveals the relationship, and it is checked like any other.
func TestSelfReferencingRelationshipsAreLearned(t *testing.T) {
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
	cleanup := `DROP TABLE IF EXISTS ln_tree, ln_tree_bad CASCADE`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE ln_tree (id bigint PRIMARY KEY, tenant_id text, up bigint)`,
		`CREATE TABLE ln_tree_bad (id bigint PRIMARY KEY, tenant_id text, other bigint)`,
		`INSERT INTO ln_tree SELECT g, 't' || ((g - 1) / 10), CASE WHEN g % 10 = 1 THEN NULL ELSE g - 1 END FROM generate_series(1, 40) g`,
		// `other` happens to hold ids, but of rows in other tenants
		`INSERT INTO ln_tree_bad SELECT g, 't' || ((g - 1) / 10), (g % 40) + 11 FROM generate_series(1, 40) g`,
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
	h := &harness{s: &proxy.Session{ID: 1, User: "app", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat}, h.record)
	good := `SELECT c.* FROM ln_tree c WHERE c.tenant_id = 't1' AND EXISTS (SELECT 1 FROM ln_tree s WHERE s.up = c.id)`
	bad := `SELECT c.* FROM ln_tree_bad c WHERE c.tenant_id = 't1' AND EXISTS (SELECT 1 FROM ln_tree_bad s WHERE s.other = c.id)`
	if e := h.attributed(t, good); e.Source == SourceExplicit {
		t.Fatalf("the subquery reads a table nothing confines yet: %+v", e)
	}
	h.attributed(t, bad)
	cat.DrainSuggestions(15 * time.Second)
	if e := h.attributed(t, good); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"t1"}) {
		t.Errorf("after learning that up points at the same table: %+v", e)
	}
	if e := h.attributed(t, bad); e.Source == SourceExplicit {
		t.Errorf("a column whose values match rows of other tenants is not a relationship: %+v", e)
	}
}

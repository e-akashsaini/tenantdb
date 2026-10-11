package attrib

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
)

// A statement whose owner has to be looked up is reported when the lookup
// returns, but the statements after it must not overtake it: events leave a
// session in the order the statements were sent.
func TestEventsKeepStatementOrderAcrossAsyncLookups(t *testing.T) {
	dsn := os.Getenv("TENANTDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TENANTDB_TEST_DSN not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	for _, q := range []string{
		`DROP TABLE IF EXISTS tdb_ordered`,
		`CREATE TABLE tdb_ordered (id bigint PRIMARY KEY, tenant_id text, note text)`,
		`INSERT INTO tdb_ordered VALUES (1, 'acme', 'a'), (2, 'globex', 'b')`,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	defer admin.Exec(ctx, `DROP TABLE IF EXISTS tdb_ordered`)

	cat := NewCatalog(dsn)
	cat.TenantColumns = []string{"tenant_id"}
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	lk, err := NewLookup(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer lk.Close()
	h := &harness{s: &proxy.Session{ID: 1, User: "app", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat, Lookup: lk,
		Strict: StrictOptions{Mode: StrictAlert}}, h.record)

	// Hold the table so the lookup cannot finish.
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `LOCK TABLE tdb_ordered IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	h.query(`SELECT * FROM tdb_ordered WHERE id = 2`) // owner not cached: looked up, blocked
	h.query(`SELECT 1`)
	h.query(`SELECT * FROM tdb_ordered WHERE tenant_id = 'acme'`)

	time.Sleep(150 * time.Millisecond)
	h.mu.Lock()
	n := len(h.events)
	h.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d events were delivered ahead of the statement waiting on its lookup", n)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		h.mu.Lock()
		n = len(h.events)
		h.mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.events) != 3 {
		t.Fatalf("got %d events", len(h.events))
	}
	want := []string{`SELECT * FROM tdb_ordered WHERE id = 2`, `SELECT 1`, `SELECT * FROM tdb_ordered WHERE tenant_id = 'acme'`}
	for i, e := range h.events {
		if e.SQL != want[i] {
			t.Errorf("event %d is %q, want %q", i, e.SQL, want[i])
		}
	}
	if e := h.events[0]; e.Source != SourceLookup || len(e.Tenants) != 1 || e.Tenants[0] != "globex" {
		t.Errorf("the looked-up statement: %+v", e)
	}
}

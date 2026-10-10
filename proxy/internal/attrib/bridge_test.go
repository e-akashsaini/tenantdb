package attrib

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// keys lists what the cache holds (test helper).
func (c *RowCache) keys() []cacheKey {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]cacheKey, 0, len(c.m))
	for k := range c.m {
		out = append(out, k)
	}
	return out
}

// Credential-style lookups: a table with no tenant column (access tokens) holds a
// unique key and points at a tenant table. Needs TENANTDB_TEST_DSN.
func TestCredentialLookupsAreAttributedThroughTheirRow(t *testing.T) {
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
	cleanup := `DROP TABLE IF EXISTS br_tokens, br_users CASCADE`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE br_users (id bigint PRIMARY KEY, tenant_id text, email text)`,
		// no tenant column, and the pointer is not called br_user_id: only the join says what it is
		`CREATE TABLE br_tokens (id bigint PRIMARY KEY, owner bigint, token text UNIQUE)`,
		`INSERT INTO br_users VALUES (1,'acme','a@x'), (2,'globex','g@x')`,
		`INSERT INTO br_tokens VALUES (10,1,'tok-acme'), (11,2,'tok-globex')`,
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
	lk, err := NewLookup(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer lk.Close()

	mk := func(mode StrictMode) *harness {
		h := &harness{s: &proxy.Session{ID: 1, User: "app", DB: "d"}}
		h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat, Lookup: lk,
			Strict: StrictOptions{Mode: mode}}, h.record)
		return h
	}
	ask := func(h *harness, sql string, params ...string) Event {
		t.Helper()
		before := len(h.events)
		if len(params) == 0 {
			h.query(sql)
		} else {
			ps := make([][]byte, len(params))
			for i, v := range params {
				ps[i] = []byte(v)
			}
			h.front(&pgproto3.Parse{Name: "b", Query: sql})
			h.front(&pgproto3.Bind{PreparedStatement: "b", Parameters: ps})
			h.front(&pgproto3.Execute{})
		}
		h.waitFor(t, before+1)
		e := h.last(t)
		h.rfq('I')
		return e
	}
	login := `SELECT u.* FROM br_users u INNER JOIN br_tokens k ON k.owner = u.id WHERE k.token = 'tok-acme'`

	h := mk(StrictAlert)
	e := ask(h, login)
	if e.Source != SourceLookup || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Fatalf("the user a token belongs to is that token's tenant: %+v", e)
	}
	// ... and the answer is remembered, without the credential being kept.
	e = ask(h, login)
	if e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("second time: %+v", e)
	}
	for _, k := range h.a.bridge.keys() {
		if strings.Contains(k.val, "tok-acme") || strings.Contains(k.table, "tok-acme") {
			t.Errorf("the credential is sitting in the cache: %+v", k)
		}
	}
	// A bound parameter works the same.
	e = ask(h, `SELECT u.* FROM br_tokens k JOIN br_users u ON u.id = k.owner WHERE k.token = $1`, "tok-globex")
	if !reflect.DeepEqual(e.Tenants, []string{"globex"}) {
		t.Errorf("bound token: %+v", e)
	}
	// An unknown token reads nothing.
	e = ask(h, `SELECT u.* FROM br_users u JOIN br_tokens k ON k.owner = u.id WHERE k.token = 'nope'`)
	if len(e.Tenants) != 0 || e.Violation != "" {
		t.Errorf("a token nobody holds: %+v", e)
	}
	// Any unique column pins the row, the primary key included.
	e = ask(h, `SELECT u.* FROM br_users u JOIN br_tokens k ON k.owner = u.id WHERE k.id = 10`)
	if !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("by token id: %+v", e)
	}
	// Alternatives that are not all pinned by a token confine nothing.
	for _, sql := range []string{
		`SELECT u.* FROM br_users u JOIN br_tokens k ON k.owner = u.id WHERE k.token = 'tok-acme' OR u.email = 'g@x'`,
		`SELECT u.* FROM br_users u LEFT JOIN br_tokens k ON k.owner = u.id AND k.token = 'tok-acme'`,
		`SELECT u.* FROM br_users u, br_tokens k WHERE k.token = 'tok-acme'`,
		`SELECT u.* FROM br_users u JOIN br_tokens k ON k.owner = u.id WHERE k.token <> 'tok-acme'`,
	} {
		e := ask(mk(StrictAlert), sql)
		if e.Source == SourceLookup || e.Source == SourceCache || e.Source == SourceExplicit {
			t.Errorf("claimed a tenant it cannot know: %s\n  %+v", sql, e)
		}
	}
	// A change to the schema forgets what credentials mapped to.
	h.a.bridge.Put("x", "y", "z", "t")
	h.query("ALTER TABLE br_tokens ADD COLUMN note text")
	h.a.OnBackend(h.s, &pgproto3.CommandComplete{CommandTag: []byte("ALTER TABLE")})
	if h.a.bridge.Len() != 0 {
		t.Error("a schema change must clear credential mappings")
	}

	// In block mode the answer is in hand before the statement is let through.
	hb := mk(StrictBlock)
	if r := hb.gateQuery(login); r != nil {
		t.Errorf("a token lookup that resolves to one tenant must pass: %+v", r)
	}
}

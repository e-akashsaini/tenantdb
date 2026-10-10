package attrib

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
)

// A Rails-shaped schema: parents carry the tenant; children point at them by
// <thing>_id and are also reachable by a unique slug.
func fkSpec() catSpec {
	var rows []colRow
	add := func(r []colRow) { rows = append(rows, r...) }
	add(tbl("public", "submissions", 'r', "id", "account_id", "slug"))
	add(tbl("public", "submitters", 'r', "id", "account_id", "submission_id", "slug", "status"))
	add(tbl("public", "notes", 'r', "id", "account_id", "body"))
	return catSpec{
		rows: rows,
		unique: map[string][]string{
			"public.submissions": {"id", "slug"},
			"public.submitters":  {"id", "slug"},
			"public.notes":       {"id"},
		},
		fks: map[string]map[string]FKRef{
			"public.submitters": {"submission_id": {ParentKey: "public.submissions", ParentCol: "id"}},
		},
	}
}

func fkHarness() *harness {
	h := specHarness(fkSpec(), StrictOptions{})
	h.a = New(Options{TenantColumns: []string{"account_id"}, KeyColumns: []string{"id"}, Catalog: fkSpec().build(), Strict: StrictOptions{}}, h.record)
	return h
}

func TestAnyUniqueColumnIsAKey(t *testing.T) {
	h := fkHarness()
	h.query(`INSERT INTO submitters (id, account_id, slug) VALUES (1, 'acme', 'abc123')`)
	h.reply(nil, nil, "INSERT 0 1")
	h.rfq('I')
	h.query(`SELECT * FROM submitters WHERE slug = 'abc123'`) // the public signing link: found by slug, not id
	if e := h.last(t); e.Source != SourceCache || e.Scan || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("slug lookup: %+v", e)
	}
	// Result rows teach every unique column they carry.
	h.rfq('I')
	h.query(`SELECT id, slug, account_id FROM submissions WHERE status = 'x'`)
	h.reply(cols(900, "id", "slug", "account_id"), [][]string{{"7", "zzz", "globex"}}, "SELECT 1")
	h.rfq('I')
	h.query(`SELECT * FROM submissions WHERE slug = 'zzz'`)
	if e := h.last(t); e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, []string{"globex"}) {
		t.Errorf("learned slug from rows: %+v", e)
	}
	h.query(`SELECT * FROM submissions WHERE id = 7`)
	if e := h.last(t); e.Source != SourceCache {
		t.Errorf("learned id from rows: %+v", e)
	}
}

func TestChildLookupResolvesThroughItsParent(t *testing.T) {
	h := fkHarness()
	h.a.Cache().Put("public.submissions", "id", "5", "acme")

	h.query(`SELECT * FROM submitters WHERE submission_id = 5`)
	if e := h.last(t); e.Source != SourceCache || e.Scan || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("via the parent's cached owner: %+v", e)
	}
	h.query(`SELECT * FROM submitters WHERE submission_id IN (5, 5)`)
	if e := h.last(t); e.Source != SourceCache {
		t.Errorf("IN list: %+v", e)
	}
	h.query(`UPDATE submitters SET status = 'done' WHERE submission_id = 5 AND status = 'open'`)
	if e := h.last(t); e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("an AND-ed ordinary predicate does not hide the identifying one: %+v", e)
	}

	// A parent nobody has seen: unknown, not a scan: it IS a lookup, just unresolved.
	h.query(`SELECT * FROM submitters WHERE submission_id = 99`)
	if e := h.last(t); e.Source != SourceUnknown || e.Scan {
		t.Errorf("unresolved lookup: %+v", e)
	}
	// Reading by an ordinary column identifies nobody: that is a scan.
	h.query(`SELECT * FROM submitters WHERE status = 'open'`)
	if e := h.last(t); e.Source != SourceScan || !e.Scan {
		t.Errorf("ordinary predicate: %+v", e)
	}
	// With an OR, a predicate that identifies nothing is an alternative the others cannot vouch for.
	h.query(`SELECT * FROM submitters WHERE submission_id = 5 OR status = 'open'`)
	if e := h.last(t); e.Source == SourceCache {
		t.Errorf("OR must not be attributed from one branch: %+v", e)
	}
}

func TestChildLookupAsksTheParentWhenNotCached(t *testing.T) {
	h := fkHarness()
	fl := &fakeLookup{rows: map[string][]string{"public.submissions|id|5": {"acme"}}}
	h.a.lk = fl
	h.query(`SELECT * FROM submitters WHERE submission_id = 5`)
	h.waitFor(t, 1)
	if e := h.last(t); e.Source != SourceLookup || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("%+v", e)
	}
	if fl.calls.Load() != 1 {
		t.Errorf("calls = %d", fl.calls.Load())
	}
	// The parent row's owner is now cached, so a sibling lookup needs no query.
	h.query(`SELECT * FROM submitters WHERE submission_id = 5 AND status = 'x'`)
	if e := h.last(t); e.Source != SourceCache || fl.calls.Load() != 1 {
		t.Errorf("%+v calls=%d", e, fl.calls.Load())
	}
}

func TestUpdatingAUniqueColumnForgetsTheTable(t *testing.T) {
	h := fkHarness()
	h.a.Cache().Put("public.submissions", "id", "5", "acme")
	h.a.Cache().Put("public.submissions", "slug", "abc", "acme")
	h.query(`UPDATE submissions SET slug = 'new' WHERE id = 5`)
	h.reply(nil, nil, "UPDATE 1")
	if h.a.Cache().Len() != 0 {
		t.Error("a changed slug leaves a stale owner under the old value")
	}
	h.rfq('I')
	h.a.Cache().Put("public.submissions", "id", "5", "acme")
	h.query(`UPDATE submissions SET slug = slug WHERE id = 5`)
	h.reply(nil, nil, "UPDATE 1")
	h.rfq('I')
	h.a.Cache().Put("public.notes", "id", "1", "acme")
	h.query(`UPDATE notes SET body = 'x' WHERE id = 1`)
	h.reply(nil, nil, "UPDATE 1")
	if _, ok := h.a.Cache().Get("public.notes", "id", "1"); !ok {
		t.Error("changing an ordinary column must not forget the table")
	}
}

func TestJoinConditionsAreNotLookups(t *testing.T) {
	h := fkHarness()
	h.a.Cache().Put("public.submissions", "id", "5", "acme")
	h.query(`SELECT * FROM submitters s JOIN submissions u ON u.id = s.submission_id`)
	if e := h.last(t); e.Source == SourceCache {
		t.Errorf("comparing two columns names no row: %+v", e)
	}
}

// Needs a live Postgres: TENANTDB_TEST_DSN=postgres://user:pw@host:5432/db
func TestForeignKeysAreInferredAndVerifiedAgainstData(t *testing.T) {
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
	cleanup := `DROP TABLE IF EXISTS fkt_children, fkt_bad_children, fkt_lonely, fkt_declared, fkt_parents CASCADE`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE fkt_parents (id bigint PRIMARY KEY, tenant_id text)`,
		`CREATE TABLE fkt_children (id bigint PRIMARY KEY, tenant_id text, fkt_parent_id bigint)`,     // inferred, consistent
		`CREATE TABLE fkt_bad_children (id bigint PRIMARY KEY, tenant_id text, fkt_parent_id bigint)`, // inferred, but tenants disagree
		`CREATE TABLE fkt_lonely (id bigint PRIMARY KEY, tenant_id text, nothing_id bigint)`,          // no table to point at
		`CREATE TABLE fkt_declared (id bigint PRIMARY KEY, tenant_id text, owner bigint REFERENCES fkt_parents(id))`,
		`INSERT INTO fkt_parents VALUES (1, 'acme'), (2, 'globex')`,
		`INSERT INTO fkt_children VALUES (1, 'acme', 1), (2, 'globex', 2), (3, 'acme', 1)`,
		`INSERT INTO fkt_bad_children VALUES (1, 'acme', 1), (2, 'acme', 2)`,
		`INSERT INTO fkt_declared VALUES (1, 'acme', 1)`,
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
	child, _ := cat.Table(TableRef{Name: "fkt_children"}, []string{"public"}, "")
	bad, _ := cat.Table(TableRef{Name: "fkt_bad_children"}, []string{"public"}, "")
	lonely, _ := cat.Table(TableRef{Name: "fkt_lonely"}, []string{"public"}, "")
	declared, _ := cat.Table(TableRef{Name: "fkt_declared"}, []string{"public"}, "")

	if ref, ok := cat.FK(child, "fkt_parent_id"); !ok || ref.ParentKey != "public.fkt_parents" || ref.ParentCol != "id" {
		t.Errorf("a consistent <thing>_id column should be inferred: %+v %v", ref, ok)
	}
	if _, ok := cat.FK(bad, "fkt_parent_id"); ok {
		t.Error("a guess whose rows disagree with the parent's tenant must be rejected")
	}
	if _, ok := cat.FK(lonely, "nothing_id"); ok {
		t.Error("nothing to point at")
	}
	if ref, ok := cat.FK(declared, "owner"); !ok || ref.ParentKey != "public.fkt_parents" {
		t.Errorf("a declared foreign key is used too: %+v %v", ref, ok)
	}
	if _, ok := cat.FK(child, "id"); ok {
		t.Error("a primary key is not a foreign key")
	}
}

func TestPredicatesInJoinsAndAliasesNameTheirTable(t *testing.T) {
	h := fkHarness()
	h.a.Cache().Put("public.submissions", "id", "5", "acme")
	h.a.Cache().Put("public.submissions", "slug", "abc", "globex")

	for sql, want := range map[string][]string{
		// the shape ActiveRecord emits for belongs_to / has_many through
		`SELECT s.* FROM submitters s INNER JOIN submissions u ON u.id = s.submission_id WHERE u.id = 5`:      {"acme"},
		`SELECT s.* FROM submitters s JOIN submissions u ON u.id = s.submission_id WHERE s.submission_id = 5`: {"acme"},
		`SELECT * FROM submissions AS sub WHERE sub.slug = 'abc'`:                                             {"globex"},
		`SELECT * FROM public.submissions WHERE submissions.id = 5`:                                           {"acme"},
		`UPDATE submissions s SET slug = slug WHERE s.id = 5`:                                                 {"acme"},
	} {
		h.query(sql)
		if e := h.last(t); e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, want) {
			t.Errorf("%s: source=%s tenants=%v, want cache %v", sql, e.Source, e.Tenants, want)
		}
		h.rfq('I')
	}
	// An unqualified column in a join could belong to either table.
	h.query(`SELECT s.* FROM submitters s JOIN submissions u ON u.id = s.submission_id WHERE id = 5`)
	if e := h.last(t); e.Source != SourceUnknown || e.Scan {
		t.Errorf("ambiguous column: %+v", e)
	}
	// A qualifier that is not a table of the statement (a subquery's alias) names nothing.
	h.query(`SELECT * FROM submitters s JOIN (SELECT 5 AS id) x ON true WHERE x.id = 5`)
	if e := h.last(t); e.Source == SourceCache {
		t.Errorf("subquery alias: %+v", e)
	}
}

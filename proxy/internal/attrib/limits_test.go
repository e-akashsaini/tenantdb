package attrib

import (
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func limitsSpec() catSpec {
	var rows []colRow
	add := func(r []colRow) { rows = append(rows, r...) }
	add(tbl("public", "orders", 'r', "id", "tenant_id", "amount"))
	add(tbl("public", "countries", 'r', "code", "name"))
	add(tbl("public", "events", 'r', "id", "tenant_id", "kind"))
	add(tbl("public", "secure", 'r', "id", "tenant_id"))
	add(tbl("t1", "docs", 'r', "id", "tenant_id", "body"))
	add(tbl("t2", "docs", 'r', "id", "title"))
	fn := func(name, lang, src string, args ...string) *FuncDef {
		return &FuncDef{Schema: "public", Name: name, Lang: lang, Kind: 'f', Src: src, ArgNames: args, NArgs: len(args)}
	}
	proc := fn("archive", "plpgsql", `BEGIN DELETE FROM orders WHERE tenant_id = p_tenant; END`, "p_tenant")
	proc.Kind = 'p'
	return catSpec{
		rows: rows,
		unique: map[string][]string{
			"public.orders": {"id"}, "public.countries": {"code"}, "public.secure": {"id"},
			"t1.docs": {"id"}, "t2.docs": {"id"},
			// events.id is deliberately not unique
		},
		rls: []string{"public.secure"},
		funcs: []*FuncDef{
			fn("orders_of", "sql", `SELECT * FROM orders WHERE tenant_id = p_tenant`, "p_tenant"),
			fn("all_orders", "sql", `SELECT * FROM orders`),
			fn("order_by_id", "sql", `SELECT * FROM orders WHERE id = p_id`, "p_id"),
			fn("list_countries", "sql", `SELECT * FROM countries`),
			fn("fixed_tenant", "sql", `SELECT * FROM orders WHERE tenant_id = 'acme'`),
			fn("outer_fn", "sql", `SELECT * FROM orders_of(p_tenant)`, "p_tenant"),
			fn("two_tenants", "sql", `SELECT * FROM orders WHERE tenant_id = a UNION ALL SELECT * FROM orders WHERE tenant_id = b`, "a", "b"),
			fn("dyn", "plpgsql", `DECLARE q text := 'select 1'; BEGIN EXECUTE q; END`),
			fn("py", "plpython3u", `return 1`),
			proc,
		},
	}
}

func TestFunctionCallsAreAnalysedByTheirBodies(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	cases := []struct {
		sql     string
		source  Source
		tenants []string
		scan    bool
	}{
		{`SELECT * FROM orders_of('acme')`, SourceExplicit, []string{"acme"}, false},
		{`SELECT orders_of('acme')`, SourceExplicit, []string{"acme"}, false},
		{`SELECT * FROM public.orders_of('globex')`, SourceExplicit, []string{"globex"}, false},
		{`SELECT * FROM outer_fn('initech')`, SourceExplicit, []string{"initech"}, false}, // argument passed through a nested call
		{`SELECT * FROM two_tenants('a', 'b')`, SourceExplicit, []string{"a", "b"}, false},
		{`SELECT * FROM fixed_tenant()`, SourceExplicit, []string{"acme"}, false}, // the body hardcodes the tenant
		{`SELECT * FROM list_countries()`, SourceUntenanted, nil, false},
		{`SELECT * FROM all_orders()`, SourceScan, nil, true}, // the body reads every tenant
		{`SELECT * FROM order_by_id(5)`, SourceUnknown, nil, false},
		{`SELECT dyn()`, SourceUnknown, nil, false},
		{`SELECT py()`, SourceUnknown, nil, false},
		{`CALL archive('acme')`, SourceExplicit, []string{"acme"}, false},
		{`CALL archive(NULL)`, SourceUnknown, nil, false},
	}
	for _, c := range cases {
		h.query(c.sql)
		e := h.last(t)
		if e.Source != c.source || e.Scan != c.scan || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s: source=%s scan=%v tenants=%v; want %s %v %v", c.sql, e.Source, e.Scan, e.Tenants, c.source, c.scan, c.tenants)
		}
	}
}

func TestFunctionArgumentsCanBeBoundParameters(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	h.front(&pgproto3.Parse{Name: "s", Query: `SELECT * FROM orders_of($1)`})
	h.front(&pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte("acme")}})
	h.front(&pgproto3.Execute{})
	if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("%+v", e)
	}
}

func TestUnresolvedFunctionOverridesWhatTheStatementClaims(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	// The statement names acme, but all_orders() reads every tenant: acme cannot vouch for it.
	h.query(`SELECT * FROM orders WHERE tenant_id = 'acme' AND id IN (SELECT id FROM all_orders())`)
	if e := h.last(t); e.Source == SourceExplicit || !e.Scan {
		t.Errorf("%+v", e)
	}
}

func TestFunctionsRedefinedByDDLAreReadAgain(t *testing.T) {
	sp := limitsSpec()
	h := specHarness(sp, StrictOptions{})
	h.query(`SELECT * FROM orders_of('acme')`)
	if e := h.last(t); e.Source != SourceExplicit {
		t.Fatalf("%+v", e)
	}
	// A new snapshot where orders_of now reads everything.
	sp2 := limitsSpec()
	for _, f := range sp2.funcs {
		if f.Name == "orders_of" {
			f.Src = `SELECT * FROM orders`
		}
	}
	h.a.cat.apply(sp2.snapshot())
	h.query(`SELECT * FROM orders_of('acme')`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("stale function analysis: %+v", e)
	}
}

func TestDoBlocks(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	h.query(`DO $$ BEGIN PERFORM 1 FROM orders WHERE tenant_id = 'acme'; END $$`)
	if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("filtered DO: %+v", e)
	}
	h.query(`DO $$ BEGIN PERFORM count(*) FROM orders; END $$`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("unfiltered DO: %+v", e)
	}
	h.query(`DO $$ BEGIN EXECUTE 'DELETE FROM orders'; END $$`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("a literal EXECUTE is read: %+v", e)
	}
	h.query(`DO $$ DECLARE t text := 'orders'; BEGIN EXECUTE 'DELETE FROM ' || t; END $$`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("computed dynamic DO: %+v", e)
	}
}

func TestTemporaryTables(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})

	// With a tenant column of its own, a temp table is filtered like any tenant table.
	h.query(`CREATE TEMP TABLE mine (id int, tenant_id text)`)
	h.reply(nil, nil, "CREATE TABLE")
	h.query(`SELECT * FROM mine WHERE tenant_id = 'acme'`)
	if e := h.last(t); e.Source != SourceExplicit {
		t.Errorf("explicit on temp: %+v", e)
	}
	h.query(`SELECT * FROM mine`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("scan of temp: %+v", e)
	}

	// Filled from one tenant's data: it inherits that tenant.
	h.rfq('I')
	h.query(`CREATE TEMP TABLE snap AS SELECT * FROM orders WHERE tenant_id = 'acme'`)
	if e := h.last(t); e.Source != SourceExplicit {
		t.Errorf("creating statement: %+v", e)
	}
	h.reply(nil, nil, "SELECT 3")
	h.query(`SELECT count(*) FROM snap`)
	if e := h.last(t); e.Source != SourceDerived || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("derived: %+v", e)
	}

	// Filled from every tenant's data: reading it cannot be attributed.
	h.rfq('I')
	h.query(`CREATE TEMP TABLE everything AS SELECT * FROM orders`)
	if e := h.last(t); !e.Scan {
		t.Errorf("a CREATE TABLE AS that reads every tenant is a scan: %+v", e)
	}
	h.reply(nil, nil, "SELECT 9")
	h.query(`SELECT * FROM everything`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("tainted: %+v", e)
	}

	// Filled from shared data: nothing to attribute.
	h.rfq('I')
	h.query(`SELECT * INTO TEMP lookup FROM countries`)
	h.reply(nil, nil, "SELECT 2")
	h.query(`SELECT * FROM lookup`)
	if e := h.last(t); e.Source != SourceUntenanted {
		t.Errorf("untenanted temp: %+v", e)
	}

	// Dropping forgets it.
	h.rfq('I')
	h.query(`DROP TABLE snap`)
	h.reply(nil, nil, "DROP TABLE")
	h.query(`SELECT count(*) FROM snap`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("dropped temp is just an unknown table: %+v", e)
	}
}

func TestTempTableShadowsARealTableAndIsNeverCached(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	h.query(`CREATE TEMP TABLE orders (x int)`)
	h.reply(nil, nil, "CREATE TABLE")
	h.query(`SELECT * FROM orders`)
	if e := h.last(t); e.Source != SourceUntenanted {
		t.Errorf("the temp table hides public.orders: %+v", e)
	}
	h.query(`CREATE TEMP TABLE keyed (id int, tenant_id text)`)
	h.reply(nil, nil, "CREATE TABLE")
	h.query(`INSERT INTO keyed (id, tenant_id) VALUES (1, 'acme')`)
	h.reply(nil, nil, "INSERT 0 1")
	if h.a.Cache().Len() != 0 {
		t.Error("a temporary table is private to its session and must not enter the shared cache")
	}
}

func TestCreateTableAsOfTenantDataIsAttributed(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	h.query(`CREATE TABLE copy AS SELECT * FROM orders`)
	if e := h.last(t); e.Source != SourceScan || !e.Scan {
		t.Errorf("%+v", e)
	}
}

func TestRoleSearchPathDefaults(t *testing.T) {
	sp := limitsSpec()
	sp.roleSearch = map[[2]string][]string{{"app", "d"}: {"t2"}, {"", ""}: {"t1"}}
	h := specHarness(sp, StrictOptions{})
	h.query(`SELECT * FROM docs`) // ALTER ROLE app IN DATABASE d SET search_path = t2: shared docs
	if e := h.last(t); e.Source != SourceUntenanted {
		t.Errorf("role+db default: %+v", e)
	}
	h2 := specHarness(sp, StrictOptions{})
	h2.s.User = "someone"
	h2.query(`SELECT * FROM docs`) // falls through to the server-wide setting: t1.docs is a tenant table
	if e := h2.last(t); e.Source != SourceScan {
		t.Errorf("global default: %+v", e)
	}
	h2.query(`SET search_path = t2`)
	h2.query(`SELECT * FROM docs`)
	if e := h2.last(t); e.Source != SourceUntenanted {
		t.Errorf("an explicit SET still wins: %+v", e)
	}
}

func TestDefaultSearchPathPrecedence(t *testing.T) {
	sp := limitsSpec()
	sp.baseSearch = []string{"base"}
	sp.roleSearch = map[[2]string][]string{{"r", "d"}: {"role_db"}, {"r", ""}: {"role"}, {"", "d"}: {"db"}, {"", ""}: {"all"}}
	c := sp.build()
	for _, tc := range []struct {
		user, db string
		want     string
	}{{"r", "d", "role_db"}, {"r", "x", "role"}, {"u", "d", "db"}, {"u", "x", "all"}} {
		if got := c.DefaultSearchPath(tc.user, tc.db); !reflect.DeepEqual(got, []string{tc.want}) {
			t.Errorf("%s/%s = %v, want %s", tc.user, tc.db, got, tc.want)
		}
	}
	sp.roleSearch = nil
	if got := sp.build().DefaultSearchPath("u", "x"); !reflect.DeepEqual(got, []string{"base"}) {
		t.Errorf("server default = %v", got)
	}
}

func TestOnlyProvablyUniqueColumnsIdentifyRows(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	// events.id has no unique index: one value can belong to several tenants.
	h.query(`INSERT INTO events (id, tenant_id) VALUES (1, 'acme')`)
	h.reply(nil, nil, "INSERT 0 1")
	if h.a.Cache().Len() != 0 {
		t.Error("learned an owner for a column that is not unique")
	}
	h.query(`SELECT id, tenant_id FROM events WHERE kind = 'x'`)
	h.reply(cols(77, "id", "tenant_id"), [][]string{{"1", "acme"}, {"1", "globex"}}, "SELECT 2")
	if h.a.Cache().Len() != 0 {
		t.Error("learned owners from result rows of a non-unique column")
	}
	h.a.Cache().Put("public.events", "id", "1", "acme") // even if something put it there
	h.query(`SELECT * FROM events WHERE id = 1`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("must not trust a non-unique key: %+v", e)
	}
	// orders.id is a primary key: fine.
	h.rfq('I')
	h.query(`INSERT INTO orders (id, tenant_id) VALUES (1, 'acme')`)
	h.reply(nil, nil, "INSERT 0 1")
	h.query(`SELECT * FROM orders WHERE id = 1`)
	if e := h.last(t); e.Source != SourceCache {
		t.Errorf("unique key: %+v", e)
	}
}

func TestRowLevelSecurityBlindsLookups(t *testing.T) {
	sp := limitsSpec()
	sp.subjectRLS = true
	h := specHarness(sp, StrictOptions{})
	fl := &fakeLookup{rows: map[string][]string{"public.secure|id|1": {"acme"}}}
	h.a.lk = fl
	h.query(`SELECT * FROM secure WHERE id = 1`)
	if e := h.last(t); e.Source != SourceUnknown || fl.calls.Load() != 0 {
		t.Errorf("a lookup through row-level security would silently miss rows: %+v (calls %d)", e, fl.calls.Load())
	}
	if got := h.a.cat.BlindTables(); !reflect.DeepEqual(got, []string{"public.secure"}) {
		t.Errorf("blind tables = %v", got)
	}
	// A role that bypasses RLS sees every row, so the lookup is trustworthy.
	sp.subjectRLS = false
	h2 := specHarness(sp, StrictOptions{})
	h2.a.lk = &fakeLookup{rows: map[string][]string{"public.secure|id|1": {"acme"}}}
	h2.query(`SELECT * FROM secure WHERE id = 1`)
	h2.waitFor(t, 1)
	if e := h2.last(t); e.Source != SourceLookup {
		t.Errorf("%+v", e)
	}
	if got := h2.a.cat.BlindTables(); got != nil {
		t.Errorf("blind tables = %v", got)
	}
}

func TestCacheEntriesExpire(t *testing.T) {
	now := time.Unix(1000, 0)
	c := NewRowCacheTTL(10, time.Minute)
	c.now = func() time.Time { return now }
	c.Put("t", "id", "1", "acme")
	now = now.Add(59 * time.Second)
	if v, ok := c.Get("t", "id", "1"); !ok || v != "acme" {
		t.Fatal("entry should still be trusted")
	}
	now = now.Add(2 * time.Second)
	if _, ok := c.Get("t", "id", "1"); ok {
		t.Error("entry outlived its TTL")
	}
	if c.Len() != 0 {
		t.Error("expired entry was not removed")
	}
	c.Put("t", "id", "2", "acme")
	now = now.Add(50 * time.Second)
	c.Put("t", "id", "2", "acme") // rewriting refreshes it
	now = now.Add(50 * time.Second)
	if _, ok := c.Get("t", "id", "2"); !ok {
		t.Error("a refreshed entry should survive")
	}
}

func TestTruncateAndDropForgetTheTable(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	h.a.Cache().Put("public.orders", "id", "1", "acme")
	h.a.Cache().Put("public.orders", "id", "2", "acme")
	h.a.Cache().Put("t1.docs", "id", "9", "acme")
	h.query(`TRUNCATE orders`)
	if h.a.Cache().Len() != 3 {
		t.Fatal("must wait for the statement to succeed")
	}
	h.reply(nil, nil, "TRUNCATE TABLE")
	if _, ok := h.a.Cache().Get("public.orders", "id", "1"); ok {
		t.Error("TRUNCATE left stale owners behind")
	}
	if _, ok := h.a.Cache().Get("t1.docs", "id", "9"); !ok {
		t.Error("another table was dropped")
	}
	h.query(`DROP TABLE t1.docs`)
	h.reply(nil, nil, "DROP TABLE")
	if h.a.Cache().Len() != 0 {
		t.Error("DROP TABLE left stale owners behind")
	}
}

func TestSQLLevelPrepareAndExecute(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	h.query(`PREPARE q (text) AS SELECT * FROM orders WHERE tenant_id = $1`)
	if len(h.events) != 0 {
		t.Fatal("PREPARE runs nothing")
	}
	h.reply(nil, nil, "PREPARE")
	h.query(`EXECUTE q('acme')`)
	e := h.last(t)
	if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) || e.SQL != `EXECUTE q('acme')` {
		t.Errorf("%+v", e)
	}
	h.reply(nil, nil, "SELECT 1")
	h.query(`PREPARE s AS SELECT * FROM orders`)
	h.reply(nil, nil, "PREPARE")
	h.query(`EXECUTE s`)
	if e := h.last(t); e.Source != SourceScan {
		t.Errorf("an unfiltered prepared statement is still a scan: %+v", e)
	}
	h.reply(nil, nil, "SELECT 1")
	before := len(h.events)
	h.query(`DEALLOCATE q`)
	h.reply(nil, nil, "DEALLOCATE")
	h.query(`EXECUTE q('acme')`) // unknown to the proxy: the server will reject it
	if len(h.events) != before {
		t.Error("executing an unknown statement name has nothing to attribute")
	}
}

func TestStatementsThatRunAQueryAreAttributed(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{})
	for sql, want := range map[string]Source{
		`EXPLAIN ANALYZE SELECT * FROM orders`:                        SourceScan,
		`EXPLAIN (ANALYZE) SELECT * FROM orders WHERE id = 5`:         SourceUnknown,
		`EXPLAIN ANALYZE SELECT * FROM orders WHERE tenant_id = 'a'`:  SourceExplicit,
		`DECLARE c CURSOR FOR SELECT * FROM orders`:                   SourceScan,
		`COPY orders TO STDOUT`:                                       SourceScan,
		`COPY (SELECT * FROM orders WHERE tenant_id = 'a') TO STDOUT`: SourceExplicit,
		`COPY countries TO STDOUT`:                                    SourceUntenanted,
	} {
		h.query(sql)
		if e := h.last(t); e.Source != want {
			t.Errorf("%s: source = %s, want %s", sql, e.Source, want)
		}
	}
	// a load from the client is attributed once its rows have been read
	held := len(h.events)
	h.query(`COPY orders FROM STDIN`)
	if len(h.events) != held {
		t.Error("COPY FROM STDIN waits for its rows")
	}
	h.front(&pgproto3.CopyFail{Message: "stop"})
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("a load that never named its tenants: %+v", e)
	}
	n := len(h.events)
	h.query(`EXPLAIN SELECT * FROM orders`)
	if len(h.events) != n {
		t.Error("a plain EXPLAIN executes nothing")
	}
}

func TestFastPathFunctionCallIsUnknownAndBlockable(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{Mode: StrictBlock})
	if rej := h.gate(&pgproto3.FunctionCall{Function: 1234}); rej == nil {
		t.Error("a fast-path call names no SQL and must be refused in block mode")
	}
	if e := h.last(t); e.Source != SourceUnknown || !e.Blocked {
		t.Errorf("%+v", e)
	}
	ex := specHarness(limitsSpec(), StrictOptions{Mode: StrictBlock, Exempt: []string{"app"}})
	if rej := ex.gate(&pgproto3.FunctionCall{Function: 1234}); rej != nil {
		t.Errorf("exempt role refused: %+v", rej)
	}
}

func TestCopyFromIsBlockedUnlessExempt(t *testing.T) {
	h := specHarness(limitsSpec(), StrictOptions{Mode: StrictBlock})
	// The rows decide: the statement goes through and the load is aborted at the
	// end of the stream if some row names no tenant.
	if rej := h.gateQuery(`COPY orders (id, tenant_id) FROM STDIN`); rej != nil {
		t.Errorf("the verdict waits for the rows: %+v", rej)
	}
	h.gate(&pgproto3.CopyData{Data: []byte("1\t\\N\n")})
	if rej := h.gate(&pgproto3.CopyDone{}); rej == nil || !rej.AbortCopy {
		t.Error("bulk-loading rows of unknown tenants must be refused")
	}
	if rej := h.gateQuery(`COPY orders FROM STDIN WITH (FORMAT binary)`); rej == nil {
		t.Error("a stream that cannot be read must be refused up front")
	}
	if rej := h.gateQuery(`COPY orders TO STDOUT`); rej == nil {
		t.Error("an unfiltered export spans every tenant")
	}
	if rej := h.gateQuery(`COPY (SELECT * FROM orders WHERE tenant_id = 'a') TO STDOUT`); rej != nil {
		t.Errorf("a filtered export is fine: %+v", rej)
	}
}

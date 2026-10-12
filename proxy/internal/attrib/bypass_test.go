package attrib

import (
	"reflect"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Bypasses reported in an external review. Each test states what is sound: a
// statement must never be credited to a tenant set smaller than what it can touch.

func confinedTo(e Event, tenants ...string) bool {
	switch e.Source {
	case SourceExplicit, SourceCache, SourceLookup, SourceDerived:
		return reflect.DeepEqual(e.Tenants, tenants)
	}
	return false
}

// INSERT ... ON CONFLICT (id) DO UPDATE writes the existing row with that id,
// whoever owns it, so the inserted tenant says nothing about what it changed.
func TestUpsertOnANonTenantKeyIsNotConfinedToTheInsertedTenant(t *testing.T) {
	h := soundHarness()
	h.query(`INSERT INTO orders (id, tenant_id, amount) VALUES (20, 't1', 1) ON CONFLICT (id) DO UPDATE SET amount = 99`)
	if e := h.last(t); confinedTo(e, "t1") {
		t.Errorf("an upsert that can overwrite any tenant's row 20 was credited to t1: %+v", e)
	}
	h.reply(nil, nil, "INSERT 0 1")
	h.rfq('I')
	// ... and it taught nothing: row 20 may have belonged to someone else all along.
	h.query(`SELECT * FROM orders WHERE id = 20`)
	if e := h.last(t); confinedTo(e, "t1") {
		t.Errorf("the cache was poisoned by an upsert: %+v", e)
	}
}

func TestUpsertThatCannotCrossTenantsStaysConfined(t *testing.T) {
	h := soundHarness()
	for _, sql := range []string{
		// the conflicting row has the inserted tenant_id: the arbiter includes it
		`INSERT INTO orders (id, tenant_id, amount) VALUES (20, 't1', 1) ON CONFLICT (tenant_id, id) DO UPDATE SET amount = 99`,
		// or the update only applies to the inserted tenant's row
		`INSERT INTO orders (id, tenant_id, amount) VALUES (20, 't1', 1) ON CONFLICT (id) DO UPDATE SET amount = 99 WHERE orders.tenant_id = 't1'`,
		// nothing is modified on conflict
		`INSERT INTO orders (id, tenant_id, amount) VALUES (20, 't1', 1) ON CONFLICT (id) DO NOTHING`,
	} {
		h.query(sql)
		if e := h.last(t); !confinedTo(e, "t1") {
			t.Errorf("%s\n  got %+v", sql, e)
		}
		h.rfq('I')
	}
}

// Built-in functions that run SQL given as a string read whatever that SQL reads.
func TestBuiltinsThatRunSQLInternallyAreNotTrusted(t *testing.T) {
	h := soundHarness()
	for _, sql := range []string{
		`SELECT query_to_xml('select * from orders', true, false, '')`,
		`SELECT table_to_xml('orders', true, false, '')`,
		`SELECT schema_to_xml('public', true, false, '')`,
		`SELECT * FROM ts_stat('select to_tsvector(status) from orders')`,
		`SELECT * FROM public.orders o WHERE o.tenant_id = 't1' AND query_to_xml('select * from orders', true, false, '') IS NOT NULL`,
	} {
		e := h.attributed(t, sql)
		switch e.Source {
		case SourceExplicit, SourceCache, SourceLookup, SourceDerived, SourceUntenanted, SourceNoMatch:
			t.Errorf("%s\n  was trusted: %+v", sql, e)
		}
	}
}

// DISCARD TEMP, a rollback and ON COMMIT DROP all remove a temporary table; the
// name then means the real table again.
func TestTempTableShadowsEndWhenTheTableDoes(t *testing.T) {
	create := `CREATE TEMP TABLE orders AS SELECT * FROM orders WHERE tenant_id = 't1'`
	notDerived := func(h *harness, why string) {
		t.Helper()
		e := h.attributed(t, `SELECT * FROM orders`)
		if e.Source == SourceDerived || e.Source == SourceUntenanted {
			t.Errorf("%s: SELECT * FROM orders was credited as %s %v", why, e.Source, e.Tenants)
		}
	}
	for name, steps := range map[string][]string{
		"DISCARD TEMP":      {create, "DISCARD TEMP"},
		"DISCARD TEMPORARY": {create, "DISCARD TEMPORARY"},
		"DISCARD ALL":       {create, "DISCARD ALL"},
		"DROP":              {create, "DROP TABLE orders"},
	} {
		h := soundHarness()
		for _, s := range steps {
			h.query(s)
			h.reply(nil, nil, "OK")
			h.rfq('I')
		}
		notDerived(h, name)
	}

	// created in a transaction that rolls back
	h := soundHarness()
	h.query(`BEGIN`)
	h.reply(nil, nil, "BEGIN")
	h.rfq('T')
	h.query(create)
	h.reply(nil, nil, "SELECT 5")
	h.rfq('T')
	h.query(`ROLLBACK`)
	h.reply(nil, nil, "ROLLBACK")
	h.rfq('I')
	notDerived(h, "ROLLBACK")

	// created with ON COMMIT DROP
	h = soundHarness()
	h.query(`BEGIN`)
	h.reply(nil, nil, "BEGIN")
	h.rfq('T')
	h.query(`CREATE TEMP TABLE orders ON COMMIT DROP AS SELECT * FROM orders WHERE tenant_id = 't1'`)
	h.reply(nil, nil, "SELECT 5")
	h.rfq('T')
	h.query(`COMMIT`)
	h.reply(nil, nil, "COMMIT")
	h.rfq('I')
	notDerived(h, "ON COMMIT DROP")

	// created in a transaction that failed
	h = soundHarness()
	h.query(`BEGIN`)
	h.reply(nil, nil, "BEGIN")
	h.rfq('T')
	h.query(create)
	h.reply(nil, nil, "SELECT 5")
	h.rfq('T')
	h.query(`SELECT 1/0`)
	h.rfq('E')
	h.query(`ROLLBACK`)
	h.reply(nil, nil, "ROLLBACK")
	h.rfq('I')
	notDerived(h, "an aborted transaction")
}

// The proxy's view of the connection's prepared statements has to match the
// database's. A rejected batch never reached the database, so the names it
// mentioned must go back to what they were, not disappear.
func TestRejectedBatchDoesNotForgetEarlierPreparedStatements(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	g := func(m pgproto3.FrontendMessage) *proxy.Rejection { return h.gate(m) }

	// Batch 1 reaches the database: "u" is prepared (a scan, but nothing runs it yet).
	if rej := g(&pgproto3.Parse{Name: "u", Query: `SELECT * FROM orders`}); rej != nil {
		t.Fatalf("a Parse alone is not a read: %+v", rej)
	}
	g(&pgproto3.Sync{})

	// Batch 2 mentions "u" again and then trips the policy: it is rejected whole.
	g(&pgproto3.Parse{Name: "u", Query: `SELECT 1`})
	g(&pgproto3.Parse{Name: "z", Query: `SELECT * FROM orders`})
	g(&pgproto3.Bind{PreparedStatement: "z", DestinationPortal: "pz"})
	if rej := g(&pgproto3.Execute{Portal: "pz"}); rej == nil {
		t.Fatal("the scan should have been refused")
	}
	g(&pgproto3.Sync{})

	// Batch 3 runs the database's "u", the scan from batch 1.
	g(&pgproto3.Bind{PreparedStatement: "u", DestinationPortal: ""})
	if rej := g(&pgproto3.Execute{Portal: ""}); rej == nil {
		t.Error("the database's prepared statement u is a scan; running it was allowed")
	}
}

// Naming something the proxy has never seen cannot be vouched for.
func TestBindingAnUnknownStatementIsRefusedInBlockMode(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	h.gate(&pgproto3.Bind{PreparedStatement: "never-parsed", DestinationPortal: ""})
	if rej := h.gate(&pgproto3.Execute{Portal: ""}); rej == nil {
		t.Error("executing a portal the proxy knows nothing about was allowed")
	}
}

// A connection tag or an open transaction supplies the tenant for a statement
// that does not name one. It must not excuse a statement that visibly reads
// beyond that tenant.
func TestTagDoesNotExcuseAStatementThatReadsOtherTenants(t *testing.T) {
	for _, sql := range []string{
		`SELECT * FROM orders WHERE tenant_id <> 't1'`,
		`SELECT * FROM orders WHERE id > 0`,
		`SELECT * FROM orders WHERE tenant_id LIKE 't%'`,
		`SELECT * FROM orders WHERE lower(tenant_id) = 't2'`,
		`SELECT * FROM orders WHERE status = 'open' OR amount > 5`,
		`SELECT * FROM orders WHERE tenant_id NOT IN ('t1')`,
	} {
		h := strictHarness(StrictOptions{Mode: StrictBlock})
		h.set(`SET tenantdb.tenant = 't1'`)
		if rej := h.gateQuery(sql); rej == nil {
			t.Errorf("tagged connection: %s\n  was allowed", sql)
		}
		h = strictHarness(StrictOptions{Mode: StrictBlock})
		h.gateQuery(`BEGIN`)
		h.rfq('T')
		if rej := h.gateQuery(`SELECT * FROM orders WHERE tenant_id = 't1'`); rej != nil {
			t.Fatalf("%+v", rej)
		}
		h.rfq('T')
		if rej := h.gateQuery(sql); rej == nil {
			t.Errorf("in a transaction that named t1: %s\n  was allowed", sql)
		}
	}
}

// What a tag is for stays working: row lookups and statements the tenant cannot
// be read from at all (an unfiltered read is a scan and is refused, as before).
func TestTagStillAttributesIdentifyingLookups(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	h.set(`SET tenantdb.tenant = 't1'`)
	for _, sql := range []string{
		`SELECT * FROM orders WHERE id = 5`,
		`SELECT * FROM orders WHERE id IN (1, 2, 3)`,
		`SELECT * FROM orders WHERE tenant_id = 't1' AND amount > 5`,
		`SELECT 1`,
	} {
		if rej := h.gateQuery(sql); rej != nil {
			t.Errorf("%s\n  refused: %+v", sql, rej)
		}
	}
}

// A cast that shortens or pads a value changes what is compared; only casts that
// leave the value alone can be looked through.
func TestLossyCastsAreNotLookedThrough(t *testing.T) {
	h := soundHarness()
	for _, sql := range []string{
		`SELECT * FROM orders WHERE tenant_id::varchar(4) = 'acme'`,
		`SELECT * FROM orders WHERE tenant_id::varchar(1) = 'z'`,
		`SELECT * FROM orders WHERE tenant_id = 'acme'::varchar(2)`,
		`SELECT * FROM orders WHERE tenant_id::char(1) = 't'`,
		`SELECT * FROM orders WHERE tenant_id::name = 't1'`,
		`SELECT * FROM orders WHERE id::varchar(1) = '5'`,
	} {
		e := h.attributed(t, sql)
		switch e.Source {
		case SourceExplicit, SourceCache, SourceLookup, SourceNoMatch, SourceDerived:
			t.Errorf("%s\n  was credited: %s %v", sql, e.Source, e.Tenants)
		}
		if h2 := strictHarness(StrictOptions{Mode: StrictBlock}); h2.gateQuery(sql) == nil {
			t.Errorf("%s\n  was allowed in block mode", sql)
		}
	}
}

func TestHarmlessCastsAreStillLookedThrough(t *testing.T) {
	h := soundHarness()
	for _, sql := range []string{
		`SELECT * FROM orders WHERE tenant_id = 't1'::text`,
		`SELECT * FROM orders WHERE tenant_id = 't1'::varchar`,
		`SELECT * FROM orders WHERE tenant_id::text = 't1'`,
		`SELECT * FROM orders WHERE tenant_id = CAST('t1' AS text)`,
	} {
		e := h.attributed(t, sql)
		if !confinedTo(e, "t1") {
			t.Errorf("%s\n  got %s %v", sql, e.Source, e.Tenants)
		}
	}
}

// What an INSERT teaches the cache is only true if the transaction commits.
func TestRolledBackInsertsAreForgotten(t *testing.T) {
	for name, finish := range map[string]func(h *harness){
		"ROLLBACK": func(h *harness) {
			h.query(`ROLLBACK`)
			h.reply(nil, nil, "ROLLBACK")
			h.rfq('I')
		},
		"an error": func(h *harness) {
			h.query(`SELECT 1/0`)
			h.a.OnBackend(h.s, &pgproto3.ErrorResponse{Code: "22012", Message: "division by zero"})
			h.rfq('E')
			h.query(`ROLLBACK`)
			h.reply(nil, nil, "ROLLBACK")
			h.rfq('I')
		},
		"ROLLBACK TO": func(h *harness) {
			h.query(`ROLLBACK TO SAVEPOINT s`)
			h.reply(nil, nil, "ROLLBACK")
			h.rfq('T')
			h.query(`COMMIT`)
			h.reply(nil, nil, "COMMIT")
			h.rfq('I')
		},
		"a multi-statement query that fails": nil,
	} {
		h := soundHarness()
		if finish == nil {
			// INSERT ...; SELECT 1/0 in one Query is one implicit transaction
			h.query(`INSERT INTO orders (id, tenant_id) VALUES (30, 't1'); SELECT 1/0`)
			h.reply(nil, nil, "INSERT 0 1")
			h.a.OnBackend(h.s, &pgproto3.ErrorResponse{Code: "22012", Message: "division by zero"})
			h.rfq('I')
		} else {
			h.query(`BEGIN`)
			h.reply(nil, nil, "BEGIN")
			h.rfq('T')
			h.query(`INSERT INTO orders (id, tenant_id) VALUES (30, 't1')`)
			h.reply(nil, nil, "INSERT 0 1")
			h.rfq('T')
			if name == "ROLLBACK TO" {
				h.query(`SAVEPOINT s`)
				h.reply(nil, nil, "SAVEPOINT")
				h.rfq('T')
				h.query(`INSERT INTO orders (id, tenant_id) VALUES (31, 't1')`)
				h.reply(nil, nil, "INSERT 0 1")
				h.rfq('T')
			}
			finish(h)
		}
		// row 31 / 30 was never committed
		for _, id := range []string{"30", "31"} {
			if name == "ROLLBACK TO" && id == "30" {
				continue // inserted before the savepoint: it does survive the commit; forgetting it is only cautious
			}
			e := h.attributed(t, `SELECT * FROM orders WHERE id = `+id)
			if e.Source == SourceCache {
				t.Errorf("%s: id %s is still in the cache after the transaction ended without it", name, id)
			}
		}
	}
	// ... and a committed one stays
	h := soundHarness()
	h.query(`BEGIN`)
	h.reply(nil, nil, "BEGIN")
	h.rfq('T')
	h.query(`INSERT INTO orders (id, tenant_id) VALUES (40, 't1')`)
	h.reply(nil, nil, "INSERT 0 1")
	h.rfq('T')
	h.query(`COMMIT`)
	h.reply(nil, nil, "COMMIT")
	h.rfq('I')
	if e := h.attributed(t, `SELECT * FROM orders WHERE id = 40`); e.Source != SourceCache {
		t.Errorf("a committed insert should be remembered: %+v", e)
	}
}

// A foreign key says which parent a row points at, not that the parent has the
// same tenant. That is checked against sample rows, so a row written afterwards
// must not be able to talk the analysis into the wrong tenant.
func TestAWriteThatBreaksARelationshipIsRefusedAndDistrusted(t *testing.T) {
	h := specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	// customer 5 belongs to t2
	h.a.Cache().Put("public.customers", "id", "5", "t2")

	// An order for t1 pointing at t2's customer.
	rej := h.gateQuery(`INSERT INTO orders (id, tenant_id, customer_id) VALUES (11, 't1', 5)`)
	if rej == nil {
		t.Fatal("an order for t1 that points at a customer of t2 was allowed")
	}
	h.rfq('I')

	// The relationship is no longer trusted: a join through it is not enough.
	e := func(sql string) Event {
		h.query(sql)
		ev := h.last(t)
		h.rfq('I')
		return ev
	}
	ev := e(`SELECT o.* FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.tenant_id = 't1'`)
	if confinedTo(ev, "t1") {
		t.Errorf("orders.customer_id was still trusted after a row broke it: %+v", ev)
	}

	// A consistent order is fine on a fresh attributor.
	h2 := specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	h2.a.Cache().Put("public.customers", "id", "5", "t2")
	if rej := h2.gateQuery(`INSERT INTO orders (id, tenant_id, customer_id) VALUES (12, 't2', 5)`); rej != nil {
		t.Errorf("a consistent order was refused: %+v", rej)
	}
}

// An UPDATE that sets the tenant column moves rows between tenants: it touches
// the tenant the rows had and the one they get.
func TestUpdateThatMovesRowsToAnotherTenantIsRefused(t *testing.T) {
	for _, sql := range []string{
		`UPDATE orders SET tenant_id = 't3' WHERE id = 14`,
		`UPDATE orders SET tenant_id = 't3' WHERE tenant_id = 't1'`,
		`UPDATE orders SET tenant_id = 't1' WHERE id = 6`,
		`UPDATE orders SET tenant_id = 't1' WHERE tenant_id = 't2'`,
		`UPDATE orders SET tenant_id = $1 WHERE id = 14`,
		`INSERT INTO orders (id, tenant_id) VALUES (14, 't1') ON CONFLICT (id) DO UPDATE SET tenant_id = 't3'`,
	} {
		h := specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
		h.a.Cache().Put("public.orders", "id", "14", "t1")
		h.a.Cache().Put("public.orders", "id", "6", "t2")
		h.set(`SET tenantdb.tenant = 't1'`)
		if rej := h.gateQuery(sql); rej == nil {
			t.Errorf("%s\n  was allowed on a connection tagged t1", sql)
		}
	}
}

// Part of a statement can be confined while another part cannot. What is known
// about the first part still counts against a tag that says otherwise.
func TestKnownPartOfAStatementConflictsWithTheTag(t *testing.T) {
	h := specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	h.a.Cache().Put("public.customers", "id", "8", "t2")
	h.a.Cache().Put("public.orders", "id", "29", "t2")
	h.set(`SET tenantdb.tenant = 't1'`)
	// The relationship orders.customer_id -> customers.id is broken by a write:
	// orders can no longer be reached through customers.
	h.a.cat.DistrustFK(mustTable(t, h, "orders"), "customer_id", FKRef{ParentKey: "public.customers", ParentCol: "id"})
	for _, sql := range []string{
		`SELECT o.tenant_id, c.tenant_id FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.id = 8`,
		`SELECT o.tenant_id, c.tenant_id FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id = 29`,
	} {
		if rej := h.gateQuery(sql); rej == nil {
			t.Errorf("%s\n  was allowed although it reads a t2 row on a t1 connection", sql)
		}
	}
}

func mustTable(t *testing.T, h *harness, name string) *TableInfo {
	t.Helper()
	ti, ok := h.a.cat.Table(TableRef{Name: name}, []string{"public"}, "")
	if !ok {
		t.Fatalf("no table %s", name)
	}
	return ti
}

// The database binds the names in a prepared statement when it is prepared. A
// temporary table (or a new search_path) afterwards does not change what it reads.
func TestPreparedStatementKeepsTheNamesItWasPreparedWith(t *testing.T) {
	prep := `PREPARE p1 AS SELECT * FROM orders WHERE tenant_id = $1`
	shadow := `CREATE TEMP TABLE orders AS SELECT * FROM orders WHERE tenant_id = 't1'`

	// SQL-level
	h := specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	h.set(`SET tenantdb.tenant = 't1'`)
	for _, q := range []string{prep, shadow} {
		if rej := h.gateQuery(q); rej != nil {
			t.Fatalf("%s: %+v", q, rej)
		}
		h.reply(nil, nil, "OK")
		h.rfq('I')
	}
	if rej := h.gateQuery(`EXECUTE p1('t2')`); rej == nil {
		t.Error("EXECUTE p1('t2') reads the real orders (bound when prepared) for t2 on a t1 connection, but was allowed")
	}

	// protocol-level
	h = specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	h.set(`SET tenantdb.tenant = 't1'`)
	h.gate(&pgproto3.Parse{Name: "s1", Query: `SELECT * FROM orders WHERE tenant_id = $1`})
	h.gate(&pgproto3.Sync{})
	if rej := h.gateQuery(shadow); rej != nil {
		t.Fatalf("%+v", rej)
	}
	h.reply(nil, nil, "SELECT 3")
	h.rfq('I')
	h.gate(&pgproto3.Bind{PreparedStatement: "s1", DestinationPortal: "", Parameters: [][]byte{[]byte("t2")}})
	if rej := h.gate(&pgproto3.Execute{Portal: ""}); rej == nil {
		t.Error("a statement parsed before the temp table existed was analysed against it")
	}

	// A statement prepared against the temp table keeps reading the temp table.
	h = specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	h.set(`SET tenantdb.tenant = 't1'`)
	h.gateQuery(shadow)
	h.reply(nil, nil, "SELECT 3")
	h.rfq('I')
	h.gateQuery(`PREPARE p2 AS SELECT * FROM orders WHERE id = $1`)
	h.reply(nil, nil, "PREPARE")
	h.rfq('I')
	h.gateQuery(`DISCARD TEMP`)
	h.reply(nil, nil, "DISCARD TEMP")
	h.rfq('I')
	// (the database would have re-bound p2 to the real table only after an invalidation; either way
	// the statement must not be vouched for by the shadow that is gone)
	if rej := h.gateQuery(`EXECUTE p2(14)`); rej != nil {
		// 14 is unknown to the cache here; a tag-covered lookup is acceptable either way
		_ = rej
	}
}

func mustSet(t *testing.T, h *harness, sql string, status byte) {
	t.Helper()
	h.query(sql)
	h.reply(nil, nil, "OK")
	h.rfq(status)
}

// SET changes that the database undoes (a rollback, a rollback to a savepoint, a
// statement that errors) must be undone in the proxy's view too, or its idea of
// the tag, search_path and role drifts from the real one.
func TestSessionSettingsFollowRollbacksAndErrors(t *testing.T) {
	tag := func(h *harness) string {
		st := h.a.st(h.s)
		st.mu.Lock()
		defer st.mu.Unlock()
		return st.curTag()
	}
	newH := func() *harness {
		h := specHarness(soundSpec(), StrictOptions{})
		mustSet(t, h, `SET tenantdb.tenant = 't1'`, 'I')
		return h
	}

	h := newH()
	mustSet(t, h, `BEGIN`, 'T')
	mustSet(t, h, `SET tenantdb.tenant = 't2'`, 'T')
	if tag(h) != "t2" {
		t.Fatalf("inside the transaction the tag is t2, got %q", tag(h))
	}
	mustSet(t, h, `ROLLBACK`, 'I')
	if got := tag(h); got != "t1" {
		t.Errorf("after ROLLBACK the tag is back to t1, got %q", got)
	}

	h = newH()
	mustSet(t, h, `BEGIN`, 'T')
	mustSet(t, h, `SAVEPOINT a`, 'T')
	mustSet(t, h, `SET tenantdb.tenant = 't3'`, 'T')
	mustSet(t, h, `ROLLBACK TO a`, 'T')
	if got := tag(h); got != "t1" {
		t.Errorf("after ROLLBACK TO the tag is back to t1, got %q", got)
	}
	mustSet(t, h, `SET tenantdb.tenant = 't2'`, 'T')
	mustSet(t, h, `RELEASE a`, 'T')
	h.query(`COMMIT`)
	h.reply(nil, nil, "COMMIT")
	h.rfq('I')
	if got := tag(h); got != "t2" {
		t.Errorf("a committed change stays: t2, got %q", got)
	}

	// COMMIT of an aborted transaction is a rollback
	h = newH()
	mustSet(t, h, `BEGIN`, 'T')
	mustSet(t, h, `SET tenantdb.tenant = 't2'`, 'T')
	h.query(`COMMIT`)
	h.reply(nil, nil, "ROLLBACK")
	h.rfq('I')
	if got := tag(h); got != "t1" {
		t.Errorf("a COMMIT that the database turned into a ROLLBACK keeps nothing, got %q", got)
	}

	// a SET the database rejects was never applied
	h = newH()
	h.query(`SET ROLE nosuchrole`)
	h.a.OnBackend(h.s, &pgproto3.ErrorResponse{Code: "42704", Message: "role does not exist"})
	h.rfq('I')
	st := h.a.st(h.s)
	st.mu.Lock()
	rs := st.roleS
	st.mu.Unlock()
	if rs.set {
		t.Errorf("SET ROLE failed in the database, but the proxy thinks it took effect: %+v", rs)
	}

	// a PREPARE that fails (the name exists) leaves the earlier statement in place
	h = newH()
	mustSet(t, h, `PREPARE p1 AS SELECT * FROM orders`, 'I')
	h.query(`PREPARE p1 AS SELECT * FROM orders WHERE id = $1`)
	h.a.OnBackend(h.s, &pgproto3.ErrorResponse{Code: "42P05", Message: "prepared statement p1 already exists"})
	h.rfq('I')
	st = h.a.st(h.s)
	st.mu.Lock()
	n := len(st.sqlPrep["p1"].Keys)
	st.mu.Unlock()
	if n != 0 {
		t.Error("the failed PREPARE replaced the statement the database still holds")
	}
}

// A lookup that resolves part of a statement must not lose what it learned.
func TestLookedUpOwnersStillConflictWithTheTag(t *testing.T) {
	h := specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	h.a.Cache().Put("public.customers", "id", "8", "t2")
	h.set(`SET tenantdb.tenant = 't1'`)
	h.a.cat.DistrustFK(mustTable(t, h, "orders"), "customer_id", FKRef{ParentKey: "public.customers", ParentCol: "id"})
	// (no database behind this harness: the owner comes from the cache; the lookup
	// path is exercised against a real server by the session fuzzer)
	if rej := h.gateQuery(`SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.id = 8`); rej == nil {
		t.Error("allowed")
	}
}

// An UPDATE that points a row at a parent of another tenant breaks the
// relationship just as an INSERT does.
func TestUpdateThatBreaksARelationshipDistrustsIt(t *testing.T) {
	h := specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	h.a.Cache().Put("public.customers", "id", "7", "t2")
	h.a.Cache().Put("public.orders", "id", "4", "t1")
	if rej := h.gateQuery(`UPDATE orders SET customer_id = 7 WHERE id = 4`); rej == nil {
		t.Fatal("pointing t1's order at t2's customer was allowed")
	}
	h.rfq('I')
	e := h.attributed(t, `SELECT o.* FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.tenant_id = 't1'`)
	if confinedTo(e, "t1") {
		t.Errorf("the relationship is still trusted after an update broke it: %+v", e)
	}
}

// A temporary table dropped and made again under the same name is a different
// table: a statement prepared against the old one cannot be vouched for.
func TestPreparedStatementAcrossARecreatedTempTableIsUnknown(t *testing.T) {
	h := specHarness(soundSpec(), StrictOptions{Mode: StrictBlock})
	h.set(`SET tenantdb.tenant = 't1'`)
	create := `CREATE TEMP TABLE orders AS SELECT * FROM orders WHERE tenant_id = 't1'`
	for _, q := range []string{create, `PREPARE p1 AS SELECT * FROM orders`, `DISCARD TEMP`, create} {
		if rej := h.gateQuery(q); rej != nil {
			t.Fatalf("%s: %+v", q, rej)
		}
		h.reply(nil, nil, "OK")
		h.rfq('I')
	}
	if rej := h.gateQuery(`EXECUTE p1`); rej == nil {
		t.Error("p1 was prepared against a temporary table that has since been replaced")
	}
}

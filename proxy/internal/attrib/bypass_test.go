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

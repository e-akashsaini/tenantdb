package attrib

import (
	"reflect"
	"testing"
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

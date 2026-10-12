package attrib

import (
	"reflect"
	"testing"
)

// notes has no tenant column of its own, but each note points at an order: it
// belongs to the tenant of that order.
func derivedSpec() catSpec {
	sp := soundSpec()
	sp.rows = append(sp.rows, tbl("public", "notes", 'r', "id", "order_id", "body")...)
	sp.rows = append(sp.rows, tbl("public", "tags", 'r', "id", "label")...)            // really shared
	sp.rows = append(sp.rows, tbl("public", "taxes", 'r', "id", "note_id", "rate")...) // a child of a child
	sp.unique["public.notes"] = []string{"id"}
	sp.unique["public.tags"] = []string{"id"}
	sp.unique["public.taxes"] = []string{"id"}
	sp.derived = map[string][]DerivedRef{
		"public.notes": {{Col: "order_id", ParentKey: "public.orders", ParentCol: "id"}},
		"public.taxes": {{Col: "note_id", ParentKey: "public.notes", ParentCol: "id"}},
	}
	return sp
}

func derivedHarness(mode StrictMode) *harness {
	h := specHarness(derivedSpec(), StrictOptions{Mode: mode})
	h.a.Cache().Put("public.orders", "id", "5", "t1")
	h.a.Cache().Put("public.orders", "id", "6", "t2")
	return h
}

func TestChildTableTakesTheTenantOfItsParent(t *testing.T) {
	h := derivedHarness(StrictOff)
	for _, c := range []struct {
		name, sql string
		tenants   []string
	}{
		{"by the foreign key", `SELECT * FROM notes WHERE order_id = 5`, []string{"t1"}},
		{"by several", `SELECT * FROM notes WHERE order_id IN (5, 6)`, []string{"t1", "t2"}},
		{"alongside other conditions", `SELECT * FROM notes WHERE order_id = 5 AND body LIKE 'x%'`, []string{"t1"}},
		{"through a join to a confined parent", `SELECT n.* FROM notes n JOIN orders o ON o.id = n.order_id WHERE o.tenant_id = 't2'`, []string{"t2"}},
		{"through a subselect", `SELECT * FROM notes WHERE order_id IN (SELECT id FROM orders WHERE tenant_id = 't1')`, []string{"t1"}},
		{"an update", `UPDATE notes SET body = 'x' WHERE order_id = 6`, []string{"t2"}},
		{"a delete", `DELETE FROM notes WHERE order_id = 5`, []string{"t1"}},
	} {
		e := h.attributed(t, c.sql)
		if e.Source == SourceUntenanted || e.Source == SourceUnknown || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s:\n  %s\n  got %s %v, want %v", c.name, c.sql, e.Source, e.Tenants, c.tenants)
		}
	}
}

func TestChildTableReadWithoutAParentIsAScan(t *testing.T) {
	for _, sql := range []string{
		`SELECT * FROM notes`,
		`SELECT * FROM notes WHERE body = 'x'`,
		`SELECT * FROM notes WHERE id > 3`,
		`SELECT * FROM notes WHERE order_id > 3`,
		`SELECT * FROM notes WHERE order_id <> 5`,
		`UPDATE notes SET body = 'x'`,
		`DELETE FROM notes WHERE body = 'x'`,
		`SELECT * FROM notes WHERE order_id = 5 OR body = 'x'`,
		`SELECT * FROM notes WHERE order_id = 99`, // an order nobody has seen
	} {
		h := derivedHarness(StrictBlock)
		if rej := h.gateQuery(sql); rej == nil {
			t.Errorf("%s\n  was allowed", sql)
		}
	}
}

func TestChildTableOfAnotherTenantConflictsWithTheTag(t *testing.T) {
	h := derivedHarness(StrictBlock)
	h.set(`SET tenantdb.tenant = 't1'`)
	if rej := h.gateQuery(`SELECT * FROM notes WHERE order_id = 5`); rej != nil {
		t.Errorf("own tenant's notes refused: %+v", rej)
	}
	if rej := h.gateQuery(`SELECT * FROM notes WHERE order_id = 6`); rej == nil {
		t.Error("another tenant's notes were allowed")
	}
}

func TestChildTableInsertsAreAttributedThroughTheParent(t *testing.T) {
	h := derivedHarness(StrictBlock)
	if rej := h.gateQuery(`INSERT INTO notes (id, order_id, body) VALUES (1, 5, 'hi')`); rej != nil {
		t.Errorf("an insert for a known order was refused: %+v", rej)
	}
	h.rfq('I')
	e := h.last(t)
	if e.Source == SourceUnknown || !reflect.DeepEqual(e.Tenants, []string{"t1"}) {
		t.Errorf("insert: %+v", e)
	}
	// an order nobody has seen cannot be vouched for
	if rej := h.gateQuery(`INSERT INTO notes (id, order_id, body) VALUES (2, 99, 'hi')`); rej == nil {
		t.Error("an insert for an unknown order was allowed")
	}
	// ... nor several rows for different tenants as one
	h.set(`SET tenantdb.tenant = 't1'`)
	if rej := h.gateQuery(`INSERT INTO notes (id, order_id, body) VALUES (3, 5, 'a'), (4, 6, 'b')`); rej == nil {
		t.Error("rows for two tenants on a t1 connection were allowed")
	}
}

func TestMovingANoteToAnotherOrderMovesItToAnotherTenant(t *testing.T) {
	h := derivedHarness(StrictBlock)
	h.set(`SET tenantdb.tenant = 't1'`)
	for _, sql := range []string{
		`UPDATE notes SET order_id = 6 WHERE order_id = 5`,
		`UPDATE notes SET order_id = $1 WHERE order_id = 5`,
	} {
		if rej := h.gateQuery(sql); rej == nil {
			t.Errorf("%s\n  was allowed", sql)
		}
	}
}

func TestSharedTablesAreNotChildTables(t *testing.T) {
	h := derivedHarness(StrictBlock)
	// tags has no foreign key to a tenant table
	if rej := h.gateQuery(`SELECT * FROM tags`); rej != nil {
		t.Errorf("a shared table was refused: %+v", rej)
	}
	if rej := h.gateQuery(`SELECT * FROM countries`); rej != nil {
		t.Errorf("a shared table was refused: %+v", rej)
	}
}

// taxes belong to notes, which belong to orders: two steps from the tenant.
func TestGrandchildTableTakesTheTenantOfItsGrandparent(t *testing.T) {
	h := derivedHarness(StrictOff)
	for _, c := range []struct {
		name, sql string
		tenants   []string
	}{
		{"through two joins", `SELECT t.* FROM taxes t JOIN notes n ON n.id = t.note_id JOIN orders o ON o.id = n.order_id WHERE o.tenant_id = 't1'`, []string{"t1"}},
		{"through a join to the child", `SELECT t.* FROM taxes t JOIN notes n ON n.id = t.note_id WHERE n.order_id = 6`, []string{"t2"}},
		{"through a subselect", `SELECT * FROM taxes WHERE note_id IN (SELECT id FROM notes WHERE order_id = 5)`, []string{"t1"}},
		{"through nested subselects", `SELECT * FROM taxes WHERE note_id IN (SELECT id FROM notes WHERE order_id IN (SELECT id FROM orders WHERE tenant_id = 't2'))`, []string{"t2"}},
	} {
		e := h.attributed(t, c.sql)
		if e.Source == SourceUntenanted || e.Source == SourceUnknown || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s:\n  %s\n  got %s %v, want %v", c.name, c.sql, e.Source, e.Tenants, c.tenants)
		}
	}
	for _, sql := range []string{
		`SELECT * FROM taxes`,
		`SELECT * FROM taxes WHERE rate > 3`,
		`SELECT * FROM taxes WHERE note_id > 3`,
		`UPDATE taxes SET note_id = 4 WHERE id = 1`,
	} {
		hb := derivedHarness(StrictBlock)
		if rej := hb.gateQuery(sql); rej == nil {
			t.Errorf("%s\n  was allowed", sql)
		}
	}
}

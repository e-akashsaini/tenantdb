package attrib

import (
	"reflect"
	"testing"
)

// `x IN (SELECT y FROM ...)`: the subquery's confinement carries to the table x
// belongs to, but only where the two columns tie rows to the same tenant.
func TestInSubselectConfinesTheOuterTable(t *testing.T) {
	h := soundHarness()
	for _, c := range []struct {
		name, sql string
		tenants   []string
	}{
		{"through a foreign key", `SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tenant_id = 't1')`, []string{"t1"}},
		{"= ANY", `SELECT * FROM orders WHERE customer_id = ANY (SELECT id FROM customers WHERE tenant_id = 't1')`, []string{"t1"}},
		{"tenant column to tenant column", `SELECT * FROM orders WHERE tenant_id IN (SELECT tenant_id FROM customers WHERE tenant_id = 't2')`, []string{"t2"}},
		{"alongside other conditions", `SELECT * FROM orders WHERE status = 'open' AND customer_id IN (SELECT id FROM customers WHERE tenant_id IN ('t1', 't2'))`, []string{"t1", "t2"}},
		{"nested twice", `SELECT * FROM items WHERE order_id IN (SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tenant_id = 't3'))`, []string{"t3"}},
		{"a subquery over a join", `SELECT * FROM items WHERE order_id IN (SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.tenant_id = 't1')`, []string{"t1"}},
		{"which then confines a joined table", `SELECT i.* FROM orders o JOIN items i ON i.order_id = o.id WHERE o.customer_id IN (SELECT id FROM customers WHERE tenant_id = 't1')`, []string{"t1"}},
		{"correlated", `SELECT * FROM orders o WHERE o.customer_id IN (SELECT c.id FROM customers c WHERE c.tenant_id = 't1' AND c.id = o.customer_id)`, []string{"t1"}},
	} {
		e := h.attributed(t, c.sql)
		if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s:\n  %s\n  got %s %v, want %v", c.name, c.sql, e.Source, e.Tenants, c.tenants)
		}
	}
}

func TestInSubselectNeverClaimsMoreThanItProves(t *testing.T) {
	h := soundHarness()
	for _, c := range []struct {
		name, sql string
	}{
		{"the subquery itself is not confined", `SELECT * FROM orders WHERE id IN (SELECT id FROM orders WHERE amount > 5)`},
		{"NOT IN", `SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM customers WHERE tenant_id = 't1')`},
		{"an OR alternative", `SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tenant_id = 't1') OR status = 'x'`},
		{"columns that are not related", `SELECT * FROM orders WHERE amount IN (SELECT id FROM customers WHERE tenant_id = 't1')`},
		{"the wrong direction of a foreign key's meaning", `SELECT * FROM customers WHERE id IN (SELECT amount FROM orders WHERE tenant_id = 't1')`},
		{"only the nullable side of an outer join is confined", `SELECT c.* FROM customers c LEFT JOIN orders o ON o.customer_id IN (SELECT id FROM customers WHERE tenant_id = 't1')`},
		{"a subquery that returns several columns", `SELECT * FROM orders WHERE (customer_id, status) IN (SELECT id, region FROM customers WHERE tenant_id = 't1')`},
		{"a UNION in the subquery", `SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tenant_id = 't1' UNION SELECT id FROM customers)`},
		{"an aggregate is not a column of the table", `SELECT * FROM orders WHERE customer_id IN (SELECT max(id) FROM customers WHERE tenant_id = 't1')`},
		{"a function of the outer column", `SELECT * FROM orders WHERE customer_id + 0 IN (SELECT id FROM customers WHERE tenant_id = 't1')`},
	} {
		e := h.attributed(t, c.sql)
		switch e.Source {
		case SourceExplicit, SourceCache, SourceLookup:
			t.Errorf("%s:\n  %s\n  claimed %v (%s)", c.name, c.sql, e.Tenants, e.Source)
		}
	}
}

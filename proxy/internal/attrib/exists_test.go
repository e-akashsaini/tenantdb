package attrib

import (
	"reflect"
	"testing"
)

// A correlated EXISTS is a semi-join: the subquery's confinement carries to the
// outer table through the correlated equality, as it does for IN.
func TestExistsConfinesTheOuterTable(t *testing.T) {
	h := soundHarness()
	for _, c := range []struct {
		name, sql string
		tenants   []string
	}{
		{"through a foreign key", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1')`, []string{"t1"}},
		{"either side of the equality", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE o.customer_id = c.id AND c.tenant_id = 't1')`, []string{"t1"}},
		{"select star", `SELECT * FROM orders o WHERE EXISTS (SELECT c.* FROM customers c WHERE c.id = o.customer_id AND c.tenant_id IN ('t1', 't2'))`, []string{"t1", "t2"}},
		{"tenant column to tenant column", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.tenant_id = o.tenant_id AND c.tenant_id = 't2')`, []string{"t2"}},
		{"alongside other conditions", `SELECT * FROM orders o WHERE o.status = 'open' AND EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1') AND o.amount > 1`, []string{"t1"}},
		{"with a limit", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1' LIMIT 1)`, []string{"t1"}},
		{"nested", `SELECT * FROM items i WHERE EXISTS (SELECT 1 FROM orders o WHERE o.id = i.order_id AND EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't3'))`, []string{"t3"}},
		{"which then confines a joined table", `SELECT i.* FROM orders o JOIN items i ON i.order_id = o.id WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1')`, []string{"t1"}},
	} {
		e := h.attributed(t, c.sql)
		if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s:\n  %s\n  got %s %v, want %v", c.name, c.sql, e.Source, e.Tenants, c.tenants)
		}
	}
}

func TestExistsNeverClaimsMoreThanItProves(t *testing.T) {
	h := soundHarness()
	for _, c := range []struct {
		name, sql string
	}{
		{"NOT EXISTS", `SELECT * FROM orders o WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1')`},
		{"an OR alternative", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1') OR o.status = 'x'`},
		{"not correlated", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.tenant_id = 't1')`},
		{"correlated through an OR", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE (c.id = o.customer_id OR c.id = 5) AND c.tenant_id = 't1')`},
		{"columns that are not related", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.amount AND c.tenant_id = 't1')`},
		{"an aggregate always returns a row", `SELECT * FROM orders o WHERE EXISTS (SELECT count(*) FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1')`},
		{"a UNION in the subquery", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1' UNION SELECT 1 FROM customers)`},
		{"a function of the column", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id + 0 AND c.tenant_id = 't1')`},
		{"the ON of a left join", `SELECT * FROM customers c0 LEFT JOIN orders o ON EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.tenant_id = 't1')`},
		{"the subquery itself is not confined", `SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)`},
	} {
		e := h.attributed(t, c.sql)
		switch e.Source {
		case SourceExplicit, SourceCache, SourceLookup:
			t.Errorf("%s:\n  %s\n  claimed %v (%s)", c.name, c.sql, e.Tenants, e.Source)
		}
	}
}

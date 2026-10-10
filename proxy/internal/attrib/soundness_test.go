package attrib

import (
	"reflect"
	"testing"
)

// Regression tests for unsound attributions the differential fuzzer found: each
// of these once claimed a tenant set that left out tenants the query really read.

func soundSpec() catSpec {
	var rows []colRow
	add := func(r []colRow) { rows = append(rows, r...) }
	add(tbl("public", "customers", 'r', "id", "tenant_id", "slug", "region"))
	add(tbl("public", "orders", 'r', "id", "tenant_id", "customer_id", "status", "amount"))
	add(tbl("public", "items", 'r', "id", "tenant_id", "order_id", "sku"))
	add(tbl("public", "countries", 'r', "code", "name"))
	return catSpec{
		rows: rows,
		funcs: []*FuncDef{{Schema: "public", Name: "orders_of", Lang: "sql", Kind: 'f', NArgs: 1, ArgNames: []string{"p"},
			Src: `SELECT * FROM orders WHERE tenant_id = p`}},
		unique: map[string][]string{
			"public.customers": {"id", "slug"}, "public.orders": {"id"}, "public.items": {"id"}, "public.countries": {"code"},
		},
		fks: map[string]map[string]FKRef{
			"public.orders": {"customer_id": {ParentKey: "public.customers", ParentCol: "id"}},
			"public.items":  {"order_id": {ParentKey: "public.orders", ParentCol: "id"}},
		},
	}
}

func soundHarness() *harness {
	h := specHarness(soundSpec(), StrictOptions{})
	return h
}

func (h *harness) attributed(t *testing.T, sql string) Event {
	t.Helper()
	h.query(sql)
	e := h.last(t)
	h.rfq('I')
	return e
}

func TestNeverClaimsTenantsAQueryDoesNotConfineItTo(t *testing.T) {
	h := soundHarness()
	h.a.Cache().Put("public.customers", "slug", "s26", "t2")
	for _, c := range []struct {
		name, sql string
	}{
		{"a tenant condition inside EXISTS restricts the subquery's table, not the outer one",
			`SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE tenant_id = 't1')`},
		{"... and inside NOT EXISTS",
			`SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM customers WHERE tenant_id = 't1')`},
		{"... and in a scalar subquery",
			`SELECT * FROM orders WHERE amount > (SELECT max(amount) FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.tenant_id = 't1')`},
		{"LEFT JOIN: the ON clause leaves the preserved side unconfined",
			`SELECT o.* FROM orders o LEFT JOIN customers c ON o.customer_id = c.id AND c.tenant_id = 't1'`},
		{"RIGHT JOIN: the ON clause leaves the preserved side unconfined",
			`SELECT o.* FROM orders o RIGHT JOIN customers c ON o.customer_id = c.id AND c.tenant_id = 't3'`},
		{"FULL JOIN: the ON clause confines neither side",
			`SELECT o.* FROM orders o FULL JOIN customers c ON o.customer_id = c.id AND c.tenant_id = 't3'`},
		{"UNION ALL: the other branch reads everything",
			`SELECT id FROM orders WHERE tenant_id = 't2' UNION ALL SELECT id FROM customers WHERE region = 'eu'`},
		{"UNION ALL where one branch's OR hides an unconfined alternative",
			`SELECT id FROM orders WHERE tenant_id = 't2' OR tenant_id = 't3' UNION ALL SELECT id FROM orders WHERE 1 = 1`},
		{"a CTE's own tenant condition does not confine the main query",
			`WITH x AS (SELECT id FROM orders WHERE tenant_id = 't3') SELECT * FROM orders WHERE id IN (SELECT id FROM x) OR status = 'open'`},
		{"an OR alternative that reads everything",
			`SELECT * FROM orders WHERE tenant_id = 't1' OR amount > 5`},
		{"an OR whose other branch is a row-owner lookup the cache cannot answer",
			`SELECT * FROM orders WHERE tenant_id = 't1' OR id = 12345`},
		{"NOT does not confine",
			`SELECT * FROM orders WHERE NOT (tenant_id = 't1')`},
		{"a function of the tenant column is not the tenant",
			`SELECT * FROM orders WHERE lower(tenant_id) = 't1'`},
		{"a tenant condition on the wrong table of a join",
			`SELECT o.* FROM orders o JOIN countries k ON true WHERE k.code = 'IE' OR o.status = 'x'`},
		{"UPDATE moving rows to another tenant",
			`UPDATE orders SET tenant_id = 't9' WHERE tenant_id = 't1'`},
	} {
		e := h.attributed(t, c.sql)
		switch e.Source {
		case SourceExplicit, SourceCache, SourceLookup:
			t.Errorf("%s:\n  %s\n  claimed %v (%s)", c.name, c.sql, e.Tenants, e.Source)
		}
	}
}

func TestStillClaimsWhatIsReallyConfined(t *testing.T) {
	h := soundHarness()
	h.a.Cache().Put("public.customers", "slug", "s26", "t2")
	h.a.Cache().Put("public.orders", "id", "7", "t1")
	for _, c := range []struct {
		sql     string
		source  Source
		tenants []string
	}{
		{`SELECT * FROM orders WHERE tenant_id = 't1'`, SourceExplicit, []string{"t1"}},
		{`SELECT * FROM orders WHERE tenant_id = 't1' AND (status = 'a' OR amount > 5)`, SourceExplicit, []string{"t1"}},
		{`SELECT * FROM orders WHERE (tenant_id = 't1' AND status = 'a') OR (tenant_id = 't2' AND status = 'b')`, SourceExplicit, []string{"t1", "t2"}},
		{`SELECT * FROM orders WHERE tenant_id IN ('t1', 't2') AND NOT (tenant_id = 't1')`, SourceExplicit, []string{"t1", "t2"}}, // NOT confines nothing, IN still does
		{`SELECT * FROM orders WHERE tenant_id = 't1' AND tenant_id = 't2'`, SourceNoMatch, nil},
		// both sides of an inner join, tied together by a foreign key
		{`SELECT i.* FROM items i JOIN orders o ON i.order_id = o.id WHERE o.tenant_id = 't1'`, SourceExplicit, []string{"t1"}},
		{`SELECT i.* FROM orders o JOIN items i ON i.order_id = o.id WHERE i.tenant_id = 't1'`, SourceExplicit, []string{"t1"}},
		{`SELECT i.* FROM items i JOIN orders o ON o.id = i.order_id JOIN customers c ON c.id = o.customer_id WHERE c.tenant_id = 't3'`, SourceExplicit, []string{"t3"}},
		// a tenant condition in a join's ON, for the side the join may filter
		{`SELECT o.* FROM orders o LEFT JOIN customers c ON o.customer_id = c.id WHERE o.tenant_id = 't1'`, SourceExplicit, []string{"t1"}},
		{`SELECT o.* FROM orders o JOIN customers c ON o.customer_id = c.id AND c.tenant_id = 't1'`, SourceExplicit, []string{"t1"}},
		// the subquery's table is confined too
		{`SELECT * FROM orders WHERE tenant_id = 't1' AND EXISTS (SELECT 1 FROM items i WHERE i.order_id = orders.id)`, SourceExplicit, []string{"t1"}},
		{`SELECT * FROM orders WHERE tenant_id = 't1' AND id IN (SELECT id FROM orders WHERE tenant_id = 't1')`, SourceExplicit, []string{"t1"}},
		// rows named by a unique column or a foreign key
		{`SELECT * FROM orders WHERE id = 7`, SourceCache, []string{"t1"}},
		{`SELECT * FROM customers WHERE slug = 's26' OR tenant_id = 't1'`, SourceCache, []string{"t1", "t2"}},
		// a UNION where every branch is confined
		{`SELECT id FROM orders WHERE tenant_id = 't1' UNION ALL SELECT id FROM customers WHERE tenant_id = 't2'`, SourceExplicit, []string{"t1", "t2"}},
		// the shared table needs no tenant
		{`SELECT o.* FROM orders o JOIN countries k ON true WHERE o.tenant_id = 't1'`, SourceExplicit, []string{"t1"}},
	} {
		e := h.attributed(t, c.sql)
		if e.Source != c.source || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s\n  got %s %v, want %s %v", c.sql, e.Source, e.Tenants, c.source, c.tenants)
		}
	}
}

func TestSubqueriesAreEachConfinedOrTheStatementIsNot(t *testing.T) {
	h := soundHarness()
	e := h.attributed(t, `SELECT * FROM orders WHERE tenant_id = 't1' AND EXISTS (SELECT 1 FROM customers c WHERE c.region = 'eu')`)
	if e.Source == SourceExplicit {
		t.Errorf("the subquery reads every tenant's customers: %+v", e)
	}
	e = h.attributed(t, `WITH x AS (SELECT * FROM orders) SELECT * FROM x WHERE x.tenant_id = 't1'`)
	if e.Source == SourceExplicit {
		t.Errorf("the CTE reads every tenant's orders: %+v", e)
	}
}

// Found by the differential fuzzer: after a row lookup finished the analysis, the
// tenants a function call contributed were forgotten, so the statement was claimed
// for the subquery's tenant alone.
func TestLookupResultsAreCombinedWithFunctionCallTenants(t *testing.T) {
	h := soundHarness()
	h.a.lk = &fakeLookup{rows: map[string][]string{"public.customers|id|35": {"t2"}}}
	h.query(`SELECT f.* FROM orders_of('t3') f WHERE NOT EXISTS (SELECT 1 FROM orders o2 WHERE o2.customer_id = 35)`)
	h.waitFor(t, 1)
	e := h.last(t)
	if e.Source != SourceLookup || !reflect.DeepEqual(e.Tenants, []string{"t2", "t3"}) {
		t.Errorf("want the subquery's tenant (found by lookup) and the function's: %+v", e)
	}
}

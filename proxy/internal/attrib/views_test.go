package attrib

import (
	"reflect"
	"testing"
)

func viewSpec() catSpec {
	sp := soundSpec()
	add := func(name string, cols ...string) { sp.rows = append(sp.rows, tbl("public", name, 'v', cols...)...) }
	add("orders_t", "id", "t", "customer_id")
	add("acme_orders", "id", "amount")
	add("cur_orders", "id", "amount")
	add("all_orders", "id", "amount")
	add("order_totals", "tenant_id", "total")
	add("region_totals", "region", "total")
	add("big_orders", "id")
	add("order_customer", "id", "tid", "name")
	add("shared_v", "code")
	add("limited", "id", "tenant_id")
	add("fn_v", "id")
	add("either_v", "id")
	sp.views = map[string]string{
		"public.orders_t":       `SELECT orders.id, orders.tenant_id AS t, orders.customer_id FROM public.orders`,
		"public.acme_orders":    `SELECT orders.id, orders.amount FROM public.orders WHERE (orders.tenant_id = 't1'::text)`,
		"public.cur_orders":     `SELECT orders.id, orders.amount FROM public.orders WHERE (orders.tenant_id = current_setting('app.tenant'::text))`,
		"public.all_orders":     `SELECT orders.id, orders.amount FROM public.orders`,
		"public.order_totals":   `SELECT orders.tenant_id, sum(orders.amount) AS total FROM public.orders GROUP BY orders.tenant_id`,
		"public.region_totals":  `SELECT customers.region, count(*) AS total FROM public.customers GROUP BY customers.region`,
		"public.big_orders":     `SELECT acme_orders.id FROM public.acme_orders WHERE (acme_orders.amount > 5)`,
		"public.order_customer": `SELECT o.id, c.tenant_id AS tid, c.slug AS name FROM (public.orders o JOIN public.customers c ON ((o.customer_id = c.id)))`,
		"public.shared_v":       `SELECT countries.code FROM public.countries`,
		"public.limited":        `SELECT orders.id, orders.tenant_id FROM public.orders LIMIT 5`,
		"public.fn_v":           `SELECT o.id FROM public.orders_of('t1'::text) o(id)`,
		"public.either_v":       `SELECT orders.id FROM public.orders WHERE ((orders.tenant_id = 't1'::text) OR (orders.amount > 3))`,
	}
	return sp
}

func viewHarness() *harness { return specHarness(viewSpec(), StrictOptions{}) }

func TestViewsAreReadThroughTheirDefinition(t *testing.T) {
	h := viewHarness()
	for _, c := range []struct {
		name, sql string
		source    Source
		tenants   []string
	}{
		{"a renamed tenant column still reaches the table's tenant column",
			`SELECT * FROM orders_t WHERE t = 't2'`, SourceExplicit, []string{"t2"}},
		{"a view that fixes the tenant", `SELECT * FROM acme_orders`, SourceExplicit, []string{"t1"}},
		{"... with more conditions on top", `SELECT * FROM acme_orders WHERE amount > 3`, SourceExplicit, []string{"t1"}},
		{"a view over a view", `SELECT * FROM big_orders`, SourceExplicit, []string{"t1"}},
		{"a join view, by the tenant column it renames", `SELECT * FROM order_customer WHERE tid = 't3'`, SourceExplicit, []string{"t3"}},
		{"a view of shared tables", `SELECT * FROM shared_v`, SourceUntenanted, nil},
		{"a view joined to a table", `SELECT o.* FROM orders o JOIN acme_orders a ON a.id = o.id`, SourceExplicit, []string{"t1"}},
		{"the view's own tenant and the statement's agree", `SELECT * FROM acme_orders a JOIN orders o ON o.id = a.id WHERE o.tenant_id = 't1'`, SourceExplicit, []string{"t1"}},
		{"the same row cannot belong to two tenants", `SELECT * FROM acme_orders a JOIN orders o ON o.id = a.id WHERE o.tenant_id = 't2'`, SourceNoMatch, nil},
	} {
		e := h.attributed(t, c.sql)
		if e.Source != c.source || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s:\n  %s\n  got %s %v, want %s %v", c.name, c.sql, e.Source, e.Tenants, c.source, c.tenants)
		}
	}
}

func TestViewsThatReadEverythingAreStillFlagged(t *testing.T) {
	h := viewHarness()
	for _, c := range []struct {
		name, sql string
	}{
		{"the view hides the tenant and does not restrict it", `SELECT * FROM all_orders`},
		{"... even when the statement filters on something else", `SELECT * FROM all_orders WHERE amount > 5`},
		{"a filter on a column that is not the tenant", `SELECT * FROM orders_t WHERE customer_id = 3`},
		{"an OR in the view leaves an alternative unconfined", `SELECT * FROM either_v`},
		{"LIMIT: not merged, and its own table is not confined", `SELECT * FROM limited`},
		{"a view that calls application code", `SELECT * FROM fn_v`},
		{"an aggregate view's own table is unconfined", `SELECT * FROM region_totals`},
		{"an unrelated condition does not confine the view's table", `SELECT * FROM acme_orders a, orders o WHERE a.id = o.id OR o.status = 'x'`},
		{"the view on the nullable side leaves the preserved side unconfined", `SELECT o.* FROM orders o LEFT JOIN acme_orders a ON a.id = o.id`},
	} {
		e := h.attributed(t, c.sql)
		switch e.Source {
		case SourceExplicit, SourceCache, SourceLookup, SourceUntenanted:
			t.Errorf("%s:\n  %s\n  claimed %v (%s)", c.name, c.sql, e.Tenants, e.Source)
		}
	}
}

func TestSessionScopedViewsFollowTheSessionVariable(t *testing.T) {
	h := viewHarness()
	if e := h.attributed(t, `SELECT * FROM cur_orders`); e.Source == SourceExplicit {
		t.Fatalf("nothing has said who app.tenant is: %+v", e)
	}
	h.query(`SET app.tenant = 'acme'`)
	h.rfq('I')
	e := h.attributed(t, `SELECT * FROM cur_orders`)
	if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("a view scoped by current_setting: %+v", e)
	}
}

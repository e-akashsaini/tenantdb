package attrib

import (
	"fmt"
	"regexp"
	"strconv"
)

// More statement shapes for the differential test: functions, views, polymorphic
// lookups, multi-table DML, derived tables, LATERAL, aggregates.

const diffExtraSchemaSQL = `
CREATE TABLE tdbdiff.notes (id bigint PRIMARY KEY, tenant_id text, owner_type text, owner_id bigint, body text);
INSERT INTO tdbdiff.notes SELECT id, 't'||((id-1)/60), CASE WHEN id%2=0 THEN 'Order' ELSE 'Customer' END,
  CASE WHEN id%2=0 THEN ((id-1)/60)*40 + (id%40) + 1 ELSE ((id-1)/60)*12 + (id%12) + 1 END, 'b'||id
  FROM generate_series(1,240) id;
CREATE FUNCTION tdbdiff.orders_of(p_tenant text) RETURNS SETOF tdbdiff.orders LANGUAGE sql AS $$ SELECT * FROM tdbdiff.orders WHERE tenant_id = p_tenant $$;
CREATE FUNCTION tdbdiff.all_orders() RETURNS SETOF tdbdiff.orders LANGUAGE sql AS $$ SELECT * FROM tdbdiff.orders $$;
CREATE FUNCTION tdbdiff.order_by_id(p_id bigint) RETURNS SETOF tdbdiff.orders LANGUAGE sql AS $$ SELECT * FROM tdbdiff.orders WHERE id = p_id $$;
CREATE FUNCTION tdbdiff.outer_orders(p text) RETURNS SETOF tdbdiff.orders LANGUAGE sql AS $$ SELECT * FROM tdbdiff.orders_of(p) $$;
CREATE FUNCTION tdbdiff.either_orders(a text, b text) RETURNS SETOF tdbdiff.orders LANGUAGE sql AS $$
  SELECT * FROM tdbdiff.orders WHERE tenant_id = a UNION ALL SELECT * FROM tdbdiff.orders WHERE tenant_id = b $$;
CREATE FUNCTION tdbdiff.plpg_orders(p text) RETURNS SETOF tdbdiff.orders LANGUAGE plpgsql AS $$
  BEGIN RETURN QUERY SELECT * FROM tdbdiff.orders WHERE tenant_id = p; END $$;
CREATE FUNCTION tdbdiff.dyn_orders(p text) RETURNS SETOF tdbdiff.orders LANGUAGE plpgsql AS $$
  BEGIN RETURN QUERY EXECUTE 'SELECT * FROM tdbdiff.orders WHERE tenant_id = ' || quote_literal(p); END $$;
CREATE FUNCTION tdbdiff.lit_fmt(p text) RETURNS SETOF tdbdiff.orders LANGUAGE plpgsql AS $$
  BEGIN RETURN QUERY EXECUTE format('SELECT * FROM tdbdiff.orders WHERE tenant_id = %L', p); END $$;
CREATE FUNCTION tdbdiff.lit_using(p text) RETURNS SETOF tdbdiff.orders LANGUAGE plpgsql AS $$
  BEGIN RETURN QUERY EXECUTE 'SELECT * FROM tdbdiff.orders WHERE tenant_id = $1' USING p; END $$;
CREATE FUNCTION tdbdiff.lit_all() RETURNS SETOF tdbdiff.orders LANGUAGE plpgsql AS $$
  BEGIN RETURN QUERY EXECUTE 'SELECT * FROM tdbdiff.orders'; END $$;
CREATE FUNCTION tdbdiff.lit_or(p text) RETURNS SETOF tdbdiff.orders LANGUAGE plpgsql AS $$
  BEGIN RETURN QUERY EXECUTE format('SELECT * FROM tdbdiff.orders WHERE tenant_id = %L OR amount > 90', p); END $$;
CREATE VIEW tdbdiff.orders_v AS SELECT * FROM tdbdiff.orders;
CREATE VIEW tdbdiff.order_amounts AS SELECT id, amount FROM tdbdiff.orders;
` + diffViewSQL

// Each view comes with a twin (<name>_x) that also exposes the tenants of the
// rows it returns, in _tn (and _tn2): the twin is the ground truth.
const diffViewSQL = `
CREATE VIEW tdbdiff.v_ren AS SELECT id, tenant_id AS t, customer_id, amount, status FROM tdbdiff.orders;
CREATE VIEW tdbdiff.v_ren_x AS SELECT id, tenant_id AS t, customer_id, amount, status, tenant_id AS _tn FROM tdbdiff.orders;
CREATE VIEW tdbdiff.v_t1 AS SELECT id, amount, status FROM tdbdiff.orders WHERE tenant_id = 't1';
CREATE VIEW tdbdiff.v_t1_x AS SELECT id, amount, status, tenant_id AS _tn FROM tdbdiff.orders WHERE tenant_id = 't1';
CREATE VIEW tdbdiff.v_cur AS SELECT id, amount FROM tdbdiff.orders WHERE tenant_id = current_setting('app.tenant');
CREATE VIEW tdbdiff.v_cur_x AS SELECT id, amount, tenant_id AS _tn FROM tdbdiff.orders WHERE tenant_id = current_setting('app.tenant');
CREATE VIEW tdbdiff.v_join AS SELECT o.id, o.status, c.tenant_id AS ctid, o.tenant_id AS otid FROM tdbdiff.orders o JOIN tdbdiff.customers c ON o.customer_id = c.id;
CREATE VIEW tdbdiff.v_join_x AS SELECT o.id, o.status, c.tenant_id AS ctid, o.tenant_id AS otid, c.tenant_id AS _tn, o.tenant_id AS _tn2 FROM tdbdiff.orders o JOIN tdbdiff.customers c ON o.customer_id = c.id;
CREATE VIEW tdbdiff.v_nested AS SELECT id, amount FROM tdbdiff.v_t1 WHERE amount > 10;
CREATE VIEW tdbdiff.v_nested_x AS SELECT id, amount, _tn FROM tdbdiff.v_t1_x WHERE amount > 10;
CREATE VIEW tdbdiff.v_agg AS SELECT tenant_id, count(*) AS n FROM tdbdiff.orders GROUP BY tenant_id;
CREATE VIEW tdbdiff.v_agg_x AS SELECT tenant_id, count(*) AS n, tenant_id AS _tn FROM tdbdiff.orders GROUP BY tenant_id;
CREATE VIEW tdbdiff.v_lim AS SELECT id, tenant_id FROM tdbdiff.orders ORDER BY id LIMIT 30;
CREATE VIEW tdbdiff.v_lim_x AS SELECT id, tenant_id, tenant_id AS _tn FROM tdbdiff.orders ORDER BY id LIMIT 30;
CREATE VIEW tdbdiff.v_left AS SELECT c.id, c.tenant_id AS ct, o.id AS oid, o.tenant_id AS ot FROM tdbdiff.customers c LEFT JOIN tdbdiff.orders o ON o.customer_id = c.id AND o.status = 'open';
CREATE VIEW tdbdiff.v_left_x AS SELECT c.id, c.tenant_id AS ct, o.id AS oid, o.tenant_id AS ot, c.tenant_id AS _tn, o.tenant_id AS _tn2 FROM tdbdiff.customers c LEFT JOIN tdbdiff.orders o ON o.customer_id = c.id AND o.status = 'open';
CREATE VIEW tdbdiff.v_or AS SELECT id, tenant_id AS tt FROM tdbdiff.orders WHERE tenant_id = 't1' OR amount > 50;
CREATE VIEW tdbdiff.v_or_x AS SELECT id, tenant_id AS tt, tenant_id AS _tn FROM tdbdiff.orders WHERE tenant_id = 't1' OR amount > 50;
`

type dview struct {
	name   string
	tcols  []string // columns that are a tenant
	icols  []string // integer columns
	scols  []string // columns of status text
	two    bool     // the twin has _tn2 as well
	joinID bool     // id is orders.id
}

var dViews = []dview{
	{"v_ren", []string{"t"}, []string{"id", "customer_id", "amount"}, []string{"status"}, false, true},
	{"v_t1", nil, []string{"id", "amount"}, []string{"status"}, false, true},
	{"v_cur", nil, []string{"id", "amount"}, nil, false, true},
	{"v_join", []string{"ctid", "otid"}, []string{"id"}, []string{"status"}, true, true},
	{"v_nested", nil, []string{"id", "amount"}, nil, false, true},
	{"v_agg", []string{"tenant_id"}, []string{"n"}, nil, false, false},
	{"v_lim", []string{"tenant_id"}, []string{"id"}, nil, false, true},
	{"v_left", []string{"ct", "ot"}, []string{"id", "oid"}, nil, true, false},
	{"v_or", []string{"tt"}, []string{"id"}, nil, false, true},
}

func (g *dgen) viewAtom(v dview, a string) string {
	tc := func() string { return a + "." + v.tcols[g.r.Intn(len(v.tcols))] }
	switch g.r.Intn(9) {
	case 0, 1:
		if len(v.tcols) > 0 {
			return tc() + " = " + g.tid()
		}
	case 2:
		if len(v.tcols) > 0 {
			return fmt.Sprintf("%s IN (%s, %s)", tc(), g.tid(), g.tid())
		}
	case 3:
		if len(v.tcols) > 0 {
			return tc() + " <> " + g.tid()
		}
	case 4:
		return fmt.Sprintf("%s.%s = %s", a, v.icols[g.r.Intn(len(v.icols))], g.num(100))
	case 5:
		return fmt.Sprintf("%s.%s > %s", a, v.icols[g.r.Intn(len(v.icols))], g.num(100))
	case 6:
		if len(v.scols) > 0 {
			return fmt.Sprintf("%s.%s = '%s'", a, v.scols[0], g.pick("open", "paid", "void"))
		}
	case 7:
		if len(v.tcols) > 0 {
			return tc() + " = current_setting('app.tenant')"
		}
	case 8:
		if len(v.tcols) > 1 {
			return a + "." + v.tcols[0] + " = " + a + "." + v.tcols[1]
		}
	}
	return a + "." + v.icols[0] + " IS NOT NULL"
}

func (g *dgen) viewWhere(v dview, a string) string {
	x, y := g.viewAtom(v, a), g.viewAtom(v, a)
	switch g.r.Intn(6) {
	case 0:
		return x + " AND " + y
	case 1:
		return x + " OR " + y
	case 2:
		return "NOT (" + x + ")"
	case 3:
		return "(" + x + " OR " + y + ") AND " + g.viewAtom(v, a)
	}
	return x
}

// viewQuery selects from a view, alone or joined to the table it is made from.
func (g *dgen) viewQuery() dquery {
	v := dViews[g.r.Intn(len(dViews))]
	a := g.newAlias()
	tn := func(al string) []string {
		out := []string{"SELECT DISTINCT " + al + "._tn"}
		if v.two {
			out = append(out, "SELECT DISTINCT "+al+"._tn2")
		}
		return out
	}
	if v.joinID && g.r.Intn(3) == 0 {
		o := g.newAlias()
		where := g.viewWhere(v, a)
		if g.r.Intn(2) == 0 {
			where += " AND " + g.pred(o, dTables["orders"], 1)
		}
		from := fmt.Sprintf("tdbdiff.orders %s JOIN tdbdiff.%s %s ON %s.id = %s.id WHERE %s", o, v.name, a, a, o, where)
		fromX := fmt.Sprintf("tdbdiff.orders %s JOIN tdbdiff.%s_x %s ON %s.id = %s.id WHERE %s", o, v.name, a, a, o, where)
		truth := []string{"SELECT DISTINCT " + o + ".tenant_id FROM " + fromX}
		for _, t := range tn(a) {
			truth = append(truth, t+" FROM "+fromX)
		}
		return dquery{sql: "SELECT " + o + ".* FROM " + from, truth: truth, tenant: true}
	}
	where := g.viewWhere(v, a)
	var truth []string
	for _, t := range tn(a) {
		truth = append(truth, fmt.Sprintf("%s FROM tdbdiff.%s_x %s WHERE %s", t, v.name, a, where))
	}
	return dquery{sql: fmt.Sprintf("SELECT %s.* FROM tdbdiff.%s %s WHERE %s", a, v.name, a, where), truth: truth, tenant: true}
}

// extraQuery generates one of the statement shapes above.
func (g *dgen) extraQuery() dquery {
	tid := func() string { return g.tid() }
	switch g.r.Intn(23) {
	case 21, 22: // EXISTS over a correlated table, through a relationship the schema does not declare
		f, sa := g.newAlias(), g.newAlias()
		var sub, corr string
		switch g.r.Intn(3) {
		case 0:
			sub, corr = "folders", fmt.Sprintf("%s.up = %s.id", sa, f)
		case 1:
			sub, corr = "folders", fmt.Sprintf("%s.id = %s.up", sa, f)
		default:
			sub, corr = "folders", fmt.Sprintf("%s.up = %s.id AND %s.label = 'f%d'", sa, f, sa, g.r.Intn(5))
		}
		pred := g.pred(f, dTables["folders"], 1)
		op := g.pick("EXISTS", "EXISTS", "NOT EXISTS")
		join := g.pick(" OR "+g.pred(f, dTables["folders"], 1), " AND "+g.pred(f, dTables["folders"], 1), "")
		where := fmt.Sprintf("%s AND %s (SELECT 1 FROM tdbdiff.%s %s WHERE %s)%s", pred, op, sub, sa, corr, join)
		return dquery{
			sql: fmt.Sprintf("SELECT %s.* FROM tdbdiff.folders %s WHERE %s", f, f, where),
			// the data keeps a folder and its parent or children in one tenant, so the rows returned say whose data was read
			truth:  []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.folders %s WHERE %s", f, f, where)},
			tenant: true,
		}
	case 16, 17, 18:
		return g.viewQuery()
	case 19:
		return fnQuery(g.pick(fmt.Sprintf("tdbdiff.lit_fmt(%s)", tid()), fmt.Sprintf("tdbdiff.lit_using(%s)", tid()), "tdbdiff.lit_all()", fmt.Sprintf("tdbdiff.lit_or(%s)", tid())), g)
	case 20:
		return fnQuery(g.pick(fmt.Sprintf("tdbdiff.lit_fmt(%s)", tid()), fmt.Sprintf("tdbdiff.lit_using(%s)", tid())), g)
	case 0:
		f := fmt.Sprintf("tdbdiff.orders_of(%s)", tid())
		return fnQuery(f, g)
	case 1:
		return fnQuery("tdbdiff.all_orders()", g)
	case 2:
		return fnQuery(fmt.Sprintf("tdbdiff.order_by_id(%s)", g.num(160)), g)
	case 3:
		return fnQuery(fmt.Sprintf("tdbdiff.outer_orders(%s)", tid()), g)
	case 4:
		return fnQuery(fmt.Sprintf("tdbdiff.either_orders(%s, %s)", tid(), tid()), g)
	case 5:
		return fnQuery(fmt.Sprintf("tdbdiff.plpg_orders(%s)", tid()), g)
	case 6:
		return fnQuery(fmt.Sprintf("tdbdiff.dyn_orders(%s)", tid()), g)
	case 7: // a function joined to a table
		f := fmt.Sprintf("tdbdiff.orders_of(%s)", tid())
		a, b := g.newAlias(), g.newAlias()
		from := fmt.Sprintf("%s %s JOIN tdbdiff.customers %s ON %s.customer_id = %s.id", f, a, b, a, b)
		return dquery{sql: "SELECT " + b + ".* FROM " + from,
			truth:  []string{"SELECT DISTINCT " + a + ".tenant_id FROM " + from, "SELECT DISTINCT " + b + ".tenant_id FROM " + from},
			tenant: true}
	case 8: // views
		v := dTables["orders"]
		a := g.newAlias()
		where := g.pred(a, v, 1)
		if g.r.Intn(2) == 0 {
			return dquery{sql: fmt.Sprintf("SELECT %s.* FROM tdbdiff.orders_v %s WHERE %s", a, a, where),
				truth:  []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.orders_v %s WHERE %s", a, a, where)},
				tenant: true}
		}
		// a view that hides the tenant column: must never be called untenanted
		return dquery{sql: fmt.Sprintf("SELECT * FROM tdbdiff.order_amounts WHERE amount > %s", g.num(90)), tenant: true}
	case 9: // polymorphic
		typ, id := "Order", g.num(160)
		if g.r.Intn(2) == 0 {
			typ, id = "Customer", g.num(48)
		}
		a := g.newAlias()
		where := fmt.Sprintf("%s.owner_type = '%s' AND %s.owner_id = %s", a, typ, a, id)
		if g.r.Intn(3) == 0 {
			where = fmt.Sprintf("%s.owner_type = '%s' AND %s.owner_id IN (%s, %s)", a, typ, a, id, id)
		}
		if g.r.Intn(4) == 0 {
			where += " OR " + a + ".body = 'b5'"
		}
		return dquery{sql: fmt.Sprintf("SELECT %s.* FROM tdbdiff.notes %s WHERE %s", a, a, where),
			truth:  []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.notes %s WHERE %s", a, a, where)},
			tenant: true}
	case 10: // UPDATE ... FROM
		ia, oa := "i", "o"
		where := g.pick(g.pred(ia, dTables["items"], 1), g.pred(oa, dTables["orders"], 1), g.pred(oa, dTables["orders"], 1)+" AND "+g.pred(ia, dTables["items"], 1))
		return dquery{sql: fmt.Sprintf("UPDATE tdbdiff.items i SET qty = i.qty FROM tdbdiff.orders o WHERE o.id = i.order_id AND %s RETURNING i.tenant_id, o.tenant_id", where), tenant: true, dml: true}
	case 11: // DELETE ... USING
		ia, oa := "i", "o"
		where := g.pick(g.pred(ia, dTables["items"], 1), g.pred(oa, dTables["orders"], 1))
		return dquery{sql: fmt.Sprintf("DELETE FROM tdbdiff.items i USING tdbdiff.orders o WHERE o.id = i.order_id AND %s RETURNING i.tenant_id, o.tenant_id", where), tenant: true, dml: true}
	case 12: // INSERT ... VALUES / SELECT
		if g.r.Intn(2) == 0 {
			return dquery{sql: fmt.Sprintf("INSERT INTO tdbdiff.orders (id, tenant_id, customer_id, status, amount) VALUES (%d, %s, 1, 'open', 1) RETURNING tenant_id", 1000+g.r.Intn(50), tid()), tenant: true, dml: true}
		}
		a := g.newAlias()
		return dquery{sql: fmt.Sprintf("INSERT INTO tdbdiff.orders SELECT %s.id + 1000, %s.tenant_id, %s.customer_id, %s.status, %s.amount FROM tdbdiff.orders %s WHERE %s RETURNING tenant_id",
			a, a, a, a, a, a, g.pred(a, dTables["orders"], 1)), tenant: true, dml: true}
	case 13: // derived table
		a, s := g.newAlias(), g.newAlias()
		inner, outer := g.pred(a, dTables["orders"], 1), g.pred(s, dTables["orders"], 1)
		from := fmt.Sprintf("(SELECT * FROM tdbdiff.orders %s WHERE %s) %s WHERE %s", a, inner, s, outer)
		return dquery{sql: "SELECT " + s + ".* FROM " + from, truth: []string{"SELECT DISTINCT " + s + ".tenant_id FROM " + from}, tenant: true}
	case 14: // LATERAL
		c, x, o := g.newAlias(), g.newAlias(), g.newAlias()
		cw, ow := g.pred(c, dTables["customers"], 1), g.pred(o, dTables["orders"], 1)
		from := fmt.Sprintf("tdbdiff.customers %s, LATERAL (SELECT * FROM tdbdiff.orders %s WHERE %s.customer_id = %s.id AND %s) %s WHERE %s", c, o, o, c, ow, x, cw)
		return dquery{sql: "SELECT " + x + ".* FROM " + from,
			truth: []string{"SELECT DISTINCT " + c + ".tenant_id FROM " + from, "SELECT DISTINCT " + x + ".tenant_id FROM " + from}, tenant: true}
	default: // aggregate
		a := g.newAlias()
		where := g.pred(a, dTables["orders"], 1)
		return dquery{sql: fmt.Sprintf("SELECT %s.status, count(*) FROM tdbdiff.orders %s WHERE %s GROUP BY 1", a, a, where),
			truth:  []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.orders %s WHERE %s", a, a, where)},
			tenant: true}
	}
}

// fnQuery selects from a set-returning function, optionally filtered.
func fnQuery(f string, g *dgen) dquery {
	a := g.newAlias()
	where := "true"
	if g.r.Intn(2) == 0 {
		where = g.pred(a, dTables["orders"], 1)
	}
	from := fmt.Sprintf("%s %s WHERE %s", f, a, where)
	return dquery{sql: fmt.Sprintf("SELECT %s.* FROM %s", a, from),
		truth: []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM %s", a, from)}, tenant: true}
}

var tenantLiteral = regexp.MustCompile(`'t[0-3]'`)

// paramize replaces tenant literals with $n placeholders and returns the values,
// so the same statement can be shown to the attributor as a prepared statement.
func paramize(sql string) (string, [][]byte) {
	var vals [][]byte
	out := tenantLiteral.ReplaceAllStringFunc(sql, func(m string) string {
		vals = append(vals, []byte(m[1:len(m)-1]))
		return "$" + strconv.Itoa(len(vals))
	})
	return out, vals
}

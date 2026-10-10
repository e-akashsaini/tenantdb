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
CREATE VIEW tdbdiff.orders_v AS SELECT * FROM tdbdiff.orders;
CREATE VIEW tdbdiff.order_amounts AS SELECT id, amount FROM tdbdiff.orders;
`

// extraQuery generates one of the statement shapes above.
func (g *dgen) extraQuery() dquery {
	tid := func() string { return g.tid() }
	switch g.r.Intn(16) {
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

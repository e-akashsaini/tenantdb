package attrib

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// A differential soundness test. Random queries are generated over a small
// multi-tenant schema and run for real; the tenants of the rows they return (or,
// for DML, affect) are the ground truth. Whatever the attributor claims must
// include every one of them: a claim that leaves a tenant out would hide a leak.
//
//   TENANTDB_TEST_DSN=postgres://postgres:pw@127.0.0.1:5432/postgres \
//   TENANTDB_FUZZ_N=3000 TENANTDB_FUZZ_SEED=1 go test -run TestDifferential ./internal/attrib

type dcol struct{ name, kind string } // kind: int or text

type dtable struct {
	name   string
	tenant bool
	cols   []dcol
}

var dTables = map[string]*dtable{
	"customers": {"customers", true, []dcol{{"id", "int"}, {"tenant_id", "text"}, {"name", "text"}, {"slug", "text"}, {"region", "text"}}},
	"orders":    {"orders", true, []dcol{{"id", "int"}, {"tenant_id", "text"}, {"customer_id", "int"}, {"status", "text"}, {"amount", "int"}}},
	"items":     {"items", true, []dcol{{"id", "int"}, {"tenant_id", "text"}, {"order_id", "int"}, {"sku", "text"}, {"qty", "int"}}},
	"countries": {"countries", false, []dcol{{"code", "text"}, {"name", "text"}}},
	// a credential table: no tenant column, a unique token, and an owner pointer that nothing names
	"tokens": {"tokens", false, []dcol{{"id", "int"}, {"owner", "int"}, {"token", "text"}}},
	// ord points at orders.id but nothing in its name says so: only the joins reveal it
	"shipments": {"shipments", true, []dcol{{"id", "int"}, {"tenant_id", "text"}, {"ord", "int"}, {"carrier", "text"}}},
}

// foreign-key joins that exist in the data: child.col = parent.id
var dJoins = [][4]string{
	{"orders", "customer_id", "customers", "id"}, {"items", "order_id", "orders", "id"},
	{"shipments", "ord", "orders", "id"}, // a real relationship the naming rule cannot see
	{"items", "qty", "orders", "id"},     // NOT a relationship: equal values across tenants; must never be learned
}

const diffSchemaSQL = `
DROP SCHEMA IF EXISTS tdbdiff CASCADE;
CREATE SCHEMA tdbdiff;
CREATE TABLE tdbdiff.customers (id bigint PRIMARY KEY, tenant_id text, name text, slug text UNIQUE, region text);
CREATE TABLE tdbdiff.orders (id bigint PRIMARY KEY, tenant_id text, customer_id bigint, status text, amount int);
CREATE TABLE tdbdiff.items (id bigint PRIMARY KEY, tenant_id text, order_id bigint, sku text, qty int);
CREATE TABLE tdbdiff.countries (code text PRIMARY KEY, name text);
CREATE TABLE tdbdiff.tokens (id bigint PRIMARY KEY, owner bigint, token text UNIQUE);
INSERT INTO tdbdiff.tokens SELECT n, ((n*7)%48)+1, 'k'||n FROM generate_series(1,48) n;
CREATE TABLE tdbdiff.shipments (id bigint PRIMARY KEY, tenant_id text, ord bigint, carrier text);
INSERT INTO tdbdiff.shipments SELECT id, 't'||((id-1)/50), ((id-1)/50)*40 + (id%40) + 1, (ARRAY['dhl','ups'])[id%2+1] FROM generate_series(1,200) id;
INSERT INTO tdbdiff.customers SELECT t*12+i, 't'||t, 'n'||i, 's'||(t*12+i), CASE WHEN i%2=0 THEN 'eu' ELSE 'us' END
  FROM generate_series(0,3) t, generate_series(1,12) i;
INSERT INTO tdbdiff.orders SELECT id, 't'||((id-1)/40), ((id-1)/40)*12 + (id%12) + 1,
  (ARRAY['open','paid','void'])[id%3+1], id%101 FROM generate_series(1,160) id;
INSERT INTO tdbdiff.items SELECT id, 't'||((id-1)/100), ((id-1)/100)*40 + (id%40) + 1,
  (ARRAY['a','b','c'])[id%3+1], id%7 FROM generate_series(1,400) id;
INSERT INTO tdbdiff.countries VALUES ('IE','Ireland'),('FR','France'),('DE','Germany');
`

type dgen struct {
	r     *rand.Rand
	alias int
}

func (g *dgen) pick(xs ...string) string { return xs[g.r.Intn(len(xs))] }
func (g *dgen) tid() string              { return "'t" + strconv.Itoa(g.r.Intn(4)) + "'" }
func (g *dgen) num(max int) string       { return strconv.Itoa(g.r.Intn(max) + 1) }
func (g *dgen) newAlias() string         { g.alias++; return "a" + strconv.Itoa(g.alias) }

func (g *dgen) lit(c dcol, tname string) string {
	switch c.name {
	case "tenant_id":
		return g.tid()
	case "id":
		return g.num(160)
	case "status":
		return "'" + g.pick("open", "paid", "void") + "'"
	case "region":
		return "'" + g.pick("eu", "us") + "'"
	case "sku":
		return "'" + g.pick("a", "b", "c") + "'"
	case "slug":
		return "'s" + g.num(48) + "'"
	case "name":
		return "'n" + g.num(12) + "'"
	case "code":
		return "'" + g.pick("IE", "FR", "DE") + "'"
	}
	return g.num(100)
}

// atom: one condition on alias a of table t.
func (g *dgen) atom(a string, t *dtable, depth int) string {
	c := t.cols[g.r.Intn(len(t.cols))]
	col := a + "." + c.name
	if t.tenant && g.r.Intn(3) == 0 {
		col = a + ".tenant_id"
		c = dcol{"tenant_id", "text"}
	}
	switch g.r.Intn(18) {
	case 16, 17:
		if t.tenant {
			return a + ".tenant_id = " + g.pick("current_setting('app.tenant')", "current_setting('app.tenant', true)", "current_setting('app.tenant')::text")
		}
		return "true"
	case 0, 1, 2, 3:
		return col + " = " + g.lit(c, t.name)
	case 4:
		return g.lit(c, t.name) + " = " + col
	case 5:
		return fmt.Sprintf("%s IN (%s, %s)", col, g.lit(c, t.name), g.lit(c, t.name))
	case 6:
		return col + " <> " + g.lit(c, t.name)
	case 7:
		if c.kind == "int" {
			return col + " > " + g.lit(c, t.name)
		}
		return col + " LIKE '%'"
	case 8:
		return col + " IS NULL"
	case 9:
		return col + " IS NOT NULL"
	case 10:
		return g.pick("true", "false", "1 = 1")
	case 11:
		if c.kind == "text" {
			return fmt.Sprintf("lower(%s) = %s", col, strings.ToLower(g.lit(c, t.name)))
		}
		return col + " BETWEEN 5 AND 50"
	case 12:
		if c.kind == "text" {
			return fmt.Sprintf("%s::text = %s", col, g.lit(c, t.name))
		}
		return col + " = ANY (ARRAY[" + g.lit(c, t.name) + ", " + g.lit(c, t.name) + "])"
	case 13, 14, 15:
		if depth > 1 {
			return col + " = " + g.lit(c, t.name)
		}
		return g.subquery(a, t, depth+1)
	}
	return "true"
}

// subquery: a condition built from a nested query over another (or the same) table.
func (g *dgen) subquery(a string, t *dtable, depth int) string {
	names := []string{"customers", "orders", "items"}
	inner := dTables[names[g.r.Intn(len(names))]]
	ia := g.newAlias()
	where := g.pred(ia, inner, depth)
	switch g.r.Intn(5) {
	case 0:
		return fmt.Sprintf("EXISTS (SELECT 1 FROM tdbdiff.%s %s WHERE %s)", inner.name, ia, where)
	case 1:
		if t.tenant {
			return fmt.Sprintf("%s.id IN (SELECT %s.id FROM tdbdiff.%s %s WHERE %s)", a, ia, inner.name, ia, where)
		}
		fallthrough
	case 2:
		if t.tenant {
			return fmt.Sprintf("%s.tenant_id IN (SELECT %s.tenant_id FROM tdbdiff.%s %s WHERE %s)", a, ia, inner.name, ia, where)
		}
		fallthrough
	case 3:
		return fmt.Sprintf("NOT EXISTS (SELECT 1 FROM tdbdiff.%s %s WHERE %s)", inner.name, ia, where)
	default:
		if t.tenant {
			return fmt.Sprintf("%s.tenant_id = (SELECT %s.tenant_id FROM tdbdiff.%s %s WHERE %s LIMIT 1)", a, ia, inner.name, ia, where)
		}
		return "true"
	}
}

func (g *dgen) pred(a string, t *dtable, depth int) string {
	if depth > 2 || g.r.Intn(3) == 0 {
		return g.atom(a, t, depth)
	}
	switch g.r.Intn(5) {
	case 0, 1:
		return "(" + g.pred(a, t, depth+1) + " AND " + g.pred(a, t, depth+1) + ")"
	case 2, 3:
		return "(" + g.pred(a, t, depth+1) + " OR " + g.pred(a, t, depth+1) + ")"
	default:
		return "NOT (" + g.pred(a, t, depth+1) + ")"
	}
}

type dquery struct {
	sql    string   // what the tool sees and the database runs
	truth  []string // statements returning the tenant_id of every row the query touches
	tenant bool     // it reads or writes a tenant table
	dml    bool
}

func (g *dgen) selectQuery() dquery {
	names := []string{"customers", "orders", "items", "countries", "shipments"}
	switch g.r.Intn(14) {
	case 12, 13: // x IN (SELECT y ...): the subquery's confinement may carry to the outer table
		j := dJoins[g.r.Intn(len(dJoins))]
		child, parent := dTables[j[0]], dTables[j[2]]
		oa, sa := g.newAlias(), g.newAlias()
		var outer, sub *dtable
		var ocol, scol string
		switch g.r.Intn(3) {
		case 0: // child.fk IN (SELECT parent.id ...)
			outer, sub, ocol, scol = child, parent, j[1], j[3]
		case 1: // parent.id IN (SELECT child.fk ...)
			outer, sub, ocol, scol = parent, child, j[3], j[1]
		default: // tenant column to tenant column
			outer, sub, ocol, scol = dTables[g.pick("customers", "orders", "items")], dTables[g.pick("customers", "orders", "items")], "tenant_id", "tenant_id"
		}
		subWhere := g.pred(sa, sub, 1)
		op := g.pick("IN", "IN", "IN", "NOT IN")
		in := fmt.Sprintf("%s.%s %s (SELECT %s.%s FROM tdbdiff.%s %s WHERE %s)", oa, ocol, op, sa, scol, sub.name, sa, subWhere)
		where := g.pick(in, in, in+" AND "+g.pred(oa, outer, 1), in+" OR "+g.pred(oa, outer, 1), g.pred(oa, outer, 1)+" AND "+in)
		return dquery{
			sql: fmt.Sprintf("SELECT %s.* FROM tdbdiff.%s %s WHERE %s", oa, outer.name, oa, where),
			truth: []string{
				fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.%s %s WHERE %s", oa, outer.name, oa, where),
				fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.%s %s WHERE %s", sa, sub.name, sa, subWhere),
			},
			tenant: true,
		}
	case 5, 6: // a credential row (no tenant column) pins a tenant row through a join
		cust := dTables["customers"]
		ca, ka := g.newAlias(), g.newAlias()
		tok := func() string { return "'k" + g.num(48) + "'" }
		var key string
		switch g.r.Intn(7) {
		case 0, 1:
			key = ka + ".token = " + tok()
		case 2:
			key = fmt.Sprintf("%s.token IN (%s, %s)", ka, tok(), tok())
		case 3:
			key = ka + ".id = " + g.num(48)
		case 4:
			key = fmt.Sprintf("%s.token = %s OR %s.token = %s", ka, tok(), ka, tok())
		case 5:
			key = fmt.Sprintf("(%s.token = %s OR %s)", ka, tok(), g.pred(ca, cust, 1))
		case 6:
			key = fmt.Sprintf("NOT (%s.token = %s)", ka, tok())
		}
		kind := g.pick("JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN")
		join := fmt.Sprintf("%s.owner = %s.id", ka, ca)
		extra := g.pick("", "", " AND "+g.pred(ca, cust, 1))
		var from, where string
		first, second := fmt.Sprintf("tdbdiff.customers %s", ca), fmt.Sprintf("tdbdiff.tokens %s", ka)
		if g.r.Intn(2) == 0 {
			first, second = second, first
		}
		if g.r.Intn(2) == 0 {
			from, where = fmt.Sprintf("%s %s %s ON %s", first, kind, second, join), key+extra
		} else {
			from, where = fmt.Sprintf("%s %s %s ON %s AND %s", first, kind, second, join, key), "true"+extra
		}
		return dquery{
			sql:    fmt.Sprintf("SELECT %s.* FROM %s WHERE %s", ca, from, where),
			truth:  []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM %s WHERE %s", ca, from, where)},
			tenant: true,
		}
	case 0, 1: // join along a foreign key
		j := dJoins[g.r.Intn(len(dJoins))]
		child, parent := dTables[j[0]], dTables[j[2]]
		ca, pa := g.newAlias(), g.newAlias()
		kind := g.pick("JOIN", "INNER JOIN", "LEFT JOIN")
		on := fmt.Sprintf("%s.%s = %s.%s", ca, j[1], pa, j[3])
		where := g.pick(g.pred(ca, child, 1), g.pred(pa, parent, 1), g.pred(ca, child, 1)+" AND "+g.pred(pa, parent, 1))
		from := fmt.Sprintf("tdbdiff.%s %s %s tdbdiff.%s %s ON %s", child.name, ca, kind, parent.name, pa, on)
		return dquery{
			sql:    fmt.Sprintf("SELECT %s.* FROM %s WHERE %s", ca, from, where),
			truth:  []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM %s WHERE %s", ca, from, where), fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM %s WHERE %s", pa, from, where)},
			tenant: true,
		}
	case 2: // a tenant condition in the join's ON clause, not in WHERE
		j := dJoins[g.r.Intn(len(dJoins))]
		child, parent := dTables[j[0]], dTables[j[2]]
		ca, pa := g.newAlias(), g.newAlias()
		kind := g.pick("JOIN", "LEFT JOIN", "RIGHT JOIN")
		on := fmt.Sprintf("%s.%s = %s.%s AND %s.tenant_id = %s", ca, j[1], pa, j[3], pa, g.tid())
		from := fmt.Sprintf("tdbdiff.%s %s %s tdbdiff.%s %s ON %s", child.name, ca, kind, parent.name, pa, on)
		return dquery{
			sql:    fmt.Sprintf("SELECT %s.* FROM %s", ca, from),
			truth:  []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM %s", ca, from), fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM %s", pa, from)},
			tenant: true,
		}
	case 3: // CTE
		t := dTables["orders"]
		a, b := g.newAlias(), g.newAlias()
		inner := g.pred(a, t, 1)
		outer := g.pred(b, t, 1)
		core := fmt.Sprintf("WITH x AS (SELECT %s.id FROM tdbdiff.orders %s WHERE %s) SELECT %s.* FROM tdbdiff.orders %s WHERE %s.id IN (SELECT id FROM x) AND %s", a, a, inner, b, b, b, outer)
		truth := fmt.Sprintf("WITH x AS (SELECT %s.id FROM tdbdiff.orders %s WHERE %s) SELECT DISTINCT %s.tenant_id FROM tdbdiff.orders %s WHERE %s.id IN (SELECT id FROM x) AND %s", a, a, inner, b, b, b, outer)
		return dquery{sql: core, truth: []string{truth}, tenant: true}
	case 4: // UNION ALL
		t1, t2 := dTables[names[g.r.Intn(3)]], dTables[names[g.r.Intn(3)]]
		a, b := g.newAlias(), g.newAlias()
		w1, w2 := g.pred(a, t1, 1), g.pred(b, t2, 1)
		return dquery{
			sql:    fmt.Sprintf("SELECT %s.id FROM tdbdiff.%s %s WHERE %s UNION ALL SELECT %s.id FROM tdbdiff.%s %s WHERE %s", a, t1.name, a, w1, b, t2.name, b, w2),
			truth:  []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.%s %s WHERE %s", a, t1.name, a, w1), fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.%s %s WHERE %s", b, t2.name, b, w2)},
			tenant: true,
		}
	default: // one table
		t := dTables[names[g.r.Intn(len(names))]]
		a := g.newAlias()
		where := g.pred(a, t, 0)
		tail := g.pick("", "", " ORDER BY 1", " LIMIT 5")
		q := dquery{sql: fmt.Sprintf("SELECT %s.* FROM tdbdiff.%s %s WHERE %s%s", a, t.name, a, where, tail), tenant: t.tenant}
		if t.tenant {
			q.truth = []string{fmt.Sprintf("SELECT DISTINCT %s.tenant_id FROM tdbdiff.%s %s WHERE %s", a, t.name, a, where)}
		}
		return q
	}
}

func (g *dgen) dmlQuery() dquery {
	t := dTables[g.pick("orders", "items", "customers")]
	where := g.pred(t.name, t, 1)
	set := map[string]string{"orders": "amount = amount", "items": "qty = qty", "customers": "name = name"}[t.name]
	if g.r.Intn(2) == 0 {
		return dquery{sql: fmt.Sprintf("UPDATE tdbdiff.%s SET %s WHERE %s RETURNING tenant_id", t.name, set, where), tenant: true, dml: true}
	}
	return dquery{sql: fmt.Sprintf("DELETE FROM tdbdiff.%s WHERE %s RETURNING tenant_id", t.name, where), tenant: true, dml: true}
}

func TestDifferentialSoundness(t *testing.T) {
	dsn := os.Getenv("TENANTDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TENANTDB_TEST_DSN not set")
	}
	n := 1500
	if v, err := strconv.Atoi(os.Getenv("TENANTDB_FUZZ_N")); err == nil && v > 0 {
		n = v
	}
	seed := int64(1)
	if v, err := strconv.ParseInt(os.Getenv("TENANTDB_FUZZ_SEED"), 10, 64); err == nil {
		seed = v
	}
	t.Logf("seed=%d n=%d (TENANTDB_FUZZ_SEED / TENANTDB_FUZZ_N to change)", seed, n)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, diffSchemaSQL); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := conn.Exec(ctx, diffExtraSchemaSQL); err != nil {
		t.Fatalf("extra schema: %v", err)
	}
	defer conn.Exec(ctx, "DROP SCHEMA IF EXISTS tdbdiff CASCADE")

	cat := NewCatalog(dsn)
	cat.TenantColumns = []string{"tenant_id"}
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}

	type config struct {
		name string
		a    *Attributor
		evs  []Event
	}
	cfgs := []*config{{name: "catalog only"}, {name: "warm cache + row lookups"}}
	for _, c := range cfgs {
		c := c
		opt := Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat}
		if c.name != "catalog only" {
			lk, err := NewLookup(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer lk.Close()
			opt.Lookup = lk
			opt.Strict = StrictOptions{Mode: StrictBlock} // makes lookups finish before the event is reported
		}
		c.a = New(opt, func(e Event) { c.evs = append(c.evs, e) })
		if c.name != "catalog only" {
			if err := c.a.Warm(ctx, dsn, 100000, discardLogger()); err != nil {
				t.Fatal(err)
			}
		}
	}

	// A connection that sets the search_path, as an application would.
	sessions := []*proxy.Session{{ID: 1, User: "postgres", DB: "postgres"}, {ID: 2, User: "postgres", DB: "postgres"}}
	for i, c := range cfgs {
		c.a.OnFrontend(sessions[i], &pgproto3.Query{String: "SET search_path = tdbdiff"})
		c.a.OnBackend(sessions[i], &pgproto3.CommandComplete{CommandTag: []byte("SET")})
		c.a.OnBackend(sessions[i], &pgproto3.ReadyForQuery{TxStatus: 'I'})
		c.evs = nil
	}
	if _, err := conn.Exec(ctx, "SET search_path = tdbdiff"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "SET app.tenant = 't2'"); err != nil {
		t.Fatal(err)
	}
	for i, c := range cfgs {
		c.a.OnFrontend(sessions[i], &pgproto3.Query{String: "SET app.tenant = 't2'"})
		c.a.OnBackend(sessions[i], &pgproto3.CommandComplete{CommandTag: []byte("SET")})
		c.a.OnBackend(sessions[i], &pgproto3.ReadyForQuery{TxStatus: 'I'})
		c.evs = nil
	}

	g := &dgen{r: rand.New(rand.NewSource(seed))}
	type failure struct{ cfg, sql, claim, truth string }
	var failures []failure
	stats := map[string]map[Source]int{}
	ran := 0
	for i := 0; i < n; i++ {
		if i%250 == 249 {
			cat.DrainSuggestions(10 * time.Second)
		}
		var q dquery
		switch k := g.r.Intn(10); {
		case k < 1:
			q = g.dmlQuery()
		case k < 4:
			q = g.extraQuery()
		default:
			q = g.selectQuery()
		}
		// Ground truth: the tenants of the rows the query really touches.
		truth := map[string]bool{}
		ok := true
		if q.dml {
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := tx.Query(ctx, q.sql)
			if err != nil {
				ok = false
			} else {
				for rows.Next() {
					vals, verr := rows.Values()
					if verr != nil {
						continue
					}
					for _, v := range vals {
						if str, isStr := v.(string); isStr {
							truth[str] = true
						}
					}
				}
				rows.Close()
				ok = rows.Err() == nil
			}
			_ = tx.Rollback(ctx)
		} else {
			for _, tq := range q.truth {
				rows, err := conn.Query(ctx, tq)
				if err != nil {
					ok = false
					break
				}
				for rows.Next() {
					var tn *string
					if rows.Scan(&tn) == nil && tn != nil {
						truth[*tn] = true
					}
				}
				rows.Close()
				if rows.Err() != nil {
					ok = false
					break
				}
			}
		}
		if !ok {
			continue // the generator produced something Postgres rejects
		}
		ran++
		tl := sortedSet(truth)
		useParams := g.r.Intn(10) < 3

		for ci, c := range cfgs {
			c.evs = nil
			sess := sessions[ci]
			if psql, pvals := paramize(q.sql); useParams && len(pvals) > 0 {
				c.a.OnFrontend(sess, &pgproto3.Parse{Name: "", Query: psql})
				c.a.OnFrontend(sess, &pgproto3.Bind{Parameters: pvals})
				c.a.OnFrontend(sess, &pgproto3.Execute{})
				c.a.OnFrontend(sess, &pgproto3.Sync{})
			} else {
				c.a.OnFrontend(sess, &pgproto3.Query{String: q.sql})
			}
			c.a.OnBackend(sess, &pgproto3.CommandComplete{CommandTag: []byte("OK")})
			c.a.OnBackend(sess, &pgproto3.ReadyForQuery{TxStatus: 'I'})
			if stats[c.name] == nil {
				stats[c.name] = map[Source]int{}
			}
			for _, e := range c.evs {
				stats[c.name][e.Source]++
				switch e.Source {
				case SourceExplicit, SourceCache, SourceLookup, SourceDerived, SourceSession, SourceTransaction:
					claim := map[string]bool{}
					for _, tn := range e.Tenants {
						claim[tn] = true
					}
					for tn := range truth {
						if !claim[tn] {
							failures = append(failures, failure{c.name, q.sql, strings.Join(sortedSet(claim), ","), strings.Join(tl, ",")})
							break
						}
					}
				case SourceUntenanted:
					if q.tenant {
						failures = append(failures, failure{c.name, q.sql, "untenanted", strings.Join(tl, ",")})
					}
				case SourceNoMatch:
					if len(truth) > 0 {
						failures = append(failures, failure{c.name, q.sql, "nomatch", strings.Join(tl, ",")})
					}
				}
			}
		}
	}

	for name, m := range stats {
		var parts []string
		for s, c := range m {
			parts = append(parts, fmt.Sprintf("%s=%d", s, c))
		}
		sort.Strings(parts)
		t.Logf("%-26s %s", name, strings.Join(parts, " "))
	}
	t.Logf("%d queries ran against Postgres; %d unsound attributions", ran, len(failures))
	seen := map[string]bool{}
	shown := 0
	for _, f := range failures {
		key := f.cfg + f.sql
		if seen[key] || shown >= 25 {
			continue
		}
		seen[key] = true
		shown++
		t.Errorf("UNSOUND [%s]\n  sql:    %s\n  claim:  [%s]\n  actual: [%s]", f.cfg, f.sql, f.claim, f.truth)
	}
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

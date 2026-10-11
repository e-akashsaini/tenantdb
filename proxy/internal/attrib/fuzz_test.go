package attrib

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Coverage-guided fuzzing. Run one for a while with:
//
//	go test -run xxx -fuzz FuzzAttributor -fuzztime 2m ./internal/attrib
//
// The invariants below are about the shape of the answer, not its truth (the
// differential test covers that): whatever bytes arrive, nothing may panic or
// hang, and every event must be internally consistent.

func fuzzSeeds(f *testing.F) {
	for _, s := range []string{
		`SELECT * FROM orders WHERE tenant_id = 'acme'`,
		`SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.tenant_id = $1`,
		`UPDATE orders SET amount = 0 WHERE tenant_id = 'a' OR id = 5 RETURNING *`,
		`WITH x AS (SELECT * FROM orders) SELECT * FROM x UNION ALL SELECT * FROM orders WHERE tenant_id = 'a'`,
		`INSERT INTO orders (id, tenant_id) VALUES (1, 'a'), (2, 'b') RETURNING id`,
		`SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE tenant_id = 'a')`,
		`SELECT orders_of('a'), all_orders()`,
		`DO $$ BEGIN PERFORM 1 FROM orders; END $$`,
		`COPY orders TO STDOUT`, `PREPARE q AS SELECT * FROM orders WHERE id = $1`, `EXECUTE q(1)`,
		`CREATE TEMP TABLE t AS SELECT * FROM orders WHERE tenant_id = 'a'`, `SELECT * FROM t`,
		`SET tenantdb.tenant = 'a'`, `SELECT set_config('search_path', 't2', false)`, `BEGIN`, `COMMIT`,
		`SELECT * FROM orders WHERE record_type = 'Template' AND record_id = 5`,
		`SELECT * FROM orders WHERE tenant_id = ANY(ARRAY['a','b'])`,
		`SELECT $1::int + 1`, `;;;`, ``, `SELECT '`, `/* ` + strings.Repeat("(", 100),
	} {
		f.Add(s)
	}
	if b, err := os.ReadFile("testdata/rails_shapes.sql"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if line != "" {
				f.Add(line)
			}
		}
	}
}

func FuzzExtractor(f *testing.F) {
	fuzzSeeds(f)
	e := NewExtractor([]string{"tenant_id"}, []string{"id"})
	f.Fuzz(func(t *testing.T, sql string) {
		a, err1 := e.Parse(sql)
		b, err2 := e.Parse(sql)
		if (err1 == nil) != (err2 == nil) {
			t.Fatalf("parsing is not deterministic: %v vs %v", err1, err2)
		}
		if err1 == nil && len(a) != len(b) {
			t.Fatalf("different statement counts: %d vs %d", len(a), len(b))
		}
		for _, st := range a {
			if st.Q != nil {
				checkModel(t, st.Q, map[string]bool{})
			}
		}
	})
}

// checkModel: every reference in the model points at an occurrence that exists.
func checkModel(t *testing.T, q *QueryNode, seen map[string]bool) {
	t.Helper()
	for _, o := range q.Occs {
		if o.ID == "" || seen[o.ID] {
			t.Fatalf("occurrence id %q empty or reused", o.ID)
		}
		seen[o.ID] = true
	}
	for _, s := range q.Subs {
		checkModel(t, s, seen)
	}
}

func FuzzAttributor(f *testing.F) {
	fuzzSeeds(f)
	spec := soundSpec()
	cat := spec.build()
	f.Fuzz(func(t *testing.T, sql string) {
		var events []Event
		a := New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat,
			Strict: StrictOptions{Mode: StrictAlert}}, func(e Event) { events = append(events, e) })
		s := &proxy.Session{ID: 1, User: "app", DB: "d"}
		a.Cache().Put("public.orders", "id", "1", "t1")

		// as a simple query, then as a prepared statement, then replies and a second round
		a.OnFrontend(s, &pgproto3.Query{String: sql})
		a.OnBackend(s, &pgproto3.CommandComplete{CommandTag: []byte("OK")})
		a.OnBackend(s, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		a.OnFrontend(s, &pgproto3.Parse{Name: "p", Query: sql})
		a.OnFrontend(s, &pgproto3.Bind{PreparedStatement: "p", Parameters: [][]byte{[]byte("t1"), []byte("1"), nil}})
		a.OnFrontend(s, &pgproto3.Execute{})
		a.OnFrontend(s, &pgproto3.Sync{})
		a.OnBackend(s, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		a.OnFrontend(s, &pgproto3.Query{String: `SELECT * FROM public.orders WHERE tenant_id = 'after'`})

		for _, e := range events {
			checkEvent(t, e, sql)
		}
		last := events[len(events)-1]
		if last.SQL != `SELECT * FROM public.orders WHERE tenant_id = 'after'` || last.Source != SourceExplicit {
			t.Fatalf("the session was damaged by %.80q: later statement became %+v", sql, last)
		}
	})
}

func checkEvent(t *testing.T, e Event, sql string) {
	t.Helper()
	bad := func(msg string) { t.Fatalf("%s for %.100q: %+v", msg, sql, e) }
	if e.Source == "" {
		bad("event without a source")
	}
	switch e.Source {
	case SourceExplicit, SourceCache, SourceLookup, SourceDerived, SourceSession, SourceTransaction:
		if len(e.Tenants) == 0 {
			bad("a tenant source with no tenants")
		}
	case SourceNoMatch, SourceUntenanted, SourceScan, SourceUnknown:
		if len(e.Tenants) != 0 {
			bad("a source that names no tenant carries tenants")
		}
	default:
		bad("unknown source")
	}
	if e.Source == SourceUntenanted && e.Scan {
		bad("untenanted yet scan")
	}
	if e.Source == SourceScan && !e.Scan {
		bad("scan source without the scan flag")
	}
	if e.Unparsed && e.Source != SourceUnknown {
		bad("an unparsed statement must be unknown")
	}
	if e.Blocked {
		bad("nothing is blocked in alert mode")
	}
	seen := map[string]bool{}
	for _, tn := range e.Tenants {
		if seen[tn] {
			bad("duplicate tenant")
		}
		seen[tn] = true
	}
	// violations follow the policy
	wantViolation := ""
	switch {
	case e.Conflict:
		wantViolation = "conflict"
	case e.Scan:
		wantViolation = "scan"
	case e.Source == SourceUnknown:
		wantViolation = "unknown"
	}
	if e.Violation != wantViolation {
		bad("violation does not follow the policy: want " + wantViolation)
	}
	_ = reflect.DeepEqual
}

// richSpec is a schema with views, triggers, rules and row-level security, so the
// fuzzers reach the code that reads them.
func richSpec() catSpec {
	sp := viewSpec()
	sp.rows = append(sp.rows, tbl("public", "audit_log", 'r', "id", "tenant_id", "msg")...)
	sp.unique["public.audit_log"] = []string{"id"}
	tf := func(name, src string) *FuncDef {
		return &FuncDef{Schema: "public", Name: name, Lang: "plpgsql", Kind: 'f', Src: src, Ret: "trigger"}
	}
	sp.funcs = append(sp.funcs,
		tf("audit_row", `BEGIN INSERT INTO audit_log (tenant_id, msg) VALUES (NEW.tenant_id, 'x'); RETURN NEW; END`),
		tf("wipe", `BEGIN DELETE FROM audit_log WHERE msg = 'x'; RETURN NEW; END`))
	sp.triggers = map[string][]trigRow{
		"public.items":     {{typ: trigInsert | trigUpdate, fn: "public.audit_row"}},
		"public.customers": {{typ: trigDelete, fn: "public.wipe"}},
	}
	sp.ruled = map[string]bool{"public.countries": true}
	sp.rls = []string{"public.orders"}
	sp.rlsd = &rlsData{
		roles:    map[string]roleInfo{"app": {}, "admin": {super: true}},
		members:  map[string][]string{},
		tables:   map[string]rlsTable{"public.orders": {owner: "owner"}},
		policies: map[string][]policy{"public.orders": {{cmd: '*', permissive: true, qual: "(tenant_id = current_setting('app.tenant'::text))"}}},
	}
	return sp
}

func FuzzAttributorRich(f *testing.F) {
	fuzzSeeds(f)
	for _, s := range []string{
		`SELECT * FROM acme_orders WHERE amount > 5`, `SELECT * FROM cur_orders`, `SELECT * FROM orders_t WHERE t = 't2'`,
		`SELECT * FROM big_orders`, `SELECT * FROM order_customer WHERE tid = 't3'`, `SELECT * FROM limited`,
		`INSERT INTO items (id, tenant_id, order_id) VALUES (1, 't1', 2)`, `DELETE FROM customers WHERE tenant_id = 't1'`,
		`UPDATE items SET sku = 'a' WHERE order_id IN (SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tenant_id = 't1'))`,
		`SET ROLE admin`, `RESET ROLE`, `SET LOCAL ROLE app`, `SET SESSION AUTHORIZATION admin`, `SET app.tenant = 't2'`,
		`SELECT * FROM orders`, `UPDATE orders SET status = 'x'`, `COPY orders (id, tenant_id) FROM STDIN`,
		`COPY orders FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';')`, `COPY audit_log FROM STDIN`,
		`SELECT * FROM orders WHERE customer_id = ANY (SELECT id FROM customers)`,
		`SELECT * FROM items i JOIN audit_log a ON a.id = i.id WHERE a.tenant_id = 't1'`,
	} {
		f.Add(s)
	}
	cat := richSpec().build()
	f.Fuzz(func(t *testing.T, sql string) {
		var events []Event
		a := New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat,
			Strict: StrictOptions{Mode: StrictAlert}}, func(e Event) { events = append(events, e) })
		s := &proxy.Session{ID: 1, User: "app", DB: "d"}
		a.Cache().Put("public.orders", "id", "1", "t1")
		a.OnFrontend(s, &pgproto3.Query{String: `SET app.tenant = 't1'`})
		a.OnBackend(s, &pgproto3.CommandComplete{CommandTag: []byte("SET")})
		a.OnBackend(s, &pgproto3.ReadyForQuery{TxStatus: 'I'})

		a.OnFrontend(s, &pgproto3.Query{String: sql})
		// a COPY waits for its data; feed some and finish
		a.OnFrontend(s, &pgproto3.CopyData{Data: []byte("1\tt1\tx\n2\tt2\ty\n")})
		a.OnFrontend(s, &pgproto3.CopyDone{})
		a.OnBackend(s, &pgproto3.CommandComplete{CommandTag: []byte("OK")})
		a.OnBackend(s, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		a.OnFrontend(s, &pgproto3.Parse{Name: "p", Query: sql})
		a.OnFrontend(s, &pgproto3.Bind{PreparedStatement: "p", Parameters: [][]byte{[]byte("t1"), []byte("1"), nil}})
		a.OnFrontend(s, &pgproto3.Execute{})
		a.OnFrontend(s, &pgproto3.Sync{})
		a.OnBackend(s, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		a.OnFrontend(s, &pgproto3.Query{String: `RESET ROLE`})
		a.OnBackend(s, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		a.OnFrontend(s, &pgproto3.Query{String: `SELECT * FROM public.customers WHERE tenant_id = 'after'`})

		for _, e := range events {
			checkEvent(t, e, sql)
		}
		if len(events) == 0 {
			t.Fatalf("no events at all for %.80q", sql)
		}
	})
}

func FuzzCopyParser(f *testing.F) {
	f.Add([]byte("1\tacme\tx\n2\tglobex\ty\n"), byte('\t'), false, false, uint8(3))
	f.Add([]byte("id,tenant,note\n1,\"a,b\",x\n2,\"say \"\"hi\"\"\",y\n"), byte(','), true, true, uint8(1))
	f.Add([]byte(`\N`+"\t"+`\\`+"\t"+`\x41`+"\n"+`\.`+"\nafter\n"), byte('\t'), false, false, uint8(7))
	f.Add([]byte("a|\"b\nc\"|d\r\n"), byte('|'), true, false, uint8(2))
	f.Fuzz(func(t *testing.T, data []byte, delim byte, csv, header bool, chunk uint8) {
		if delim == '\n' || delim == '\r' || delim == '\\' || delim == '"' || delim == 0 {
			delim = ','
		}
		sp := &CopySpec{Format: "text", Delim: delim, Null: `\N`, Quote: '"', Escape: '"', Header: header}
		if csv {
			sp.Format, sp.Null = "csv", ""
		}
		rows := 0
		p := newCopyParser(sp, 1, func(v string, null, ok bool) {
			rows++
			if !ok && (v != "" || null) {
				t.Fatalf("a row that could not be read carries a value: %q %v", v, null)
			}
			if len(v) > copyMaxField {
				t.Fatalf("a value longer than the cap: %d", len(v))
			}
		})
		n := int(chunk%16) + 1
		for b := data; len(b) > 0; {
			k := n
			if k > len(b) {
				k = len(b)
			}
			p.Write(b[:k])
			b = b[k:]
		}
		p.Close()
		// reading the same bytes in one piece gives the same rows
		var one, split []copyRow
		collect := func(dst *[]copyRow) *copyParser {
			return newCopyParser(sp, 1, func(v string, null, ok bool) { *dst = append(*dst, copyRow{v, null, ok}) })
		}
		a, b := collect(&one), collect(&split)
		a.Write(data)
		a.Close()
		for i := 0; i < len(data); i++ {
			b.Write(data[i : i+1])
		}
		b.Close()
		if !reflect.DeepEqual(one, split) {
			t.Fatalf("chunking changed the result:\n whole %+v\n bytes %+v\n for %q", one, split, data)
		}
	})
}

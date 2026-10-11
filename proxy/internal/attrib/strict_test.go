package attrib

import (
	"errors"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

func strictHarness(opt StrictOptions) *harness {
	h := &harness{s: &proxy.Session{ID: 1, User: "app", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: testCatalog(), Strict: opt}, h.record)
	return h
}

func (h *harness) gateQuery(sql string) *proxy.Rejection {
	return h.a.gate(h.s, &pgproto3.Query{String: sql})
}

func (h *harness) gate(m pgproto3.FrontendMessage) *proxy.Rejection { return h.a.gate(h.s, m) }

func TestParseStrictMode(t *testing.T) {
	for in, want := range map[string]StrictMode{"": StrictOff, "OFF": StrictOff, " alert ": StrictAlert, "block": StrictBlock} {
		if got, err := ParseStrictMode(in); err != nil || got != want {
			t.Errorf("%q -> %q, %v", in, got, err)
		}
	}
	if _, err := ParseStrictMode("deny"); err == nil {
		t.Error("expected an error for an unknown mode")
	}
}

func TestPolicyFlagsButNeverBlocksInAlertMode(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictAlert})
	cases := []struct {
		sql, violation string
	}{
		{`SELECT * FROM orders WHERE id = 5`, "unknown"},
		{`SELECT * FROM orders`, "scan"},
		{`SELECT * FROM countries`, ""},
		{`SELECT * FROM orders WHERE tenant_id = 'acme'`, ""},
		{`SELECT my_func()`, "unknown"},
	}
	for _, c := range cases {
		h.query(c.sql)
		if e := h.last(t); e.Violation != c.violation || e.Blocked {
			t.Errorf("%s: violation=%q blocked=%v, want %q unblocked", c.sql, e.Violation, e.Blocked, c.violation)
		}
	}
	if _, ok := h.a.Observer().(proxy.Gate); ok {
		t.Error("alert mode must not turn on the gated relay")
	}
}

func TestPolicyOffFlagsNothing(t *testing.T) {
	h := strictHarness(StrictOptions{})
	h.query(`SELECT * FROM orders`)
	if e := h.last(t); e.Violation != "" {
		t.Errorf("%+v", e)
	}
}

func TestExemptAndAllowScanRoles(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictAlert, Exempt: []string{"migrator"}, AllowScan: []string{"reports"}})
	h.s.User = "migrator"
	h.query(`SELECT * FROM orders WHERE id = 5`)
	h.query(`SELECT * FROM orders`)
	for _, e := range h.events {
		if e.Violation != "" {
			t.Errorf("exempt role flagged: %+v", e)
		}
	}
	h2 := strictHarness(StrictOptions{Mode: StrictAlert, AllowScan: []string{"reports"}})
	h2.s.User = "reports"
	h2.query(`SELECT * FROM orders`)
	if e := h2.last(t); e.Violation != "" || e.Source != SourceScan {
		t.Errorf("allowed scan: %+v", e)
	}
	h2.query(`SELECT * FROM orders WHERE id = 5`)
	if e := h2.last(t); e.Violation != "unknown" {
		t.Errorf("allow-scan does not excuse unknown: %+v", e)
	}
}

func TestBlockRejectsViolationsAndPassesTheRest(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	if _, ok := h.a.Observer().(proxy.Gate); !ok {
		t.Fatal("block mode must expose a Gate")
	}
	if rej := h.gateQuery(`SELECT * FROM orders WHERE tenant_id = 'acme'`); rej != nil {
		t.Fatalf("attributed statement rejected: %+v", rej)
	}
	if rej := h.gateQuery(`SELECT * FROM countries`); rej != nil {
		t.Fatalf("shared-table statement rejected: %+v", rej)
	}
	for _, q := range []string{`SELECT * FROM orders WHERE id = 5`, `SELECT * FROM orders`, `SELECT my_func()`} {
		rej := h.gateQuery(q)
		if rej == nil || rej.Code != "42501" {
			t.Errorf("%s: rejection = %+v", q, rej)
			continue
		}
		if e := h.last(t); !e.Blocked || e.Violation == "" {
			t.Errorf("%s: event = %+v", q, e)
		}
	}
	// Non-statement traffic is never touched.
	if rej := h.gate(&pgproto3.Terminate{}); rej != nil {
		t.Errorf("terminate rejected: %+v", rej)
	}
}

func TestRejectedMessageLeavesNoTrace(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	// One Query that opens a transaction, changes search_path, then violates.
	if rej := h.gateQuery(`BEGIN; SET search_path = t2; SELECT * FROM countries; SELECT * FROM orders WHERE id = 5`); rej == nil {
		t.Fatal("expected rejection")
	}
	st := h.a.st(h.s)
	st.mu.Lock()
	inTxn, qlen, path, tenants := st.inTxn, len(st.queue), st.path, st.txnTenant
	st.mu.Unlock()
	if inTxn || qlen != 0 || path != nil || len(tenants) != 0 {
		t.Errorf("state leaked: inTxn=%v queue=%d path=%v txnTenant=%v", inTxn, qlen, path, tenants)
	}
	h.mu.Lock()
	for _, e := range h.events {
		if !e.Blocked {
			t.Errorf("a statement of a rejected message was reported as run: %+v", e)
		}
	}
	h.mu.Unlock()
	// The reply queue is still aligned: the next statement's learning works.
	if rej := h.gateQuery(`INSERT INTO orders (id, tenant_id) VALUES (1, 'acme')`); rej != nil {
		t.Fatalf("%+v", rej)
	}
	h.reply(nil, nil, "INSERT 0 1")
	if _, ok := h.a.Cache().Get("public.orders", "id", "1"); !ok {
		t.Error("reply no longer lines up with its statement after a rejection")
	}
}

func TestRejectedBatchForgetsItsPreparedStatements(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	h.gate(&pgproto3.Parse{Name: "good", Query: `SELECT * FROM orders WHERE tenant_id = $1`})
	h.gate(&pgproto3.Sync{})
	h.gate(&pgproto3.Parse{Name: "bad", Query: `SELECT * FROM orders WHERE id = $1`})
	h.gate(&pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "bad", Parameters: [][]byte{[]byte("5")}})
	if rej := h.gate(&pgproto3.Execute{Portal: "p"}); rej == nil {
		t.Fatal("expected rejection")
	}
	st := h.a.st(h.s)
	st.mu.Lock()
	_, hasBad := st.prepared["bad"]
	_, hasPortal := st.portals["p"]
	_, hasGood := st.prepared["good"]
	st.mu.Unlock()
	if hasBad || hasPortal {
		t.Error("the database never saw the rejected batch, but its statements are remembered")
	}
	if !hasGood {
		t.Error("a statement from an earlier, synced batch must survive")
	}
	h.gate(&pgproto3.Sync{})
	h.gate(&pgproto3.Bind{DestinationPortal: "", PreparedStatement: "good", Parameters: [][]byte{[]byte("acme")}})
	if rej := h.gate(&pgproto3.Execute{}); rej != nil {
		t.Errorf("good statement rejected: %+v", rej)
	}
}

func TestBlockResolvesLookupsBeforeDeciding(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	h.a.lk = &fakeLookup{rows: map[string][]string{"public.orders|id|42": {"acme"}}}
	if rej := h.gateQuery(`SELECT * FROM orders WHERE id = 42`); rej != nil {
		t.Fatalf("lookup-resolved statement rejected: %+v", rej)
	}
	// The event exists by the time Gate returns: no waiting.
	h.mu.Lock()
	n := len(h.events)
	h.mu.Unlock()
	if n != 1 {
		t.Fatalf("events = %d, want the lookup result already delivered", n)
	}
	if e := h.last(t); e.Source != SourceLookup || e.Blocked {
		t.Errorf("%+v", e)
	}
	// A row that does not exist is not a violation...
	if rej := h.gateQuery(`SELECT * FROM orders WHERE id = 7`); rej != nil {
		t.Errorf("nomatch rejected: %+v", rej)
	}
	// ...but a failing database is: nothing is known, so nothing is allowed.
	h.a.lk = &fakeLookup{err: errors.New("down")}
	if rej := h.gateQuery(`SELECT * FROM orders WHERE id = 8`); rej == nil {
		t.Error("an unresolved statement must be blocked when the lookup fails")
	}
}

func TestBlockRejectsTagConflicts(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	if rej := h.gateQuery(`SET tenantdb.tenant = 'acme'`); rej != nil {
		t.Fatalf("%+v", rej)
	}
	if rej := h.gateQuery(`SELECT * FROM orders WHERE tenant_id = 'acme'`); rej != nil {
		t.Fatalf("own tenant rejected: %+v", rej)
	}
	rej := h.gateQuery(`SELECT * FROM orders WHERE tenant_id = 'globex'`)
	if rej == nil {
		t.Fatal("another tenant's data must be blocked")
	}
	if e := h.last(t); e.Violation != "conflict" || !e.Blocked {
		t.Errorf("%+v", e)
	}
	// An unfiltered read on a tagged connection is a scan, still blocked.
	if rej := h.gateQuery(`SELECT * FROM orders`); rej == nil {
		t.Error("a tagged scan must be blocked")
	}
}

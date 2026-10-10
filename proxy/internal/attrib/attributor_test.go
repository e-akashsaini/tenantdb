package attrib

import (
	"reflect"
	"testing"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

type harness struct {
	a      *Attributor
	s      *proxy.Session
	events []Event
}

func newHarness() *harness {
	h := &harness{s: &proxy.Session{ID: 1, User: "u", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}}, func(e Event) { h.events = append(h.events, e) })
	return h
}

func (h *harness) query(sql string)                 { h.a.OnFrontend(h.s, &pgproto3.Query{String: sql}) }
func (h *harness) rfq(status byte)                  { h.a.OnBackend(h.s, &pgproto3.ReadyForQuery{TxStatus: status}) }
func (h *harness) front(m pgproto3.FrontendMessage) { h.a.OnFrontend(h.s, m) }

func (h *harness) last(t *testing.T) Event {
	t.Helper()
	if len(h.events) == 0 {
		t.Fatal("no event emitted")
	}
	return h.events[len(h.events)-1]
}

func TestSimpleQueryExplicit(t *testing.T) {
	h := newHarness()
	h.query(`SELECT * FROM orders WHERE tenant_id = 'acme'`)
	e := h.last(t)
	if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) || e.User != "u" || e.DB != "d" {
		t.Errorf("event = %+v", e)
	}
}

func TestUnknownWhenNoTenant(t *testing.T) {
	h := newHarness()
	h.query(`SELECT * FROM orders WHERE id = 42`)
	if e := h.last(t); e.Source != SourceUnknown || e.Tenants != nil {
		t.Errorf("event = %+v", e)
	}
}

func TestNonDMLIsNotEmitted(t *testing.T) {
	h := newHarness()
	h.query(`SET search_path = public`)
	h.query(`BEGIN`)
	if len(h.events) != 0 {
		t.Errorf("unexpected events: %+v", h.events)
	}
}

func TestTransactionInheritance(t *testing.T) {
	h := newHarness()
	h.query(`BEGIN`)
	h.query(`SELECT * FROM orders WHERE tenant_id = 'acme'`)
	h.query(`SELECT * FROM items WHERE id = 1`)
	if e := h.last(t); e.Source != SourceTransaction || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("in txn: %+v", e)
	}
	h.query(`COMMIT`)
	h.query(`SELECT * FROM items WHERE id = 1`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("after commit: %+v", e)
	}
}

func TestInheritanceDoesNotLeakAcrossAutocommit(t *testing.T) {
	h := newHarness()
	h.query(`SELECT * FROM orders WHERE tenant_id = 'acme'`)
	h.rfq('I')
	h.query(`SELECT * FROM items WHERE id = 1`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("leaked: %+v", e)
	}
}

func TestMultiStatementQuery(t *testing.T) {
	h := newHarness()
	h.query(`BEGIN; UPDATE t SET x = 1 WHERE tenant_id = 'a'; DELETE FROM u WHERE id = 3; COMMIT`)
	if len(h.events) != 2 {
		t.Fatalf("events = %+v", h.events)
	}
	if h.events[1].Source != SourceTransaction || h.events[1].Tenants[0] != "a" {
		t.Errorf("second = %+v", h.events[1])
	}
}

func TestExtendedProtocolBoundParam(t *testing.T) {
	h := newHarness()
	h.front(&pgproto3.Parse{Name: "s1", Query: `SELECT * FROM orders WHERE tenant_id = $1 AND id = $2`})
	h.front(&pgproto3.Bind{DestinationPortal: "", PreparedStatement: "s1", Parameters: [][]byte{[]byte("acme"), []byte("42")}})
	if len(h.events) != 0 {
		t.Fatal("emitted before Execute")
	}
	h.front(&pgproto3.Execute{Portal: ""})
	if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("event = %+v", e)
	}
	// Reuse the prepared statement with another tenant.
	h.front(&pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("globex"), []byte("1")}})
	h.front(&pgproto3.Execute{})
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"globex"}) {
		t.Errorf("reuse: %+v", e)
	}
}

func TestBinaryUUIDParam(t *testing.T) {
	h := newHarness()
	id := []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	h.front(&pgproto3.Parse{Name: "s", Query: `SELECT 1 FROM t WHERE tenant_id = $1`})
	h.front(&pgproto3.Bind{PreparedStatement: "s", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{id}})
	h.front(&pgproto3.Execute{})
	want := "12345678-9abc-def0-1234-56789abcdef0"
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{want}) {
		t.Errorf("event = %+v", e)
	}
}

func TestInsertWithParams(t *testing.T) {
	h := newHarness()
	h.front(&pgproto3.Parse{Name: "i", Query: `INSERT INTO orders (id, tenant_id) VALUES ($1, $2)`})
	h.front(&pgproto3.Bind{PreparedStatement: "i", Parameters: [][]byte{[]byte("9"), []byte("acme")}})
	h.front(&pgproto3.Execute{})
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("event = %+v", e)
	}
}

// reply feeds the server's answer for one statement back through the observer.
func (h *harness) reply(fields []pgproto3.FieldDescription, rows [][]string, tag string) {
	if fields != nil {
		h.a.OnBackend(h.s, &pgproto3.RowDescription{Fields: fields})
		for _, r := range rows {
			vals := make([][]byte, len(r))
			for i, v := range r {
				vals[i] = []byte(v)
			}
			h.a.OnBackend(h.s, &pgproto3.DataRow{Values: vals})
		}
	}
	h.a.OnBackend(h.s, &pgproto3.CommandComplete{CommandTag: []byte(tag)})
}

func cols(oid uint32, names ...string) []pgproto3.FieldDescription {
	var f []pgproto3.FieldDescription
	for _, n := range names {
		f = append(f, pgproto3.FieldDescription{Name: []byte(n), TableOID: oid})
	}
	return f
}

func TestCacheLearnsFromInsert(t *testing.T) {
	h := newHarness()
	h.query(`INSERT INTO orders (id, tenant_id) VALUES (7, 'acme')`)
	h.reply(nil, nil, "INSERT 0 1")
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE id = 7`)
	if e := h.last(t); e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("event = %+v", e)
	}
}

func TestCacheNotLearnedFromFailedInsert(t *testing.T) {
	h := newHarness()
	h.query(`INSERT INTO orders (id, tenant_id) VALUES (7, 'acme')`)
	h.a.OnBackend(h.s, &pgproto3.ErrorResponse{Message: "duplicate key"})
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE id = 7`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("learned from failed insert: %+v", e)
	}
}

func TestCacheLearnsFromResultRows(t *testing.T) {
	h := newHarness()
	h.query(`SELECT id, tenant_id FROM orders WHERE amount > 5`)
	h.reply(cols(16384, "id", "tenant_id"), [][]string{{"1", "acme"}, {"2", "globex"}}, "SELECT 2")
	h.rfq('I')
	h.query(`UPDATE orders SET amount = 0 WHERE id = 2`)
	if e := h.last(t); e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, []string{"globex"}) {
		t.Errorf("event = %+v", e)
	}
	h.query(`SELECT * FROM orders WHERE id IN (1, 2)`)
	if e := h.last(t); e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, []string{"acme", "globex"}) {
		t.Errorf("in list: %+v", e)
	}
	h.query(`SELECT * FROM orders WHERE id IN (1, 99)`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("partial hit must not attribute: %+v", e)
	}
}

func TestCacheIgnoresComputedAndMixedTableColumns(t *testing.T) {
	h := newHarness()
	h.query(`SELECT o.id, c.tenant_id FROM orders o JOIN customers c ON c.id = o.cid`)
	h.reply([]pgproto3.FieldDescription{
		{Name: []byte("id"), TableOID: 1}, {Name: []byte("tenant_id"), TableOID: 2},
	}, [][]string{{"5", "acme"}}, "SELECT 1")
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE id = 5`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("learned from a join: %+v", e)
	}
	h.query(`SELECT id, 'acme' AS tenant_id FROM orders WHERE amount > 1`)
	h.reply(cols(0, "id", "tenant_id"), [][]string{{"6", "acme"}}, "SELECT 1")
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE id = 6`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("learned from computed column: %+v", e)
	}
}

func TestCacheForgetsOnDeleteAndKeyUpdate(t *testing.T) {
	h := newHarness()
	h.a.Cache().Put("orders", "id", "1", "acme")
	h.a.Cache().Put("orders", "id", "2", "acme")
	h.query(`DELETE FROM orders WHERE id = 1`)
	h.reply(nil, nil, "DELETE 1")
	if _, ok := h.a.Cache().Get("orders", "id", "1"); ok {
		t.Error("deleted row still cached")
	}
	h.query(`UPDATE orders SET tenant_id = 'globex' WHERE id = 2`)
	h.reply(nil, nil, "UPDATE 1")
	if h.a.Cache().Len() != 0 {
		t.Error("table not dropped after tenant column changed")
	}
}

func TestCacheWithBoundParams(t *testing.T) {
	h := newHarness()
	h.front(&pgproto3.Parse{Name: "i", Query: `INSERT INTO orders (id, tenant_id) VALUES ($1, $2)`})
	h.front(&pgproto3.Bind{PreparedStatement: "i", Parameters: [][]byte{[]byte("11"), []byte("acme")}})
	h.front(&pgproto3.Execute{})
	h.reply(nil, nil, "INSERT 0 1")
	h.front(&pgproto3.Parse{Name: "g", Query: `SELECT * FROM orders WHERE id = $1`})
	h.front(&pgproto3.Bind{PreparedStatement: "g", Parameters: [][]byte{[]byte("11")}})
	h.front(&pgproto3.Execute{})
	if e := h.last(t); e.Source != SourceCache || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("event = %+v", e)
	}
}

func TestCacheIsPerTable(t *testing.T) {
	h := newHarness()
	h.a.Cache().Put("orders", "id", "1", "acme")
	h.query(`SELECT * FROM invoices WHERE id = 1`)
	if e := h.last(t); e.Source != SourceUnknown {
		t.Errorf("cross-table hit: %+v", e)
	}
}

func TestTransactionConflict(t *testing.T) {
	h := newHarness()
	h.query(`BEGIN`)
	h.query(`SELECT * FROM orders WHERE tenant_id = 'acme'`)
	h.query(`SELECT * FROM orders WHERE tenant_id = 'globex'`)
	if e := h.last(t); !e.Conflict {
		t.Errorf("expected conflict: %+v", e)
	}
}

func TestCacheLRUEvicts(t *testing.T) {
	c := NewRowCache(2)
	c.Put("t", "id", "1", "a")
	c.Put("t", "id", "2", "a")
	c.Get("t", "id", "1")
	c.Put("t", "id", "3", "a")
	if _, ok := c.Get("t", "id", "2"); ok {
		t.Error("least recently used entry was not evicted")
	}
	if _, ok := c.Get("t", "id", "1"); !ok {
		t.Error("recently used entry was evicted")
	}
}

func TestUUIDKeysAreCaseInsensitive(t *testing.T) {
	c := NewRowCache(10)
	c.Put("t", "id", "ABCDEF12-0000-0000-0000-000000000001", "a")
	if v, ok := c.Get("t", "id", "abcdef12-0000-0000-0000-000000000001"); !ok || v != "a" {
		t.Error("uuid lookup should ignore case")
	}
}

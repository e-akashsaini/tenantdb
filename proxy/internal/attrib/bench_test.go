package attrib

import (
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Per-statement cost of the analysis, with a catalog, a warm cache and a session.

func benchAttributor(b *testing.B, mode StrictMode) (*Attributor, *proxy.Session) {
	b.Helper()
	cat := soundSpec().build()
	a := New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat,
		Strict: StrictOptions{Mode: mode}}, func(Event) {})
	for i := 1; i <= 1000; i++ {
		a.Cache().Put("public.orders", "id", itoa(i), "t1")
	}
	return a, &proxy.Session{ID: 1, User: "app", DB: "d"}
}

func benchStatements() map[string]string {
	return map[string]string{
		"point lookup by tenant":     `SELECT * FROM orders WHERE tenant_id = 'acme' AND id = 5`,
		"row owner from the cache":   `SELECT * FROM orders WHERE id = 7`,
		"join through a foreign key": `SELECT i.* FROM items i INNER JOIN orders o ON o.id = i.order_id WHERE o.tenant_id = 'acme' AND i.sku = 'a' ORDER BY i.id LIMIT 20`,
		"a Rails-sized query":        `SELECT "orders".* FROM "orders" INNER JOIN "customers" ON "customers"."id" = "orders"."customer_id" WHERE "orders"."tenant_id" = 't1' AND "orders"."status" IN ('open', 'paid') AND ("orders"."amount" > 5 OR "orders"."amount" IS NULL) AND EXISTS (SELECT 1 FROM "items" WHERE "items"."order_id" = "orders"."id" AND "items"."tenant_id" = 't1') ORDER BY "orders"."id" DESC LIMIT 25 OFFSET 50`,
		"shared table":               `SELECT * FROM countries WHERE code = 'IE'`,
	}
}

func BenchmarkAttributeSimpleQuery(b *testing.B) {
	for name, sql := range benchStatements() {
		b.Run(name, func(b *testing.B) {
			a, s := benchAttributor(b, StrictAlert)
			q := &pgproto3.Query{String: sql}
			cc := &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}
			rfq := &pgproto3.ReadyForQuery{TxStatus: 'I'}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				a.OnFrontend(s, q)
				a.OnBackend(s, cc)
				a.OnBackend(s, rfq)
			}
		})
	}
}

// A prepared statement is parsed once and executed many times: the steady-state
// cost of an application using a driver's statement cache.
func BenchmarkAttributePreparedExecute(b *testing.B) {
	a, s := benchAttributor(b, StrictAlert)
	a.OnFrontend(s, &pgproto3.Parse{Name: "p", Query: `SELECT * FROM orders WHERE tenant_id = $1 AND status = $2`})
	bind := &pgproto3.Bind{PreparedStatement: "p", Parameters: [][]byte{[]byte("acme"), []byte("open")}}
	ex, sync := &pgproto3.Execute{}, &pgproto3.Sync{}
	cc := &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}
	rfq := &pgproto3.ReadyForQuery{TxStatus: 'I'}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.OnFrontend(s, bind)
		a.OnFrontend(s, ex)
		a.OnFrontend(s, sync)
		a.OnBackend(s, cc)
		a.OnBackend(s, rfq)
	}
}

func BenchmarkGuardScan(b *testing.B) {
	sql := benchStatements()["a Rails-sized query"]
	b.SetBytes(int64(len(sql)))
	for i := 0; i < b.N; i++ {
		_ = checkParseable(sql)
	}
}

func BenchmarkParseOnly(b *testing.B) {
	for name, sql := range benchStatements() {
		b.Run(name, func(b *testing.B) {
			e := NewExtractor([]string{"tenant_id"}, []string{"id"})
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := e.Parse(sql); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

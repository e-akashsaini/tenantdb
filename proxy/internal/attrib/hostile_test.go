package attrib

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Statements designed to hurt: the parser is C code reached through cgo, so a
// crash or a hang here would take the whole proxy down with it.

func hostile() map[string]string {
	return map[string]string{
		"deep parentheses":       "SELECT " + strings.Repeat("(", 20000) + "1" + strings.Repeat(")", 20000),
		"deep subqueries":        strings.Repeat("SELECT * FROM (", 3000) + "SELECT 1" + strings.Repeat(") x", 3000),
		"deep NOT":               "SELECT * FROM orders WHERE " + strings.Repeat("NOT (", 5000) + "tenant_id = 'a'" + strings.Repeat(")", 5000),
		"deep boolean chain":     "SELECT * FROM orders WHERE " + strings.Repeat("tenant_id = 'a' AND ", 20000) + "true",
		"deep OR chain":          "SELECT * FROM orders WHERE " + strings.Repeat("tenant_id = 'a' OR ", 20000) + "true",
		"huge IN list":           "SELECT * FROM orders WHERE id IN (" + strings.Repeat("1,", 200000) + "1)",
		"huge tenant IN list":    "SELECT * FROM orders WHERE tenant_id IN (" + strings.Repeat("'a',", 100000) + "'a')",
		"long arithmetic":        "SELECT " + strings.Repeat("1+", 100000) + "1",
		"many joins":             "SELECT * FROM orders o0" + manyJoins(500),
		"many CTEs":              manyCTEs(500) + "SELECT * FROM c499",
		"very long identifier":   "SELECT " + strings.Repeat("x", 100000) + " FROM orders",
		"huge string literal":    "SELECT * FROM orders WHERE tenant_id = '" + strings.Repeat("a", 5_000_000) + "'",
		"NUL byte":               "SELECT * FROM orders WHERE tenant_id = 'a\x00b'",
		"invalid UTF-8":          "SELECT * FROM orders WHERE tenant_id = '\xff\xfe\xfd'",
		"unterminated string":    "SELECT * FROM orders WHERE tenant_id = 'abc",
		"unterminated comment":   "SELECT * /* never ends FROM orders",
		"dollar quote":           "SELECT $$" + strings.Repeat("x", 100000) + "$$",
		"nested comments":        strings.Repeat("/*", 5000) + strings.Repeat("*/", 5000) + "SELECT 1",
		"many statements":        strings.Repeat("SELECT * FROM orders WHERE tenant_id = 'a'; ", 20000),
		"empty":                  "",
		"only semicolons":        strings.Repeat(";", 10000),
		"unicode identifiers":    `SELECT * FROM "ordérs" WHERE "tenänt_id" = 'ä' OR "🙂" = 1`,
		"quote-laden identifier": `SELECT * FROM "or""ders" WHERE "tenant""_id" = 'a'`,
		"huge VALUES":            "INSERT INTO orders (id, tenant_id) VALUES " + strings.Repeat("(1,'a'),", 50000) + "(1,'a')",
	}
}

func manyJoins(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		fmt.Fprintf(&b, " JOIN orders o%d ON o%d.id = o%d.id", i, i, i-1)
	}
	return b.String()
}

func manyCTEs(n int) string {
	var b strings.Builder
	b.WriteString("WITH c0 AS (SELECT * FROM orders)")
	for i := 1; i < n; i++ {
		fmt.Fprintf(&b, ", c%d AS (SELECT * FROM c%d)", i, i-1)
	}
	b.WriteString(" ")
	return b.String()
}

func TestHostileStatementsNeitherCrashNorHang(t *testing.T) {
	for name, sql := range hostile() {
		sql := sql
		t.Run(name, func(t *testing.T) {
			h := catalogHarness()
			h.s = &proxy.Session{ID: 1, User: "u", DB: "d"}
			done := make(chan struct{})
			start := time.Now()
			go func() {
				defer close(done)
				h.a.OnFrontend(h.s, &pgproto3.Query{String: sql})
				h.a.OnBackend(h.s, &pgproto3.CommandComplete{CommandTag: []byte("OK")})
				h.a.OnBackend(h.s, &pgproto3.ReadyForQuery{TxStatus: 'I'})
			}()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatalf("still running after 30s (%d bytes)", len(sql))
			}
			t.Logf("%d bytes in %s", len(sql), time.Since(start).Round(time.Millisecond))
			// Whatever it did, the session must still work afterwards.
			h.events = nil
			h.query(`SELECT * FROM orders WHERE tenant_id = 'acme'`)
			if e := h.last(t); e.Source != SourceExplicit {
				t.Errorf("session damaged by the hostile statement: %+v", e)
			}
		})
	}
}

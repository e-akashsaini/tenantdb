package attrib

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Random sessions through the real proxy in block mode, on a connection tagged as
// tenant t1, with a real PostgreSQL behind it. The oracle is simple: a statement
// the proxy let through may only return (or write) rows of t1. Unlike the
// statement-level fuzzers this one drives session state: transactions,
// savepoints, temporary tables, prepared statements, upserts and writes that
// change what the analysis believes.

const sessMe = "t1"

type sessGen struct {
	r *rand.Rand
}

func (g *sessGen) id(max int) string { return strconv.Itoa(g.r.Intn(max) + 1) }
func (g *sessGen) tid() string       { return fmt.Sprintf("'t%d'", g.r.Intn(3)+1) }
func (g *sessGen) pick(xs ...string) string {
	return xs[g.r.Intn(len(xs))]
}

// op returns one statement. kind "q" returns rows, "x" does not.
func (g *sessGen) op() (sql, kind string) {
	switch g.r.Intn(44) {
	case 0, 1:
		return "SELECT * FROM s_orders WHERE id = " + g.id(40), "q"
	case 2:
		return fmt.Sprintf("SELECT * FROM s_orders WHERE id IN (%s, %s, %s)", g.id(40), g.id(40), g.id(40)), "q"
	case 3:
		var ids []string
		for i := 0; i < 25; i++ {
			ids = append(ids, g.id(40))
		}
		return "SELECT * FROM s_orders WHERE id IN (" + strings.Join(ids, ",") + ")", "q"
	case 4:
		return g.pick("SELECT * FROM s_orders WHERE id > "+g.id(30), "SELECT * FROM s_orders WHERE tenant_id <> 't1'",
			"SELECT * FROM s_orders WHERE tenant_id LIKE 't%'", "SELECT * FROM s_orders WHERE amount > "+g.id(50)), "q"
	case 5, 6:
		return "SELECT * FROM s_orders WHERE tenant_id = " + g.tid(), "q"
	case 7:
		return fmt.Sprintf("SELECT o.tenant_id AS t_o, c.tenant_id AS t_c FROM s_orders o JOIN s_customers c ON c.id = o.customer_id WHERE %s",
			g.pick("o.tenant_id = "+g.tid(), "c.id = "+g.id(12), "o.id = "+g.id(40), "c.tenant_id = "+g.tid())), "q"
	case 8:
		return fmt.Sprintf("SELECT * FROM s_orders o WHERE EXISTS (SELECT 1 FROM s_customers c WHERE c.id = o.customer_id AND c.tenant_id = %s)", g.tid()), "q"
	case 9:
		return fmt.Sprintf("SELECT * FROM s_items WHERE order_id IN (SELECT id FROM s_orders WHERE %s)",
			g.pick("tenant_id = "+g.tid(), "id = "+g.id(40))), "q"
	case 10:
		return fmt.Sprintf("SELECT tenant_id FROM s_orders WHERE tenant_id = 't1' UNION SELECT tenant_id FROM s_orders WHERE id = %s", g.id(40)), "q"
	case 11:
		return fmt.Sprintf("WITH x AS (SELECT * FROM s_orders WHERE id = %s) SELECT * FROM x", g.id(40)), "q"
	case 12, 13:
		oc := g.pick("", "", " ON CONFLICT (id) DO UPDATE SET amount = 99", " ON CONFLICT (id) DO NOTHING",
			" ON CONFLICT (id) DO UPDATE SET amount = 7 WHERE s_orders.tenant_id = 't1'")
		return fmt.Sprintf("INSERT INTO s_orders (id, tenant_id, customer_id, amount) VALUES (%s, %s, %s, 1)%s RETURNING *",
			g.id(45), g.tid(), g.id(12), oc), "q"
	case 14, 15:
		return g.pick(
			"UPDATE s_orders SET amount = amount + 1 WHERE id = "+g.id(40)+" RETURNING *",
			"UPDATE s_orders SET amount = amount + 1 WHERE tenant_id = "+g.tid()+" RETURNING *",
			"UPDATE s_orders SET customer_id = "+g.id(12)+" WHERE id = "+g.id(40)+" RETURNING *",
			"UPDATE s_orders SET tenant_id = "+g.tid()+" WHERE id = "+g.id(40)+" RETURNING *",
		), "q"
	case 16:
		return fmt.Sprintf("UPDATE s_orders o SET amount = 0 FROM s_customers c WHERE c.id = o.customer_id AND c.tenant_id = %s RETURNING o.*", g.tid()), "q"
	case 17:
		return "DELETE FROM s_items WHERE id = " + g.id(60) + " RETURNING *", "q"
	case 18:
		return g.pick("BEGIN", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT sp", "ROLLBACK TO sp", "RELEASE sp"), "x"
	case 19, 20:
		return "CREATE TEMP TABLE s_orders AS SELECT * FROM s_orders WHERE tenant_id = 't1'", "x"
	case 21:
		return "CREATE TEMP TABLE s_orders ON COMMIT DROP AS SELECT * FROM s_orders WHERE tenant_id = 't1'", "x"
	case 22:
		return g.pick("DISCARD TEMP", "DISCARD TEMPORARY", "DROP TABLE pg_temp.s_orders"), "x"
	case 23:
		return "SELECT * FROM s_orders WHERE id = " + g.id(40), "q"
	case 24:
		return g.pick("PREPARE p1 AS SELECT * FROM s_orders WHERE id = $1", "PREPARE p1 AS SELECT * FROM s_orders",
			"PREPARE p1 AS SELECT * FROM s_orders WHERE tenant_id = $1"), "x"
	case 25:
		return g.pick("EXECUTE p1("+g.id(40)+")", "EXECUTE p1", "EXECUTE p1('t1')", "EXECUTE p1('t2')"), "q"
	case 26:
		return "DEALLOCATE p1", "x"
	case 27:
		return "SELECT set_config('tenantdb.tenant', 't1', " + g.pick("true", "false") + ")", "x"
	case 28:
		return g.pick("SET tenantdb.tenant = 't1'", "SET LOCAL tenantdb.tenant = 't1'"), "x"
	case 29:
		return "SELECT * FROM s_items WHERE order_id = " + g.id(40), "q"
	case 30:
		return fmt.Sprintf("SELECT * FROM s_customers WHERE id = %s OR tenant_id = 't1'", g.id(12)), "q"
	case 31:
		return "SELECT * FROM s_items WHERE id = " + g.id(60), "q"
	case 32:
		return "INSERT INTO s_items (id, tenant_id, order_id, qty) VALUES (" + g.id(70) + ", " + g.tid() + ", " + g.id(40) + ", 1) RETURNING *", "q"
	case 33, 34:
		return "SELECT * FROM s_notes WHERE id = " + g.id(70), "q"
	case 35, 36:
		return g.pick("SELECT * FROM s_notes WHERE order_id = "+g.id(40),
			fmt.Sprintf("SELECT * FROM s_notes WHERE order_id IN (%s, %s)", g.id(40), g.id(40))), "q"
	case 37:
		return g.pick("SELECT * FROM s_notes", "SELECT * FROM s_notes WHERE body LIKE 'n1%'", "SELECT * FROM s_notes WHERE id > "+g.id(30),
			"SELECT * FROM s_notes WHERE order_id > "+g.id(30)), "q"
	case 38:
		return fmt.Sprintf("SELECT n.id, n.order_id FROM s_notes n JOIN s_orders o ON o.id = n.order_id WHERE %s",
			g.pick("o.tenant_id = "+g.tid(), "o.id = "+g.id(40), "n.id = "+g.id(70))), "q"
	case 39:
		return "INSERT INTO s_notes (id, order_id, body) VALUES (" + g.id(90) + ", " + g.id(40) + ", 'x') RETURNING *", "q"
	case 40, 41:
		return g.pick("UPDATE s_notes SET order_id = "+g.id(40)+" WHERE id = "+g.id(70)+" RETURNING *",
			"UPDATE s_notes SET body = 'y' WHERE order_id = "+g.id(40)+" RETURNING *",
			"UPDATE s_notes SET order_id = "+g.id(40)+" WHERE order_id = "+g.id(40)+" RETURNING *"), "q"
	case 42:
		return "DELETE FROM s_notes WHERE id = " + g.id(70) + " RETURNING *", "q"
	}
	return "SELECT 1", "q"
}

const sessSchema = `
DROP TABLE IF EXISTS s_notes, s_items, s_orders, s_customers CASCADE;
CREATE TABLE s_customers (id bigint PRIMARY KEY, tenant_id text NOT NULL, name text);
CREATE TABLE s_orders (id bigint PRIMARY KEY, tenant_id text NOT NULL, customer_id bigint REFERENCES s_customers(id), amount int);
CREATE TABLE s_items (id bigint PRIMARY KEY, tenant_id text NOT NULL, order_id bigint REFERENCES s_orders(id), qty int);
CREATE TABLE s_notes (id bigint PRIMARY KEY, order_id bigint REFERENCES s_orders(id), body text);
`

const sessData = `
TRUNCATE s_notes, s_items, s_orders, s_customers CASCADE;
INSERT INTO s_customers SELECT g, 't' || ((g - 1) / 4 + 1), 'c' || g FROM generate_series(1, 12) g;
INSERT INTO s_orders SELECT g, c.tenant_id, c.id, g % 50 FROM generate_series(1, 30) g JOIN s_customers c ON c.id = (g - 1) % 12 + 1;
INSERT INTO s_items SELECT g, o.tenant_id, o.id, g % 7 FROM generate_series(1, 60) g JOIN s_orders o ON o.id = (g - 1) % 30 + 1;
INSERT INTO s_notes SELECT g, (g - 1) % 30 + 1, 'n' || g FROM generate_series(1, 60) g;
`

func TestSessionFuzzBlockMode(t *testing.T) {
	dsn := os.Getenv("TENANTDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TENANTDB_TEST_DSN not set")
	}
	sessions, seed := 25, int64(1)
	if v, err := strconv.Atoi(os.Getenv("TENANTDB_FUZZ_N")); err == nil && v > 0 {
		sessions = v / 120
		if sessions < 5 {
			sessions = 5
		}
	}
	if v, err := strconv.ParseInt(os.Getenv("TENANTDB_FUZZ_SEED"), 10, 64); err == nil {
		seed = v
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, sessSchema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, `DROP TABLE IF EXISTS s_notes, s_items, s_orders, s_customers CASCADE`)

	cat := NewCatalog(dsn)
	cat.TenantColumns = []string{"tenant_id"}
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	lk, err := NewLookup(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer lk.Close()
	var evMu sync.Mutex
	var events []Event
	a := New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat, Lookup: lk,
		Strict: StrictOptions{Mode: StrictBlock}}, func(e Event) { evMu.Lock(); events = append(events, e); evMu.Unlock() })
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	spy := &sessSpy{Observer: a.Observer(), g: a.Observer().(proxy.Gate)}
	srv := proxy.New(proxy.Config{Listen: "127.0.0.1:0", Upstream: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Observer: spy, Logger: discardLogger()})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go srv.Serve(sctx)
	defer srv.Close()
	pcfg := cfg.Copy()
	pcfg.Host, pcfg.Port = "127.0.0.1", uint16(srv.Addr().(*net.TCPAddr).Port)
	pcfg.TLSConfig, pcfg.Fallbacks = nil, nil

	modes := []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec}
	allowed, refused, leaks := 0, 0, 0
	for s := 0; s < sessions && leaks < 3; s++ {
		g := &sessGen{r: rand.New(rand.NewSource(seed*1000 + int64(s)))}
		if _, err := admin.Exec(ctx, sessData); err != nil {
			t.Fatal(err)
		}
		a.Cache().Clear()
		cat.mu.Lock()
		cat.fkMemo = map[string]fkVerdict{}
		cat.mu.Unlock()
		cat.Load(ctx)

		pcfg.DefaultQueryExecMode = modes[g.r.Intn(len(modes))]
		var conn *pgx.Conn
		var log []string
		brokenSeen := false
		connect := func() bool {
			if conn != nil && !conn.IsClosed() {
				return true
			}
			var err error
			if conn, err = pgx.ConnectConfig(ctx, pcfg); err != nil {
				t.Errorf("connect: %v", err)
				return false
			}
			conn.Exec(ctx, "SET tenantdb.tenant = '"+sessMe+"'")
			return true
		}
		if !connect() {
			return
		}
		for i := 0; i < 35; i++ {
			if !connect() {
				return
			}
			if len(log) > 0 && !brokenSeen {
				var bad int
				admin.QueryRow(ctx, `SELECT count(*) FROM s_orders o JOIN s_customers c ON c.id = o.customer_id WHERE o.tenant_id <> c.tenant_id`).Scan(&bad)
				if bad > 0 {
					brokenSeen = true
					log[len(log)-1] += "    <<< after this, an order points at another tenant's customer"
				}
			}
			if len(log) > 0 {
				// where does the proxy's idea of the temporary table part from the database's?
				var dbTemp bool
				if err := conn.QueryRow(ctx, `SELECT to_regclass('pg_temp.s_orders') IS NOT NULL`).Scan(&dbTemp); err == nil {
					proxyTemp := false
					if sess := spy.session(); sess != nil {
						st := a.st(sess)
						st.mu.Lock()
						proxyTemp = st.temps["s_orders"] != nil
						st.mu.Unlock()
					}
					if dbTemp != proxyTemp {
						log[len(log)-1] += fmt.Sprintf("    <<< proxy believes temp=%v, database has temp=%v", proxyTemp, dbTemp)
					}
				}
			}
			sql, kind := g.op()
			log = append(log, sql)
			if kind == "x" {
				_, err := conn.Exec(ctx, sql)
				a.countSess(err, &allowed, &refused)
				if err != nil && !blockedByTenantdb(err) {
					conn.Exec(ctx, "ROLLBACK")
				}
				continue
			}
			// A temporary table named like the real one is the session's own data.
			var hasTemp bool
			conn.QueryRow(ctx, `SELECT to_regclass('pg_temp.s_orders') IS NOT NULL`).Scan(&hasTemp)
			rows, err := conn.Query(ctx, sql)
			if err != nil {
				a.countSess(err, &allowed, &refused)
				if !blockedByTenantdb(err) {
					conn.Exec(ctx, "ROLLBACK")
				}
				continue
			}
			leak := ""
			descs := rows.FieldDescriptions()
			for rows.Next() {
				vals, verr := rows.Values()
				if verr != nil {
					break
				}
				for j, v := range vals {
					n := descs[j].Name
					if n == "order_id" && strings.Contains(sql, "s_notes") && v != nil && leak == "" {
						var owner string
						if admin.QueryRow(ctx, `SELECT tenant_id FROM s_orders WHERE id = $1`, v).Scan(&owner) == nil && owner != sessMe {
							leak = fmt.Sprintf("a note of order %v, which belongs to %s", v, owner)
						}
					}
					if (n == "tenant_id" || strings.HasPrefix(n, "t_")) && v != nil && fmt.Sprint(v) != sessMe && leak == "" {
						leak = fmt.Sprintf("column %s = %v", n, v)
					}
				}
			}
			err = rows.Err()
			rows.Close()
			a.countSess(err, &allowed, &refused)
			if err != nil && !blockedByTenantdb(err) {
				conn.Exec(ctx, "ROLLBACK")
			}
			if leak != "" && err == nil && !(hasTemp && (strings.Contains(sql, "s_orders") || strings.HasPrefix(sql, "EXECUTE"))) {
				leaks++
				from := 0
				evMu.Lock()
				verdict := "no event"
				for k := len(events) - 1; k >= 0 && k >= len(events)-4; k-- {
					if strings.HasPrefix(sql, events[k].SQL) || strings.HasPrefix(events[k].SQL, sql) {
						e := events[k]
						verdict = fmt.Sprintf("source=%s tenants=%v scan=%v conflict=%v violation=%q", e.Source, e.Tenants, e.Scan, e.Conflict, e.Violation)
						break
					}
				}
				evMu.Unlock()
				belief := ""
				if sess := spy.session(); sess != nil {
					st := a.st(sess)
					st.mu.Lock()
					for n, tt := range st.temps {
						belief += fmt.Sprintf(" temp:%s(kind=%d)", n, tt.kind)
					}
					belief += fmt.Sprintf(" inTxn=%v tag=%q", st.inTxn, st.curTag())
					st.mu.Unlock()
				}
				verdict += " | proxy believes:" + belief
				if rs, qerr := conn.Query(ctx, `SELECT name, statement FROM pg_prepared_statements WHERE name = 'p1'`); qerr == nil {
					for rs.Next() {
						var n, stmt string
						rs.Scan(&n, &stmt)
						verdict += fmt.Sprintf(" | database holds %s = %s", n, stmt)
					}
					rs.Close()
				}
				t.Errorf("LEAK (%s) seed=%d session=%d mode=%v\n  the statement was allowed: %s\n  tenantdb's verdict: %s\n  the session so far:\n    %s",
					leak, seed, s, pcfg.DefaultQueryExecMode, sql, verdict, strings.Join(log[from:], "\n    "))
				break
			}
		}
		if conn != nil {
			conn.Close(ctx)
		}
	}
	t.Logf("%d sessions: %d statements ran, %d refused by tenantdb, %d leaks", sessions, allowed, refused, leaks)
}

func blockedByTenantdb(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && strings.Contains(pe.Message, "tenantdb: statement blocked")
}

func (a *Attributor) countSess(err error, allowed, refused *int) {
	if blockedByTenantdb(err) {
		*refused++
	} else if err == nil {
		*allowed++
	}
}

// sessSpy remembers the session of the last message so the test can look at what
// the proxy believes about it.
type sessSpy struct {
	proxy.Observer
	g    proxy.Gate
	mu   sync.Mutex
	last *proxy.Session
}

func (s *sessSpy) Gate(sess *proxy.Session, msg pgproto3.FrontendMessage) *proxy.Rejection {
	s.mu.Lock()
	s.last = sess
	s.mu.Unlock()
	return s.g.Gate(sess, msg)
}

func (s *sessSpy) session() *proxy.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

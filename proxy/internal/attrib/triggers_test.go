package attrib

import (
	"context"
	"os"
	"reflect"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
)

func trigSpec() catSpec {
	sp := soundSpec()
	sp.rows = append(sp.rows, tbl("public", "audit_log", 'r', "id", "tenant_id", "msg")...)
	sp.rows = append(sp.rows, tbl("public", "counters", 'r', "name", "n")...) // shared
	sp.unique["public.audit_log"] = []string{"id"}
	tf := func(name, lang, src string) *FuncDef {
		return &FuncDef{Schema: "public", Name: name, Lang: lang, Kind: 'f', Src: src, Ret: "trigger"}
	}
	sp.funcs = append(sp.funcs,
		tf("audit_row", "plpgsql", `BEGIN INSERT INTO audit_log (tenant_id, msg) VALUES (NEW.tenant_id, 'changed'); RETURN NEW; END`),
		tf("audit_old", "plpgsql", `BEGIN DELETE FROM audit_log WHERE tenant_id = OLD.tenant_id AND msg = 'x'; RETURN OLD; END`),
		tf("bump", "plpgsql", `BEGIN UPDATE counters SET n = n + 1 WHERE name = 'orders'; RETURN NEW; END`),
		tf("wipe", "plpgsql", `BEGIN DELETE FROM audit_log WHERE msg = 'x'; RETURN NEW; END`),
		tf("cross", "plpgsql", `BEGIN INSERT INTO audit_log (tenant_id, msg) VALUES ('other', 'x'); RETURN NEW; END`),
		tf("py", "plpython3u", `return None`),
		tf("calc", "plpgsql", `BEGIN IF NEW.amount > 5 THEN NEW.status := 'big'; END IF; RETURN NEW; END`),
		tf("nested", "plpgsql", `BEGIN UPDATE orders SET status = status WHERE id = NEW.id AND tenant_id = NEW.tenant_id; RETURN NEW; END`),
	)
	sp.triggers = map[string][]trigRow{}
	return sp
}

func withTrigger(table string, typ int, fn string) catSpec {
	sp := trigSpec()
	sp.triggers[table] = append(sp.triggers[table], trigRow{typ: typ, fn: "public." + fn})
	return sp
}

func TestTriggersAreReadWithNewBoundToTheRowsTenant(t *testing.T) {
	for _, c := range []struct {
		name    string
		sp      catSpec
		sql     string
		source  Source
		tenants []string
	}{
		{"an audit trigger writing NEW.tenant_id adds nothing", withTrigger("public.orders", trigInsert|trigUpdate, "audit_row"),
			`INSERT INTO orders (id, tenant_id, amount) VALUES (1, 't1', 5)`, SourceExplicit, []string{"t1"}},
		{"... on an UPDATE too", withTrigger("public.orders", trigInsert|trigUpdate, "audit_row"),
			`UPDATE orders SET status = 'x' WHERE tenant_id = 't2'`, SourceExplicit, []string{"t2"}},
		{"OLD.tenant_id on a delete trigger", withTrigger("public.orders", trigDelete, "audit_old"),
			`DELETE FROM orders WHERE tenant_id = 't3'`, SourceExplicit, []string{"t3"}},
		{"a trigger that touches only shared data", withTrigger("public.orders", trigInsert, "bump"),
			`INSERT INTO orders (id, tenant_id) VALUES (2, 't1')`, SourceExplicit, []string{"t1"}},
		{"a trigger that only edits NEW", withTrigger("public.orders", trigInsert, "calc"),
			`INSERT INTO orders (id, tenant_id, amount) VALUES (3, 't1', 9)`, SourceExplicit, []string{"t1"}},
		{"a trigger for another event does not fire", withTrigger("public.orders", trigDelete, "wipe"),
			`INSERT INTO orders (id, tenant_id) VALUES (4, 't1')`, SourceExplicit, []string{"t1"}},
		{"a trigger on another table does not fire", withTrigger("public.customers", trigInsert, "wipe"),
			`INSERT INTO orders (id, tenant_id) VALUES (5, 't1')`, SourceExplicit, []string{"t1"}},
		{"a trigger whose update is confined by NEW", withTrigger("public.orders", trigInsert, "nested"),
			`INSERT INTO orders (id, tenant_id) VALUES (6, 't1')`, SourceExplicit, []string{"t1"}},
		{"a statement that touches several tenants binds each", withTrigger("public.orders", trigUpdate, "audit_row"),
			`UPDATE orders SET status = 'x' WHERE tenant_id IN ('t1', 't2')`, SourceExplicit, []string{"t1", "t2"}},
		{"a trigger that writes another tenant's rows says so", withTrigger("public.orders", trigInsert, "cross"),
			`INSERT INTO orders (id, tenant_id) VALUES (7, 't1')`, SourceExplicit, []string{"other", "t1"}},
	} {
		h := specHarness(c.sp, StrictOptions{})
		e := h.attributed(t, c.sql)
		if e.Source != c.source || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s:\n  %s\n  got %s %v (%s), want %s %v", c.name, c.sql, e.Source, e.Tenants, e.Violation, c.source, c.tenants)
		}
	}
}

func TestTriggersThatCannotBeVouchedForAreFlagged(t *testing.T) {
	for _, c := range []struct {
		name string
		sp   catSpec
		sql  string
	}{
		{"a trigger that wipes every tenant's rows", withTrigger("public.orders", trigInsert, "wipe"),
			`INSERT INTO orders (id, tenant_id) VALUES (1, 't1')`},
		{"... when the statement itself is confined", withTrigger("public.orders", trigDelete, "wipe"),
			`DELETE FROM orders WHERE tenant_id = 't1'`},
		{"a trigger in a language that cannot be read", withTrigger("public.orders", trigInsert, "py"),
			`INSERT INTO orders (id, tenant_id) VALUES (2, 't1')`},
		{"a trigger whose function is missing", func() catSpec {
			sp := trigSpec()
			sp.triggers["public.orders"] = []trigRow{{typ: trigInsert, fn: "public.gone"}}
			return sp
		}(), `INSERT INTO orders (id, tenant_id) VALUES (3, 't1')`},
		{"an UPSERT also fires update triggers", withTrigger("public.orders", trigUpdate, "wipe"),
			`INSERT INTO orders (id, tenant_id) VALUES (4, 't1') ON CONFLICT (id) DO UPDATE SET status = 'x'`},
		{"a trigger on a table written through a CTE", withTrigger("public.audit_log", trigInsert, "wipe"),
			`WITH x AS (INSERT INTO audit_log (id, tenant_id) VALUES (5, 't1') RETURNING id) SELECT * FROM orders WHERE tenant_id = 't1'`},
	} {
		h := specHarness(c.sp, StrictOptions{})
		e := h.attributed(t, c.sql)
		switch e.Source {
		case SourceExplicit, SourceCache, SourceLookup:
			t.Errorf("%s:\n  %s\n  claimed %v (%s)", c.name, c.sql, e.Tenants, e.Source)
		}
	}
}

// The same through a real database: triggers come from pg_trigger, the function
// bodies (RETURN NEW, assignments) from pg_proc.
func TestTriggersFromTheCatalog(t *testing.T) {
	dsn := os.Getenv("TENANTDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TENANTDB_TEST_DSN not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	cleanup := `DROP TABLE IF EXISTS tg_orders, tg_audit CASCADE; DROP FUNCTION IF EXISTS tg_audit_fn(), tg_wipe_fn()`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE tg_orders (id bigint PRIMARY KEY, tenant_id text, status text)`,
		`CREATE TABLE tg_audit (id bigserial PRIMARY KEY, tenant_id text, note text)`,
		`CREATE FUNCTION tg_audit_fn() RETURNS trigger LANGUAGE plpgsql AS $$
		   DECLARE n int;
		   BEGIN
		     n := 1;
		     INSERT INTO tg_audit (tenant_id, note) VALUES (NEW.tenant_id, 'row ' || n);
		     RETURN NEW;
		   END $$`,
		`CREATE FUNCTION tg_wipe_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN DELETE FROM tg_audit WHERE note = 'x'; RETURN OLD; END $$`,
		`CREATE TRIGGER tg_a AFTER INSERT ON tg_orders FOR EACH ROW EXECUTE FUNCTION tg_audit_fn()`,
		`CREATE TRIGGER tg_w AFTER DELETE ON tg_orders FOR EACH ROW EXECUTE FUNCTION tg_wipe_fn()`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer conn.Exec(ctx, cleanup)

	cat := NewCatalog(dsn)
	cat.TenantColumns = []string{"tenant_id"}
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	h := &harness{s: &proxy.Session{ID: 1, User: "app", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat}, h.record)

	if e := h.attributed(t, `INSERT INTO tg_orders (id, tenant_id) VALUES (1, 'acme')`); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("an audit trigger on the row's own tenant: %+v", e)
	}
	if e := h.attributed(t, `DELETE FROM tg_orders WHERE tenant_id = 'acme'`); e.Source == SourceExplicit {
		t.Errorf("a delete trigger that wipes every tenant's audit rows: %+v", e)
	}
	if e := h.attributed(t, `UPDATE tg_orders SET status = 'x' WHERE tenant_id = 'acme'`); e.Source != SourceExplicit {
		t.Errorf("no update trigger exists: %+v", e)
	}
}

func TestWritesToATableWithRulesAreNotVouchedFor(t *testing.T) {
	sp := trigSpec()
	sp.ruled = map[string]bool{"public.orders": true}
	h := specHarness(sp, StrictOptions{})
	if e := h.attributed(t, `INSERT INTO orders (id, tenant_id) VALUES (1, 't1')`); e.Source == SourceExplicit {
		t.Errorf("a rule may redirect the write: %+v", e)
	}
	if e := h.attributed(t, `SELECT * FROM orders WHERE tenant_id = 't1'`); e.Source != SourceExplicit {
		t.Errorf("reads are not rewritten by rules: %+v", e)
	}
	if e := h.attributed(t, `INSERT INTO customers (id, tenant_id) VALUES (1, 't1')`); e.Source != SourceExplicit {
		t.Errorf("another table: %+v", e)
	}
}

package analyze

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/akashsaini/tenantdb/internal/attrib"
	"github.com/jackc/pgx/v5"
)

func TestReadPostgresLog(t *testing.T) {
	log := strings.Join([]string{
		"2026-10-10 18:00:00.001 UTC [4242] app@shop LOG:  statement: SELECT *",
		"\tFROM orders",
		"\tWHERE id = 5",
		"2026-10-10 18:00:00.002 UTC [4242] app@shop LOG:  execute <unnamed>: SELECT * FROM orders WHERE tenant_id = $1 AND note = $2 AND x = $3",
		"2026-10-10 18:00:00.002 UTC [4242] app@shop DETAIL:  parameters: $1 = 'acme', $2 = 'it''s, fine', $3 = NULL",
		"2026-10-10 18:00:01.000 UTC [4242] app@shop LOG:  duration: 1.234 ms  statement: UPDATE orders SET a = 1",
		"2026-10-10 18:00:02.000 UTC [77] other@shop LOG:  checkpoint starting: time",
		"2026-10-10 18:00:03.000 UTC [77] other@shop ERROR:  syntax error",
		"2026-10-10 18:00:04.000 UTC [99] job@analytics LOG:  duration: 2.0 ms  execute S_1: SELECT 1",
		"",
	}, "\n")
	got, err := Read(strings.NewReader(log), FormatAuto)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("entries = %d: %+v", len(got), got)
	}
	if got[0].SQL != "SELECT *\nFROM orders\nWHERE id = 5" || got[0].Session != "4242" || got[0].User != "app" || got[0].DB != "shop" {
		t.Errorf("multi-line statement: %+v", got[0])
	}
	if !got[1].HasParams || !reflect.DeepEqual(got[1].Params, []string{"acme", "it's, fine", NullParam}) {
		t.Errorf("parameters: %+v", got[1])
	}
	if got[2].SQL != "UPDATE orders SET a = 1" {
		t.Errorf("duration prefix: %+v", got[2])
	}
	if got[3].Session != "99" || got[3].User != "job" || got[3].SQL != "SELECT 1" {
		t.Errorf("named execute: %+v", got[3])
	}
}

func TestReadStatementsCSV(t *testing.T) {
	in := "userid,query,calls,total_exec_time\n10,\"SELECT * FROM orders WHERE id = $1\",120,5.5\n10,\"INSERT INTO t VALUES ($1, $2)\",3,0.1\n"
	got, err := Read(strings.NewReader(in), FormatAuto)
	if err != nil || len(got) != 2 {
		t.Fatalf("%v %+v", err, got)
	}
	if got[0].Calls != 120 || got[0].SQL != "SELECT * FROM orders WHERE id = $1" || got[1].Calls != 3 {
		t.Errorf("%+v", got)
	}
	if _, err := Read(strings.NewReader("a,b\n1,2\n"), FormatStatements); err == nil {
		t.Error("a CSV without a query column should be an error")
	}
}

func TestReadSQLSplitsOnStatementsNotSemicolons(t *testing.T) {
	got, err := Read(strings.NewReader("SELECT 'a;b'; -- c;d\nSELECT 2;\n\n;"), FormatSQL)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || !strings.Contains(got[0].SQL, "'a;b'") {
		t.Errorf("%+v", got)
	}
}

func TestParseFormat(t *testing.T) {
	for in, want := range map[string]Format{"": FormatAuto, "LOG": FormatLog, "csv": FormatStatements, "sql": FormatSQL} {
		if got, err := ParseFormat(in); err != nil || got != want {
			t.Errorf("%q -> %q, %v", in, got, err)
		}
	}
	if _, err := ParseFormat("xml"); err == nil {
		t.Error("expected an error")
	}
}

// Needs a live Postgres: TENANTDB_TEST_DSN=postgres://user:pw@host:5432/db
func TestRunnerAgainstALiveSchema(t *testing.T) {
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
	cleanup := `DROP TABLE IF EXISTS an_orders, an_countries`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE an_orders (id bigint PRIMARY KEY, tenant_id text, amount int)`,
		`CREATE TABLE an_countries (code text PRIMARY KEY)`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer conn.Exec(ctx, cleanup)

	cat := attrib.NewCatalog(dsn)
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	run := NewRunner(Options{User: "app", DB: "d", Attrib: attrib.Options{
		TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat,
	}})
	feed := func(sql string, calls int64) { run.Feed(Entry{SQL: sql, Calls: calls, Session: "s"}) }
	feed(`SELECT * FROM an_orders WHERE tenant_id = 'acme'`, 10)
	feed(`SELECT * FROM an_orders WHERE tenant_id = 'globex'`, 5)
	feed(`SELECT * FROM an_countries`, 7)
	feed(`SELECT * FROM an_orders WHERE amount > 5`, 3)
	feed(`SELECT * FROM an_orders WHERE amount > 9`, 4) // same shape as the line above
	feed(`SELECT * FROM an_orders WHERE id = $1`, 20)
	feed(`SELEKT broken`, 2)
	feed(`SET search_path = public`, 1)
	run.Feed(Entry{SQL: `SELECT * FROM an_orders WHERE tenant_id = $1`, Calls: 1, HasParams: true, Params: []string{"acme"}})

	rep := run.Report()
	if rep.Unparsed != 2 || rep.Other != 1 {
		t.Errorf("unparsed=%d other=%d", rep.Unparsed, rep.Other)
	}
	want := map[string]int64{"explicit": 16, "untenanted": 7, "scan": 7, "unknown": 20}
	if !reflect.DeepEqual(rep.BySource, want) {
		t.Errorf("by source = %v, want %v", rep.BySource, want)
	}
	if rep.Violations["scan"] != 7 || rep.Violations["unknown"] != 20 {
		t.Errorf("violations = %v", rep.Violations)
	}
	if len(rep.Shapes) != 2 || rep.Shapes[0].Violation != "unknown" || rep.Shapes[0].Calls != 20 || rep.Shapes[1].Calls != 7 {
		t.Fatalf("shapes = %+v", rep.Shapes)
	}
	if !strings.Contains(rep.Shapes[1].Query, "$1") || strings.Contains(rep.Shapes[1].Query, "5") {
		t.Errorf("literals should be normalised away: %q", rep.Shapes[1].Query)
	}
	var tr *TableRow
	for _, r := range rep.Tables {
		if strings.HasSuffix(r.Table, ".an_orders") {
			tr = r
		}
	}
	if tr == nil || tr.Violating != 27 {
		t.Errorf("an_orders row = %+v", tr)
	}

	var text bytes.Buffer
	rep.Text(&text, 10)
	for _, frag := range []string{"What strict mode would flag: 27", "[unknown] x20", "[scan] x7", "an_orders"} {
		if !strings.Contains(text.String(), frag) {
			t.Errorf("report is missing %q:\n%s", frag, text.String())
		}
	}
	var js bytes.Buffer
	if err := rep.JSON(&js); err != nil {
		t.Fatal(err)
	}
	var back Report
	if err := json.Unmarshal(js.Bytes(), &back); err != nil || back.Violations["scan"] != 7 {
		t.Errorf("JSON round trip: %v %+v", err, back.Violations)
	}
	_ = time.Second
}

func TestTransactionsAreTrackedPerSession(t *testing.T) {
	// Without a catalog nothing can be attributed, but the session bookkeeping
	// must not panic or mix sessions up.
	run := NewRunner(Options{Attrib: attrib.Options{TenantColumns: []string{"tenant_id"}}})
	run.Feed(Entry{SQL: "BEGIN", Session: "a"})
	run.Feed(Entry{SQL: "SELECT 1", Session: "b"})
	run.Feed(Entry{SQL: "COMMIT", Session: "a"})
	rep := run.Report()
	if rep.Entries != 3 || rep.Other != 2 {
		t.Errorf("entries=%d other=%d", rep.Entries, rep.Other)
	}
}

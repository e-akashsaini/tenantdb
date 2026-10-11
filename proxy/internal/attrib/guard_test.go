package attrib

import (
	"fmt"
	"strings"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestGuardRefusesTheChainsThatCrashTheParser(t *testing.T) {
	r := strings.Repeat
	for name, sql := range map[string]string{
		"operator chain":     "SELECT " + r("1+", 5000) + "1",
		"|| chain":           "SELECT " + r("'a' || ", 5000) + "'a'",
		"cast chain":         "SELECT 1" + r("::int", 5000),
		"JOIN chain":         "SELECT * FROM t t0" + r(" JOIN t x ON true", 5000),
		"UNION chain":        "SELECT 1" + r(" UNION SELECT 1", 5000),
		"AT TIME ZONE":       "SELECT now()" + r(" AT TIME ZONE 'utc'", 5000),
		"IS NULL chain":      "SELECT 1" + r(" IS NULL", 5000),
		"mixed operators":    "SELECT " + r("a * b - ", 3000) + "c",
		"operators in WHERE": "SELECT * FROM t WHERE x = " + r("1 + ", 5000) + "1",
	} {
		if err := checkParseable(sql); err != ErrTooComplex {
			t.Errorf("%s: %d bytes were allowed through to the parser", name, len(sql))
		}
	}
	if err := checkParseable(strings.Repeat("x", MaxStatementBytes+1)); err != ErrTooComplex {
		t.Error("statements over the size limit must be refused")
	}
}

func TestGuardLetsLegitimateLargeStatementsThrough(t *testing.T) {
	r := strings.Repeat
	for name, sql := range map[string]string{
		"bulk insert with negatives and arithmetic": "INSERT INTO t (a, b) VALUES " + r("(-1, 2+3), ", 60000) + "(-1, 2+3)",
		"huge IN list":                            "SELECT * FROM t WHERE id IN (" + r("1,", 200000) + "1)",
		"long AND chain":                          "SELECT * FROM t WHERE " + r("a = 1 AND ", 50000) + "true",
		"long OR chain":                           "SELECT * FROM t WHERE " + r("a = 1 OR ", 50000) + "true",
		"select list of expressions":              "SELECT " + r("a + 1, ", 50000) + "a",
		"a chain under the limit":                 "SELECT " + r("1+", 2900) + "1",
		"operators inside a string":               "SELECT '" + r("+ ", 100000) + "'",
		"operators inside a dollar string":        "SELECT $$" + r("1 + ", 100000) + "$$",
		"operators inside a tagged dollar string": "SELECT $tag$" + r("1 + $$ ", 100000) + "$tag$",
		"operators inside a comment":              "SELECT 1 -- " + r("+ ", 100000) + "\n",
		"operators inside a block comment":        "SELECT 1 /* " + r("+ /* ", 3000) + r(" */", 3000) + " */",
		"operators inside an identifier":          `SELECT "` + r("+", 100000) + `" FROM t`,
		"escaped quote does not end a string":     "SELECT 'it''s " + r("+ ", 100000) + "'",
		"backslash escape in E string":            `SELECT E'\'` + r(" + ", 100000) + `'`,
		"parameters and negation":                 "SELECT " + r("$1 - -$2, ", 50000) + "1",
		"many statements":                         r("SELECT 1 + 2; ", 50000),
		"a JOIN chain under the limit":            "SELECT * FROM t t0" + r(" JOIN t x ON true", 2000),
	} {
		if err := checkParseable(sql); err != nil {
			t.Errorf("%s: refused (%v)", name, err)
		}
	}
}

// Anything the guard lets through must not crash the parser.
func TestWhatTheGuardAllowsTheParserSurvives(t *testing.T) {
	r := strings.Repeat
	n := maxOperatorChain - 1
	for name, sql := range map[string]string{
		"operator chain": "SELECT " + r("1+", n) + "1",
		"|| chain":       "SELECT " + r("'a' || ", n) + "'a'",
		"cast chain":     "SELECT 1" + r("::int", n),
		"JOIN chain":     "SELECT * FROM t t0" + r(" JOIN t x ON true", maxStructChain-1),
		"UNION chain":    "SELECT 1" + r(" UNION SELECT 1", maxStructChain-1),
		"AT TIME ZONE":   "SELECT now()" + r(" AT TIME ZONE 'utc'", n/2),
		"IS NULL chain":  "SELECT 1" + r(" IS NULL", n/2),
	} {
		if err := checkParseable(sql); err != nil {
			t.Errorf("%s: refused by the guard: %v", name, err)
			continue
		}
		if _, err := Parse(sql); err != nil {
			t.Logf("%s: parser error (fine, not a crash): %.60s", name, err)
		}
	}
}

func TestUnparsedStatementsAreUnknownNotInvisible(t *testing.T) {
	h := catalogHarness()
	h.query(`SELEKT * FROM orders WHERE tenant_id = 'acme'`)
	e := h.last(t)
	if e.Source != SourceUnknown || !e.Unparsed {
		t.Errorf("a statement that cannot be read must be reported as unknown: %+v", e)
	}
	// ... through the prepared-statement path too
	h2 := catalogHarness()
	h2.front(&pgproto3.Parse{Name: "bad", Query: `SELEKT 1`})
	h2.front(&pgproto3.Bind{PreparedStatement: "bad"})
	h2.front(&pgproto3.Execute{})
	if e := h2.last(t); e.Source != SourceUnknown || !e.Unparsed {
		t.Errorf("prepared: %+v", e)
	}
	// ... and one too complex to hand to the parser
	h3 := catalogHarness()
	h3.query("SELECT " + strings.Repeat("1+", 100000) + "1")
	if e := h3.last(t); !e.Unparsed || len(e.SQL) > 5000 {
		t.Errorf("complex statement: unparsed=%v, logged %d bytes", e.Unparsed, len(e.SQL))
	}
}

func TestUnparsedStatementsAreRefusedInBlockMode(t *testing.T) {
	h := strictHarness(StrictOptions{Mode: StrictBlock})
	if rej := h.gateQuery(`SELEKT 1`); rej == nil {
		t.Error("a statement nobody could analyse must not be forwarded in block mode")
	}
	if rej := h.gateQuery("SELECT " + strings.Repeat("1+", 100000) + "1"); rej == nil {
		t.Error("... nor one too complex to analyse")
	}
	// A role allowed to do anything still can.
	ex := strictHarness(StrictOptions{Mode: StrictBlock, Exempt: []string{"app"}})
	if rej := ex.gateQuery(`SELEKT 1`); rej != nil {
		t.Errorf("exempt role: %+v", rej)
	}
	// The failure leaves the connection usable and the reply queue aligned.
	h.s = &proxy.Session{ID: 9, User: "app", DB: "d"}
	if rej := h.gateQuery(`SELECT * FROM orders WHERE tenant_id = 'acme'`); rej != nil {
		t.Errorf("a good statement after a refused one: %+v", rej)
	}
}

func TestUnparsedStatementKeepsTheReplyQueueAligned(t *testing.T) {
	h := catalogHarness()
	h.query(`SELEKT broken`)
	h.a.OnBackend(h.s, &pgproto3.ErrorResponse{Message: "syntax error"})
	h.rfq('I')
	h.query(`INSERT INTO orders (id, tenant_id) VALUES (1, 'acme')`)
	h.reply(nil, nil, "INSERT 0 1")
	if _, ok := h.a.Cache().Get("public.orders", "id", "1"); !ok {
		t.Error("the reply after a broken statement was matched to the wrong statement")
	}
	h.a.OnBackend(h.s, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	h.query(fmt.Sprintf("SELECT * FROM orders WHERE tenant_id = '%s'", "acme"))
	if e := h.last(t); e.Source != SourceExplicit {
		t.Errorf("%+v", e)
	}
}

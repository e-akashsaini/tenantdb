package attrib

import (
	"reflect"
	"testing"
)

func TestExtract(t *testing.T) {
	e := NewExtractor([]string{"tenant_id", "org_id"}, nil)
	tests := []struct {
		name    string
		sql     string
		tenants []string
		params  []int
	}{
		{"equality", `SELECT * FROM orders WHERE tenant_id = 'acme'`, []string{"acme"}, nil},
		{"reversed", `SELECT * FROM orders WHERE 'acme' = tenant_id`, []string{"acme"}, nil},
		{"qualified and anded", `SELECT * FROM orders o WHERE o.tenant_id = 'acme' AND o.id = 42`, []string{"acme"}, nil},
		{"in list", `SELECT * FROM orders WHERE tenant_id IN ('a', 'b')`, []string{"a", "b"}, nil},
		{"integer id", `SELECT * FROM orders WHERE org_id = 7`, []string{"7"}, nil},
		{"cast", `SELECT * FROM orders WHERE tenant_id = 'acme'::text`, []string{"acme"}, nil},
		{"param", `SELECT * FROM orders WHERE tenant_id = $1 AND id = $2`, nil, []int{1}},
		{"update", `UPDATE orders SET x = 1 WHERE tenant_id = 'acme'`, []string{"acme"}, nil},
		{"delete", `DELETE FROM orders WHERE tenant_id = $1`, nil, []int{1}},
		{"insert values", `INSERT INTO orders (id, tenant_id) VALUES (1, 'a'), (2, 'b')`, []string{"a", "b"}, nil},
		{"insert param", `INSERT INTO orders (id, tenant_id) VALUES ($1, $2)`, nil, []int{2}},
		{"subquery", `SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE tenant_id = 'x')`, []string{"x"}, nil},
		{"no tenant", `SELECT * FROM orders WHERE id = 42`, nil, nil},
		{"not equal", `SELECT * FROM orders WHERE tenant_id <> 'acme'`, nil, nil},
		{"under NOT", `SELECT * FROM orders WHERE NOT (tenant_id = 'acme')`, nil, nil},
		{"join on columns", `SELECT * FROM a JOIN b ON a.tenant_id = b.tenant_id`, nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := e.Parse(tc.sql)
			if err != nil || len(stmts) != 1 {
				t.Fatalf("parse: %v (%d stmts)", err, len(stmts))
			}
			s := stmts[0]
			if !s.DML {
				t.Fatalf("expected DML")
			}
			if !reflect.DeepEqual(s.Tenants, tc.tenants) {
				t.Errorf("tenants = %v, want %v", s.Tenants, tc.tenants)
			}
			if !reflect.DeepEqual(s.Params, tc.params) {
				t.Errorf("params = %v, want %v", s.Params, tc.params)
			}
		})
	}
}

func TestSplitsStatementsAndTxnControl(t *testing.T) {
	e := NewExtractor([]string{"tenant_id"}, nil)
	stmts, err := e.Parse(`BEGIN; SELECT 1 FROM t WHERE tenant_id = 'a'; COMMIT`)
	if err != nil || len(stmts) != 3 {
		t.Fatalf("got %d stmts, err %v", len(stmts), err)
	}
	if stmts[0].Txn != txnBegin || stmts[2].Txn != txnEnd || !stmts[1].DML {
		t.Errorf("unexpected classification: %+v", stmts)
	}
	if stmts[1].SQL != `SELECT 1 FROM t WHERE tenant_id = 'a'` {
		t.Errorf("sql = %q", stmts[1].SQL)
	}
}

func TestFilteredFlag(t *testing.T) {
	e := NewExtractor([]string{"tenant_id"}, []string{"id"})
	for sql, want := range map[string]bool{
		`SELECT * FROM t`:                                             false,
		`SELECT id FROM t`:                                            false, // selecting a key is not filtering by it
		`SELECT * FROM t WHERE amount > 1`:                            false,
		`SELECT * FROM t WHERE id = 1`:                                true,
		`SELECT * FROM t WHERE tenant_id <> 'a'`:                      true,
		`SELECT * FROM t WHERE x IN (SELECT 1 FROM u WHERE u.id = 2)`: true,
		`SELECT * FROM a JOIN b ON a.id = b.a_id`:                     true,
		`SELECT * FROM a JOIN b ON a.x = b.y`:                         false,
		`UPDATE t SET x = 1`:                                          false,
		`DELETE FROM t WHERE id = 1`:                                  true,
	} {
		stmts, err := e.Parse(sql)
		if err != nil || len(stmts) != 1 {
			t.Fatalf("%s: %v", sql, err)
		}
		if stmts[0].Filtered != want {
			t.Errorf("%s: Filtered = %v, want %v", sql, stmts[0].Filtered, want)
		}
	}
}

func TestTablesAndFuncsCarrySchemas(t *testing.T) {
	e := NewExtractor([]string{"tenant_id"}, nil)
	stmts, _ := e.Parse(`WITH c AS (SELECT 1) SELECT s.t.x, lower(a), app.f(b) FROM s.t JOIN u ON true JOIN c ON true`)
	st := stmts[0]
	if want := []TableRef{{"s", "t"}, {"", "u"}}; !reflect.DeepEqual(st.Tables, want) {
		t.Errorf("tables = %v, want %v (CTE excluded)", st.Tables, want)
	}
	if want := []TableRef{{"app", "f"}, {"", "lower"}}; !reflect.DeepEqual(st.Funcs, want) {
		t.Errorf("funcs = %v, want %v", st.Funcs, want)
	}
	if st.Table.Name != "" {
		t.Error("a join is not a single-table statement")
	}
}

func TestSetStatements(t *testing.T) {
	e := NewExtractor([]string{"tenant_id"}, nil)
	for sql, want := range map[string]*SetVar{
		`SET search_path = a, "B"`:             {Name: "search_path", Values: []string{"a", "B"}},
		`SET LOCAL x.y = 'v'`:                  {Name: "x.y", Values: []string{"v"}, Local: true},
		`SET SESSION x.y TO 7`:                 {Name: "x.y", Values: []string{"7"}},
		`RESET x.y`:                            {Name: "x.y", Reset: true},
		`SET x.y TO DEFAULT`:                   {Name: "x.y", Reset: true},
		`RESET ALL`:                            {ResetAll: true, Name: ""},
		`DISCARD ALL`:                          {ResetAll: true},
		`SELECT set_config('x.y', 'v', true)`:  {Name: "x.y", Values: []string{"v"}, Local: true},
		`SELECT set_config('x.y', 'v', false)`: {Name: "x.y", Values: []string{"v"}},
	} {
		stmts, err := e.Parse(sql)
		if err != nil || len(stmts) != 1 {
			t.Fatalf("%s: %v", sql, err)
		}
		if !reflect.DeepEqual(stmts[0].Set, want) {
			t.Errorf("%s: %+v, want %+v", sql, stmts[0].Set, want)
		}
	}
}

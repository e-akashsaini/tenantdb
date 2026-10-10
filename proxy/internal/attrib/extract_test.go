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

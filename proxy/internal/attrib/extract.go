// Package attrib works out which tenant each SQL statement belongs to.
package attrib

import (
	"sort"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Stmt is the tenant information found in one parsed statement.
type Stmt struct {
	SQL     string
	DML     bool     // SELECT/INSERT/UPDATE/DELETE/MERGE: statements that should be attributed
	Txn     txnKind  // transaction control, if any
	Tenants []string // literal tenant values named by the statement (sorted, unique)
	Params  []int    // 1-based $n parameters compared to the tenant column

	// Row-cache inputs, filled only for statements that touch a single table.
	Table   string   // lower-cased table name, empty when not a single-table statement
	Keys    []Pred   // equality predicates on key columns in the WHERE clause
	Inserts []InsRow // INSERT ... VALUES rows
	Delete  bool     // a DELETE statement
	SetsKey bool     // an UPDATE that assigns a key or tenant column
}

type txnKind int

const (
	txnNone txnKind = iota
	txnBegin
	txnEnd // COMMIT / ROLLBACK / PREPARE TRANSACTION
)

// Extractor finds tenant values for a configured set of column names.
type Extractor struct {
	cols map[string]bool // tenant columns
	keys map[string]bool // columns that identify a row (e.g. id)
}

func NewExtractor(tenantColumns, keyColumns []string) *Extractor {
	e := &Extractor{cols: lowerSet(tenantColumns), keys: lowerSet(keyColumns)}
	return e
}

func lowerSet(in []string) map[string]bool {
	m := map[string]bool{}
	for _, c := range in {
		if c = strings.ToLower(strings.TrimSpace(c)); c != "" {
			m[c] = true
		}
	}
	return m
}

// Parse splits sql into statements and extracts tenant information from each.
func (e *Extractor) Parse(sql string) ([]Stmt, error) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, err
	}
	out := make([]Stmt, 0, len(tree.Stmts))
	for _, raw := range tree.Stmts {
		st := Stmt{SQL: stmtText(sql, raw)}
		switch n := raw.Stmt.GetNode().(type) {
		case *pg_query.Node_SelectStmt, *pg_query.Node_InsertStmt, *pg_query.Node_UpdateStmt,
			*pg_query.Node_DeleteStmt, *pg_query.Node_MergeStmt:
			st.DML = true
		case *pg_query.Node_TransactionStmt:
			switch n.TransactionStmt.Kind {
			case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN, pg_query.TransactionStmtKind_TRANS_STMT_START:
				st.Txn = txnBegin
			case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT, pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK,
				pg_query.TransactionStmtKind_TRANS_STMT_PREPARE:
				st.Txn = txnEnd
			}
		}
		if st.DML {
			vals, params := map[string]bool{}, map[int]bool{}
			e.collect(raw.Stmt, vals, params)
			st.Tenants, st.Params = sortedKeys(vals), sortedInts(params)
			e.shape(raw.Stmt, &st)
		}
		out = append(out, st)
	}
	return out, nil
}

func stmtText(sql string, raw *pg_query.RawStmt) string {
	start := int(raw.StmtLocation)
	end := len(sql)
	if raw.StmtLen > 0 {
		end = start + int(raw.StmtLen)
	}
	if start < 0 || end > len(sql) || start > end {
		return sql
	}
	return strings.TrimSpace(sql[start:end])
}

// collect walks a statement tree gathering tenant values from `col = x`,
// `col IN (...)` and INSERT ... VALUES. Anything under NOT is skipped because
// it excludes a tenant rather than selecting one.
func (e *Extractor) collect(n *pg_query.Node, vals map[string]bool, params map[int]bool) {
	walk(n.ProtoReflect(), func(m proto.Message) bool {
		switch v := m.(type) {
		case *pg_query.BoolExpr:
			return v.Boolop != pg_query.BoolExprType_NOT_EXPR
		case *pg_query.A_Expr:
			e.aExpr(v, vals, params)
		case *pg_query.InsertStmt:
			e.insert(v, vals, params)
		}
		return true
	})
}

func (e *Extractor) aExpr(x *pg_query.A_Expr, vals map[string]bool, params map[int]bool) {
	other, ok := eqOther(x, e.cols)
	if !ok {
		return
	}
	if list := other.GetList(); list != nil {
		for _, item := range list.Items {
			addValue(item, vals, params)
		}
		return
	}
	addValue(other, vals, params)
}

// eqOther returns the non-column side of `col = x` / `col IN (...)` when col
// is one of cols, in either order.
func eqOther(x *pg_query.A_Expr, cols map[string]bool) (*pg_query.Node, bool) {
	if x.Kind != pg_query.A_Expr_Kind_AEXPR_OP && x.Kind != pg_query.A_Expr_Kind_AEXPR_IN {
		return nil, false
	}
	if len(x.Name) != 1 || x.Name[0].GetString_().GetSval() != "=" {
		return nil, false // <>, <, LIKE, NOT IN ... do not select a row
	}
	if colName(x.Lexpr, cols) != "" {
		return x.Rexpr, true
	}
	if colName(x.Rexpr, cols) != "" {
		return x.Lexpr, true
	}
	return nil, false
}

// colName returns the lower-cased column name if n is a reference to one of cols.
func colName(n *pg_query.Node, cols map[string]bool) string {
	ref := unwrap(n).GetColumnRef()
	if ref == nil || len(ref.Fields) == 0 {
		return ""
	}
	name := strings.ToLower(ref.Fields[len(ref.Fields)-1].GetString_().GetSval())
	if cols[name] {
		return name
	}
	return ""
}

func (e *Extractor) insert(s *pg_query.InsertStmt, vals map[string]bool, params map[int]bool) {
	idx := -1
	for i, c := range s.Cols {
		if e.cols[strings.ToLower(c.GetResTarget().GetName())] {
			idx = i
		}
	}
	sel := s.SelectStmt.GetSelectStmt()
	if idx < 0 || sel == nil {
		return
	}
	for _, row := range sel.ValuesLists {
		if items := row.GetList(); items != nil && idx < len(items.Items) {
			addValue(items.Items[idx], vals, params)
		}
	}
}

func addValue(n *pg_query.Node, vals map[string]bool, params map[int]bool) {
	op := operand(n)
	switch {
	case !op.Set:
	case op.Param > 0:
		params[op.Param] = true
	default:
		vals[op.Lit] = true
	}
}

// unwrap strips casts such as 'acme'::text or $1::uuid.
func unwrap(n *pg_query.Node) *pg_query.Node {
	for n != nil && n.GetTypeCast() != nil {
		n = n.GetTypeCast().Arg
	}
	return n
}

func walk(m protoreflect.Message, fn func(proto.Message) bool) {
	if !m.IsValid() || !fn(m.Interface()) {
		return
	}
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsMap() {
			return true
		}
		if fd.IsList() {
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				walk(l.Get(i).Message(), fn)
			}
		} else {
			walk(v.Message(), fn)
		}
		return true
	})
}

func sortedKeys(m map[string]bool) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedInts(m map[int]bool) []int {
	if len(m) == 0 {
		return nil
	}
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

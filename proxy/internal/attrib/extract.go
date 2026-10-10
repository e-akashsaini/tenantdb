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
	DML     bool        // SELECT/INSERT/UPDATE/DELETE/MERGE: statements that should be attributed
	Insert  bool        // an INSERT
	Txn     txnKind     // transaction control, if any
	Tenants []string    // literal tenant values named by the statement (sorted, unique)
	Params  []int       // 1-based $n parameters compared to the tenant column
	Set     *SetVar     // a session variable change (SET, RESET, DISCARD ALL, set_config)
	Temp    *TempDef    // creates a temporary table
	Prepare *SQLPrepare // SQL-level PREPARE
	Exec    *SQLExec    // SQL-level EXECUTE
	Dealloc *string     // DEALLOCATE name ("" for ALL)
	Drops   []TableRef  // tables dropped or truncated

	// Row-cache inputs, filled only for statements that touch a single table.
	Table TableRef // zero value when not a single-table statement
	// Q is the structural model of the tables the statement reads and the
	// conditions over them: what decides which tenants it is confined to.
	Q *QueryNode
	// InsertRows counts the VALUES rows of an INSERT, to tell whether every row names a tenant.
	InsertRows int
	// Aliases maps the name or alias a column can be qualified with to its table,
	// for SELECT/UPDATE/DELETE, so `submissions.id = 5` can be tied to a table in a join.
	Aliases map[string]TableRef
	Keys    []Pred   // equality predicates on key columns in the WHERE clause
	Inserts []InsRow // INSERT ... VALUES rows
	Delete  bool     // a DELETE statement
	SetsKey bool     // an UPDATE that assigns a key or tenant column
	SetCols []string // every column an UPDATE assigns
	Or      bool     // the WHERE clause has an OR: a predicate seen may be only one alternative

	// Filtered is true when a WHERE or JOIN condition mentions a tenant or key
	// column. A statement on tenant data where it is false reads every tenant.
	Filtered bool

	// Writes lists the tables the statement inserts into, updates or deletes from
	// (including inside CTEs), with the trigger events that fire.
	// Copy is set for COPY ... FROM STDIN: its rows arrive after the statement.
	Copy       *CopySpec
	Writes     []Write
	Tables     []TableRef  // every table the statement names (CTE names excluded)
	Funcs      []TableRef  // every function it calls
	Calls      []Call      // the same calls, with their arguments
	Body       *FuncBody   // a DO block's code
	RawInserts [][]Operand // INSERT ... VALUES without a column list: positions need the schema
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

	// Set while reading a function body: the function's name and its named
	// parameters (lower-cased -> 1-based position), so `tenant_id = p_tenant`
	// is read as "the tenant is argument 1".
	fname string
	args  map[string]int
	// Set while reading a trigger function: NEW.<tenant column> and
	// OLD.<tenant column> stand for the tenant of the row that fired it,
	// which the caller binds as argument 1.
	trig bool

	// anyKey: collect equality predicates on every column, not only the
	// configured key columns. The catalog then decides which ones identify a row
	// (a unique column, or a foreign key to one).
	anyKey bool
}

func NewExtractor(tenantColumns, keyColumns []string) *Extractor {
	e := &Extractor{cols: lowerSet(tenantColumns), keys: lowerSet(keyColumns)}
	e.anyKey = len(e.keys) > 0
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
	tree, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	out := make([]Stmt, 0, len(tree.Stmts))
	for _, raw := range tree.Stmts {
		out = append(out, e.stmtFor(stmtText(sql, raw), raw.Stmt))
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
	other, ok := eqOther(x, func(c string) bool { return e.cols[c] })
	if !ok {
		return
	}
	if list := other.GetList(); list != nil {
		for _, item := range list.Items {
			e.addValue(item, vals, params)
		}
		return
	}
	e.addValue(other, vals, params)
}

// eqOther returns the non-column side of `col = x` / `col IN (...)` when col
// is one of cols, in either order.
func eqOther(x *pg_query.A_Expr, match func(string) bool) (*pg_query.Node, bool) {
	if x.Kind != pg_query.A_Expr_Kind_AEXPR_OP && x.Kind != pg_query.A_Expr_Kind_AEXPR_IN {
		return nil, false
	}
	if len(x.Name) != 1 || x.Name[0].GetString_().GetSval() != "=" {
		return nil, false // <>, <, LIKE, NOT IN ... do not select a row
	}
	if colName(x.Lexpr, match) != "" {
		return x.Rexpr, true
	}
	if colName(x.Rexpr, match) != "" {
		return x.Lexpr, true
	}
	return nil, false
}

// colName returns the lower-cased column name if n is a reference to one of cols.
func colName(n *pg_query.Node, match func(string) bool) string {
	ref := unwrap(n).GetColumnRef()
	if ref == nil || len(ref.Fields) == 0 {
		return ""
	}
	name := strings.ToLower(ref.Fields[len(ref.Fields)-1].GetString_().GetSval())
	if match(name) {
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
			e.addValue(items.Items[idx], vals, params)
		}
	}
}

func (e *Extractor) addValue(n *pg_query.Node, vals map[string]bool, params map[int]bool) {
	op := e.operand(n)
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

package attrib

import (
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// TableRef names a table or function, with the schema if the SQL gave one.
// Both parts are lower-cased.
type TableRef struct{ Schema, Name string }

func (r TableRef) String() string {
	if r.Schema == "" {
		return r.Name
	}
	return r.Schema + "." + r.Name
}

// SetVar is a change to a session variable.
type SetVar struct {
	Name     string // lower-cased
	Values   []string
	Local    bool // SET LOCAL: undone when the transaction ends
	Reset    bool // RESET name / SET name TO DEFAULT
	ResetAll bool // RESET ALL / DISCARD ALL
	// ValueOps: the value was not a literal (set_config('x', $1, false)); resolved when run.
	ValueOps []Operand
}

// varCurrentUser stands for current_user: the role the session runs as.
const varCurrentUser = "\x00current_user"

// Operand is a literal, a $n parameter reference, or the current value of a
// session variable: current_setting('app.tenant'), the way applications that use
// Postgres row-level security carry the tenant.
type Operand struct {
	Lit   string
	Param int    // 1-based; 0 for a literal
	Var   string // session variable name (lower-cased), for current_setting('...')
	Set   bool
}

// Call is a call to a function or procedure, with its argument operands.
type Call struct {
	Ref  TableRef
	Args []Operand
}

// Pred is `Col = op` (or `Col IN (ops)`) on a column that might identify a row.
type Pred struct {
	Qual string // the table name or alias the column was qualified with, if any
	Col  string
	Ops  []Operand
}

// InsRow is one row of INSERT ... VALUES: its tenant operand and key operands.
type InsRow struct {
	Tenant Operand
	Keys   []KeyOp
}

type KeyOp struct {
	Col string
	Op  Operand
}

func (e *Extractor) operand(n *pg_query.Node) Operand {
	n = unwrap(n)
	if p := n.GetParamRef(); p != nil {
		return Operand{Param: int(p.Number), Set: true}
	}
	if c := n.GetAConst(); c != nil {
		switch {
		case c.GetSval() != nil:
			return Operand{Lit: c.GetSval().Sval, Set: true}
		case c.GetIval() != nil:
			return Operand{Lit: strconv.Itoa(int(c.GetIval().Ival)), Set: true}
		case c.GetFval() != nil:
			return Operand{Lit: c.GetFval().Fval, Set: true}
		}
	}
	if f := n.GetSqlvalueFunction(); f != nil {
		switch f.Op {
		case pg_query.SQLValueFunctionOp_SVFOP_CURRENT_USER, pg_query.SQLValueFunctionOp_SVFOP_USER:
			return Operand{Var: varCurrentUser, Set: true}
		}
	}
	if fc := n.GetFuncCall(); fc != nil && len(fc.Funcname) > 0 && len(fc.Args) >= 1 &&
		strings.EqualFold(fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval(), "current_setting") {
		if c := unwrap(fc.Args[0]).GetAConst(); c != nil && c.GetSval() != nil {
			return Operand{Var: strings.ToLower(c.GetSval().Sval), Set: true}
		}
	}
	if ref := n.GetColumnRef(); ref != nil && e.trig && len(ref.Fields) == 2 {
		q := strings.ToLower(ref.Fields[0].GetString_().GetSval())
		if c := strings.ToLower(ref.Fields[1].GetString_().GetSval()); (q == "new" || q == "old") && e.cols[c] {
			return Operand{Param: 1, Set: true}
		}
	}
	if ref := n.GetColumnRef(); ref != nil && len(e.args) > 0 {
		name := ""
		switch len(ref.Fields) {
		case 1:
			name = ref.Fields[0].GetString_().GetSval()
		case 2: // function_name.param
			if strings.EqualFold(ref.Fields[0].GetString_().GetSval(), e.fname) {
				name = ref.Fields[1].GetString_().GetSval()
			}
		}
		if pos, ok := e.args[strings.ToLower(name)]; ok {
			return Operand{Param: pos, Set: true}
		}
	}
	return Operand{}
}

// shape fills the single-table fields of st. Statements with joins, CTEs,
// set operations or USING/FROM lists are left without a Table: the cache only
// reasons about statements where every column belongs to one known table.
func (e *Extractor) shape(n *pg_query.Node, st *Stmt) {
	switch v := n.GetNode().(type) {
	case *pg_query.Node_SelectStmt:
		s := v.SelectStmt
		if s.WithClause == nil && s.Op == pg_query.SetOperation_SETOP_NONE {
			if len(s.FromClause) == 1 && s.FromClause[0].GetRangeVar() != nil {
				st.Table = relRef(s.FromClause[0].GetRangeVar())
			}
			// Joins too: a predicate on a qualified column still names its table.
			st.Aliases = aliasesOf(s.FromClause...)
			st.Keys, st.Or = e.keyPreds(s.WhereClause)
			e.confine(st, s.WhereClause)
		}
	case *pg_query.Node_UpdateStmt:
		u := v.UpdateStmt
		if len(u.FromClause) == 0 && u.WithClause == nil && u.Relation != nil {
			st.Table = relRef(u.Relation)
			st.Aliases = aliasesOf(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: u.Relation}})
			st.Keys, st.Or = e.keyPreds(u.WhereClause)
			e.confine(st, u.WhereClause)
			for _, t := range u.TargetList {
				name := strings.ToLower(t.GetResTarget().GetName())
				st.SetCols = append(st.SetCols, name)
				if e.keys[name] || e.cols[name] {
					st.SetsKey = true
				}
			}
		}
	case *pg_query.Node_DeleteStmt:
		d := v.DeleteStmt
		if len(d.UsingClause) == 0 && d.WithClause == nil && d.Relation != nil {
			st.Table = relRef(d.Relation)
			st.Aliases = aliasesOf(&pg_query.Node{Node: &pg_query.Node_RangeVar{RangeVar: d.Relation}})
			st.Keys, st.Or = e.keyPreds(d.WhereClause)
			e.confine(st, d.WhereClause)
			st.Delete = true
		}
	case *pg_query.Node_InsertStmt:
		if v.InsertStmt.Relation != nil && v.InsertStmt.WithClause == nil {
			st.Table = relRef(v.InsertStmt.Relation)
			if sel := v.InsertStmt.SelectStmt.GetSelectStmt(); sel != nil {
				st.InsertRows = len(sel.ValuesLists)
			}
			st.Inserts = e.insertRows(v.InsertStmt)
			st.RawInserts = e.rawInsertRows(v.InsertStmt)
		}
	}
}

// aliasesOf maps every name a FROM clause gives its tables (the table name and
// any alias) to the table. Subqueries and functions in FROM are not tables.
func aliasesOf(from ...*pg_query.Node) map[string]TableRef {
	m := map[string]TableRef{}
	for _, f := range from {
		walkTop(f, func(rv *pg_query.RangeVar) {
			ref := relRef(rv)
			m[ref.Name] = ref
			if rv.Alias != nil {
				m[strings.ToLower(rv.Alias.Aliasname)] = ref
			}
		})
	}
	return m
}

// walkTop visits the tables of a FROM item and of the joins under it, without
// descending into subqueries.
func walkTop(n *pg_query.Node, fn func(*pg_query.RangeVar)) {
	switch {
	case n == nil:
	case n.GetRangeVar() != nil:
		fn(n.GetRangeVar())
	case n.GetJoinExpr() != nil:
		walkTop(n.GetJoinExpr().Larg, fn)
		walkTop(n.GetJoinExpr().Rarg, fn)
	}
}

func qualOf(n *pg_query.Node) string {
	ref := unwrap(n).GetColumnRef()
	if ref == nil || len(ref.Fields) < 2 {
		return ""
	}
	return strings.ToLower(ref.Fields[len(ref.Fields)-2].GetString_().GetSval())
}

func relRef(r *pg_query.RangeVar) TableRef {
	return TableRef{Schema: strings.ToLower(r.Schemaname), Name: strings.ToLower(r.Relname)}
}

// confine replaces the tenants found anywhere in a statement with the ones its
// WHERE clause actually confines it to, when the clause has an OR. Without an OR
// every tenant predicate is an AND-ed restriction and the plain scan is right.
// With one, `tenant_id = 'a' OR amount > 5` names a tenant but also reads the
// rows of every other.
func (e *Extractor) confine(st *Stmt, where *pg_query.Node) {
	if !st.Or {
		return
	}
	vals, params, ok := e.confined(where)
	if !ok {
		st.Tenants, st.Params = nil, nil
		return
	}
	st.Tenants, st.Params = sortedKeys(vals), sortedInts(params)
}

// confined says which tenants a boolean expression restricts rows to. AND is
// restricted by any one of its parts; OR only if every alternative is.
func (e *Extractor) confined(n *pg_query.Node) (vals map[string]bool, params map[int]bool, ok bool) {
	if n == nil {
		return nil, nil, false
	}
	vals, params = map[string]bool{}, map[int]bool{}
	merge := func(v map[string]bool, p map[int]bool) {
		for k := range v {
			vals[k] = true
		}
		for k := range p {
			params[k] = true
		}
	}
	switch {
	case n.GetBoolExpr() != nil:
		b := n.GetBoolExpr()
		switch b.Boolop {
		case pg_query.BoolExprType_AND_EXPR:
			for _, arg := range b.Args {
				if v, p, found := e.confined(arg); found {
					merge(v, p)
					ok = true
				}
			}
			return vals, params, ok
		case pg_query.BoolExprType_OR_EXPR:
			for _, arg := range b.Args {
				v, p, found := e.confined(arg)
				if !found {
					return nil, nil, false
				}
				merge(v, p)
			}
			return vals, params, len(b.Args) > 0
		}
		return nil, nil, false
	case n.GetAExpr() != nil:
		other, found := eqOther(n.GetAExpr(), func(c string) bool { return e.cols[c] })
		if !found {
			return nil, nil, false
		}
		if list := other.GetList(); list != nil {
			for _, it := range list.Items {
				e.addValue(it, vals, params)
			}
		} else {
			e.addValue(other, vals, params)
		}
		return vals, params, len(vals)+len(params) > 0
	}
	return nil, nil, false
}

// isKeyish: could this column identify a row? Tenant columns never do. With
// anyKey every other column might; the catalog decides which really do.
func (e *Extractor) isKeyish(c string) bool {
	return !e.cols[c] && (e.anyKey || e.keys[c])
}

// keyPreds collects equality predicates `col = value` / `col IN (values)` from a
// WHERE clause, on columns that might identify a row. It does not descend into
// subqueries or under NOT. or reports whether the clause contains an OR, in
// which case a predicate found is only one alternative.
func (e *Extractor) keyPreds(where *pg_query.Node) (preds []Pred, or bool) {
	if where == nil || (!e.anyKey && len(e.keys) == 0) {
		return nil, false
	}
	walk(where.ProtoReflect(), func(m proto.Message) bool {
		switch v := m.(type) {
		case *pg_query.BoolExpr:
			if v.Boolop == pg_query.BoolExprType_OR_EXPR {
				or = true
			}
			return v.Boolop != pg_query.BoolExprType_NOT_EXPR
		case *pg_query.SubLink:
			return false
		case *pg_query.A_Expr:
			other, ok := eqOther(v, e.isKeyish)
			if !ok {
				return true
			}
			col, qual := colName(v.Lexpr, e.isKeyish), qualOf(v.Lexpr)
			if col == "" {
				col, qual = colName(v.Rexpr, e.isKeyish), qualOf(v.Rexpr)
			}
			p := Pred{Qual: qual, Col: col}
			if list := other.GetList(); list != nil {
				for _, it := range list.Items {
					if op := e.operand(it); op.Set {
						p.Ops = append(p.Ops, op)
					}
				}
			} else if op := e.operand(other); op.Set {
				p.Ops = []Operand{op}
			}
			if len(p.Ops) > 0 { // `a.x = b.y` compares two columns: nothing to look up
				preds = append(preds, p)
			}
		}
		return true
	})
	return preds, or
}

func (e *Extractor) insertRows(s *pg_query.InsertStmt) []InsRow {
	sel := s.SelectStmt.GetSelectStmt()
	if sel == nil || len(sel.ValuesLists) == 0 || len(s.Cols) == 0 {
		return nil // no column list: see rawInsertRows
	}
	var out []InsRow
	for _, row := range sel.ValuesLists {
		items := row.GetList()
		if items == nil {
			continue
		}
		var r InsRow
		for i, c := range s.Cols {
			if i >= len(items.Items) {
				break
			}
			name := strings.ToLower(c.GetResTarget().GetName())
			switch {
			case e.cols[name]:
				r.Tenant = e.operand(items.Items[i])
			case e.isKeyish(name):
				r.Keys = append(r.Keys, KeyOp{Col: name, Op: e.operand(items.Items[i])})
			}
		}
		if r.Tenant.Set {
			out = append(out, r)
		}
	}
	return out
}

// rawInsertRows returns the operands of each VALUES row of an INSERT that has
// no column list. Which position is the tenant needs the table schema.
func (e *Extractor) rawInsertRows(s *pg_query.InsertStmt) [][]Operand {
	sel := s.SelectStmt.GetSelectStmt()
	if sel == nil || len(sel.ValuesLists) == 0 || len(s.Cols) != 0 {
		return nil
	}
	var out [][]Operand
	for _, row := range sel.ValuesLists {
		items := row.GetList()
		if items == nil {
			continue
		}
		ops := make([]Operand, len(items.Items))
		for i, it := range items.Items {
			ops[i] = e.operand(it)
		}
		out = append(out, ops)
	}
	return out
}

// tablesOf lists every table and function a statement names. Names that are
// really CTEs are left out.
func (e *Extractor) tablesOf(n *pg_query.Node) (tables, funcs []TableRef, calls []Call) {
	ctes := map[string]bool{}
	walk(n.ProtoReflect(), func(m proto.Message) bool {
		if c, ok := m.(*pg_query.CommonTableExpr); ok {
			ctes[strings.ToLower(c.Ctename)] = true
		}
		return true
	})
	tset, fset := map[TableRef]bool{}, map[TableRef]bool{}
	walk(n.ProtoReflect(), func(m proto.Message) bool {
		switch v := m.(type) {
		case *pg_query.IntoClause:
			return false // the table being created is not one being read
		case *pg_query.RangeVar:
			if r := relRef(v); r.Schema != "" || !ctes[r.Name] {
				tset[r] = true
			}
		case *pg_query.FuncCall:
			var parts []string
			for _, p := range v.Funcname {
				parts = append(parts, strings.ToLower(p.GetString_().GetSval()))
			}
			var ref TableRef
			switch len(parts) {
			case 0:
				return true
			case 1:
				ref = TableRef{Name: parts[0]}
			default:
				ref = TableRef{Schema: parts[len(parts)-2], Name: parts[len(parts)-1]}
			}
			fset[ref] = true
			c := Call{Ref: ref}
			for _, a := range v.Args {
				c.Args = append(c.Args, e.operand(a))
			}
			calls = append(calls, c)
		}
		return true
	})
	return sortedRefs(tset), sortedRefs(fset), calls
}

func sortedRefs(m map[TableRef]bool) []TableRef {
	if len(m) == 0 {
		return nil
	}
	out := make([]TableRef, 0, len(m))
	for r := range m {
		out = append(out, r)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].String() < out[j-1].String(); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// filtered reports whether any WHERE or JOIN condition in the statement refers
// to a tenant or key column, in whatever form. If none does, the statement
// cannot be restricting itself to particular rows or tenants.
func (e *Extractor) filtered(n *pg_query.Node) bool {
	found := false
	scan := func(root *pg_query.Node) {
		if root == nil || found {
			return
		}
		walk(root.ProtoReflect(), func(m proto.Message) bool {
			if ref, ok := m.(*pg_query.ColumnRef); ok && len(ref.Fields) > 0 {
				name := strings.ToLower(ref.Fields[len(ref.Fields)-1].GetString_().GetSval())
				if e.cols[name] || e.keys[name] {
					found = true
				}
			}
			return !found
		})
	}
	walk(n.ProtoReflect(), func(m proto.Message) bool {
		switch v := m.(type) {
		case *pg_query.SelectStmt:
			scan(v.WhereClause)
		case *pg_query.UpdateStmt:
			scan(v.WhereClause)
		case *pg_query.DeleteStmt:
			scan(v.WhereClause)
		case *pg_query.JoinExpr:
			scan(v.Quals)
		case *pg_query.MergeStmt:
			scan(v.JoinCondition)
		}
		return !found
	})
	return found
}

// setVar reads a SET / RESET statement. It returns nil for forms it does not
// need to track.
func (e *Extractor) setVar(v *pg_query.VariableSetStmt) *SetVar {
	sv := &SetVar{Name: strings.ToLower(v.Name), Local: v.IsLocal}
	switch v.Kind {
	case pg_query.VariableSetKind_VAR_SET_VALUE:
		for _, a := range v.Args {
			if op := e.operand(a); op.Set {
				sv.Values = append(sv.Values, op.Lit)
			}
		}
	case pg_query.VariableSetKind_VAR_SET_DEFAULT, pg_query.VariableSetKind_VAR_RESET:
		sv.Reset = true
	case pg_query.VariableSetKind_VAR_RESET_ALL:
		sv.ResetAll = true
	default:
		return nil
	}
	return sv
}

// selectSetConfig recognises `SELECT set_config('name', 'value', is_local)`,
// the form most drivers and ORMs use to set a session variable.
func (e *Extractor) selectSetConfig(n *pg_query.Node) *SetVar {
	sel := n.GetSelectStmt()
	if sel == nil {
		return nil
	}
	for _, t := range sel.TargetList {
		fc := t.GetResTarget().GetVal().GetFuncCall()
		if fc == nil || len(fc.Funcname) == 0 || len(fc.Args) < 2 ||
			strings.ToLower(fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval()) != "set_config" {
			continue
		}
		name, val := e.operand(fc.Args[0]), e.operand(fc.Args[1])
		if !name.Set || name.Param != 0 || name.Var != "" || !val.Set || val.Var != "" {
			continue
		}
		sv := &SetVar{Name: strings.ToLower(name.Lit)}
		if val.Param != 0 {
			sv.ValueOps = []Operand{val}
		} else {
			sv.Values = []string{val.Lit}
		}
		if len(fc.Args) >= 3 {
			if c := unwrap(fc.Args[2]).GetAConst(); c != nil && c.GetBoolval() != nil {
				sv.Local = c.GetBoolval().Boolval
			}
		}
		return sv
	}
	return nil
}

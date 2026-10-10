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
}

// Operand is a literal or a $n parameter reference.
type Operand struct {
	Lit   string
	Param int // 1-based; 0 for a literal
	Set   bool
}

// Pred is `Col = op` (or `Col IN (ops)`) on a key column.
type Pred struct {
	Col string
	Ops []Operand
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

func operand(n *pg_query.Node) Operand {
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
	return Operand{}
}

// shape fills the single-table fields of st. Statements with joins, CTEs,
// set operations or USING/FROM lists are left without a Table: the cache only
// reasons about statements where every column belongs to one known table.
func (e *Extractor) shape(n *pg_query.Node, st *Stmt) {
	switch v := n.GetNode().(type) {
	case *pg_query.Node_SelectStmt:
		s := v.SelectStmt
		if len(s.FromClause) == 1 && s.FromClause[0].GetRangeVar() != nil && s.WithClause == nil &&
			s.Op == pg_query.SetOperation_SETOP_NONE {
			st.Table = relRef(s.FromClause[0].GetRangeVar())
			st.Keys = e.keyPreds(s.WhereClause)
		}
	case *pg_query.Node_UpdateStmt:
		u := v.UpdateStmt
		if len(u.FromClause) == 0 && u.WithClause == nil && u.Relation != nil {
			st.Table = relRef(u.Relation)
			st.Keys = e.keyPreds(u.WhereClause)
			for _, t := range u.TargetList {
				name := strings.ToLower(t.GetResTarget().GetName())
				if e.keys[name] || e.cols[name] {
					st.SetsKey = true
				}
			}
		}
	case *pg_query.Node_DeleteStmt:
		d := v.DeleteStmt
		if len(d.UsingClause) == 0 && d.WithClause == nil && d.Relation != nil {
			st.Table = relRef(d.Relation)
			st.Keys = e.keyPreds(d.WhereClause)
			st.Delete = true
		}
	case *pg_query.Node_InsertStmt:
		if v.InsertStmt.Relation != nil && v.InsertStmt.WithClause == nil {
			st.Table = relRef(v.InsertStmt.Relation)
			st.Inserts = e.insertRows(v.InsertStmt)
			st.RawInserts = rawInsertRows(v.InsertStmt)
		}
	}
}

func relRef(r *pg_query.RangeVar) TableRef {
	return TableRef{Schema: strings.ToLower(r.Schemaname), Name: strings.ToLower(r.Relname)}
}

// keyPreds collects key-column equality predicates from a WHERE clause. It
// does not descend into subqueries or under NOT.
func (e *Extractor) keyPreds(where *pg_query.Node) []Pred {
	if where == nil || len(e.keys) == 0 {
		return nil
	}
	var out []Pred
	walk(where.ProtoReflect(), func(m proto.Message) bool {
		switch v := m.(type) {
		case *pg_query.BoolExpr:
			return v.Boolop != pg_query.BoolExprType_NOT_EXPR
		case *pg_query.SubLink:
			return false
		case *pg_query.A_Expr:
			other, ok := eqOther(v, e.keys)
			if !ok {
				return true
			}
			col := colName(v.Lexpr, e.keys)
			if col == "" {
				col = colName(v.Rexpr, e.keys)
			}
			p := Pred{Col: col}
			if list := other.GetList(); list != nil {
				for _, it := range list.Items {
					p.Ops = append(p.Ops, operand(it))
				}
			} else {
				p.Ops = []Operand{operand(other)}
			}
			out = append(out, p)
		}
		return true
	})
	return out
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
				r.Tenant = operand(items.Items[i])
			case e.keys[name]:
				r.Keys = append(r.Keys, KeyOp{Col: name, Op: operand(items.Items[i])})
			}
		}
		if r.Tenant.Set && len(r.Keys) > 0 {
			out = append(out, r)
		}
	}
	return out
}

// rawInsertRows returns the operands of each VALUES row of an INSERT that has
// no column list. Which position is the tenant needs the table schema.
func rawInsertRows(s *pg_query.InsertStmt) [][]Operand {
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
			ops[i] = operand(it)
		}
		out = append(out, ops)
	}
	return out
}

// tablesOf lists every table and function a statement names. Names that are
// really CTEs are left out.
func tablesOf(n *pg_query.Node) (tables, funcs []TableRef) {
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
		case *pg_query.RangeVar:
			if r := relRef(v); r.Schema != "" || !ctes[r.Name] {
				tset[r] = true
			}
		case *pg_query.FuncCall:
			var parts []string
			for _, p := range v.Funcname {
				parts = append(parts, strings.ToLower(p.GetString_().GetSval()))
			}
			switch len(parts) {
			case 0:
			case 1:
				fset[TableRef{Name: parts[0]}] = true
			default:
				fset[TableRef{Schema: parts[len(parts)-2], Name: parts[len(parts)-1]}] = true
			}
		}
		return true
	})
	return sortedRefs(tset), sortedRefs(fset)
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
func setVar(v *pg_query.VariableSetStmt) *SetVar {
	sv := &SetVar{Name: strings.ToLower(v.Name), Local: v.IsLocal}
	switch v.Kind {
	case pg_query.VariableSetKind_VAR_SET_VALUE:
		for _, a := range v.Args {
			if op := operand(a); op.Set {
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
func selectSetConfig(n *pg_query.Node) *SetVar {
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
		name, val := operand(fc.Args[0]), operand(fc.Args[1])
		if !name.Set || name.Param != 0 || !val.Set || val.Param != 0 {
			continue
		}
		sv := &SetVar{Name: strings.ToLower(name.Lit), Values: []string{val.Lit}}
		if len(fc.Args) >= 3 {
			if c := unwrap(fc.Args[2]).GetAConst(); c != nil && c.GetBoolval() != nil {
				sv.Local = c.GetBoolval().Boolval
			}
		}
		return sv
	}
	return nil
}

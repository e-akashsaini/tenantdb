package attrib

import (
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

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
			st.Table = relName(s.FromClause[0].GetRangeVar())
			st.Keys = e.keyPreds(s.WhereClause)
		}
	case *pg_query.Node_UpdateStmt:
		u := v.UpdateStmt
		if len(u.FromClause) == 0 && u.WithClause == nil && u.Relation != nil {
			st.Table = relName(u.Relation)
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
			st.Table = relName(d.Relation)
			st.Keys = e.keyPreds(d.WhereClause)
			st.Delete = true
		}
	case *pg_query.Node_InsertStmt:
		if v.InsertStmt.Relation != nil && v.InsertStmt.WithClause == nil {
			st.Table = relName(v.InsertStmt.Relation)
			st.Inserts = e.insertRows(v.InsertStmt)
		}
	}
}

func relName(r *pg_query.RangeVar) string { return strings.ToLower(r.Relname) }

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
		return nil // no column list: positions unknown without the table schema
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

package attrib

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// The structural model behind tenant attribution.
//
// A statement may be attributed to a tenant only if every table it reads, at
// every level of nesting, is confined to that tenant. Looking for "tenant_id =
// 'a'" anywhere in the text is not enough: it may sit inside an EXISTS that
// restricts a different table, in one branch of a UNION, in an OR whose other
// alternative reads everything, or in the ON clause of an outer join that
// preserves the other side. So the extractor does not guess; it records, per
// query level, which table occurrences there are and the boolean structure of
// the conditions over them, and the attributor decides what each condition
// confines once the catalog says which tables carry a tenant.

// CRef is a reference to a column of a table occurrence.
type CRef struct {
	Occ   string   // the occurrence it belongs to; "" if it could not be tied to one
	Cands []string // occurrences it might belong to, when unqualified in a join (the catalog decides)
	Col   string   // lower-cased
}

// Leaf is one comparison.
type Leaf struct {
	L CRef
	// Eq: L = value, or L IN (values); Ops are the values.
	Eq  bool
	Ops []Operand
	// R is set for a comparison of two columns (a join condition).
	R *CRef
	// In is set for `L IN (SELECT out FROM ...)`: the rows of L's table that can
	// match are those whose L value some row of the subquery produces.
	In *InSub
	// Mentions lists the columns of any other comparison (ranges, LIKE, functions of
	// a column, ...): they say nothing about which tenant, but they do mean the
	// statement is restricted by something.
	Mentions []CRef
}

// InSub is the subquery of `x IN (SELECT out ...)`.
type InSub struct {
	Q   *QueryNode
	Out CRef
}

type CondKind int

const (
	CondOther CondKind = iota // anything that confines nothing: NOT, subquery tests, functions, constants
	CondAnd
	CondOr
	CondLeaf
)

// Cond is a boolean expression.
type Cond struct {
	Kind     CondKind
	Kids     []*Cond
	Leaf     *Leaf
	Mentions []CRef // for CondOther: the columns it mentions
}

// Scoped is a condition together with the occurrences it can confine: a WHERE
// clause and the ON of an inner join confine every occurrence of the level; the
// ON of a LEFT JOIN only the right side, and a FULL JOIN's ON none.
type Scoped struct {
	C        *Cond
	Confines []string // nil: every occurrence of the level
}

// Occ is one appearance of a table in a FROM clause.
type Occ struct {
	ID    string // unique within the statement
	Ref   TableRef
	Alias string
}

// QueryNode is one query level: a SELECT, or the target of an UPDATE/DELETE.
type QueryNode struct {
	Occs  []Occ
	Conds []Scoped
	// Out is the one column a plain SELECT returns, if that is all it returns.
	Out  *CRef
	Subs []*QueryNode // nested queries: subselects anywhere, CTEs, derived tables, set-operation branches
	// Unsupported marks constructs whose reads cannot be bounded (MERGE ...).
	Unsupported bool
}

type bscope struct {
	parent *bscope
	names  map[string]string // table name or alias -> occurrence id ("" for a CTE / derived table)
	occs   []string
}

func (s *bscope) lookup(name string) (string, bool) {
	for c := s; c != nil; c = c.parent {
		if id, ok := c.names[name]; ok {
			return id, true
		}
	}
	return "", false
}

type builder struct {
	e       *Extractor
	n       int
	subSeen map[*pg_query.SubLink]bool
	subNode map[*pg_query.SubLink]*QueryNode
}

// buildQuery builds the model of a statement, or nil if it has no table access to model.
func (e *Extractor) buildQuery(node *pg_query.Node) *QueryNode {
	b := &builder{e: e, subSeen: map[*pg_query.SubLink]bool{}, subNode: map[*pg_query.SubLink]*QueryNode{}}
	return b.stmt(node, &bscope{names: map[string]string{}})
}

// stmt builds the model of one statement under a scope.
func (b *builder) stmt(node *pg_query.Node, root *bscope) *QueryNode {
	switch v := node.GetNode().(type) {
	case *pg_query.Node_SelectStmt:
		return b.selectStmt(v.SelectStmt, root)
	case *pg_query.Node_UpdateStmt:
		u := v.UpdateStmt
		q := &QueryNode{}
		sc := &bscope{parent: root, names: map[string]string{}}
		b.withClause(q, u.WithClause, sc)
		b.addOcc(q, sc, u.Relation)
		for _, f := range u.FromClause {
			b.addFrom(q, sc, f)
		}
		b.cond(q, sc, u.WhereClause, nil)
		b.sweep(q, sc, node)
		return q
	case *pg_query.Node_DeleteStmt:
		d := v.DeleteStmt
		q := &QueryNode{}
		sc := &bscope{parent: root, names: map[string]string{}}
		b.withClause(q, d.WithClause, sc)
		b.addOcc(q, sc, d.Relation)
		for _, f := range d.UsingClause {
			b.addFrom(q, sc, f)
		}
		b.cond(q, sc, d.WhereClause, nil)
		b.sweep(q, sc, node)
		return q
	case *pg_query.Node_InsertStmt:
		// The target's rows are described by the VALUES; what the source query
		// reads still has to be confined.
		i := v.InsertStmt
		q := &QueryNode{}
		sc := &bscope{parent: root, names: map[string]string{}}
		b.withClause(q, i.WithClause, sc)
		if sel := i.SelectStmt.GetSelectStmt(); sel != nil && len(sel.ValuesLists) == 0 {
			q.Subs = append(q.Subs, b.selectStmt(sel, sc))
		}
		b.sweep(q, sc, node)
		return q
	case *pg_query.Node_MergeStmt:
		return &QueryNode{Unsupported: true}
	case *pg_query.Node_CopyStmt:
		c := v.CopyStmt
		q := &QueryNode{}
		sc := &bscope{parent: root, names: map[string]string{}}
		if c.Relation != nil && !c.IsFrom {
			b.addOcc(q, sc, c.Relation)
		}
		if c.Query != nil {
			if sub := b.query(c.Query, sc); sub != nil {
				q.Subs = append(q.Subs, sub)
			}
		}
		return q
	}
	// Anything else that names tables (CALL arguments, EXPLAIN, ...): sweep for subqueries.
	q := &QueryNode{}
	sc := &bscope{parent: root, names: map[string]string{}}
	b.sweep(q, sc, node)
	if len(q.Subs) == 0 {
		return nil
	}
	return q
}

// query builds any query node (select, set operation).
func (b *builder) query(n *pg_query.Node, sc *bscope) *QueryNode {
	if s := n.GetSelectStmt(); s != nil {
		return b.selectStmt(s, sc)
	}
	return nil
}

func (b *builder) selectStmt(s *pg_query.SelectStmt, parent *bscope) *QueryNode {
	q := &QueryNode{}
	sc := &bscope{parent: parent, names: map[string]string{}}
	b.withClause(q, s.WithClause, sc)
	if s.Op != pg_query.SetOperation_SETOP_NONE {
		// UNION / INTERSECT / EXCEPT: each branch reads on its own.
		for _, br := range []*pg_query.SelectStmt{s.Larg, s.Rarg} {
			if br != nil {
				q.Subs = append(q.Subs, b.selectStmt(br, sc))
			}
		}
		b.sweep(q, sc, &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s}})
		return q
	}
	for _, f := range s.FromClause {
		b.addFrom(q, sc, f)
	}
	b.cond(q, sc, s.WhereClause, nil)
	b.sweep(q, sc, &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: s}})
	if len(s.TargetList) == 1 && len(s.GroupClause) == 0 {
		if rt := s.TargetList[0].GetResTarget(); rt != nil {
			if c, ok := b.colRef(sc, rt.Val); ok {
				q.Out = &c
			}
		}
	}
	return q
}

func (b *builder) withClause(q *QueryNode, w *pg_query.WithClause, sc *bscope) {
	if w == nil {
		return
	}
	for _, c := range w.Ctes {
		cte := c.GetCommonTableExpr()
		if cte == nil {
			continue
		}
		sc.names[strings.ToLower(cte.Ctename)] = "" // a derived relation, not a table
		if sub := b.queryOrDML(cte.Ctequery, sc); sub != nil {
			q.Subs = append(q.Subs, sub)
		}
	}
}

// queryOrDML builds a CTE body, which may be a data-modifying statement.
func (b *builder) queryOrDML(n *pg_query.Node, sc *bscope) *QueryNode {
	switch {
	case n.GetSelectStmt() != nil:
		return b.selectStmt(n.GetSelectStmt(), sc)
	case n.GetUpdateStmt() != nil, n.GetDeleteStmt() != nil, n.GetInsertStmt() != nil:
		return b.stmt(n, sc)
	}
	return nil
}

func (b *builder) addOcc(q *QueryNode, sc *bscope, rv *pg_query.RangeVar) string {
	if rv == nil {
		return ""
	}
	ref := relRef(rv)
	if rv.Schemaname == "" {
		if id, ok := sc.lookup(ref.Name); ok && id == "" {
			return "" // a CTE
		}
	}
	b.n++
	o := Occ{ID: "o" + itoa(b.n), Ref: ref}
	sc.names[ref.Name] = o.ID
	if rv.Alias != nil && rv.Alias.Aliasname != "" {
		o.Alias = strings.ToLower(rv.Alias.Aliasname)
		sc.names[o.Alias] = o.ID
	}
	q.Occs = append(q.Occs, o)
	sc.occs = append(sc.occs, o.ID)
	return o.ID
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var d []byte
	for ; n > 0; n /= 10 {
		d = append([]byte{byte('0' + n%10)}, d...)
	}
	return string(d)
}

// addFrom adds a FROM item and returns the occurrences it contributes.
func (b *builder) addFrom(q *QueryNode, sc *bscope, n *pg_query.Node) []string {
	switch {
	case n.GetRangeVar() != nil:
		if id := b.addOcc(q, sc, n.GetRangeVar()); id != "" {
			return []string{id}
		}
	case n.GetJoinExpr() != nil:
		j := n.GetJoinExpr()
		left := b.addFrom(q, sc, j.Larg)
		right := b.addFrom(q, sc, j.Rarg)
		var confines []string
		switch j.Jointype {
		case pg_query.JoinType_JOIN_INNER:
			confines = nil // like WHERE
		case pg_query.JoinType_JOIN_LEFT:
			confines = append(confines, right...)
		case pg_query.JoinType_JOIN_RIGHT:
			confines = append(confines, left...)
		default:
			confines = []string{} // FULL: the ON clause confines neither side
		}
		if j.Quals != nil {
			b.cond(q, sc, j.Quals, confines)
		}
		return append(left, right...)
	case n.GetRangeSubselect() != nil:
		rs := n.GetRangeSubselect()
		if sub := b.query(rs.Subquery, sc); sub != nil {
			q.Subs = append(q.Subs, sub)
		}
		if rs.Alias != nil {
			sc.names[strings.ToLower(rs.Alias.Aliasname)] = ""
		}
	}
	return nil
}

// cond adds a condition to the level.
func (b *builder) cond(q *QueryNode, sc *bscope, n *pg_query.Node, confines []string) {
	if n == nil {
		return
	}
	q.Conds = append(q.Conds, Scoped{C: b.expr(q, sc, n), Confines: confines})
}

// sweep builds every subquery of a statement that a condition did not already
// reach (select-list subqueries, HAVING, RETURNING, function arguments, ...).
// A subquery reads tables however it is used.
func (b *builder) sweep(q *QueryNode, sc *bscope, root *pg_query.Node) {
	walk(root.ProtoReflect(), func(m proto.Message) bool {
		if sl, ok := m.(*pg_query.SubLink); ok && !b.subSeen[sl] {
			b.subSeen[sl] = true
			if sub := b.query(sl.Subselect, sc); sub != nil {
				q.Subs = append(q.Subs, sub)
				b.subNode[sl] = sub
			}
			return false
		}
		return true
	})
}

// subLinks registers every subquery under n (which a condition is built from).
func (b *builder) subLinks(q *QueryNode, sc *bscope, n *pg_query.Node) {
	if n == nil {
		return
	}
	walk(n.ProtoReflect(), func(m proto.Message) bool {
		if sl, ok := m.(*pg_query.SubLink); ok && !b.subSeen[sl] {
			b.subSeen[sl] = true
			if sub := b.query(sl.Subselect, sc); sub != nil {
				q.Subs = append(q.Subs, sub)
				b.subNode[sl] = sub
			}
			return false
		}
		return true
	})
}

// colRef resolves a column reference to the occurrence it belongs to.
func (b *builder) colRef(sc *bscope, n *pg_query.Node) (CRef, bool) {
	ref := unwrap(n).GetColumnRef()
	if ref == nil || len(ref.Fields) == 0 {
		return CRef{}, false
	}
	last := ref.Fields[len(ref.Fields)-1]
	if last.GetAStar() != nil {
		return CRef{}, false
	}
	c := CRef{Col: strings.ToLower(last.GetString_().GetSval())}
	if len(ref.Fields) >= 2 {
		qual := strings.ToLower(ref.Fields[len(ref.Fields)-2].GetString_().GetSval())
		if id, ok := sc.lookup(qual); ok {
			c.Occ = id
		}
		return c, true
	}
	// Unqualified: it binds in the innermost level that has tables.
	for s := sc; s != nil; s = s.parent {
		switch len(s.occs) {
		case 0:
			continue
		case 1:
			c.Occ = s.occs[0]
		default:
			c.Cands = append([]string(nil), s.occs...)
		}
		return c, true
	}
	return c, true
}

func (b *builder) mentions(sc *bscope, n *pg_query.Node) []CRef {
	var out []CRef
	if n == nil {
		return nil
	}
	walk(n.ProtoReflect(), func(m proto.Message) bool {
		if cr, ok := m.(*pg_query.ColumnRef); ok {
			if c, ok := b.colRef(sc, &pg_query.Node{Node: &pg_query.Node_ColumnRef{ColumnRef: cr}}); ok {
				out = append(out, c)
			}
		}
		if sl, ok := m.(*pg_query.SubLink); ok {
			// The subselect's columns belong to its own query; the expression
			// tested against it (the `x` of `x IN (SELECT ...)`) is ours.
			out = append(out, b.mentions(sc, sl.Testexpr)...)
			return false
		}
		return true
	})
	return out
}

// expr turns a boolean expression into a Cond.
func (b *builder) expr(q *QueryNode, sc *bscope, n *pg_query.Node) *Cond {
	b.subLinks(q, sc, n)
	switch {
	case n.GetBoolExpr() != nil:
		be := n.GetBoolExpr()
		switch be.Boolop {
		case pg_query.BoolExprType_AND_EXPR, pg_query.BoolExprType_OR_EXPR:
			c := &Cond{Kind: CondAnd}
			if be.Boolop == pg_query.BoolExprType_OR_EXPR {
				c.Kind = CondOr
			}
			for _, k := range be.Args {
				c.Kids = append(c.Kids, b.expr(q, sc, k))
			}
			return c
		}
		return &Cond{Kind: CondOther, Mentions: b.mentions(sc, n)}
	case n.GetAExpr() != nil:
		if leaf := b.leaf(sc, n.GetAExpr()); leaf != nil {
			return &Cond{Kind: CondLeaf, Leaf: leaf}
		}
	case n.GetSubLink() != nil:
		// x IN (SELECT out ...) and x = ANY (SELECT out ...)
		sl := n.GetSubLink()
		if sl.SubLinkType == pg_query.SubLinkType_ANY_SUBLINK && (len(sl.OperName) == 0 ||
			(len(sl.OperName) == 1 && sl.OperName[0].GetString_().GetSval() == "=")) {
			sub := b.subNode[sl]
			lc, ok := b.colRef(sc, sl.Testexpr)
			if ok && sub != nil && sub.Out != nil && !b.e.operand(sl.Testexpr).Set {
				return &Cond{Kind: CondLeaf, Leaf: &Leaf{L: lc, In: &InSub{Q: sub, Out: *sub.Out},
					Mentions: b.mentions(sc, n)}}
			}
		}
	}
	return &Cond{Kind: CondOther, Mentions: b.mentions(sc, n)}
}

// leaf reads `col = value`, `col IN (values)`, `col = ANY (ARRAY[values])` and
// `col = col'; any other comparison becomes a leaf that only mentions columns.
func (b *builder) leaf(sc *bscope, x *pg_query.A_Expr) *Leaf {
	isEq := len(x.Name) == 1 && x.Name[0].GetString_().GetSval() == "="
	switch x.Kind {
	case pg_query.A_Expr_Kind_AEXPR_OP:
		if !isEq {
			return &Leaf{Mentions: b.mentions(sc, &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: x}})}
		}
		return b.eqLeaf(sc, x.Lexpr, []*pg_query.Node{x.Rexpr}, x)
	case pg_query.A_Expr_Kind_AEXPR_IN:
		if !isEq {
			break // NOT IN
		}
		list := x.Rexpr.GetList()
		if list == nil {
			break
		}
		return b.eqLeaf(sc, x.Lexpr, list.Items, x)
	case pg_query.A_Expr_Kind_AEXPR_OP_ANY:
		if !isEq {
			break
		}
		if arr := x.Rexpr.GetAArrayExpr(); arr != nil {
			return b.eqLeaf(sc, x.Lexpr, arr.Elements, x)
		}
	}
	return &Leaf{Mentions: b.mentions(sc, &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: x}})}
}

func (b *builder) eqLeaf(sc *bscope, lhs *pg_query.Node, rhs []*pg_query.Node, x *pg_query.A_Expr) *Leaf {
	other := func() *Leaf {
		return &Leaf{Mentions: b.mentions(sc, &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: x}})}
	}
	// Single value on the right, possibly a column on either side.
	if len(rhs) == 1 && x.Kind == pg_query.A_Expr_Kind_AEXPR_OP {
		lop, rop := b.e.operand(lhs), b.e.operand(rhs[0])
		lc, lok := b.colRef(sc, lhs)
		rc, rok := b.colRef(sc, rhs[0])
		switch {
		case lok && !lop.Set && rop.Set:
			return &Leaf{L: lc, Eq: true, Ops: []Operand{rop}}
		case rok && !rop.Set && lop.Set:
			return &Leaf{L: rc, Eq: true, Ops: []Operand{lop}}
		case lok && rok && !lop.Set && !rop.Set:
			r := rc
			return &Leaf{L: lc, R: &r}
		}
		return other()
	}
	lc, ok := b.colRef(sc, lhs)
	if !ok || b.e.operand(lhs).Set {
		return other()
	}
	var ops []Operand
	for _, it := range rhs {
		op := b.e.operand(it)
		if !op.Set {
			return other()
		}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return other()
	}
	return &Leaf{L: lc, Eq: true, Ops: ops}
}

package attrib

import (
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// TempDef describes a temporary table being created.
type TempDef struct {
	Name         string
	Cols         []string   // columns named in the definition (CREATE TEMP TABLE t (a int, b text), or AS t(a, b))
	Like         []TableRef // CREATE TEMP TABLE t (LIKE other)
	FromQuery    bool       // CREATE TEMP TABLE t AS SELECT ... / SELECT ... INTO TEMP t: columns come from a query
	OnCommitDrop bool       // ON COMMIT DROP: gone when the transaction ends
}

// SavepointOp is SAVEPOINT (Op 's'), RELEASE ('r') or ROLLBACK TO ('b').
type SavepointOp struct {
	Op   byte
	Name string
}

// SQLPrepare is a SQL-level PREPARE name AS query.
type SQLPrepare struct {
	Name string
	Stmt Stmt
}

// SQLExec is a SQL-level EXECUTE name(args).
type SQLExec struct {
	Name string
	Args []Operand
}

// stmtFor builds the Stmt for one parsed statement. Statements that run a
// query without being a plain SELECT/INSERT/UPDATE/DELETE (EXPLAIN ANALYZE,
// cursors, CREATE TABLE AS, COPY, CALL, EXECUTE) are attributed like the query
// they run: otherwise they would be a way around the policy.
func (e *Extractor) stmtFor(text string, node *pg_query.Node) Stmt {
	st := Stmt{SQL: text}
	switch n := node.GetNode().(type) {
	case *pg_query.Node_InsertStmt:
		st.DML, st.Insert = true, true
		st.OnConflict = n.InsertStmt.OnConflictClause != nil
	case *pg_query.Node_SelectStmt:
		st.DML = true
		if into := n.SelectStmt.IntoClause; into != nil {
			st.Temp = tempOfInto(into)
		}
	case *pg_query.Node_UpdateStmt, *pg_query.Node_DeleteStmt, *pg_query.Node_MergeStmt:
		st.DML = true
	case *pg_query.Node_CopyStmt:
		st.DML, st.Insert = true, n.CopyStmt.IsFrom // COPY FROM writes rows of unknown tenants
		st.Copy = copySpecOf(n.CopyStmt)
	case *pg_query.Node_CallStmt:
		st.DML = true
	case *pg_query.Node_DoStmt:
		st.DML = true
		st.Body = e.doBody(n.DoStmt)
	case *pg_query.Node_RefreshMatViewStmt:
		st.DML = true
	case *pg_query.Node_ExplainStmt:
		if n.ExplainStmt.Query != nil && explainRuns(n.ExplainStmt) {
			return e.stmtFor(text, n.ExplainStmt.Query)
		}
	case *pg_query.Node_DeclareCursorStmt:
		return e.stmtFor(text, n.DeclareCursorStmt.Query)
	case *pg_query.Node_CreateTableAsStmt:
		inner := e.stmtFor(text, n.CreateTableAsStmt.Query)
		inner.Temp = tempOfInto(n.CreateTableAsStmt.Into)
		return inner
	case *pg_query.Node_CreateStmt:
		if r := n.CreateStmt.Relation; r != nil && r.Relpersistence == "t" {
			st.Temp = tempOfCreate(n.CreateStmt)
		}
	case *pg_query.Node_PrepareStmt:
		st.Prepare = &SQLPrepare{Name: n.PrepareStmt.Name, Stmt: e.stmtFor(text, n.PrepareStmt.Query)}
	case *pg_query.Node_ExecuteStmt:
		x := &SQLExec{Name: n.ExecuteStmt.Name}
		for _, a := range n.ExecuteStmt.Params {
			x.Args = append(x.Args, e.operand(a))
		}
		st.Exec = x
	case *pg_query.Node_DeallocateStmt:
		name := n.DeallocateStmt.Name
		if n.DeallocateStmt.Isall {
			name = ""
		}
		st.Dealloc = &name
	case *pg_query.Node_DropStmt:
		if n.DropStmt.RemoveType == pg_query.ObjectType_OBJECT_TABLE {
			for _, o := range n.DropStmt.Objects {
				if r, ok := refOfNameList(o); ok {
					st.Drops = append(st.Drops, r)
				}
			}
		}
	case *pg_query.Node_TruncateStmt:
		for _, r := range n.TruncateStmt.Relations {
			if rv := r.GetRangeVar(); rv != nil {
				st.Drops = append(st.Drops, relRef(rv))
			}
		}
	case *pg_query.Node_VariableSetStmt:
		st.Set = e.setVar(n.VariableSetStmt)
	case *pg_query.Node_DiscardStmt:
		switch n.DiscardStmt.Target {
		case pg_query.DiscardMode_DISCARD_ALL:
			st.Set = &SetVar{ResetAll: true}
			none := ""
			st.Dealloc = &none
			st.DiscardTemp = true
		case pg_query.DiscardMode_DISCARD_TEMP:
			st.DiscardTemp = true
		}
	case *pg_query.Node_TransactionStmt:
		switch n.TransactionStmt.Kind {
		case pg_query.TransactionStmtKind_TRANS_STMT_BEGIN, pg_query.TransactionStmtKind_TRANS_STMT_START:
			st.Txn = txnBegin
		case pg_query.TransactionStmtKind_TRANS_STMT_COMMIT, pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK,
			pg_query.TransactionStmtKind_TRANS_STMT_PREPARE:
			st.Txn = txnEnd
			st.TxnRollback = n.TransactionStmt.Kind == pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK
		case pg_query.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
			st.RollbackTo = true
			st.SP = &SavepointOp{Op: 'b', Name: n.TransactionStmt.SavepointName}
		case pg_query.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
			st.SP = &SavepointOp{Op: 's', Name: n.TransactionStmt.SavepointName}
		case pg_query.TransactionStmtKind_TRANS_STMT_RELEASE:
			st.SP = &SavepointOp{Op: 'r', Name: n.TransactionStmt.SavepointName}
		}
	}
	if st.DML {
		vals, params := map[string]bool{}, map[int]bool{}
		e.collect(node, vals, params)
		st.Tenants, st.Params = sortedKeys(vals), sortedInts(params)
		e.shape(node, &st)
		st.Q = e.buildQuery(node)
		st.Tables, st.Funcs, st.Calls = e.tablesOf(node)
		st.Writes = writesOf(node)
		st.Filtered = e.filtered(node)
		if sv := e.selectSetConfig(node); sv != nil {
			st.Set = sv
		}
	}
	return st
}

// explainRuns reports whether an EXPLAIN executes its query (EXPLAIN ANALYZE).
func explainRuns(x *pg_query.ExplainStmt) bool {
	for _, o := range x.Options {
		d := o.GetDefElem()
		if d == nil || strings.ToLower(d.Defname) != "analyze" {
			continue
		}
		if d.Arg == nil {
			return true
		}
		switch strings.ToLower(operandText(d.Arg)) {
		case "false", "off", "0", "f", "no":
			return false
		}
		return true
	}
	return false
}

func operandText(n *pg_query.Node) string {
	if n == nil {
		return ""
	}
	if s := n.GetString_(); s != nil {
		return s.Sval
	}
	if c := n.GetAConst(); c != nil {
		switch {
		case c.GetBoolval() != nil:
			if c.GetBoolval().Boolval {
				return "true"
			}
			return "false"
		case c.GetSval() != nil:
			return c.GetSval().Sval
		case c.GetIval() != nil:
			if c.GetIval().Ival == 0 {
				return "0"
			}
			return "1"
		}
	}
	return ""
}

func tempOfInto(into *pg_query.IntoClause) *TempDef {
	if into == nil || into.Rel == nil || into.Rel.Relpersistence != "t" {
		return nil
	}
	t := &TempDef{Name: strings.ToLower(into.Rel.Relname), FromQuery: true, OnCommitDrop: into.OnCommit == pg_query.OnCommitAction_ONCOMMIT_DROP}
	for _, c := range into.ColNames {
		t.Cols = append(t.Cols, strings.ToLower(c.GetString_().GetSval()))
	}
	return t
}

func tempOfCreate(c *pg_query.CreateStmt) *TempDef {
	t := &TempDef{Name: strings.ToLower(c.Relation.Relname), OnCommitDrop: c.Oncommit == pg_query.OnCommitAction_ONCOMMIT_DROP}
	for _, el := range c.TableElts {
		switch {
		case el.GetColumnDef() != nil:
			t.Cols = append(t.Cols, strings.ToLower(el.GetColumnDef().Colname))
		case el.GetTableLikeClause() != nil && el.GetTableLikeClause().Relation != nil:
			t.Like = append(t.Like, relRef(el.GetTableLikeClause().Relation))
		}
	}
	return t
}

// refOfNameList reads the (schema.)name list DROP uses for its targets.
func refOfNameList(n *pg_query.Node) (TableRef, bool) {
	l := n.GetList()
	if l == nil || len(l.Items) == 0 {
		return TableRef{}, false
	}
	var parts []string
	for _, it := range l.Items {
		parts = append(parts, strings.ToLower(it.GetString_().GetSval()))
	}
	if len(parts) == 1 {
		return TableRef{Name: parts[0]}, true
	}
	return TableRef{Schema: parts[len(parts)-2], Name: parts[len(parts)-1]}, true
}

// Write is a table a statement modifies and the trigger events that causes.
type Write struct {
	Ref    TableRef
	Events int // trigInsert | trigUpdate | trigDelete
}

// writesOf finds every INSERT, UPDATE, DELETE and COPY FROM in a statement,
// data-modifying CTEs included.
func writesOf(node *pg_query.Node) []Write {
	var out []Write
	walk(node.ProtoReflect(), func(m proto.Message) bool {
		switch v := m.(type) {
		case *pg_query.InsertStmt:
			if v.Relation != nil {
				ev := trigInsert
				if oc := v.OnConflictClause; oc != nil && oc.Action == pg_query.OnConflictAction_ONCONFLICT_UPDATE {
					ev |= trigUpdate
				}
				out = append(out, Write{relRef(v.Relation), ev})
			}
		case *pg_query.UpdateStmt:
			if v.Relation != nil {
				out = append(out, Write{relRef(v.Relation), trigUpdate})
			}
		case *pg_query.DeleteStmt:
			if v.Relation != nil {
				out = append(out, Write{relRef(v.Relation), trigDelete})
			}
		case *pg_query.CopyStmt:
			if v.IsFrom && v.Relation != nil {
				out = append(out, Write{relRef(v.Relation), trigInsert})
			}
		}
		return true
	})
	return out
}

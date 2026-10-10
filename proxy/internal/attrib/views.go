package attrib

import (
	"strings"
	"sync"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// Views.
//
// A view that hides the tenant column used to make a statement "unsure". Its
// definition says what it reads, so it is read like a function body: the view
// is replaced by the query it stands for, and the usual rules decide whether
// that query is confined (`WHERE tenant_id = current_setting('app.tenant')` is
// the classic way to make a per-tenant view).
//
// A simple view (a select-project-join: no grouping, no limit, no set
// operation) is merged into the statement, so a condition on one of its columns
// reaches the table the column comes from, whatever the view calls it. Any
// other view is analysed as a subquery: only what its own definition confines
// counts.

const (
	maxViewDepth = 4  // views defined over views
	maxViewOccs  = 64 // table occurrences all the expanded views may add to one statement
)

// viewParse is a view definition, parsed once per catalog snapshot.
type viewParse struct {
	once  sync.Once
	sel   *pg_query.SelectStmt
	funcs []TableRef // functions the body calls
}

// viewSelect returns the parsed definition, or nil if it is not available or not
// a plain SELECT.
func (t *TableInfo) viewSelect() (*pg_query.SelectStmt, []TableRef) {
	if t.Kind != 'v' || t.viewDef == "" {
		return nil, nil
	}
	t.vp.once.Do(func() {
		res, err := Parse(t.viewDef)
		if err != nil || len(res.Stmts) != 1 {
			return
		}
		sel := res.Stmts[0].Stmt.GetSelectStmt()
		if sel == nil {
			return
		}
		t.vp.sel = sel
		walk(sel.ProtoReflect(), func(m proto.Message) bool {
			if fc, ok := m.(*pg_query.FuncCall); ok {
				var ref TableRef
				for i, n := range fc.Funcname {
					s := strings.ToLower(n.GetString_().GetSval())
					switch {
					case i == len(fc.Funcname)-1:
						ref.Name = s
					case i == len(fc.Funcname)-2:
						ref.Schema = s
					}
				}
				t.vp.funcs = append(t.vp.funcs, ref)
			}
			return true
		})
	})
	return t.vp.sel, t.vp.funcs
}

// flat is what a query level contributes once the views in it are merged in.
type flat struct {
	occs  []Occ
	conds []Scoped
	subs  []*QueryNode
}

// expandView merges or attaches the definition of the view o is an occurrence of.
// It reports false, leaving everything as it was, if the definition cannot be
// used (then a view without a tenant column makes the statement unsure, as
// before, and one with a tenant column is treated as a tenant table).
func (ev *confEval) expandView(q *QueryNode, mg *flat, o Occ, r *occRes, depth int) bool {
	ti := r.ti
	sel, funcs := ti.viewSelect()
	if sel == nil || depth >= maxViewDepth || ev.viewOccs >= maxViewOccs {
		return false
	}
	for _, f := range funcs {
		if ev.a.cat.UserFunc(f, ev.a.pathOf(ev.st), ev.st.user) {
			return false // a view that calls application code cannot be vouched for
		}
	}
	iq := ev.a.ex.buildSelect(sel, o.ID+":")
	if iq == nil || unsupported(iq) {
		return false
	}
	if !iq.Simple && depth == 0 && ev.a.hasTenantCol(ti) {
		return false // an aggregate view that exposes the tenant: conditions on that column still apply to it
	}
	ev.viewOccs += len(iq.Occs)
	ev.expanded[ti] = true
	r.tenant, r.tcol = false, ""
	ev.collect(iq, depth+1)
	im := ev.merged[iq]
	if !iq.Simple {
		mg.subs = append(mg.subs, iq)
		return true
	}
	ids := make([]string, 0, len(iq.Occs)+len(im.occs))
	for _, x := range iq.Occs {
		ids = append(ids, x.ID)
	}
	for _, x := range im.occs {
		ids = append(ids, x.ID)
	}
	mg.occs = append(mg.occs, iq.Occs...)
	mg.occs = append(mg.occs, im.occs...)
	for _, sc := range iq.Conds {
		if sc.Confines == nil {
			sc.Confines = ids
		}
		mg.conds = append(mg.conds, sc)
	}
	mg.conds = append(mg.conds, im.conds...)
	mg.subs = append(mg.subs, iq.Subs...)
	mg.subs = append(mg.subs, im.subs...)
	r.viewMap = map[string]CRef{}
	for _, out := range iq.Outs {
		if out.Ref != nil && out.Name != "" {
			if _, dup := r.viewMap[out.Name]; !dup {
				r.viewMap[out.Name] = *out.Ref
			}
		}
	}
	ev.inner[o.ID] = ids
	return true
}

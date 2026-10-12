package attrib

import (
	"sort"
	"strings"
)

// tableFor finds the table a reference names, the way Postgres would: this
// session's temporary tables first, then the catalog along the search_path.
func (a *Attributor) tableFor(st *state, ref TableRef) (ti *TableInfo, tmp *tempTable, ok bool) {
	if ref.Schema == "" || ref.Schema == "pg_temp" {
		if t := st.temps[ref.Name]; t != nil {
			return t.info, t, true
		}
	}
	if a.cat == nil || !a.cat.Loaded() {
		return nil, nil, false
	}
	ti, ok = a.cat.Table(ref, a.pathOf(st), st.user)
	return ti, nil, ok
}

// tableKey is the cache's name for a table: schema-qualified when the catalog
// resolved it, otherwise as written.
func tableKey(ref TableRef, ti *TableInfo) string {
	if ti != nil {
		return ti.Key()
	}
	return ref.String()
}

// derivedOf lists the foreign keys through which a table with no tenant column of
// its own takes its tenant from a parent row.
func (a *Attributor) derivedOf(ti *TableInfo) []DerivedRef {
	if ti == nil || a.cat == nil || ti.Kind != 'r' || ti.Schema == "pg_temp" || a.hasTenantCol(ti) {
		return nil
	}
	return a.cat.Derived(ti)
}

func (a *Attributor) hasTenantCol(ti *TableInfo) bool {
	if ti == nil {
		return false
	}
	for _, c := range ti.Cols {
		if a.tenants[c] {
			return true
		}
	}
	return false
}

// keyUsable reports whether col can identify a row of ti. With the catalog, only
// a column proven unique (a single-column unique index) can: otherwise one
// value could belong to several rows of different tenants. Temporary tables are
// private to a session and never cached. Without the catalog the configured key
// columns are trusted.
func (a *Attributor) keyUsable(ti *TableInfo, col string) bool {
	if ti == nil {
		return a.ex.keys[col] && (a.cat == nil || !a.cat.Loaded())
	}
	if ti.Schema == "pg_temp" {
		return false
	}
	return ti.IsUnique(col)
}

type scope int

const (
	scopeUnknown    scope = iota // something the proxy cannot vouch for
	scopeUntenanted              // provably touches no tenant data
	scopeTenant                  // touches at least one tenant table
	scopeDerived                 // reads only temporary tables filled from known tenants' data
)

// analysis is what a statement (or a function body it calls) touches.
type analysis struct {
	tenantTouched bool     // reads or writes a table that carries a tenant column
	unsure        bool     // touches something the proxy cannot vouch for
	scan          bool     // tenant data with no tenant or key filter at all
	unresolvedFn  bool     // a called function reads tenant data nothing says the tenant of
	tenants       []string // tenants the statement is confined to (itself, or through its function calls' arguments)
	derived       []string // tenants whose data a temporary table holds
	unconfined    bool     // a tenant table is read that nothing confines: no set of tenants can vouch for the statement
	cr            confResult
}

func (r *analysis) merge(o analysis) {
	r.tenantTouched = r.tenantTouched || o.tenantTouched
	r.unsure = r.unsure || o.unsure
	r.scan = r.scan || o.scan
	r.unconfined = r.unconfined || o.unconfined
	r.unresolvedFn = r.unresolvedFn || o.unresolvedFn
	r.tenants = append(r.tenants, o.tenants...)
	r.derived = append(r.derived, o.derived...)
}

func (r analysis) scope() scope {
	switch {
	case r.tenantTouched:
		return scopeTenant
	case r.unsure:
		return scopeUnknown
	case len(r.derived) > 0:
		return scopeDerived
	}
	return scopeUntenanted
}

const maxCallDepth = 6

// analyze decides what kind of data a statement touches. It fails closed:
// tables the catalog does not know, views without a tenant column (which may
// wrap tenant tables), temporary tables filled from unattributed data, and
// functions whose code cannot be read all make the answer "unsure".
//
// A call to an application-defined function is analysed by reading its body:
// each body statement is analysed with the call's arguments bound to the
// function's parameters, so `tenant_id = p_tenant` inside the function yields
// the tenant the caller passed. Dynamic SQL cannot be read and stays unsure.
func (a *Attributor) analyze(st *state, x Stmt, pt *portal, depth int, owners ownerSet) analysis {
	var r analysis
	r.cr = a.confine(st, x, pt, owners, depth == 0)
	switch {
	case x.Insert:
		// The tenants are those of the rows being written, and every row must name one.
		r.tenants = insertTenants(st, x, pt)
		if r.cr.tenantOccs > 0 && !r.cr.confined {
			r.tenants, r.unconfined = nil, true
		}
	case r.cr.tenantOccs > 0 && !r.cr.confined:
		r.unconfined = true
	case r.cr.confined && r.cr.tenantOccs > 0:
		r.tenants = r.cr.tenants
	}
	if a.movesRows(st, x) {
		r.tenants, r.unconfined = nil, true // rows may move to a tenant the statement does not name
	}
	if a.cat == nil || !a.cat.Loaded() {
		// Without the schema nothing can be vouched for.
		r.unsure = true
		return r
	}
	direct := false
	for _, t := range x.Tables {
		ti, tmp, ok := a.tableFor(st, t)
		switch {
		case !ok:
			r.unsure = true
		case tmp != nil:
			switch tmp.kind {
			case tempTenant:
				direct = true
			case tempDerived:
				r.derived = append(r.derived, tmp.tenants...)
			case tempTainted:
				r.unsure = true
			}
		case ti.Kind == 'v' && r.cr.expanded[ti]:
			// read through its definition: the tables it is made of were analysed
		case a.hasTenantCol(ti) || len(a.derivedOf(ti)) > 0:
			direct = true
		case (ti.Kind == 'v' || ti.Kind == 'm') && !isSystemSchema(ti.Schema):
			r.unsure = true
		}
	}
	if r.cr.unsure {
		r.unsure = true
	}
	if len(r.cr.expanded) > 0 && r.cr.tenantOccs > 0 {
		direct = true
	}
	if direct {
		r.tenantTouched = true
		r.scan = r.cr.scan
	}
	for _, f := range x.Funcs {
		if (f.Schema == "" || strings.EqualFold(f.Schema, "pg_catalog")) && sqlRunners[strings.ToLower(f.Name)] {
			r.unresolvedFn = true // it runs the SQL in its argument: nothing says what that reads
			r.tenantTouched = true
		}
	}
	own := r.tenants
	for _, w := range x.Writes {
		r.merge(a.triggerEffect(st, w, own, depth, owners))
	}
	path := a.pathOf(st)
	for _, c := range x.Calls {
		if defs := a.cat.Funcs(c.Ref, path, st.user); len(defs) > 0 {
			r.merge(a.callEffect(st, c, defs, pt, depth, owners))
		}
	}
	if x.Body != nil {
		r.merge(a.bodyEffect(st, x.Body, nil, depth+1, owners))
	}
	return r
}

// callEffect analyses a call to an application-defined function.
func (a *Attributor) callEffect(st *state, c Call, defs []*FuncDef, pt *portal, depth int, owners ownerSet) analysis {
	var r analysis
	if depth >= maxCallDepth {
		r.unsure = true
		return r
	}
	args := &portal{params: make([]string, len(c.Args)), valid: make([]bool, len(c.Args))}
	for i, op := range c.Args {
		args.params[i], args.valid[i] = st.val(pt, op)
	}
	matched := false
	for _, def := range defs {
		if def.NArgs < len(c.Args) {
			continue // a different overload
		}
		matched = true
		r.merge(a.bodyEffect(st, a.funcBody(def), args, depth+1, owners))
	}
	if !matched {
		r.unsure = true
	}
	return r
}

// bodyEffect analyses the statements of a function body. args holds the values
// of the call's arguments (nil for a DO block).
func (a *Attributor) bodyEffect(st *state, b *FuncBody, args *portal, depth int, owners ownerSet) analysis {
	var r analysis
	if b.Opaque {
		r.unsure = true
		return r
	}
	if b.NoArgs {
		args = nil
	}
	for _, s := range b.Stmts {
		br := a.analyze(st, s, args, depth, owners)
		if br.tenantTouched && len(br.tenants) == 0 {
			// It reads tenant data and neither an argument nor the body says whose.
			br.unresolvedFn = true
		}
		r.merge(br)
	}
	return r
}

// sqlRunners are built-in functions that run SQL given to them as a string (or a
// whole schema's worth of tables) and return what it reads. A statement that
// calls one reads whatever that SQL reads, which cannot be known from the text.
var sqlRunners = map[string]bool{
	"query_to_xml": true, "query_to_xml_and_xmlschema": true, "query_to_xmlschema": true,
	"table_to_xml": true, "table_to_xml_and_xmlschema": true, "table_to_xmlschema": true,
	"schema_to_xml": true, "schema_to_xml_and_xmlschema": true, "schema_to_xmlschema": true,
	"database_to_xml": true, "database_to_xml_and_xmlschema": true, "database_to_xmlschema": true,
	"cursor_to_xml": true, "cursor_to_xmlschema": true,
	"ts_stat": true,
	"dblink":  true, "dblink_exec": true, "dblink_send_query": true, "dblink_connect": true,
}

// funcBody reads (and caches) a function's code.
func (a *Attributor) funcBody(def *FuncDef) *FuncBody {
	gen := a.cat.Generation()
	a.fmu.Lock()
	defer a.fmu.Unlock()
	if a.fcache == nil || a.fgen != gen {
		a.fcache, a.tcache, a.fgen = map[*FuncDef]*FuncBody{}, map[*FuncDef]*FuncBody{}, gen
	}
	if b, ok := a.fcache[def]; ok {
		return b
	}
	b := a.ex.bodyOf(def)
	a.fcache[def] = b
	return b
}

// triggerBody reads (and caches) a trigger function's code.
func (a *Attributor) triggerBody(def *FuncDef) *FuncBody {
	gen := a.cat.Generation()
	a.fmu.Lock()
	defer a.fmu.Unlock()
	if a.fcache == nil || a.fgen != gen {
		a.fcache, a.tcache, a.fgen = map[*FuncDef]*FuncBody{}, map[*FuncDef]*FuncBody{}, gen
	}
	if b, ok := a.tcache[def]; ok {
		return b
	}
	b := a.ex.triggerBodyOf(def)
	a.tcache[def] = b
	return b
}

// triggerEffect analyses the triggers a write fires. NEW.tenant_id in a trigger
// body is the tenant of the row being written, so it is bound to each of the
// statement's tenants in turn; a trigger that touches tenant data it cannot tie
// to those tenants makes the statement unresolved, as a function would.
func (a *Attributor) triggerEffect(st *state, w Write, tenants []string, depth int, owners ownerSet) analysis {
	var r analysis
	ti, tmp, ok := a.tableFor(st, w.Ref)
	if !ok || tmp != nil || ti == nil {
		return r
	}
	if a.cat.HasRules(ti) {
		r.unsure = true // a rule rewrites the statement into others that are not read
	}
	for _, t := range a.cat.Triggers(ti, w.Events) {
		if depth >= maxCallDepth || len(t.Funcs) == 0 {
			r.unsure = true
			continue
		}
		for _, def := range t.Funcs {
			b := a.triggerBody(def)
			if len(tenants) == 0 {
				r.merge(a.bodyEffect(st, b, &portal{params: []string{""}, valid: []bool{false}}, depth+1, owners))
				continue
			}
			for _, tv := range tenants {
				r.merge(a.bodyEffect(st, b, &portal{params: []string{tv}, valid: []bool{true}}, depth+1, owners))
			}
		}
	}
	return r
}

// insertTenants returns the tenants an INSERT writes rows for. Every row must
// name one, as a literal or a bound value; otherwise nothing is claimed.
func insertTenants(st *state, x Stmt, pt *portal) []string {
	if len(x.Inserts) == 0 || len(x.Inserts) != x.InsertRows {
		return nil
	}
	var out []string
	for _, r := range x.Inserts {
		v, ok := st.val(pt, r.Tenant)
		if !ok {
			return nil
		}
		out = append(out, v)
	}
	return dedupe(out)
}

// assignsTenant: does an UPDATE set the tenant column? The rows then move to a
// tenant the statement's own predicates say nothing about.
func (a *Attributor) movesRows(st *state, x Stmt) bool {
	if a.assignsTenant(x) {
		return true
	}
	if len(x.SetCols) == 0 {
		return false
	}
	ref := x.Table
	if ref.Name == "" {
		ref = x.SetTable
	}
	if ref.Name == "" {
		return false
	}
	ti, tmp, ok := a.tableFor(st, ref)
	if !ok || tmp != nil {
		return false
	}
	for _, d := range a.derivedOf(ti) {
		for _, c := range x.SetCols {
			if c == d.Col {
				return true // pointing a row at another parent gives it another tenant
			}
		}
	}
	return false
}

func (a *Attributor) assignsTenant(x Stmt) bool {
	for _, c := range x.SetCols {
		if a.tenants[c] {
			return true
		}
	}
	return false
}

// judge decides how far an analysis can be believed. sound: nothing in the
// statement is beyond the proxy's sight (an unknown table, a view that hides its
// tenant column, a function whose code cannot be read). trusted: and no tenant
// table is read unconfined, so the tenants found speak for the whole statement.
func (a *Attributor) judge(r analysis) (sound, trusted bool, tenants []string) {
	catalog := a.cat != nil && a.cat.Loaded()
	sound = !r.unresolvedFn && !(r.unsure && catalog)
	trusted = sound && !r.unconfined
	if trusted {
		tenants = dedupe(r.tenants)
		sort.Strings(tenants)
	}
	return sound, trusted, tenants
}

// target works out which row's tenant answers a lookup by ti.col: the row
// itself if col is unique, or the parent row if col is a verified foreign key.
// ok is false if neither (an ordinary column says nothing about one tenant).
func (a *Attributor) target(ti *TableInfo, col string) (tgt *TableInfo, tcol, pcol string, ok bool) {
	if ti == nil || a.cat == nil || ti.Schema == "pg_temp" {
		return nil, "", "", false
	}
	if ti.IsUnique(col) && !a.tenants[col] {
		if tc, _ := a.scopeOf(ti); tc != "" {
			return ti, tc, col, true
		}
		if len(a.derivedOf(ti)) == 0 {
			return nil, "", "", false
		}
		// a child table: the row's tenant comes from its parent (below), if col is the key to it
	}
	if fk, found := a.cat.FK(ti, col); found {
		if parent, found := a.cat.TableByKey(fk.ParentKey); found {
			if tc, _ := a.scopeOf(parent); tc != "" {
				return parent, tc, fk.ParentCol, true
			}
		}
	}
	return nil, "", "", false
}

// rawInserts turns the rows of a column-less INSERT into InsRows using the
// table's column order from the catalog. It returns nothing if the table is
// unknown or has no tenant column.
// deriveInsertTenants gives each row inserted into a child table the tenant of
// the parent row it points at, where that is known (cached, or in block mode
// looked up). A row whose parent is unknown keeps no tenant, and the statement
// stays unattributed.
func (a *Attributor) deriveInsertTenants(st *state, pt *portal, ti *TableInfo, x *Stmt) {
	refs := a.derivedOf(ti)
	if len(refs) == 0 {
		return
	}
	for i := range x.Inserts {
		row := &x.Inserts[i]
		owner, ok := "", true
		for _, d := range refs {
			var op *Operand
			for j := range row.Refs {
				if row.Refs[j].Col == d.Col {
					op = &row.Refs[j].Op
				}
			}
			if op == nil {
				ok = false // the foreign key is left to a default
				break
			}
			v, set := st.val(pt, *op)
			if !set {
				ok = false
				break
			}
			o, known := a.parentOwner(FKRef{ParentKey: d.ParentKey, ParentCol: d.ParentCol}, v)
			if !known || (owner != "" && owner != o) {
				ok = false
				break
			}
			owner = o
		}
		if ok && owner != "" {
			row.Tenant = Operand{Lit: owner, Set: true}
		}
	}
}

func (a *Attributor) rawInserts(x Stmt, ti *TableInfo) []InsRow {
	if len(x.RawInserts) == 0 || ti == nil {
		return nil
	}
	tIdx := -1
	keyIdx := map[string]int{}
	for i, c := range ti.Cols {
		switch {
		case tIdx < 0 && a.tenants[c]:
			tIdx = i
		case !a.tenants[c] && a.keyUsable(ti, c):
			keyIdx[c] = i
		}
	}
	if tIdx < 0 && len(a.derivedOf(ti)) == 0 {
		return nil
	}
	var out []InsRow
	for _, row := range x.RawInserts {
		var r InsRow
		if tIdx >= 0 {
			if tIdx >= len(row) {
				continue
			}
			r.Tenant = row[tIdx]
		}
		for c, i := range keyIdx {
			if i < len(row) {
				r.Keys = append(r.Keys, KeyOp{Col: c, Op: row[i]})
			}
		}
		for i, c := range ti.Cols {
			if i < len(row) && !a.tenants[c] {
				r.Refs = append(r.Refs, KeyOp{Col: c, Op: row[i]})
			}
		}
		out = append(out, r)
	}
	return out
}

// tableNames lists the tables a statement touches, schema-qualified where the
// catalog resolved them.
func (a *Attributor) tableNames(st *state, x Stmt) []string {
	var out []string
	for _, t := range x.Tables {
		ti, tmp, _ := a.tableFor(st, t)
		if tmp != nil {
			out = append(out, "pg_temp."+t.Name)
			continue
		}
		out = append(out, tableKey(t, ti))
	}
	return out
}

// assignsKey reports whether an UPDATE sets the tenant column or a unique column:
// either can change which tenant a cached row belongs to.
func (a *Attributor) assignsKey(ti *TableInfo, x Stmt) bool {
	for _, c := range x.SetCols {
		if a.tenants[c] || (ti != nil && ti.IsUnique(c)) {
			return true
		}
	}
	return false
}

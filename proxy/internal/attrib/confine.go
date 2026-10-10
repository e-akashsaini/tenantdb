package attrib

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// Deciding which tenants a statement is confined to.
//
// See query.go for the model. The rule here is the one soundness depends on:
// a statement is confined only if EVERY tenant-table occurrence in it, at every
// nesting level, is confined to a known set of tenants; the statement's tenants
// are then the union of those sets. One occurrence that nothing confines (the
// preserved side of an outer join, the other branch of a UNION, a table read by
// a subquery, an alternative of an OR) leaves the whole statement unconfined.

// ownerKey names one row of a table by one of its unique columns.
type ownerKey struct{ table, col, val string }

// ownerSet holds lookup results: the tenants that own each row asked about. A
// key that is present with no tenants means the row does not exist.
type ownerSet map[ownerKey][]string

// tset is a known set of tenants. A nil *tset means "not confined".
type tset struct{ m map[string]bool }

func setOf(vals ...string) *tset {
	s := &tset{m: map[string]bool{}}
	for _, v := range vals {
		s.m[v] = true
	}
	return s
}

// inter: rows must satisfy both, so their tenant is in both. A side that is not
// confined imposes nothing.
func inter(a, b *tset) *tset {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	out := setOf()
	for v := range a.m {
		if b.m[v] {
			out.m[v] = true
		}
	}
	return out
}

func union(a, b *tset) *tset {
	out := setOf()
	for v := range a.m {
		out.m[v] = true
	}
	for v := range b.m {
		out.m[v] = true
	}
	return out
}

func (s *tset) list() []string {
	out := make([]string, 0, len(s.m))
	for v := range s.m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// confResult is what a statement is confined to.
type confResult struct {
	confined   bool // every tenant-table occurrence is confined (trivially true if there are none)
	tenantOccs int  // how many tenant-table occurrences there are
	tenants    []string
	// scan: some tenant table is read with nothing at all restricting it.
	scan      bool
	needs     []keyReq // rows whose owner was not cached; a lookup could settle them
	viaKeys   bool     // some confinement came from a row's owner
	viaLookup bool     // ... and some of those owners came from asking the database
}

type occRes struct {
	id     string
	ref    TableRef
	ti     *TableInfo
	tenant bool   // it carries a tenant column
	tcol   string // which
}

type joinEq struct{ a, b, ca, cb string }

type condOut struct {
	bridge     map[string][]bridgeKey
	conf       map[string]*tset
	eqs        []joinEq
	restricted map[string]bool
}

type propItem struct {
	eq        joinEq
	updatable map[string]bool
}

type confEval struct {
	a      *Attributor
	st     *state
	pt     *portal
	owners ownerSet

	occ        map[string]*occRes
	bridge     map[string][]bridgeKey // non-tenant occurrences pinned to one row by a unique key
	conf       map[string]*tset
	restricted map[string]bool
	needs      []keyReq
	needSeen   map[ownerKey]bool
	viaKeys    bool
	viaLookup  bool
}

// confine works out what statement x is confined to. owners, if non-nil, holds
// the results of lookups for rows the cache did not know.
func (a *Attributor) confine(st *state, x Stmt, pt *portal, owners ownerSet) confResult {
	if x.Q == nil || unsupported(x.Q) {
		return confResult{}
	}
	ev := &confEval{a: a, st: st, pt: pt, owners: owners,
		occ: map[string]*occRes{}, bridge: map[string][]bridgeKey{}, conf: map[string]*tset{}, restricted: map[string]bool{}, needSeen: map[ownerKey]bool{}}
	ev.collect(x.Q)
	ev.level(x.Q)
	return ev.result()
}

func unsupported(q *QueryNode) bool {
	if q.Unsupported {
		return true
	}
	for _, s := range q.Subs {
		if unsupported(s) {
			return true
		}
	}
	return false
}

// collect resolves every table occurrence of the statement.
func (ev *confEval) collect(q *QueryNode) {
	catalog := ev.a.cat != nil && ev.a.cat.Loaded()
	for _, o := range q.Occs {
		r := &occRes{id: o.ID, ref: o.Ref}
		ti, tmp, ok := ev.a.tableFor(ev.st, o.Ref)
		switch {
		case !ok && !catalog:
			r.tenant = true // no schema: assume every table is a tenant table
		case !ok:
			// an unknown table: the analysis marks the statement unsure
		case tmp != nil:
			r.ti = ti
			r.tenant = tmp.kind == tempTenant && ti != nil
		default:
			r.ti = ti
			r.tenant = ev.a.hasTenantCol(ti)
		}
		if r.tenant && r.ti != nil {
			r.tcol, _ = ev.a.scopeOf(r.ti)
		}
		ev.occ[o.ID] = r
	}
	for _, s := range q.Subs {
		ev.collect(s)
	}
}

// level confines the occurrences of one query level from its conditions, then
// does the same for the queries nested in it.
func (ev *confEval) level(q *QueryNode) {
	own := map[string]bool{}
	for _, o := range q.Occs {
		own[o.ID] = true
	}
	var props []propItem
	for _, sc := range q.Conds {
		out := ev.cond(sc.C)
		updatable := own
		if sc.Confines != nil {
			updatable = map[string]bool{}
			for _, id := range sc.Confines {
				if own[id] {
					updatable[id] = true
				}
			}
		}
		for id, set := range out.conf {
			if updatable[id] {
				ev.conf[id] = inter(ev.conf[id], set)
			}
		}
		for id := range out.restricted {
			ev.restricted[id] = true
		}
		for id, ks := range out.bridge {
			if updatable[id] {
				ev.bridge[id] = append(ev.bridge[id], ks...)
			}
		}
		for _, e := range out.eqs {
			props = append(props, propItem{e, updatable})
		}
	}
	// A join condition passes confinement along: rows of B that match rows of A
	// are A's tenants, if the two columns tie the rows to the same tenant.
	for i := 0; i <= len(props)*2+1; i++ {
		changed := false
		for _, p := range props {
			for _, dir := range [2]bool{true, false} {
				srcID, dstID, sc, dc := p.eq.a, p.eq.b, p.eq.ca, p.eq.cb
				if !dir {
					srcID, dstID, sc, dc = dstID, srcID, dc, sc
				}
				src, dst := ev.occ[srcID], ev.occ[dstID]
				if ev.viaBridge(src, sc, dst, dc, p.updatable[dstID]) {
					changed = true
					continue
				}
				if src == nil || dst == nil || !p.updatable[dstID] || !dst.tenant || ev.conf[srcID] == nil {
					continue
				}
				if !ev.related(src, sc, dst, dc) {
					ev.suggest(src, sc, dst, dc)
					continue
				}
				n := inter(ev.conf[dstID], ev.conf[srcID])
				if ev.conf[dstID] == nil || len(n.m) != len(ev.conf[dstID].m) {
					ev.conf[dstID] = n
					changed = true
				}
			}
		}
		if !changed {
			break
		}
	}
	for _, s := range q.Subs {
		ev.level(s)
	}
}

// suggest offers the catalog a relationship the join between two tenant tables
// hints at: whichever side's column is unique is the parent.
func (ev *confEval) suggest(a *occRes, ac string, b *occRes, bc string) {
	if ev.a.cat == nil || a.ti == nil || b.ti == nil || !a.tenant || !b.tenant {
		return
	}
	switch {
	case b.ti.IsUnique(bc) && !ev.a.tenants[ac]:
		ev.a.cat.SuggestFK(a.ti, ac, b.ti, bc)
	case a.ti.IsUnique(ac) && !ev.a.tenants[bc]:
		ev.a.cat.SuggestFK(b.ti, bc, a.ti, ac)
	}
}

// related: do the equal values of src.scol and dst.dcol tie a dst row to the
// same tenant as the src row it matches? Yes for two tenant columns, and for a
// verified foreign key in either direction.
func (ev *confEval) related(src *occRes, scol string, dst *occRes, dcol string) bool {
	a := ev.a
	if a.tenants[scol] && a.tenants[dcol] {
		return true
	}
	if a.cat == nil || src.ti == nil || dst.ti == nil {
		return false
	}
	if fk, ok := a.cat.FK(dst.ti, dcol); ok && fk.ParentKey == src.ti.Key() && fk.ParentCol == scol {
		return true
	}
	if fk, ok := a.cat.FK(src.ti, scol); ok && fk.ParentKey == dst.ti.Key() && fk.ParentCol == dcol {
		return true
	}
	return false
}

func (ev *confEval) resolve(r CRef) (*occRes, bool) {
	if r.Occ != "" {
		o := ev.occ[r.Occ]
		return o, o != nil
	}
	var found *occRes
	for _, id := range r.Cands {
		o := ev.occ[id]
		if o == nil || o.ti == nil {
			continue
		}
		for _, c := range o.ti.Cols {
			if c == r.Col {
				if found != nil {
					return nil, false // ambiguous
				}
				found = o
			}
		}
	}
	return found, found != nil
}

func (ev *confEval) cond(c *Cond) condOut {
	out := condOut{conf: map[string]*tset{}, restricted: map[string]bool{}, bridge: map[string][]bridgeKey{}}
	switch c.Kind {
	case CondLeaf:
		ev.leaf(c.Leaf, &out)
	case CondOther:
		ev.mention(c.Mentions, &out)
	case CondAnd:
		for _, k := range c.Kids {
			ko := ev.cond(k)
			for id, s := range ko.conf {
				out.conf[id] = inter(out.conf[id], s)
			}
			for id := range ko.restricted {
				out.restricted[id] = true
			}
			for id, ks := range ko.bridge {
				out.bridge[id] = append(out.bridge[id], ks...)
			}
			out.eqs = append(out.eqs, ko.eqs...)
		}
		ev.poly(c, &out)
	case CondOr:
		// Rows satisfy one alternative or another: an occurrence is confined only
		// if every alternative confines it, to the union of what they allow.
		var kids []condOut
		for _, k := range c.Kids {
			kids = append(kids, ev.cond(k))
		}
		if len(kids) == 0 {
			break
		}
		for id := range kids[0].conf {
			set := kids[0].conf[id]
			all := true
			for _, ko := range kids[1:] {
				s, ok := ko.conf[id]
				if !ok {
					all = false
					break
				}
				set = union(set, s)
			}
			if all {
				out.conf[id] = set
			}
		}
		for id := range kids[0].restricted {
			all := true
			for _, ko := range kids[1:] {
				if !ko.restricted[id] {
					all = false
					break
				}
			}
			if all {
				out.restricted[id] = true
			}
		}
	}
	return out
}

// mention notes columns a condition touches without saying which tenant. A
// condition on the tenant column or on a row-identifying column means the
// statement is restricting itself by something, so it is not a blind scan.
func (ev *confEval) mention(refs []CRef, out *condOut) {
	for _, r := range refs {
		o, ok := ev.resolve(r)
		if !ok || !o.tenant {
			continue
		}
		if ev.restrictsBy(o, r.Col) {
			out.restricted[o.id] = true
		}
	}
}

// restrictsBy: does a condition on this column mean the statement is limiting
// itself by something that bears on one tenant: the tenant column, a column that
// identifies a row, or one of the configured key columns (even if the catalog
// cannot prove it unique)?
func (ev *confEval) restrictsBy(o *occRes, col string) bool {
	return ev.a.tenants[col] || ev.a.ex.keys[col] || ev.identifying(o, col)
}

// identifying: does a value of this column identify one row (or one parent row)?
func (ev *confEval) identifying(o *occRes, col string) bool {
	if o.ti == nil {
		return ev.a.ex.keys[col] && (ev.a.cat == nil || !ev.a.cat.Loaded())
	}
	if _, _, _, ok := ev.a.target(o.ti, col); ok {
		return true
	}
	return o.ti.HasPolyID(col)
}

func (ev *confEval) values(ops []Operand) ([]string, bool) {
	vals := make([]string, 0, len(ops))
	for _, op := range ops {
		v, ok := ev.st.val(ev.pt, op)
		if !ok {
			return nil, false
		}
		vals = append(vals, v)
	}
	return vals, true
}

func (ev *confEval) leaf(l *Leaf, out *condOut) {
	if l.R != nil {
		a, ok1 := ev.resolve(l.L)
		b, ok2 := ev.resolve(*l.R)
		if ok1 && ok2 {
			out.eqs = append(out.eqs, joinEq{a.id, b.id, l.L.Col, l.R.Col})
			out.restricted[a.id], out.restricted[b.id] = true, true
		}
		return
	}
	if !l.Eq {
		ev.mention(l.Mentions, out)
		return
	}
	o, ok := ev.resolve(l.L)
	if ok && !o.tenant && o.ti != nil && o.ti.IsUnique(l.L.Col) && ev.a.lk != nil {
		// A credential-style lookup on a table with no tenant column: `token = $1`.
		// Alone it says nothing, but joined to a tenant table it pins that table.
		if vals, resolved := ev.values(l.Ops); resolved {
			out.bridge[o.id] = append(out.bridge[o.id], bridgeKey{col: l.L.Col, vals: vals})
		}
		return
	}
	if !ok || !o.tenant {
		return
	}
	vals, resolved := ev.values(l.Ops)
	if ev.restrictsBy(o, l.L.Col) {
		out.restricted[o.id] = true
	}
	switch {
	case ev.a.tenants[l.L.Col]:
		if resolved {
			out.conf[o.id] = setOf(vals...)
		}
	case ev.identifying(o, l.L.Col):
		if resolved {
			if set, ok := ev.ownersOf(o, l.L.Col, vals); ok {
				out.conf[o.id] = set
			}
		}
	}
}

// ownersOf finds which tenants own the rows o.col takes the given values for:
// the rows themselves when col is unique, their parents when it is a foreign
// key. Owners come from the cache, or from lookup results; a row nobody has
// seen is recorded as a need and the occurrence stays unconfined for now.
func (ev *confEval) ownersOf(o *occRes, col string, vals []string) (*tset, bool) {
	a := ev.a
	if o.ti == nil {
		// No schema: only the configured key columns, on this table, by name.
		set, all := setOf(), true
		for _, v := range vals {
			t, hit := a.cache.Get(o.ref.String(), col, v)
			if !hit {
				all = false
				continue
			}
			set.m[t] = true
			ev.viaKeys = true
		}
		return set, all
	}
	tgt, tcol, pcol, ok := a.target(o.ti, col)
	if !ok {
		return nil, false
	}
	return ev.ownersIn(tgt, tcol, pcol, vals)
}

func (ev *confEval) ownersIn(tgt *TableInfo, tcol, pcol string, vals []string) (*tset, bool) {
	set, all := setOf(), true
	for _, v := range vals {
		k := ownerKey{tgt.Key(), pcol, normVal(v)}
		// A fresh lookup answer first: it is also in the cache by now, but the
		// event should say the database was asked.
		if ts, found := ev.owners[k]; found {
			for _, t := range ts {
				set.m[t] = true
			}
			ev.viaKeys, ev.viaLookup = true, true
			continue
		}
		if t, hit := ev.a.cache.Get(tgt.Key(), pcol, v); hit {
			set.m[t] = true
			ev.viaKeys = true
			continue
		}
		all = false
		if !ev.needSeen[k] {
			ev.needSeen[k] = true
			ev.needs = append(ev.needs, keyReq{col: pcol, val: v, ti: tgt, tcol: tcol})
		}
	}
	if !all {
		return nil, false
	}
	return set, true
}

// poly resolves `X_type = 'Class' AND X_id = n` among the kids of an AND.
func (ev *confEval) poly(c *Cond, out *condOut) {
	for _, k := range c.Kids {
		if k.Kind != CondLeaf || k.Leaf == nil || !k.Leaf.Eq || k.Leaf.R != nil {
			continue
		}
		idLeaf := k.Leaf
		o, ok := ev.resolve(idLeaf.L)
		base, isID := strings.CutSuffix(idLeaf.L.Col, "_id")
		if !ok || !o.tenant || o.ti == nil || !isID || !o.ti.HasPolyID(idLeaf.L.Col) {
			continue
		}
		out.restricted[o.id] = true
		for _, s := range c.Kids {
			if s.Kind != CondLeaf || s.Leaf == nil || !s.Leaf.Eq || s.Leaf.R != nil || s.Leaf.L.Col != base+"_type" || len(s.Leaf.Ops) != 1 {
				continue
			}
			so, ok := ev.resolve(s.Leaf.L)
			if !ok || so.id != o.id {
				continue
			}
			class, resolved := ev.st.val(ev.pt, s.Leaf.Ops[0])
			ids, idsResolved := ev.values(idLeaf.Ops)
			if !resolved || !idsResolved {
				break
			}
			ref, verified := ev.a.cat.Poly(o.ti, base, class)
			if !verified {
				break
			}
			parent, found := ev.a.cat.TableByKey(ref.ParentKey)
			if !found {
				break
			}
			tcol, _ := ev.a.scopeOf(parent)
			if tcol == "" {
				break
			}
			if set, ok := ev.ownersIn(parent, tcol, ref.ParentCol, ids); ok {
				out.conf[o.id] = inter(out.conf[o.id], set)
			}
			break
		}
	}
}

func (ev *confEval) result() confResult {
	r := confResult{confined: true, needs: ev.needs, viaKeys: ev.viaKeys, viaLookup: ev.viaLookup}
	var all *tset
	for id, o := range ev.occ {
		if !o.tenant {
			continue
		}
		r.tenantOccs++
		s := ev.conf[id]
		if s == nil {
			r.confined = false
			if !ev.restricted[id] {
				r.scan = true
			}
			continue
		}
		if all == nil {
			all = setOf()
		}
		all = union(all, s)
	}
	if r.confined && all != nil {
		r.tenants = all.list()
	}
	return r
}

// bridgeKey: occurrence.col = one of vals, where col is a unique column of a
// table that has no tenant column of its own.
type bridgeKey struct {
	col  string
	vals []string
}

// bridgeSpec names a probe: the tenants of the t rows joined (t.tjoin = b.bfk) to
// the one b row whose unique column bk has a given value.
type bridgeSpec struct {
	b, t           *TableInfo
	bk, bfk, tjoin string
	ttenant        string
}

func (s bridgeSpec) table() string {
	return "via:" + s.b.Key() + "." + s.bfk + ">" + s.t.Key() + "." + s.tjoin
}

// bridgeVal keeps the credential itself out of the cache and the owner sets.
func bridgeVal(v string) string {
	h := sha256.Sum256([]byte(v))
	return hex.EncodeToString(h[:16])
}

// viaBridge: src is a non-tenant occurrence pinned to one row by a unique key
// (an access token, a session id) and dst a tenant occurrence joined to it. The
// dst rows that can match are the ones that row points at, whatever the
// relationship is called: the probe asks for exactly those, with the statement's
// own join, so no foreign key needs to be known. It reports whether dst was newly
// confined.
func (ev *confEval) viaBridge(src *occRes, scol string, dst *occRes, dcol string, updatable bool) bool {
	if src == nil || dst == nil || src.tenant || !dst.tenant || !updatable || len(ev.bridge[src.id]) == 0 ||
		src.ti == nil || dst.ti == nil || dst.tcol == "" || ev.a.lk == nil {
		return false
	}
	changed := false
	for _, bk := range ev.bridge[src.id] {
		spec := bridgeSpec{b: src.ti, t: dst.ti, bk: bk.col, bfk: scol, tjoin: dcol, ttenant: dst.tcol}
		set, all := setOf(), true
		for _, v := range bk.vals {
			k := ownerKey{spec.table(), bk.col, bridgeVal(normVal(v))}
			if ts, found := ev.owners[k]; found {
				for _, t := range ts {
					set.m[t] = true
				}
				ev.viaKeys, ev.viaLookup = true, true
				continue
			}
			if t, hit := ev.a.bridge.Get(spec.table(), bk.col, bridgeVal(normVal(v))); hit {
				set.m[t] = true
				ev.viaKeys = true
				continue
			}
			all = false
			if !ev.needSeen[k] {
				ev.needSeen[k] = true
				ev.needs = append(ev.needs, keyReq{col: bk.col, val: v, ti: dst.ti, tcol: dst.tcol, via: &spec})
			}
		}
		if !all {
			continue
		}
		n := inter(ev.conf[dst.id], set)
		if ev.conf[dst.id] == nil || len(n.m) != len(ev.conf[dst.id].m) {
			ev.conf[dst.id] = n
			changed = true
		}
	}
	return changed
}

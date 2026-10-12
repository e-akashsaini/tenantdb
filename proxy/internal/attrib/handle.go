package attrib

import (
	"context"
	"strings"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// pending is a statement sent to the server whose reply has not arrived yet.
// Replies come back in order, so a FIFO lines results up with statements.
type pending struct {
	stmt   Stmt
	table  string     // cache name of the table, empty for multi-table statements
	learn  []learned  // committed to the cache when the statement succeeds
	forget []cacheKey // dropped from the cache when the statement succeeds
	shape  rowShape   // how to read tenant and key columns out of result rows
	ti     *TableInfo // the catalog's view of table, if known

	dropKeys  []string // cache tables to forget when the statement succeeds (DROP, TRUNCATE)
	dropTemps []string // temporary tables to forget when it succeeds
	temp      *tempReg // a temporary table to remember when it succeeds
	dropTable bool     // the statement may have moved rows between tenants: forget the table's owners
	// undo puts back the session state the statement changed, if the database
	// rejects the statement.
	undo func()
}

type learned struct {
	key    cacheKey
	tenant string
}

type rowShape struct {
	ok     bool
	tenant int
	keys   map[string]int
}

// keyReq is one row to look up: the value of a unique column of ti, whose
// tenant column is tcol. For a foreign key, ti is the parent table.
type keyReq struct {
	col, val string
	ti       *TableInfo
	tcol     string
	via      *bridgeSpec // set: ti is the tenant table a credential-style row points at (see viaBridge)
}

// shapeOf finds which result columns hold the tenant and the row keys. It only
// trusts columns that come straight from one table (non-zero TableOID, same
// OID for all of them) and are sent as text.
func (a *Attributor) shapeOf(p *pending, rd *pgproto3.RowDescription) rowShape {
	if p.table == "" || p.stmt.Delete || !a.ex.anyKey {
		return rowShape{}
	}
	sh := rowShape{tenant: -1, keys: map[string]int{}}
	var oid uint32
	for i, f := range rd.Fields {
		if f.TableOID == 0 || f.Format != 0 {
			continue
		}
		name := strings.ToLower(string(f.Name))
		switch {
		case a.tenants[name] && sh.tenant < 0:
			sh.tenant = i
		case a.keyUsable(p.ti, name):
			sh.keys[name] = i
		default:
			continue
		}
		if oid != 0 && oid != f.TableOID {
			return rowShape{}
		}
		oid = f.TableOID
	}
	sh.ok = sh.tenant >= 0 && len(sh.keys) > 0
	return sh
}

func (a *Attributor) learnRow(p *pending, row *pgproto3.DataRow) {
	if p.shape.tenant >= len(row.Values) || row.Values[p.shape.tenant] == nil {
		return
	}
	tenant := string(row.Values[p.shape.tenant])
	for col, i := range p.shape.keys {
		if i < len(row.Values) && row.Values[i] != nil {
			a.cache.Put(p.table, col, string(row.Values[i]), tenant)
		}
	}
}

// handle queues x for reply matching and attributes it. pt is nil for the
// simple protocol. Must be called with st.mu held.
func (a *Attributor) handle(s *proxy.Session, st *state, x Stmt, pt *portal) {
	p := &pending{stmt: x}
	st.queue = append(st.queue, p)
	if x.Set != nil || x.Dealloc != nil || x.Prepare != nil {
		p.undo = st.undoPoint() // the database may refuse it: a role that does not exist, a name that is taken
	}

	if x.Set != nil {
		sv := *x.Set
		for _, op := range sv.ValueOps {
			if v, ok := st.val(pt, op); ok {
				sv.Values = append(sv.Values, v)
			}
		}
		a.applySet(st, &sv)
	}
	if x.Dealloc != nil {
		// The database keeps SQL-level and protocol-level statements in one place.
		if *x.Dealloc == "" {
			st.sqlPrep = map[string]Stmt{}
			st.prepared = map[string]*prepared{}
		} else {
			delete(st.sqlPrep, *x.Dealloc)
			delete(st.prepared, *x.Dealloc)
		}
	}
	if x.Prepare != nil {
		ps := x.Prepare.Stmt
		ps.env = a.captureEnv(st)
		st.sqlPrep[x.Prepare.Name] = ps
	}
	if x.DiscardTemp {
		st.temps, st.txnTemps = map[string]*tempTable{}, nil
	}
	if x.RollbackTo {
		a.txnRolledBack(st) // part of the transaction is undone: what survives is not known
	}
	if sp := x.SP; sp != nil && st.inTxn {
		a.savepointOp(st, sp)
	}
	switch x.Txn {
	case txnBegin:
		if !st.inTxn {
			s := st.saveSettings()
			st.txnStart, st.savepoints = &s, nil
		}
		st.inTxn = true
		return
	case txnEnd:
		if start := st.txnStart; start != nil {
			if x.TxnRollback {
				st.restoreSettings(*start)
			} else {
				p.undo = func() { st.restoreSettings(*start) } // a COMMIT the database turned into a ROLLBACK
			}
		}
		st.txnStart, st.savepoints = nil, nil
		st.resetTxn()
		return
	}
	for _, ref := range x.Drops {
		if _, tmp, ok := a.tableFor(st, ref); ok && tmp != nil {
			p.dropTemps = append(p.dropTemps, ref.Name)
			continue
		}
		ti, _, _ := a.tableFor(st, ref)
		p.dropKeys = append(p.dropKeys, tableKey(ref, ti))
	}
	if x.Temp != nil && !x.DML {
		p.temp = &tempReg{def: x.Temp}
	}

	if x.Exec != nil {
		// SQL-level EXECUTE runs a statement PREPAREd earlier, with its own arguments.
		prep, ok := st.sqlPrep[x.Exec.Name]
		if !ok {
			// ... or one parsed through the protocol, which shares the namespace.
			if pr := st.prepared[x.Exec.Name]; pr != nil && len(pr.stmts) == 1 {
				prep, ok = pr.stmts[0], true
			}
		}
		if !ok {
			a.emit(st, Event{SessionID: s.ID, User: s.User, DB: s.DB, Source: SourceUnknown, SQL: x.SQL})
			return
		}
		args := &portal{params: make([]string, len(x.Exec.Args)), valid: make([]bool, len(x.Exec.Args))}
		for i, op := range x.Exec.Args {
			args.params[i], args.valid[i] = st.val(pt, op)
		}
		prep.SQL = x.SQL
		x, pt, p.stmt = prep, args, prep
	}
	if x.env != nil {
		// The database binds a prepared statement's names when it is prepared, and
		// binds them again, with the search_path it was prepared under, whenever the
		// statement is invalidated (dropping a temporary table it used does that). A
		// temporary table that has appeared or gone since means the name may now
		// stand for either table, and neither can be vouched for.
		for _, ref := range x.Tables {
			if (ref.Schema == "" || ref.Schema == "pg_temp") && st.temps[ref.Name] != x.env.temps[ref.Name] {
				a.emit(st, Event{SessionID: s.ID, User: s.User, DB: s.DB, Source: SourceUnknown, SQL: x.SQL})
				return
			}
		}
		st.frozen = x.env
		defer func() { st.frozen = nil }()
	}
	if !x.DML {
		return
	}

	var ti *TableInfo
	if x.Table.Name != "" {
		var tmp *tempTable
		ti, tmp, _ = a.tableFor(st, x.Table)
		if tmp == nil {
			p.table = tableKey(x.Table, ti)
		} // a temporary table is private to its session: never cached
		p.ti = ti
	}

	// INSERT without a column list: the schema says which position is the tenant.
	x.Inserts = append(x.Inserts, a.rawInserts(x, ti)...)
	a.deriveInsertTenants(st, pt, ti, &x)
	p.dropTable = x.SetsKey || a.assignsKey(ti, x)

	r := a.analyze(st, x, pt, 0, nil)
	sound, trusted, tenants := a.judge(r)
	if x.Temp != nil {
		kind, ten := a.provenance(st, r, tenants)
		p.temp = &tempReg{def: x.Temp, kind: kind, tenants: ten}
	}

	// A row that points at a parent owned by another tenant breaks the assumption
	// every foreign-key step of the analysis rests on.
	brokenRelationship := a.checkRelationships(st, pt, ti, x, tenants)

	// What this statement teaches the cache once it succeeds.
	for _, row := range x.Inserts {
		tv, ok := st.val(pt, row.Tenant)
		if !ok || p.table == "" || x.OnConflict {
			continue // after ON CONFLICT the row may be one that was already there, owned by someone else
		}
		for _, k := range row.Keys {
			if kv, ok := st.val(pt, k.Op); ok && a.keyUsable(ti, k.Col) {
				p.learn = append(p.learn, learned{cacheKey{p.table, k.Col, normVal(kv)}, tv})
			}
		}
	}
	if x.Delete && p.table != "" {
		for _, pr := range x.Keys {
			for _, o := range pr.Ops {
				if v, ok := st.val(pt, o); ok {
					p.forget = append(p.forget, cacheKey{p.table, pr.Col, normVal(v)})
				}
			}
		}
	}

	a.verifyHits(r.cr.hits)

	sc := r.scope()
	ev := Event{SessionID: s.ID, User: s.User, DB: s.DB, SQL: x.SQL, Scan: r.scan, Tables: a.tableNames(st, x), Conflict: brokenRelationship}
	// A connection tag or a transaction's tenant stands in for what the statement
	// does not say, but not for one that visibly reads beyond it.
	ev.partial = r.cr.partial
	ev.loose = r.cr.loose || (sound && len(r.cr.needs) > maxLookupKeys) || a.movesRows(st, x) // moving rows to another tenant is not covered by a tag

	// The statement's own tenants are trustworthy, and either name some, or the
	// predicates contradict each other so it reads nothing at all.
	confinedToNothing := trusted && !x.Insert && r.cr.confined && r.cr.tenantOccs > 0 && len(tenants) == 0
	_ = sound

	switch {
	case len(tenants) > 0 || confinedToNothing:
		claimEvent(&ev, tenants, r.cr, x.Insert)
	case sc == scopeUntenanted:
		ev.Source = SourceUntenanted
	case sc == scopeDerived:
		ev.Tenants, ev.Source = dedupe(r.derived), SourceDerived
	case sound && len(r.cr.needs) > 0 && a.lk != nil && sc == scopeTenant && len(r.cr.needs) <= maxLookupKeys && a.lookupable(r.cr.needs):
		// Some rows' owners are not cached. Ask the database, off the query path;
		// the event is emitted when the answer arrives.
		if a.strict == StrictBlock {
			// The decision must be made before the statement is forwarded.
			owners, failed := a.lookupOwners(r.cr.needs)
			a.finishLookup(st, ev, x, pt, owners, failed, true)
		} else {
			gen := st.txnGen
			needs := r.cr.needs
			slot := &outSlot{}
			st.out = append(st.out, slot)
			go func() {
				owners, failed := a.lookupOwners(needs)
				st.mu.Lock()
				defer st.mu.Unlock()
				defer func() {
					// Whatever happens, the events queued behind this one must not wait forever.
					if !slot.ready {
						st.fill = nil
						slot.ev, slot.ready = ev, true
						slot.ev.Source = SourceUnknown
						slot.ev.Violation = a.violation(slot.ev)
						a.flush(st)
					}
				}()
				// If the transaction ended while we waited, its tenant no longer applies.
				st.fill = slot
				a.finishLookup(st, ev, x, pt, owners, failed, gen == st.txnGen)
				st.fill = nil
			}()
		}
		return
	}
	if x.Copy != nil && a.beginCopy(st, ev, x) {
		return // the event is completed when the rows have been read
	}
	a.settle(st, &ev, true)
}

// claimEvent fills in an event for a statement whose tenants are known.
func claimEvent(ev *Event, tenants []string, cr confResult, insert bool) {
	if len(tenants) > 0 {
		ev.Tenants = tenants
	}
	switch {
	case len(tenants) == 0:
		ev.Source = SourceNoMatch // its predicates leave no row it could read
	case !insert && cr.viaLookup:
		ev.Source = SourceLookup
	case !insert && cr.viaKeys:
		ev.Source = SourceCache
	default:
		ev.Source = SourceExplicit
	}
}

// checkRelationships compares the tenant an INSERT gives a row with the owner of
// the parent row it points at, where the cache knows that owner. A mismatch means
// the foreign key does not tie rows to one tenant after all: it stops being
// trusted until it has been re-checked against the data, and the statement is a
// conflict. It reports whether any row did that.
func (a *Attributor) checkRelationships(st *state, pt *portal, ti *TableInfo, x Stmt, tenants []string) bool {
	if ti == nil || a.cat == nil {
		return false
	}
	broken := false
	if len(x.SetVals) > 0 && len(tenants) == 1 {
		// UPDATE ... SET fk = value: the rows keep their tenant (tenants[0]) and now point at that parent.
		for _, sv := range x.SetVals {
			fk, found := a.cat.FK(ti, sv.Col)
			if !found {
				continue
			}
			rv, ok := st.val(pt, sv.Op)
			if !ok {
				continue
			}
			if owner, known := a.parentOwner(fk, rv); known && owner != tenants[0] {
				a.cat.DistrustFK(ti, sv.Col, fk)
				broken = true
			}
		}
	}
	if !x.Insert {
		return broken
	}
	for _, row := range x.Inserts {
		tv, ok := st.val(pt, row.Tenant)
		if !ok {
			continue
		}
		for _, ref := range row.Refs {
			fk, found := a.cat.FK(ti, ref.Col)
			if !found {
				continue
			}
			rv, ok := st.val(pt, ref.Op)
			if !ok {
				continue
			}
			if owner, known := a.parentOwner(fk, rv); known && owner != tv {
				a.cat.DistrustFK(ti, ref.Col, fk)
				broken = true
			}
		}
	}
	return broken
}

// parentOwner is the tenant of the parent row a foreign key value points at: from
// the cache, or in block mode (where the answer is wanted before the statement
// runs) from the database.
func (a *Attributor) parentOwner(fk FKRef, val string) (string, bool) {
	if owner, ok := a.cache.Get(fk.ParentKey, fk.ParentCol, normVal(val)); ok {
		return owner, true
	}
	if a.strict != StrictBlock || a.lk == nil || a.cat == nil {
		return "", false
	}
	parent, ok := a.cat.TableByKey(fk.ParentKey)
	if !ok {
		return "", false
	}
	tcol, _ := a.scopeOf(parent)
	if tcol == "" {
		// The parent is itself a child table: ask through its own parents.
		refs := a.derivedOf(parent)
		bl, isBridge := a.lk.(bridgeLookup)
		if len(refs) == 0 || !isBridge {
			return "", false
		}
		spec, found := a.chainSpec(parent, refs[0], fk.ParentCol)
		if !found {
			return "", false
		}
		ctx, cancel := context.WithTimeout(context.Background(), a.lkTimeout)
		defer cancel()
		a.lookups.Add(1)
		ts, err := bl.viaTenants(ctx, spec, val)
		if err != nil {
			a.lookupFailures.Add(1)
			return "", false
		}
		if len(ts) == 1 {
			return ts[0], true
		}
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.lkTimeout)
	defer cancel()
	a.lookups.Add(1)
	ts, err := a.lk.tenants(ctx, parent, tcol, fk.ParentCol, val)
	if err != nil {
		a.lookupFailures.Add(1)
		return "", false
	}
	if len(ts) == 1 {
		a.cache.Put(fk.ParentKey, fk.ParentCol, normVal(val), ts[0])
		return ts[0], true
	}
	return "", false
}

// verifyHits re-checks a sample of the owners the cache supplied. The sampling
// is deterministic (every 1/rate-th hit), so behaviour does not depend on chance;
// the check runs in the background and never delays the statement.
func (a *Attributor) verifyHits(hits []cacheHit) {
	if a.verifyRate <= 0 || a.lk == nil || len(hits) == 0 {
		return
	}
	for _, h := range hits {
		n := a.verifyN.Add(1)
		if a.verifyRate < 1 && float64(n%1000) >= a.verifyRate*1000 {
			continue
		}
		go a.verifyOne(h)
	}
}

func (a *Attributor) verifyOne(h cacheHit) {
	ctx, cancel := context.WithTimeout(context.Background(), a.lkTimeout)
	defer cancel()
	ts, err := a.lk.tenants(ctx, h.ti, h.tcol, h.col, h.val)
	if err != nil {
		return
	}
	a.verifies.Add(1)
	if len(ts) == 1 && ts[0] == h.tenant {
		return
	}
	a.stale.Add(1)
	a.cache.Delete(h.ti.Key(), h.col, h.val)
	if a.onStale != nil {
		a.onStale(h.ti.Key(), h.col, h.tenant, ts)
	}
}

// lookupable: every row can be asked about, and the database role sees all of it.
func (a *Attributor) lookupable(need []keyReq) bool {
	for _, k := range need {
		if k.ti == nil || k.tcol == "" || !a.cat.CanRead(k.ti) || (k.via != nil && !a.cat.CanRead(k.via.b)) {
			return false
		}
	}
	return true
}

// lookupOwners asks the database who owns the rows the cache did not know. It
// takes no locks. A row whose lookup failed is simply absent from the result,
// so it is never mistaken for a row that does not exist.
func (a *Attributor) lookupOwners(need []keyReq) (owners ownerSet, failed bool) {
	ctx, cancel := context.WithTimeout(context.Background(), a.lkTimeout)
	defer cancel()
	owners = ownerSet{}
	for _, k := range need {
		a.lookups.Add(1)
		if k.via != nil {
			bl, ok := a.lk.(bridgeLookup)
			if !ok {
				a.lookupFailures.Add(1)
				failed = true
				continue
			}
			ts, err := bl.viaTenants(ctx, *k.via, k.val)
			if err != nil {
				a.lookupFailures.Add(1)
				failed = true
				continue
			}
			hv := bridgeVal(normVal(k.val))
			owners[ownerKey{k.via.table(), k.col, hv}] = ts
			if len(ts) == 1 {
				a.bridge.Put(k.via.table(), k.col, hv, ts[0])
			}
			continue
		}
		ts, err := a.lk.tenants(ctx, k.ti, k.tcol, k.col, k.val)
		if err != nil {
			a.lookupFailures.Add(1)
			failed = true
			continue
		}
		owners[ownerKey{k.ti.Key(), k.col, normVal(k.val)}] = ts
		if len(ts) == 1 {
			a.cache.Put(k.ti.Key(), k.col, k.val, ts[0])
		}
	}
	return owners, failed
}

// finishLookup re-reads the statement with the owners the database named: the
// whole analysis again (function calls included), so the verdict is made exactly
// as it would have been had the cache known. st.mu must be held.
func (a *Attributor) finishLookup(st *state, ev Event, x Stmt, pt *portal, owners ownerSet, failed bool, txnOK bool) {
	r := a.analyze(st, x, pt, 0, owners)
	sound, trusted, tenants := a.judge(r)
	// What the lookup taught applies to the verdict too, claimed or not.
	ev.partial = r.cr.partial
	ev.loose = r.cr.loose || (sound && len(r.cr.needs) > maxLookupKeys) || a.movesRows(st, x)
	if x.Table.Name != "" {
		// Now that the rows' owners are known, a write that points one at another tenant's parent shows.
		if ti, tmp, ok := a.tableFor(st, x.Table); ok && tmp == nil && a.checkRelationships(st, pt, ti, x, tenants) {
			ev.Conflict = true
		}
	}
	if trusted && (len(tenants) > 0 || (!x.Insert && r.cr.confined && r.cr.tenantOccs > 0)) {
		claimEvent(&ev, tenants, r.cr, x.Insert)
	}
	a.settle(st, &ev, txnOK)
}

// settle fills in whatever the statement itself did not determine, in order of
// how directly it was declared: the connection's tag, then the transaction,
// then the fact that nothing restricts the statement (scan), then unknown. It
// also flags disagreement between sources. Must be called with st.mu held.
func (a *Attributor) settle(st *state, ev *Event, txnOK bool) {
	tag := st.curTag()
	switch ev.Source {
	case SourceUntenanted, SourceNoMatch:
	case SourceExplicit, SourceCache, SourceLookup, SourceDerived:
		if tag != "" && !sameSet([]string{tag}, ev.Tenants) {
			ev.Conflict = true
		}
		if txnOK && st.inTxn {
			if len(st.txnTenant) > 0 && !sameSet(st.txnTenant, ev.Tenants) {
				ev.Conflict = true
			}
			st.txnTenant = dedupe(append(st.txnTenant, ev.Tenants...))
		}
	default:
		switch {
		case ev.loose:
			ev.Source = SourceUnknown
		case tag != "":
			ev.Tenants, ev.Source = []string{tag}, SourceSession
			ev.Conflict = ev.Conflict || !subsetOf(ev.partial, []string{tag})
		case txnOK && st.inTxn && len(st.txnTenant) > 0:
			ev.Tenants, ev.Source = append([]string(nil), st.txnTenant...), SourceTransaction
			ev.Conflict = ev.Conflict || !subsetOf(ev.partial, st.txnTenant)
		case ev.Scan:
			ev.Source = SourceScan
		default:
			ev.Source = SourceUnknown
		}
	}
	a.emit(st, *ev)
}

package attrib

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Source says how a tenant was determined.
type Source string

const (
	// SourceExplicit: the statement itself names the tenant.
	SourceExplicit Source = "explicit"
	// SourceCache: the statement names a row (e.g. id = 42) whose tenant was
	// learned earlier from an INSERT, from result rows, or from pre-warming.
	SourceCache Source = "cache"
	// SourceLookup: like cache, but the row was not cached so the database was
	// asked.
	SourceLookup Source = "lookup"
	// SourceSession: the connection declared its tenant (startup option or SET).
	SourceSession Source = "session"
	// SourceTransaction: the statement names no tenant, but an earlier
	// statement in the same transaction did.
	SourceTransaction Source = "transaction"
	// SourceUntenanted: every table the statement touches has no tenant
	// column (shared reference data, SELECT 1, ...), so there is no tenant to
	// attribute. Needs the schema catalog.
	SourceUntenanted Source = "untenanted"
	// SourceScan: the statement reads tenant data with no tenant or key filter
	// at all, so it spans every tenant. Definite, unlike unknown.
	SourceScan Source = "scan"
	// SourceNoMatch: the statement names a row that does not exist.
	SourceNoMatch Source = "nomatch"
	// SourceUnknown: the statement touches tenant data but no tenant could be
	// determined. This is the one to alert on.
	SourceUnknown Source = "unknown"
)

// Event is one attributed statement.
type Event struct {
	SessionID uint64
	User, DB  string
	SQL       string
	Tenants   []string // more than one means the statement spans tenants
	Source    Source
	// Scan is set when the statement reads tenant data without any tenant or
	// key filter, whether or not a tenant could still be attributed to it.
	Scan bool
	// Conflict is set when the statement belongs to a different tenant than
	// the one the connection declared or the transaction already established.
	Conflict bool
	// Violation is set when strict mode objects to the statement: "unknown",
	// "scan" or "conflict". Blocked is set when the statement was not forwarded
	// to the database (block mode only).
	Violation string
	Blocked   bool
}

// Options configures an Attributor.
type Options struct {
	TenantColumns []string // columns that hold the tenant, e.g. tenant_id
	KeyColumns    []string // columns that identify a row across tenants, e.g. id; empty disables the row cache
	CacheSize     int      // max cached rows (default 100000)
	// Catalog is optional. With it the proxy can tell shared tables from
	// tenant tables, resolve schemas, and read column-less INSERTs.
	Catalog *Catalog
	// Lookup is optional. With it a row missing from the cache is resolved by
	// asking the database.
	Lookup *Lookup
	// TagVar is the session variable a connection sets to declare its tenant,
	// e.g. `SET tenantdb.tenant = 'acme'` or the startup option
	// `-c tenantdb.tenant=acme`. Default "tenantdb.tenant".
	TagVar string
	// Strict configures what happens to statements that cannot be pinned to a
	// tenant. The zero value only logs.
	Strict StrictOptions
}

const (
	maxLookupKeys = 20
	lookupTimeout = 3 * time.Second
)

// Attributor is a proxy.Observer that attributes DML statements to tenants.
type Attributor struct {
	ex      *Extractor
	cache   *RowCache
	onEvent func(Event)
	tenants map[string]bool
	cat     *Catalog
	lk      rowLookup
	tagVar  string

	strict    StrictMode
	exempt    map[string]bool
	allowScan map[string]bool
}

func New(opt Options, onEvent func(Event)) *Attributor {
	a := &Attributor{
		ex:      NewExtractor(opt.TenantColumns, opt.KeyColumns),
		cache:   NewRowCache(opt.CacheSize),
		onEvent: onEvent,
		tenants: lowerSet(opt.TenantColumns),
		cat:     opt.Catalog,
		tagVar:  strings.ToLower(opt.TagVar),
	}
	a.strict = opt.Strict.Mode
	if a.strict == "" {
		a.strict = StrictOff
	}
	a.exempt, a.allowScan = nameSet(opt.Strict.Exempt), nameSet(opt.Strict.AllowScan)
	if a.tagVar == "" {
		a.tagVar = "tenantdb.tenant"
	}
	if opt.Lookup != nil {
		a.lk = opt.Lookup
	}
	return a
}

// Cache exposes the row cache (for inspection and tests).
func (a *Attributor) Cache() *RowCache { return a.cache }

var _ proxy.Observer = (*Attributor)(nil)

type prepared struct{ stmts []Stmt }

type portal struct {
	stmts  []Stmt
	params []string // decoded bound values; valid[i] is false for NULL
	valid  []bool
}

// resolve turns an operand into its value, using bound parameters if any.
func (p *portal) resolve(o Operand) (string, bool) {
	if !o.Set {
		return "", false
	}
	if o.Param == 0 {
		return o.Lit, true
	}
	if p == nil || o.Param > len(p.params) || !p.valid[o.Param-1] {
		return "", false
	}
	return p.params[o.Param-1], true
}

// pending is a statement sent to the server whose reply has not arrived yet.
// Replies come back in order, so a FIFO lines results up with statements.
type pending struct {
	stmt   Stmt
	table  string     // cache name of the table, empty for multi-table statements
	learn  []learned  // committed to the cache when the statement succeeds
	forget []cacheKey // dropped from the cache when the statement succeeds
	shape  rowShape   // how to read tenant and key columns out of result rows
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

type keyReq struct{ col, val string }

type state struct {
	mu   sync.Mutex
	user string

	prepared map[string]*prepared
	portals  map[string]*portal
	queue    []*pending

	inTxn     bool
	txnTenant []string // tenants seen so far in the open transaction
	txnGen    uint64   // bumped whenever a transaction ends

	path, localPath []string // search_path: session value and SET LOCAL value
	tag, localTag   string   // declared tenant: session value and SET LOCAL value

	gather   bool    // Gate is collecting events instead of emitting them
	gathered []Event // events produced while gathering

	// Names registered since the last Sync/Flush, so a rejected batch can be
	// forgotten (the database never saw it).
	batchParsed, batchBound []string
}

func (st *state) searchPath() []string {
	switch {
	case st.localPath != nil:
		return st.localPath
	case st.path != nil:
		return st.path
	}
	return defaultSearchPath
}

func (st *state) curTag() string {
	if st.localTag != "" {
		return st.localTag
	}
	return st.tag
}

// resetTxn is called when a transaction ends: SET LOCAL values go away and any
// lookup still in flight must not touch the next transaction.
func (st *state) resetTxn() {
	st.inTxn, st.txnTenant = false, nil
	st.localPath, st.localTag = nil, ""
	st.txnGen++
}

// initMu serialises lazy creation of per-session state: the frontend and
// backend goroutines can both be the first to touch a session.
var initMu sync.Mutex

func (a *Attributor) st(s *proxy.Session) *state {
	initMu.Lock()
	defer initMu.Unlock()
	if v, ok := s.State.(*state); ok {
		return v
	}
	v := &state{user: s.User, prepared: map[string]*prepared{}, portals: map[string]*portal{}}
	opts := parseStartupOptions(s.Params["options"])
	if sp, ok := opts["search_path"]; ok {
		v.path = splitPath(sp)
	} else if sp, ok := s.Params["search_path"]; ok {
		v.path = splitPath(sp)
	}
	if t, ok := opts[a.tagVar]; ok {
		v.tag = t
	} else if t, ok := s.Params[a.tagVar]; ok {
		v.tag = t
	}
	s.State = v
	return v
}

func (a *Attributor) OnFrontend(s *proxy.Session, msg pgproto3.FrontendMessage) {
	switch m := msg.(type) {
	case *pgproto3.Query:
		stmts, err := a.ex.Parse(m.String)
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		if err != nil {
			// Still expect one reply we cannot interpret; ReadyForQuery clears it.
			return
		}
		for _, x := range stmts {
			a.handle(s, st, x, nil)
		}
	case *pgproto3.Parse:
		stmts, err := a.ex.Parse(m.Query)
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		if err != nil {
			delete(st.prepared, m.Name)
			return
		}
		st.prepared[m.Name] = &prepared{stmts: stmts}
		st.batchParsed = append(st.batchParsed, m.Name)
	case *pgproto3.Bind:
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		p := st.prepared[m.PreparedStatement]
		if p == nil {
			delete(st.portals, m.DestinationPortal)
			return
		}
		pt := &portal{stmts: p.stmts, params: make([]string, len(m.Parameters)), valid: make([]bool, len(m.Parameters))}
		for i, v := range m.Parameters {
			if v != nil {
				pt.params[i], pt.valid[i] = decodeParam(v, formatFor(m.ParameterFormatCodes, i)), true
			}
		}
		st.portals[m.DestinationPortal] = pt
		st.batchBound = append(st.batchBound, m.DestinationPortal)
	case *pgproto3.Sync, *pgproto3.Flush:
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		st.batchParsed, st.batchBound = nil, nil
	case *pgproto3.Execute:
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		pt := st.portals[m.Portal]
		if pt == nil {
			return
		}
		for _, x := range pt.stmts {
			a.handle(s, st, x, pt)
		}
	case *pgproto3.Close:
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		if m.ObjectType == 'S' {
			delete(st.prepared, m.Name)
		} else {
			delete(st.portals, m.Name)
		}
	}
}

func (a *Attributor) OnBackend(s *proxy.Session, msg pgproto3.BackendMessage) {
	st := a.st(s)
	st.mu.Lock()
	defer st.mu.Unlock()
	switch m := msg.(type) {
	case *pgproto3.RowDescription:
		if len(st.queue) > 0 {
			st.queue[0].shape = a.shapeOf(st.queue[0], m)
		}
	case *pgproto3.DataRow:
		if len(st.queue) > 0 && st.queue[0].shape.ok {
			a.learnRow(st.queue[0], m)
		}
	case *pgproto3.CommandComplete:
		if a.cat != nil && isDDL(string(m.CommandTag)) {
			a.cat.ReloadAsync()
		}
		if p := st.pop(); p != nil {
			for _, l := range p.learn {
				a.cache.Put(l.key.table, l.key.col, l.key.val, l.tenant)
			}
			for _, k := range p.forget {
				a.cache.Delete(k.table, k.col, k.val)
			}
			if p.stmt.SetsKey && p.table != "" {
				a.cache.DropTable(p.table)
			}
		}
	case *pgproto3.PortalSuspended, *pgproto3.EmptyQueryResponse:
		st.pop()
	case *pgproto3.ErrorResponse:
		st.queue = nil
	case *pgproto3.ReadyForQuery:
		st.queue = nil
		if m.TxStatus == 'I' {
			st.resetTxn()
		} else {
			st.inTxn = true
		}
	}
}

func (st *state) pop() *pending {
	if len(st.queue) == 0 {
		return nil
	}
	p := st.queue[0]
	st.queue = st.queue[1:]
	return p
}

// shapeOf finds which result columns hold the tenant and the row keys. It only
// trusts columns that come straight from one table (non-zero TableOID, same
// OID for all of them) and are sent as text.
func (a *Attributor) shapeOf(p *pending, rd *pgproto3.RowDescription) rowShape {
	if p.table == "" || p.stmt.Delete || len(a.ex.keys) == 0 {
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
		case a.ex.keys[name]:
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

// resolve finds the table a reference names, using the session search_path.
func (a *Attributor) resolve(st *state, ref TableRef) (*TableInfo, bool) {
	if a.cat == nil || !a.cat.Loaded() {
		return nil, false
	}
	return a.cat.Table(ref, st.searchPath(), st.user)
}

// tableKey is the cache's name for a table: schema-qualified when the catalog
// resolved it, otherwise as written.
func tableKey(ref TableRef, ti *TableInfo) string {
	if ti != nil {
		return ti.Key()
	}
	return ref.String()
}

func (a *Attributor) hasTenantCol(ti *TableInfo) bool {
	for _, c := range ti.Cols {
		if a.tenants[c] {
			return true
		}
	}
	return false
}

type scope int

const (
	scopeUnknown    scope = iota // something the proxy cannot vouch for
	scopeUntenanted              // provably touches no tenant data
	scopeTenant                  // touches at least one tenant table
)

// classify decides what kind of data a statement touches. It fails closed:
// tables the catalog does not know, views without a tenant column (which may
// wrap tenant tables), and calls to application-defined functions (whose
// bodies the proxy cannot read) all make the answer "unknown", not "untenanted".
func (a *Attributor) classify(st *state, x Stmt) scope {
	if a.cat == nil || !a.cat.Loaded() {
		return scopeUnknown
	}
	tenant, unsure := false, false
	for _, t := range x.Tables {
		ti, ok := a.cat.Table(t, st.searchPath(), st.user)
		switch {
		case !ok:
			unsure = true
		case a.hasTenantCol(ti):
			tenant = true
		case (ti.Kind == 'v' || ti.Kind == 'm') && !isSystemSchema(ti.Schema):
			unsure = true
		}
	}
	for _, f := range x.Funcs {
		if a.cat.UserFunc(f, st.searchPath(), st.user) {
			unsure = true
		}
	}
	switch {
	case tenant:
		return scopeTenant
	case unsure:
		return scopeUnknown
	}
	return scopeUntenanted
}

// handle queues x for reply matching and attributes it. pt is nil for the
// simple protocol. Must be called with st.mu held.
func (a *Attributor) handle(s *proxy.Session, st *state, x Stmt, pt *portal) {
	p := &pending{stmt: x}
	st.queue = append(st.queue, p)

	if x.Set != nil {
		a.applySet(st, x.Set)
	}
	switch x.Txn {
	case txnBegin:
		st.inTxn = true
		return
	case txnEnd:
		st.resetTxn()
		return
	}
	if !x.DML {
		return
	}

	var ti *TableInfo
	if x.Table.Name != "" {
		ti, _ = a.resolve(st, x.Table)
		p.table = tableKey(x.Table, ti)
	}

	// INSERT without a column list: the schema says which position is the tenant.
	x.Inserts = append(x.Inserts, a.rawInserts(x, ti)...)

	// Tenant values named by the statement, including bound parameters.
	tenants := append([]string(nil), x.Tenants...)
	for _, n := range x.Params {
		if v, ok := pt.resolve(Operand{Param: n, Set: true}); ok {
			tenants = append(tenants, v)
		}
	}
	for _, r := range x.Inserts {
		if v, ok := pt.resolve(r.Tenant); ok {
			tenants = append(tenants, v)
		}
	}
	tenants = dedupe(tenants)

	// What this statement teaches the cache once it succeeds.
	for _, r := range x.Inserts {
		tv, ok := pt.resolve(r.Tenant)
		if !ok {
			continue
		}
		for _, k := range r.Keys {
			if kv, ok := pt.resolve(k.Op); ok {
				p.learn = append(p.learn, learned{cacheKey{p.table, k.Col, normVal(kv)}, tv})
			}
		}
	}
	if x.Delete {
		for _, pr := range x.Keys {
			for _, o := range pr.Ops {
				if v, ok := pt.resolve(o); ok {
					p.forget = append(p.forget, cacheKey{p.table, pr.Col, normVal(v)})
				}
			}
		}
	}

	sc := a.classify(st, x)
	ev := Event{SessionID: s.ID, User: s.User, DB: s.DB, SQL: x.SQL}
	ev.Scan = sc == scopeTenant && !x.Filtered && !x.Insert

	switch {
	case len(tenants) > 0:
		ev.Tenants, ev.Source = tenants, SourceExplicit
	case sc == scopeUntenanted:
		ev.Source = SourceUntenanted
	default:
		have, need, ok := a.keyTenants(p.table, x, pt)
		switch {
		case ok && len(need) == 0:
			ev.Tenants, ev.Source = have, SourceCache
		case ok && a.lk != nil && sc == scopeTenant && ti != nil && len(need) <= maxLookupKeys:
			// Ask the database, off the query path; the event is emitted when
			// the answer arrives.
			if tcol, _ := a.scopeOf(ti); tcol != "" && hasCols(ti, need) {
				if a.strict == StrictBlock {
					// The decision must be made before the statement is forwarded.
					found, failed := a.lookupMissing(ti, tcol, have, need)
					a.finishLookup(st, ev, found, failed, true)
				} else {
					go a.resolveAsync(st, ev, ti, tcol, have, need, st.txnGen)
				}
				return
			}
		}
	}
	a.settle(st, &ev, true)
}

func hasCols(ti *TableInfo, need []keyReq) bool {
	for _, k := range need {
		found := false
		for _, c := range ti.Cols {
			if c == k.col {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// resolveAsync finishes an event whose rows were not all cached by asking the
// database for the missing ones, without holding up the query.
func (a *Attributor) resolveAsync(st *state, ev Event, ti *TableInfo, tcol string, have []string, need []keyReq, gen uint64) {
	found, failed := a.lookupMissing(ti, tcol, have, need)
	st.mu.Lock()
	defer st.mu.Unlock()
	// If the transaction ended while we waited, its tenant no longer applies.
	a.finishLookup(st, ev, found, failed, gen == st.txnGen)
}

// lookupMissing asks the database about the keys the cache did not have. It
// takes no locks. failed is set if any lookup errored.
func (a *Attributor) lookupMissing(ti *TableInfo, tcol string, have []string, need []keyReq) (found []string, failed bool) {
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	found = append([]string(nil), have...)
	for _, k := range need {
		ts, err := a.lk.tenants(ctx, ti, tcol, k.col, k.val)
		if err != nil {
			failed = true
			continue
		}
		if len(ts) == 1 {
			a.cache.Put(ti.Key(), k.col, k.val, ts[0])
		}
		found = append(found, ts...)
	}
	return dedupe(found), failed
}

// finishLookup turns lookup results into the event's source. st.mu must be held.
func (a *Attributor) finishLookup(st *state, ev Event, found []string, failed bool, txnOK bool) {
	switch {
	case len(found) > 0:
		ev.Tenants, ev.Source = found, SourceLookup
	case !failed:
		ev.Source = SourceNoMatch
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
	case SourceExplicit, SourceCache, SourceLookup:
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
		case tag != "":
			ev.Tenants, ev.Source = []string{tag}, SourceSession
		case txnOK && st.inTxn && len(st.txnTenant) > 0:
			ev.Tenants, ev.Source = append([]string(nil), st.txnTenant...), SourceTransaction
		case ev.Scan:
			ev.Source = SourceScan
		default:
			ev.Source = SourceUnknown
		}
	}
	a.emit(st, *ev)
}

// keyTenants resolves a statement's key predicates through the row cache. ok
// is false if the statement has no usable key predicate. need lists the keys
// that were not cached.
func (a *Attributor) keyTenants(table string, x Stmt, pt *portal) (have []string, need []keyReq, ok bool) {
	if table == "" || len(x.Keys) == 0 {
		return nil, nil, false
	}
	n := 0
	for _, pr := range x.Keys {
		for _, o := range pr.Ops {
			v, resolved := pt.resolve(o)
			if !resolved {
				return nil, nil, false
			}
			n++
			if t, hit := a.cache.Get(table, pr.Col, v); hit {
				have = append(have, t)
			} else {
				need = append(need, keyReq{pr.Col, v})
			}
		}
	}
	return dedupe(have), need, n > 0
}

// applySet records changes to the variables the attributor cares about.
func (a *Attributor) applySet(st *state, sv *SetVar) {
	if sv.ResetAll {
		st.path, st.localPath, st.tag, st.localTag = nil, nil, "", ""
		return
	}
	switch sv.Name {
	case "search_path":
		var p []string
		if !sv.Reset {
			for _, v := range sv.Values {
				p = append(p, splitPath(v)...)
			}
		}
		if sv.Local {
			st.localPath = p
		} else {
			st.path = p
		}
	case a.tagVar:
		tag := ""
		if !sv.Reset && len(sv.Values) > 0 {
			tag = sv.Values[0]
		}
		if sv.Local {
			st.localTag = tag
		} else {
			st.tag = tag
		}
	}
}

// splitPath turns "a, \"B\", $user" into schema names.
func splitPath(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		if len(p) >= 2 && p[0] == '"' && p[len(p)-1] == '"' {
			p = p[1 : len(p)-1]
		} else {
			p = strings.ToLower(p)
		}
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseStartupOptions reads the `options` startup parameter, e.g.
// "-c search_path=tenant1 -c tenantdb.tenant=acme".
func parseStartupOptions(opts string) map[string]string {
	out := map[string]string{}
	toks := strings.Fields(opts)
	add := func(kv string) {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[strings.ToLower(k)] = v
		}
	}
	for i := 0; i < len(toks); i++ {
		switch t := toks[i]; {
		case t == "-c" && i+1 < len(toks):
			i++
			add(toks[i])
		case strings.HasPrefix(t, "-c"):
			add(t[2:])
		case strings.HasPrefix(t, "--"):
			add(t[2:])
		}
	}
	return out
}

// rawInserts turns the rows of a column-less INSERT into InsRows using the
// table's column order from the catalog. It returns nothing if the table is
// unknown or has no tenant column.
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
		case a.ex.keys[c]:
			keyIdx[c] = i
		}
	}
	if tIdx < 0 {
		return nil
	}
	var out []InsRow
	for _, row := range x.RawInserts {
		if tIdx >= len(row) {
			continue
		}
		r := InsRow{Tenant: row[tIdx]}
		for c, i := range keyIdx {
			if i < len(row) {
				r.Keys = append(r.Keys, KeyOp{Col: c, Op: row[i]})
			}
		}
		out = append(out, r)
	}
	return out
}

func isDDL(tag string) bool {
	return strings.HasPrefix(tag, "CREATE ") || strings.HasPrefix(tag, "ALTER ") || strings.HasPrefix(tag, "DROP ")
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]bool{}
	for _, v := range a {
		m[v] = true
	}
	for _, v := range b {
		if !m[v] {
			return false
		}
	}
	return true
}

func formatFor(codes []int16, i int) int16 {
	switch len(codes) {
	case 0:
		return 0
	case 1:
		return codes[0]
	}
	if i < len(codes) {
		return codes[i]
	}
	return 0
}

// decodeParam turns a bound parameter into the string form a tenant id would
// have in SQL. Binary values are decoded by size: 16 bytes as a UUID, 4 or 8
// as an integer, anything else as text.
func decodeParam(b []byte, format int16) string {
	if format == 0 {
		return string(b)
	}
	switch len(b) {
	case 16:
		h := hex.EncodeToString(b)
		return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:])
	case 8:
		return fmt.Sprint(int64(binary.BigEndian.Uint64(b)))
	case 4:
		return fmt.Sprint(int32(binary.BigEndian.Uint32(b)))
	}
	return string(b)
}

func dedupe(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

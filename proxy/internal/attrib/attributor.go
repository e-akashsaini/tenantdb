package attrib

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
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
	// SourceDerived: the statement reads a temporary table that was filled
	// from one tenant's data, so that is whose data it reads.
	SourceDerived Source = "derived"
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
	Tables    []string // the tables it touches, as the catalog names them (schema.table)
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
	// Unparsed: the statement could not be analysed (a syntax the parser does not
	// know, or too large or deeply chained to hand to it safely). Nothing can be
	// said about it, so it is unknown.
	Unparsed bool
	// loose: the statement reads tenant data limited only by a range, a pattern or a
	// list too long to look up; a tag or transaction tenant does not vouch for it.
	loose bool
	// partial: tenants some part of the statement is known to be confined to.
	partial []string
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
	// CacheVerify is the fraction (0 to 1) of cache hits that are re-checked
	// against the database in the background. A hit that disagrees (the row was
	// deleted and its key reused, or moved to another tenant by something the
	// proxy did not see) is evicted and reported to OnStale. 0 turns it off.
	CacheVerify float64
	// OnStale is called, from a background goroutine, with the owner the cache
	// held and what the database says.
	OnStale func(table, col, cached string, actual []string)
	// CacheTTL bounds how long a cached row owner is trusted (0: forever). Row
	// owners almost never change, but a delete the proxy did not see (a
	// cascade, another path to the database) followed by key reuse would
	// leave a stale entry; the TTL caps how long that can last.
	CacheTTL time.Duration
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
	bridge  *RowCache // credential-style lookups (viaBridge), keyed by a hash of the credential
	tagVar  string

	strict    StrictMode
	exempt    map[string]bool
	allowScan map[string]bool

	fmu  sync.Mutex
	fgen uint64

	lookups, lookupFailures atomic.Uint64
	verifyRate              float64
	onStale                 func(table, col, cached string, actual []string)
	verifies, stale         atomic.Uint64
	verifyN                 atomic.Uint64
	fcache                  map[*FuncDef]*FuncBody
	tcache                  map[*FuncDef]*FuncBody // trigger functions, read with NEW/OLD bound
}

// Stats are the attributor's own counters.
type Stats struct {
	CacheEntries, BridgeEntries int
	CacheHits, CacheMisses      uint64
	Lookups, LookupFailures     uint64
	Verifications, StaleFound   uint64
}

// Stats returns the row cache and lookup counters.
func (a *Attributor) Stats() Stats {
	h, m := a.cache.Counts()
	return Stats{CacheEntries: a.cache.Len(), BridgeEntries: a.bridge.Len(), CacheHits: h, CacheMisses: m,
		Lookups: a.lookups.Load(), LookupFailures: a.lookupFailures.Load(),
		Verifications: a.verifies.Load(), StaleFound: a.stale.Load()}
}

func New(opt Options, onEvent func(Event)) *Attributor {
	a := &Attributor{
		ex:      NewExtractor(opt.TenantColumns, opt.KeyColumns),
		cache:   NewRowCacheTTL(opt.CacheSize, opt.CacheTTL),
		onEvent: onEvent,
		tenants: lowerSet(opt.TenantColumns),
		cat:     opt.Catalog,
		tagVar:  strings.ToLower(opt.TagVar),
	}
	a.bridge = NewRowCacheTTL(opt.CacheSize, bridgeTTL(opt.CacheTTL))
	a.verifyRate, a.onStale = opt.CacheVerify, opt.OnStale
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

// bindEnv is what a statement's names were resolved against when it was prepared.
type bindEnv struct {
	temps map[string]*tempTable
	path  []string
}

func (a *Attributor) captureEnv(st *state) *bindEnv {
	e := &bindEnv{path: a.pathOf(st)}
	if len(st.temps) > 0 {
		e.temps = make(map[string]*tempTable, len(st.temps))
		for k, v := range st.temps {
			e.temps[k] = v
		}
	}
	return e
}

type prepared struct {
	stmts    []Stmt
	unparsed string // the SQL, if it could not be analysed
}

type portal struct {
	unparsed string
	stmts    []Stmt
	params   []string // decoded bound values; valid[i] is false for NULL
	valid    []bool
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
	ti     *TableInfo // the catalog's view of table, if known

	dropKeys  []string // cache tables to forget when the statement succeeds (DROP, TRUNCATE)
	dropTemps []string // temporary tables to forget when it succeeds
	temp      *tempReg // a temporary table to remember when it succeeds
	dropTable bool     // the statement may have moved rows between tenants: forget the table's owners
	// undo puts back the session state the statement changed, if the database
	// rejects the statement.
	undo func()
}

// tempReg is a temporary table created by a statement still awaiting its reply.
type tempReg struct {
	def     *TempDef
	kind    tempKind
	tenants []string
}

type tempKind int

const (
	// tempTainted: filled from data the proxy could not attribute; reading it is unknown.
	tempTainted tempKind = iota
	// tempUntenanted: filled from data that belongs to no tenant.
	tempUntenanted
	// tempDerived: filled from one set of tenants' data, which it inherits.
	tempDerived
	// tempTenant: it has a tenant column of its own, so it is filtered like any tenant table.
	tempTenant
)

// tempTable is a temporary table of one session.
type tempTable struct {
	dropOnCommit bool
	kind         tempKind
	tenants      []string
	info         *TableInfo // columns, for tempTenant
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

type state struct {
	mu       sync.Mutex
	user, db string

	temps  map[string]*tempTable // this session's temporary tables
	frozen *bindEnv              // set while analysing a prepared statement: its names mean what they meant when it was prepared
	// txnStart: the session settings when the explicit transaction began; savepoints
	// keep theirs. A rollback gives them back, as the database does.
	txnStart   *settings
	savepoints []savepoint
	txnTemps   []string // the ones created in the current transaction
	// txnLearned: what the cache was taught during the current transaction, to be
	// forgotten if it does not commit.
	txnLearned []cacheKey
	sqlPrep    map[string]Stmt // SQL-level PREPAREd statements

	prepared map[string]*prepared
	portals  map[string]*portal
	queue    []*pending

	inTxn     bool
	txnTenant []string // tenants seen so far in the open transaction
	txnGen    uint64   // bumped whenever a transaction ends

	vars, localVars map[string]string // custom session variables (SET app.x = ..., set_config): current_setting() reads them
	copyIn          *copyIn           // a COPY FROM STDIN waiting for its data
	roleS, roleL    roleState         // SET ROLE / SET SESSION AUTHORIZATION: session and SET LOCAL
	path, localPath []string          // search_path: session value and SET LOCAL value
	tag, localTag   string            // declared tenant: session value and SET LOCAL value

	gather   bool    // Gate is collecting events instead of emitting them
	gathered []Event // events produced while gathering

	// Events leave a session in the order its statements arrived. A statement
	// waiting on a lookup holds a slot; the events after it queue behind it.
	out  []*outSlot
	fill *outSlot // the slot the event being settled belongs to

	// Names registered since the last Sync/Flush, so a rejected batch can be
	// forgotten (the database never saw it).
	// undo puts back what the messages of the unsynced batch changed (prepared
	// statements and portals), if the batch is rejected and never reaches the
	// database, which still holds whatever it held before.
	undo []func()
}

// pathOf is the search_path in force: SET LOCAL, then SET, then what the
// database gives this role (ALTER ROLE / ALTER DATABASE ... SET), then the
// built-in default.
func (a *Attributor) pathOf(st *state) []string {
	switch {
	case st.frozen != nil:
		return st.frozen.path
	case st.localPath != nil:
		return st.localPath
	case st.path != nil:
		return st.path
	case a.cat != nil && a.cat.Loaded():
		return a.cat.DefaultSearchPath(st.user, st.db)
	}
	return defaultSearchPath
}

func (st *state) curTag() string {
	if st.localTag != "" {
		return st.localTag
	}
	return st.tag
}

// val resolves an operand: a literal, a bound parameter, or the current value of
// a session variable (unknown if nothing on this connection has set it).
func (st *state) val(pt *portal, op Operand) (string, bool) {
	if op.Var == varCurrentUser {
		return st.effRole()
	}
	if op.Var != "" {
		if v, ok := st.localVars[op.Var]; ok {
			return v, true
		}
		v, ok := st.vars[op.Var]
		return v, ok
	}
	return pt.resolve(op)
}

// resetTxn is called when a transaction ends: SET LOCAL values go away and any
// lookup still in flight must not touch the next transaction.
func (st *state) resetTxn() {
	st.inTxn, st.txnTenant = false, nil
	st.localPath, st.localTag, st.localVars = nil, "", nil
	st.roleL = roleState{}
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
	v := &state{user: s.User, db: s.DB, temps: map[string]*tempTable{}, sqlPrep: map[string]Stmt{},
		prepared: map[string]*prepared{}, portals: map[string]*portal{}}
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
	v.vars = map[string]string{}
	for k, val := range s.Params {
		if strings.Contains(k, ".") {
			v.vars[strings.ToLower(k)] = val
		}
	}
	for k, val := range opts {
		if strings.Contains(k, ".") {
			v.vars[k] = val
		}
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
			a.unparsed(s, st, m.String)
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
			st.setPrepared(m.Name, &prepared{unparsed: m.Query})
			return
		}
		env := a.captureEnv(st)
		for i := range stmts {
			stmts[i].env = env
		}
		st.setPrepared(m.Name, &prepared{stmts: stmts})
	case *pgproto3.Bind:
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		p := st.prepared[m.PreparedStatement]
		if p == nil {
			// SQL-level PREPARE and protocol-level Parse share one namespace in the database.
			if sp, ok := st.sqlPrep[m.PreparedStatement]; ok {
				p = &prepared{stmts: []Stmt{sp}}
			}
		}
		if p == nil {
			st.setPortal(m.DestinationPortal, nil)
			return
		}
		pt := &portal{stmts: p.stmts, unparsed: p.unparsed, params: make([]string, len(m.Parameters)), valid: make([]bool, len(m.Parameters))}
		for i, v := range m.Parameters {
			if v != nil {
				pt.params[i], pt.valid[i] = decodeParam(v, formatFor(m.ParameterFormatCodes, i)), true
			}
		}
		st.setPortal(m.DestinationPortal, pt)
	case *pgproto3.Sync, *pgproto3.Flush:
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		st.undo = nil
	case *pgproto3.Execute:
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		pt := st.portals[m.Portal]
		if pt == nil {
			// Running something the proxy never saw created (or has lost track of)
			// cannot be vouched for.
			st.queue = append(st.queue, &pending{})
			a.emit(st, Event{SessionID: s.ID, User: s.User, DB: s.DB, Source: SourceUnknown,
				SQL: "<execute of a portal the proxy did not see created>"})
			return
		}
		if pt.unparsed != "" {
			a.unparsed(s, st, pt.unparsed)
			return
		}
		for _, x := range pt.stmts {
			a.handle(s, st, x, pt)
		}
	case *pgproto3.CopyData, *pgproto3.CopyDone, *pgproto3.CopyFail:
		a.onCopyFrontend(s, msg)
	case *pgproto3.FunctionCall:
		// The legacy fast-path protocol calls a function by OID with no SQL to
		// read, so nothing about what it touches is known.
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		a.emit(st, Event{SessionID: s.ID, User: s.User, DB: s.DB, Source: SourceUnknown,
			SQL: fmt.Sprintf("<fast-path function call, oid %d>", m.Function)})
	case *pgproto3.Close:
		st := a.st(s)
		st.mu.Lock()
		defer st.mu.Unlock()
		if m.ObjectType == 'S' {
			st.setPrepared(m.Name, nil)
		} else {
			st.setPortal(m.Name, nil)
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
			a.bridge.Clear()
		}
		if p := st.pop(); p != nil {
			if p.stmt.Txn == txnEnd {
				if string(m.CommandTag) == "COMMIT" {
					a.txnCommitted(st)
				} else {
					a.txnRolledBack(st) // a rollback, or a commit that turned into one, keeps nothing
					if p.undo != nil {
						p.undo()
					}
				}
			}
			for _, l := range p.learn {
				a.cache.Put(l.key.table, l.key.col, l.key.val, l.tenant)
				st.txnLearned = append(st.txnLearned, l.key)
			}
			for _, k := range p.forget {
				a.cache.Delete(k.table, k.col, k.val)
			}
			if p.dropTable && p.table != "" {
				a.cache.DropTable(p.table)
			}
			for _, k := range p.dropKeys {
				a.cache.DropTable(k)
			}
			if len(p.dropKeys) > 0 || p.dropTable {
				a.bridge.Clear()
			}
			for _, n := range p.dropTemps {
				delete(st.temps, n)
			}
			if p.temp != nil {
				a.registerTemp(st, p.temp)
			}
		}
	case *pgproto3.PortalSuspended, *pgproto3.EmptyQueryResponse:
		st.pop()
	case *pgproto3.ErrorResponse:
		for i := len(st.queue) - 1; i >= 0; i-- {
			if u := st.queue[i].undo; u != nil {
				u() // what the database refused (or never got to) did not happen
			}
		}
		st.queue = nil
		a.txnRolledBack(st)  // the statement, or the whole transaction, was rolled back
		a.endCopy(st, false) // the load stopped: what it had named so far is what it addressed
	case *pgproto3.ReadyForQuery:
		st.queue = nil
		a.endCopy(st, false)
		if m.TxStatus == 'I' {
			a.txnCommitted(st) // the implicit transaction committed
			st.resetTxn()
		} else {
			st.inTxn = true
		}
	}
}

// setPrepared records a named statement (nil removes it), remembering what it
// replaced so a rejected batch can put it back.
func (st *state) setPrepared(name string, p *prepared) {
	old, had := st.prepared[name]
	st.undo = append(st.undo, func() {
		if had {
			st.prepared[name] = old
		} else {
			delete(st.prepared, name)
		}
	})
	if p == nil {
		delete(st.prepared, name)
	} else {
		st.prepared[name] = p
	}
}

// setPortal is setPrepared for portals.
func (st *state) setPortal(name string, p *portal) {
	old, had := st.portals[name]
	st.undo = append(st.undo, func() {
		if had {
			st.portals[name] = old
		} else {
			delete(st.portals, name)
		}
	})
	if p == nil {
		delete(st.portals, name)
	} else {
		st.portals[name] = p
	}
}

// settings are the session values the proxy tracks that a rollback undoes.
type settings struct {
	path, localPath []string
	tag, localTag   string
	vars, localVars map[string]string
	roleS, roleL    roleState
}

type savepoint struct {
	name string
	s    settings
}

func copyVars(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (st *state) saveSettings() settings {
	return settings{st.path, st.localPath, st.tag, st.localTag, copyVars(st.vars), copyVars(st.localVars), st.roleS, st.roleL}
}

func (st *state) restoreSettings(s settings) {
	st.path, st.localPath, st.tag, st.localTag = s.path, s.localPath, s.tag, s.localTag
	st.vars, st.localVars = copyVars(s.vars), copyVars(s.localVars)
	if st.vars == nil {
		st.vars = map[string]string{}
	}
	st.roleS, st.roleL = s.roleS, s.roleL
}

// undoPoint captures what a statement that changes session state may change, to
// be put back if the database rejects it.
func (st *state) undoPoint() func() {
	s := st.saveSettings()
	sqlPrep := make(map[string]Stmt, len(st.sqlPrep))
	for k, v := range st.sqlPrep {
		sqlPrep[k] = v
	}
	prep := make(map[string]*prepared, len(st.prepared))
	for k, v := range st.prepared {
		prep[k] = v
	}
	return func() {
		st.restoreSettings(s)
		st.sqlPrep, st.prepared = sqlPrep, prep
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

// registerTemp remembers a temporary table once its CREATE succeeded.
func (a *Attributor) registerTemp(st *state, reg *tempReg) {
	def := reg.def
	t := &tempTable{kind: reg.kind, tenants: reg.tenants}
	cols := append([]string(nil), def.Cols...)
	complete := true
	for _, l := range def.Like {
		li, ltmp, ok := a.tableFor(st, l)
		switch {
		case !ok:
			complete = false
		case ltmp != nil && ltmp.info != nil:
			cols = append(cols, ltmp.info.Cols...)
		case ltmp != nil:
			complete = false
		default:
			cols = append(cols, li.Cols...)
		}
	}
	hasTenant := false
	for _, c := range cols {
		hasTenant = hasTenant || a.tenants[c]
	}
	switch {
	case hasTenant && (!def.FromQuery || len(def.Cols) > 0):
		// It has a tenant column of its own, so it is filtered like any tenant table.
		t.kind = tempTenant
		t.info = &TableInfo{Schema: "pg_temp", Name: def.Name, Kind: 'r', Cols: cols, raw: cols}
	case !def.FromQuery && complete:
		t.kind = tempUntenanted
	case !def.FromQuery:
		t.kind = tempTainted
	}
	t.dropOnCommit = def.OnCommitDrop
	st.temps[def.Name] = t
	st.txnTemps = append(st.txnTemps, def.Name)
}

// savepointOp keeps the settings as they were at each savepoint.
func (a *Attributor) savepointOp(st *state, sp *SavepointOp) {
	find := func() int {
		for i := len(st.savepoints) - 1; i >= 0; i-- {
			if st.savepoints[i].name == sp.Name {
				return i
			}
		}
		return -1
	}
	switch sp.Op {
	case 's':
		st.savepoints = append(st.savepoints, savepoint{sp.Name, st.saveSettings()})
	case 'r':
		if i := find(); i >= 0 {
			st.savepoints = st.savepoints[:i]
		}
	case 'b':
		if i := find(); i >= 0 {
			st.restoreSettings(st.savepoints[i].s)
			st.savepoints = st.savepoints[:i+1]
		}
	}
}

// txnCommitted: what the transaction created is permanent now (except ON COMMIT DROP tables).
func (a *Attributor) txnCommitted(st *state) {
	st.dropTxnTemps(true)
	st.txnLearned = nil
}

// txnRolledBack forgets what a transaction that did not commit taught the cache
// and the temporary tables it made.
func (a *Attributor) txnRolledBack(st *state) {
	st.dropTxnTemps(false)
	for _, k := range st.txnLearned {
		a.cache.Delete(k.table, k.col, k.val)
	}
	st.txnLearned = nil
}

// dropTxnTemps forgets the temporary tables created in the current transaction:
// all of them (a rollback, an error), or only those made ON COMMIT DROP (a
// commit). A name that is forgotten means the real table again, or an unknown
// table, and both are treated with suspicion; a table remembered after it is
// gone would be treated as the temporary table it no longer is.
func (st *state) dropTxnTemps(onlyOnCommit bool) {
	keep := st.txnTemps[:0]
	for _, n := range st.txnTemps {
		t := st.temps[n]
		switch {
		case t == nil:
		case !onlyOnCommit || t.dropOnCommit:
			delete(st.temps, n)
		default:
			keep = append(keep, n)
		}
	}
	if onlyOnCommit {
		st.txnTemps = nil // the transaction is over: the survivors are permanent for the session
	} else {
		st.txnTemps = keep[:0]
	}
}

// provenance says what a temporary table created from a query inherits.
func (a *Attributor) provenance(st *state, r analysis, tenants []string) (tempKind, []string) {
	switch {
	case r.unresolvedFn || r.scan || r.unsure:
		return tempTainted, nil
	case len(tenants) > 0 || len(r.derived) > 0:
		return tempDerived, dedupe(append(append([]string(nil), tenants...), r.derived...))
	case !r.tenantTouched:
		return tempUntenanted, nil
	case st.curTag() != "":
		return tempDerived, []string{st.curTag()}
	}
	return tempTainted, nil
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
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
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
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
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
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
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

// applySet records changes to the variables the attributor cares about.
func (a *Attributor) applySet(st *state, sv *SetVar) {
	if sv.ResetAll {
		st.path, st.localPath, st.tag, st.localTag = nil, nil, "", ""
		st.vars, st.localVars = map[string]string{}, nil
		st.roleS, st.roleL = roleState{}, roleState{}
		return
	}
	if sv.Name == "role" || sv.Name == "session_authorization" {
		st.applyRole(sv)
		return
	}
	if strings.Contains(sv.Name, ".") {
		// A custom variable (app.tenant, tenantdb.tenant, ...): current_setting() reads it back.
		target := &st.vars
		if sv.Local {
			target = &st.localVars
		}
		if *target == nil {
			*target = map[string]string{}
		}
		if sv.Reset || len(sv.Values) == 0 {
			delete(*target, sv.Name)
			if sv.Local {
				// SET LOCAL ... then RESET: back to the session value
			}
		} else {
			(*target)[sv.Name] = sv.Values[0]
		}
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

// unparsed records a statement the proxy could not analyse. It is queued like any
// statement, so the server's reply stays lined up, and reported as unknown: in
// block mode it is refused unless the role is exempt. Must be called with st.mu held.
func (a *Attributor) unparsed(s *proxy.Session, st *state, sql string) {
	st.queue = append(st.queue, &pending{})
	const maxLogged = 4 << 10
	if len(sql) > maxLogged {
		sql = sql[:maxLogged] + "..."
	}
	ev := Event{SessionID: s.ID, User: s.User, DB: s.DB, SQL: sql, Source: SourceUnknown, Unparsed: true}
	a.emit(st, ev)
}

// subsetOf: is every element of a in b?
func subsetOf(a, b []string) bool {
	in := make(map[string]bool, len(b))
	for _, v := range b {
		in[v] = true
	}
	for _, v := range a {
		if !in[v] {
			return false
		}
	}
	return true
}

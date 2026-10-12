package attrib

import (
	"fmt"
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
	// LookupTimeout bounds one lookup of who owns a row (default 3s). In block
	// mode a statement waits for it; one that times out is treated as unknown.
	LookupTimeout time.Duration
}

const (
	maxLookupKeys = 20
	// DefaultLookupTimeout bounds one lookup of who owns a row.
	DefaultLookupTimeout = 3 * time.Second
)

// Attributor is a proxy.Observer that attributes DML statements to tenants.
type Attributor struct {
	ex        *Extractor
	cache     *RowCache
	onEvent   func(Event)
	tenants   map[string]bool
	cat       *Catalog
	lk        rowLookup
	lkTimeout time.Duration
	bridge    *RowCache // credential-style lookups (viaBridge), keyed by a hash of the credential
	tagVar    string

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
	if a.lkTimeout = opt.LookupTimeout; a.lkTimeout <= 0 {
		a.lkTimeout = DefaultLookupTimeout
	}
	return a
}

// Cache exposes the row cache (for inspection and tests).
func (a *Attributor) Cache() *RowCache { return a.cache }

var _ proxy.Observer = (*Attributor)(nil)

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

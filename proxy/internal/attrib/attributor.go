package attrib

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Source says how a tenant was determined.
type Source string

const (
	// SourceExplicit: the statement itself names the tenant.
	SourceExplicit Source = "explicit"
	// SourceCache: the statement names a row (e.g. id = 42) whose tenant was
	// learned earlier from an INSERT or from result rows.
	SourceCache Source = "cache"
	// SourceTransaction: the statement names no tenant, but an earlier
	// statement in the same transaction did.
	SourceTransaction Source = "transaction"
	// SourceUnknown: nothing available to attribute the statement.
	SourceUnknown Source = "unknown"
)

// Event is one attributed statement.
type Event struct {
	SessionID uint64
	User, DB  string
	SQL       string
	Tenants   []string // more than one means the statement spans tenants
	Source    Source
	// Conflict is set when the statement belongs to a different tenant than
	// the one already established earlier in the same transaction.
	Conflict bool
}

// Options configures an Attributor.
type Options struct {
	TenantColumns []string // columns that hold the tenant, e.g. tenant_id
	KeyColumns    []string // columns that identify a row across tenants, e.g. id; empty disables the row cache
	CacheSize     int      // max cached rows (default 100000)
}

// Attributor is a proxy.Observer that attributes DML statements to tenants.
type Attributor struct {
	ex      *Extractor
	cache   *RowCache
	onEvent func(Event)
	tenants map[string]bool
}

func New(opt Options, onEvent func(Event)) *Attributor {
	return &Attributor{
		ex:      NewExtractor(opt.TenantColumns, opt.KeyColumns),
		cache:   NewRowCache(opt.CacheSize),
		onEvent: onEvent,
		tenants: lowerSet(opt.TenantColumns),
	}
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

type state struct {
	mu        sync.Mutex
	prepared  map[string]*prepared
	portals   map[string]*portal
	queue     []*pending
	inTxn     bool
	txnTenant []string // tenants seen so far in the open transaction
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
	v := &state{prepared: map[string]*prepared{}, portals: map[string]*portal{}}
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
			st.queue[0].shape = a.shapeOf(st.queue[0].stmt, m)
		}
	case *pgproto3.DataRow:
		if len(st.queue) > 0 && st.queue[0].shape.ok {
			a.learnRow(st.queue[0], m)
		}
	case *pgproto3.CommandComplete:
		if p := st.pop(); p != nil {
			for _, l := range p.learn {
				a.cache.Put(l.key.table, l.key.col, l.key.val, l.tenant)
			}
			for _, k := range p.forget {
				a.cache.Delete(k.table, k.col, k.val)
			}
			if p.stmt.SetsKey {
				a.cache.DropTable(p.stmt.Table)
			}
		}
	case *pgproto3.PortalSuspended, *pgproto3.EmptyQueryResponse:
		st.pop()
	case *pgproto3.ErrorResponse:
		st.queue = nil
	case *pgproto3.ReadyForQuery:
		st.queue = nil
		if m.TxStatus == 'I' {
			st.inTxn, st.txnTenant = false, nil
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
func (a *Attributor) shapeOf(x Stmt, rd *pgproto3.RowDescription) rowShape {
	if x.Table == "" || x.Delete || len(a.ex.keys) == 0 {
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
			a.cache.Put(p.stmt.Table, col, string(row.Values[i]), tenant)
		}
	}
}

// handle queues x for reply matching and attributes it. pt is nil for the
// simple protocol. Must be called with st.mu held.
func (a *Attributor) handle(s *proxy.Session, st *state, x Stmt, pt *portal) {
	p := &pending{stmt: x}
	st.queue = append(st.queue, p)

	switch x.Txn {
	case txnBegin:
		st.inTxn = true
		return
	case txnEnd:
		st.inTxn, st.txnTenant = false, nil
		return
	}
	if !x.DML {
		return
	}

	// Tenant values named by the statement, including bound parameters.
	tenants := append([]string(nil), x.Tenants...)
	for _, n := range x.Params {
		if v, ok := pt.resolve(Operand{Param: n, Set: true}); ok {
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
				p.learn = append(p.learn, learned{cacheKey{x.Table, k.Col, normVal(kv)}, tv})
			}
		}
	}
	if x.Delete {
		for _, pr := range x.Keys {
			for _, o := range pr.Ops {
				if v, ok := pt.resolve(o); ok {
					p.forget = append(p.forget, cacheKey{x.Table, pr.Col, normVal(v)})
				}
			}
		}
	}

	ev := Event{SessionID: s.ID, User: s.User, DB: s.DB, SQL: x.SQL}
	switch {
	case len(tenants) > 0:
		ev.Tenants, ev.Source = tenants, SourceExplicit
	default:
		if t, ok := a.lookup(x, pt); ok {
			ev.Tenants, ev.Source = t, SourceCache
		} else if st.inTxn && len(st.txnTenant) > 0 {
			ev.Tenants, ev.Source = append([]string(nil), st.txnTenant...), SourceTransaction
		} else {
			ev.Source = SourceUnknown
		}
	}

	if ev.Source == SourceExplicit || ev.Source == SourceCache {
		if st.inTxn {
			if len(st.txnTenant) > 0 && !sameSet(st.txnTenant, ev.Tenants) {
				ev.Conflict = true
			}
			st.txnTenant = dedupe(append(st.txnTenant, ev.Tenants...))
		}
	}
	a.onEvent(ev)
}

// lookup resolves a statement's key predicates through the row cache. It
// succeeds only if every key value is cached.
func (a *Attributor) lookup(x Stmt, pt *portal) ([]string, bool) {
	var out []string
	for _, pr := range x.Keys {
		for _, o := range pr.Ops {
			v, ok := pt.resolve(o)
			if !ok {
				return nil, false
			}
			t, hit := a.cache.Get(x.Table, pr.Col, v)
			if !hit {
				return nil, false
			}
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return nil, false
	}
	return dedupe(out), true
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

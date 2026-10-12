package attrib

import (
	"strings"
	"sync"
)

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

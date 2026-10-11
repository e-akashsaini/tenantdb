package attrib

import (
	"fmt"
	"strings"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// StrictMode says what to do about a statement that cannot be pinned to a tenant.
type StrictMode string

const (
	// StrictOff only reports sources; nothing is flagged as a violation.
	StrictOff StrictMode = "off"
	// StrictAlert flags violations on the event (and so in the log) but lets
	// every statement through.
	StrictAlert StrictMode = "alert"
	// StrictBlock refuses to forward a statement that violates the policy. It
	// also resolves cache misses before answering, so it can add latency to
	// first-time row lookups.
	StrictBlock StrictMode = "block"
)

// ParseStrictMode validates a flag value.
func ParseStrictMode(s string) (StrictMode, error) {
	switch m := StrictMode(strings.ToLower(strings.TrimSpace(s))); m {
	case "", StrictOff:
		return StrictOff, nil
	case StrictAlert, StrictBlock:
		return m, nil
	}
	return "", fmt.Errorf("unknown strict mode %q (want off, alert or block)", s)
}

// StrictOptions is the strict-mode policy.
//
// A statement violates the policy when it is
//   - "unknown": it touches tenant data and no tenant could be determined;
//   - "scan": it reads tenant data with no tenant or key filter, so it spans
//     every tenant;
//   - "conflict": it belongs to a different tenant than the one the connection
//     declared or the transaction already used.
//
// Untenanted statements (shared tables, SELECT 1) and statements naming a row
// that does not exist are never violations.
type StrictOptions struct {
	Mode StrictMode
	// Exempt lists roles for which nothing is ever a violation: migrations,
	// admin tools, anything meant to cross tenants.
	Exempt []string
	// AllowScan lists roles that may run scans (reports, analytics) but are
	// still held to the other rules.
	AllowScan []string
}

func nameSet(in []string) map[string]bool {
	m := map[string]bool{}
	for _, n := range in {
		if n = strings.TrimSpace(n); n != "" {
			m[n] = true
		}
	}
	return m
}

// violation applies the policy to one event.
func (a *Attributor) violation(ev Event) string {
	if a.strict == StrictOff || a.exempt[ev.User] {
		return ""
	}
	switch {
	case ev.Conflict:
		return "conflict"
	case ev.Scan && !a.allowScan[ev.User]:
		return "scan"
	case ev.Source == SourceUnknown:
		return "unknown"
	}
	return ""
}

// emit delivers an event: straight to the callback, or into the batch Gate is
// collecting so the verdict can be reached before anything is forwarded.
// st.mu must be held.
func (a *Attributor) emit(st *state, ev Event) {
	ev.Violation = a.violation(ev)
	if st.gather {
		st.gathered = append(st.gathered, ev)
		return
	}
	switch {
	case st.fill != nil:
		st.fill.ev, st.fill.ready = ev, true
		st.fill = nil
		a.flush(st)
	case len(st.out) > 0:
		st.out = append(st.out, &outSlot{ev: ev, ready: true})
	default:
		a.onEvent(ev)
	}
}

// outSlot is a place in a session's event order. It is empty until the lookup
// the statement waits for has finished.
type outSlot struct {
	ev    Event
	ready bool
}

// flush delivers the events at the head of the queue that are ready.
// st.mu must be held.
func (a *Attributor) flush(st *state) {
	for len(st.out) > 0 && st.out[0].ready {
		ev := st.out[0].ev
		st.out = st.out[1:]
		a.onEvent(ev)
	}
}

// Observer returns what to hand the proxy. In block mode that is a Gate, which
// changes how the proxy relays extended-protocol batches (it holds them until
// Sync), so in every other mode the plain observer is returned.
func (a *Attributor) Observer() proxy.Observer {
	if a.strict == StrictBlock {
		return gated{a}
	}
	return a
}

type gated struct{ *Attributor }

var _ proxy.Gate = gated{}

func (g gated) Gate(s *proxy.Session, msg pgproto3.FrontendMessage) *proxy.Rejection {
	return g.gate(s, msg)
}

// snapshot is the per-connection bookkeeping a rejected message must undo,
// because the database never saw the message.
type snapshot struct {
	queueLen        int
	inTxn           bool
	txnTenant       []string
	path, localPath []string
	tag, localTag   string
	copyIn          *copyIn
}

func (st *state) snapshot() snapshot {
	return snapshot{
		queueLen: len(st.queue), inTxn: st.inTxn, txnTenant: append([]string(nil), st.txnTenant...),
		path: st.path, localPath: st.localPath, tag: st.tag, localTag: st.localTag,
		copyIn: st.copyIn,
	}
}

// reject undoes everything since snap. Statements the database will never
// receive must not stay in the reply queue, move the transaction state, change
// search_path or the tag, or leave prepared statements behind.
func (st *state) reject(snap snapshot) {
	if len(st.queue) > snap.queueLen {
		st.queue = st.queue[:snap.queueLen]
	}
	st.inTxn, st.txnTenant = snap.inTxn, snap.txnTenant
	st.path, st.localPath, st.tag, st.localTag = snap.path, snap.localPath, snap.tag, snap.localTag
	st.copyIn = snap.copyIn // a COPY the database never received has no rows to read
	// Everything registered in the current batch, including this message.
	for _, n := range st.batchParsed {
		delete(st.prepared, n)
	}
	for _, n := range st.batchBound {
		delete(st.portals, n)
	}
	st.batchParsed, st.batchBound = nil, nil
}

// gate observes one client message and decides whether it may be forwarded.
func (a *Attributor) gate(s *proxy.Session, msg pgproto3.FrontendMessage) *proxy.Rejection {
	switch msg.(type) {
	case *pgproto3.CopyData:
		a.onCopyFrontend(s, msg) // rows are read as they pass; the verdict comes at CopyDone
		return nil
	case *pgproto3.CopyFail:
		a.onCopyFrontend(s, msg) // the client gave up: nothing ran, nothing to refuse
		return nil
	}
	st := a.st(s)
	st.mu.Lock()
	snap := st.snapshot()
	st.gather, st.gathered = true, nil
	st.mu.Unlock()

	// If attribution panics, put the bookkeeping back before the proxy decides
	// what to do: the statement may be refused and never reach the database.
	defer func() {
		if r := recover(); r != nil {
			st.mu.Lock()
			st.gather, st.gathered = false, nil
			st.reject(snap)
			st.mu.Unlock()
			panic(r)
		}
	}()

	a.OnFrontend(s, msg)

	st.mu.Lock()
	evs := st.gathered
	st.gather, st.gathered = false, nil
	var rej *proxy.Rejection
	for _, ev := range evs {
		if ev.Violation != "" {
			rej = &proxy.Rejection{Code: "42501", Message: rejectionText(ev)}
			_, rej.AbortCopy = msg.(*pgproto3.CopyDone)
			break
		}
	}
	if rej != nil {
		st.reject(snap)
		for i := range evs {
			evs[i].Blocked = true // none of this message's statements ran
		}
	}
	st.mu.Unlock()

	for _, ev := range evs {
		a.onEvent(ev)
	}
	return rej
}

func rejectionText(ev Event) string {
	var why string
	switch ev.Violation {
	case "unknown":
		why = "it touches tenant data but could not be attributed to a tenant"
	case "scan":
		why = "it reads tenant data with no tenant or key filter, so it spans every tenant"
	case "conflict":
		why = "it belongs to a different tenant than the one this connection or transaction already uses"
	}
	return fmt.Sprintf("tenantdb: statement blocked (%s): %s", ev.Violation, why)
}

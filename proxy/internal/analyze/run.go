package analyze

import (
	"context"
	"log/slog"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/akashsaini/tenantdb/internal/attrib"
	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Options configures a run.
type Options struct {
	Attrib attrib.Options
	// Lookup, if set, lets rows be resolved by asking the database, as the live
	// proxy does. Without it a statement that names a row by id stays unknown.
	Lookup *attrib.Lookup
	// User and DB are used for entries that do not record them; they pick the
	// search_path the database gives that role.
	User, DB string
}

// Runner feeds recorded statements through the attribution engine.
type Runner struct {
	a   *attrib.Attributor
	opt Options
	rep *Report

	cur      int64 // weight of the entry being fed
	produced bool  // the entry being fed produced at least one event
	sessions map[string]*session
}

type session struct {
	s    *proxy.Session
	inTx bool
}

// NewRunner returns a Runner. Strict mode defaults to alert so that violations
// are flagged.
func NewRunner(opt Options) *Runner {
	r := &Runner{opt: opt, rep: newReport(), sessions: map[string]*session{}}
	ao := opt.Attrib
	ao.Lookup = opt.Lookup
	if ao.Strict.Mode == "" || ao.Strict.Mode == attrib.StrictOff {
		ao.Strict.Mode = attrib.StrictAlert
	}
	if ao.Lookup != nil {
		// Lookups finish before an event is reported only in block mode; nothing
		// is blocked here (the gate is never consulted), the mode just makes the
		// replay wait for each answer so the report is complete.
		ao.Strict.Mode = attrib.StrictBlock
	}
	r.a = attrib.New(ao, r.record)
	return r
}

func (r *Runner) record(ev attrib.Event) {
	r.produced = true
	r.rep.add(ev, r.cur)
}

var (
	placeholder = regexp.MustCompile(`\$(\d+)`)
	txBegin     = regexp.MustCompile(`(?i)^\s*(begin|start\s+transaction)\b`)
	txEnd       = regexp.MustCompile(`(?i)^\s*(commit|rollback|end|abort)\b`)
)

func (r *Runner) session(e Entry) *session {
	key := e.Session
	if s, ok := r.sessions[key]; ok {
		return s
	}
	user, db := e.User, e.DB
	if user == "" {
		user = r.opt.User
	}
	if db == "" {
		db = r.opt.DB
	}
	s := &session{s: &proxy.Session{ID: uint64(len(r.sessions) + 1), User: user, DB: db, Params: map[string]string{}}}
	r.sessions[key] = s
	return s
}

// Feed runs one recorded statement.
func (r *Runner) Feed(e Entry) {
	if e.Calls <= 0 {
		e.Calls = 1
	}
	r.cur, r.produced = e.Calls, false
	r.rep.Entries += e.Calls
	if _, err := attrib.Parse(e.SQL); err != nil {
		r.rep.Unparsed += e.Calls
		return
	}
	sess := r.session(e)

	maxParam := 0
	for _, m := range placeholder.FindAllStringSubmatch(e.SQL, -1) {
		if n, _ := strconv.Atoi(m[1]); n > maxParam {
			maxParam = n
		}
	}
	if maxParam > 0 || e.HasParams {
		// Recorded as a prepared statement: replay it that way, with the
		// parameter values if the log has them and as unknown otherwise.
		n := maxParam
		if len(e.Params) > n {
			n = len(e.Params)
		}
		params := make([][]byte, n)
		for i := 0; i < n && i < len(e.Params); i++ {
			if e.Params[i] != NullParam {
				params[i] = []byte(e.Params[i])
			}
		}
		r.a.OnFrontend(sess.s, &pgproto3.Parse{Name: "", Query: e.SQL})
		r.a.OnFrontend(sess.s, &pgproto3.Bind{Parameters: params})
		r.a.OnFrontend(sess.s, &pgproto3.Execute{})
		r.a.OnFrontend(sess.s, &pgproto3.Sync{})
	} else {
		r.a.OnFrontend(sess.s, &pgproto3.Query{String: e.SQL})
	}

	// Answer as the server would, so the attributor learns from successful
	// statements and tracks transactions.
	switch {
	case txBegin.MatchString(e.SQL):
		sess.inTx = true
	case txEnd.MatchString(e.SQL):
		sess.inTx = false
	}
	status := byte('I')
	if sess.inTx {
		status = 'T'
	}
	r.a.OnBackend(sess.s, &pgproto3.CommandComplete{CommandTag: []byte(commandTag(e.SQL))})
	r.a.OnBackend(sess.s, &pgproto3.ReadyForQuery{TxStatus: status})

	if !r.produced {
		r.rep.Other += e.Calls // SET, BEGIN, DDL, ...: nothing to attribute
	}
}

// commandTag guesses the tag the server would send, which the attributor uses
// to notice schema changes.
func commandTag(sql string) string {
	f := strings.Fields(strings.ToUpper(strings.TrimSpace(sql)))
	if len(f) >= 2 && (f[0] == "CREATE" || f[0] == "ALTER" || f[0] == "DROP") {
		return f[0] + " " + f[1]
	}
	return "OK"
}

// Warm fills the row cache from the database before the replay, so statements
// that name a row by id can be attributed.
func (r *Runner) Warm(ctx context.Context, dsn string, maxRows int, log *slog.Logger) error {
	return r.a.Warm(ctx, dsn, maxRows, log)
}

// Report returns the results so far.
func (r *Runner) Report() *Report {
	r.rep.finish()
	return r.rep
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

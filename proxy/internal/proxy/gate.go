package proxy

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Rejection tells the proxy not to forward a client message. The client gets
// an ErrorResponse with this SQLSTATE code and message instead.
type Rejection struct {
	Code    string // SQLSTATE, e.g. "42501"
	Message string

	// AbortCopy is set when the rejected message ends a COPY ... FROM STDIN
	// (CopyDone). The database is waiting for the rest of the stream, so the
	// proxy sends it a CopyFail carrying Message instead: the statement is
	// rolled back and the database's own error and ReadyForQuery reach the
	// client, which keeps the session usable.
	AbortCopy bool
}

// Gate is an Observer that can also veto client messages. When the configured
// Observer implements Gate, the proxy calls Gate in place of OnFrontend, once
// per client message and in order.
//
// A rejection leaves the database exactly as Postgres would have after the
// same error:
//   - simple protocol: the Query is dropped; the client gets an error and the
//     connection stays usable.
//   - extended protocol: the proxy holds Parse/Bind/Describe/Close/Execute
//     until Sync (or Flush), so a rejected Execute drops the whole batch before
//     the database has seen any of it; the client gets the error and a
//     ReadyForQuery when it sends Sync.
//   - if the database may already hold work the client believes has failed (an
//     open transaction, a batch partly flushed, a reply still in flight), the
//     proxy cannot fake an abort faithfully. It sends a FATAL error and closes
//     the connection, which makes the database roll everything back.
//
// Messages that are not statements (COPY data, function calls, ...) are
// passed through ungated.
type Gate interface {
	Observer
	Gate(s *Session, msg pgproto3.FrontendMessage) *Rejection
}

// gateState is what the relay needs to know about a connection to reject
// safely. It is a pointer inside Session so Session stays copyable.
type gateState struct {
	ready atomic.Bool   // the last reply was ReadyForQuery and nothing has been sent since
	tx    atomic.Uint32 // transaction status from the last ReadyForQuery: 'I', 'T' or 'E'

	wmu sync.Mutex // serialises writes to the client from both pumps
}

func (g *gateState) idle() bool { return g.ready.Load() && byte(g.tx.Load()) == 'I' }

func errorResponse(severity string, r *Rejection) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{Severity: severity, SeverityUnlocalized: severity, Code: r.Code, Message: r.Message}
}

// writeClient sends messages to the client; both pumps write to it.
func writeClient(sess *Session, backend *pgproto3.Backend, msgs ...pgproto3.BackendMessage) error {
	sess.gs.wmu.Lock()
	defer sess.gs.wmu.Unlock()
	for _, m := range msgs {
		backend.Send(m)
	}
	return backend.Flush()
}

func isBatchMessage(msg pgproto3.FrontendMessage) bool {
	switch msg.(type) {
	case *pgproto3.Parse, *pgproto3.Bind, *pgproto3.Describe, *pgproto3.Close, *pgproto3.Execute:
		return true
	}
	return false
}

// pumpClientGated relays client messages through a Gate. It returns when the
// connection should be closed.
func (s *Server) pumpClientGated(log *slog.Logger, sess *Session, gate Gate, backend *pgproto3.Backend, frontend *pgproto3.Frontend) {
	var (
		held       []pgproto3.FrontendMessage // extended-protocol messages waiting for Sync
		heldBytes  int
		partial    bool // part of the current unsynced batch has already gone upstream
		discarding bool // a batch was rejected; drop everything until Sync
	)
	send := func(msgs ...pgproto3.FrontendMessage) bool {
		for _, m := range msgs {
			frontend.Send(m)
		}
		if err := frontend.Flush(); err != nil {
			logRelayErr(log, "upstream write", err)
			return false
		}
		return true
	}

	for {
		msg, err := backend.Receive()
		if err != nil {
			logRelayErr(log, "client read", err)
			return
		}

		if discarding {
			switch msg.(type) {
			case *pgproto3.Sync:
				discarding = false
				if writeClient(sess, backend, &pgproto3.ReadyForQuery{TxStatus: 'I'}) != nil {
					return
				}
			case *pgproto3.Terminate:
				send(msg)
				return
			}
			continue
		}

		var rej *Rejection
		if s.safeObserve(log, func() { rej = gate.Gate(sess, msg) }) && s.cfg.GateFailClosed {
			rej = &Rejection{Code: "XX000", Message: "tenantdb: internal error while checking the statement; refusing to run it"}
		}

		if rej != nil && rej.AbortCopy {
			log.Info("copy aborted", "code", rej.Code, "message", rej.Message)
			s.stats.RejectedSoft.Add(1)
			if !send(&pgproto3.CopyFail{Message: rej.Message}) {
				return
			}
			continue
		}

		if rej != nil {
			soft := sess.gs.idle() && !partial
			log.Info("statement rejected", "code", rej.Code, "message", rej.Message, "soft", soft)
			if !soft {
				s.stats.RejectedHard.Add(1)
				_ = writeClient(sess, backend, errorResponse("FATAL", rej))
				return
			}
			s.stats.RejectedSoft.Add(1)
			held, heldBytes = nil, 0
			if _, isQuery := msg.(*pgproto3.Query); isQuery {
				if writeClient(sess, backend, errorResponse("ERROR", rej), &pgproto3.ReadyForQuery{TxStatus: 'I'}) != nil {
					return
				}
				continue
			}
			if writeClient(sess, backend, errorResponse("ERROR", rej)) != nil {
				return
			}
			discarding = true
			continue
		}

		switch msg.(type) {
		case *pgproto3.Query:
			sess.gs.ready.Store(false)
			if !send(msg) {
				return
			}
		case *pgproto3.Flush:
			partial = partial || len(held) > 0
			if !send(append(held, msg)...) {
				return
			}
			held, heldBytes = nil, 0
		case *pgproto3.Sync:
			sess.gs.ready.Store(false)
			batch := append(held, msg)
			held, heldBytes, partial = nil, 0, false
			if !send(batch...) {
				return
			}
		case *pgproto3.Terminate:
			send(append(held, msg)...)
			return
		default:
			if isBatchMessage(msg) {
				heldBytes += frontendSize(msg)
				if heldBytes > s.cfg.MaxHeldBytes {
					_ = writeClient(sess, backend, errorResponse("FATAL", &Rejection{Code: "54000",
						Message: "tenantdb: too much was sent without a Sync to check it"}))
					return
				}
				held = append(held, cloneFrontend(msg))
				continue
			}
			if !send(msg) {
				return
			}
		}
	}
}

// cloneFrontend copies a message the decoder will overwrite on the next
// Receive, so it can be held back.
func cloneFrontend(msg pgproto3.FrontendMessage) pgproto3.FrontendMessage {
	switch m := msg.(type) {
	case *pgproto3.Parse:
		return &pgproto3.Parse{Name: m.Name, Query: m.Query, ParameterOIDs: append([]uint32(nil), m.ParameterOIDs...)}
	case *pgproto3.Bind:
		c := &pgproto3.Bind{
			DestinationPortal:    m.DestinationPortal,
			PreparedStatement:    m.PreparedStatement,
			ParameterFormatCodes: append([]int16(nil), m.ParameterFormatCodes...),
			ResultFormatCodes:    append([]int16(nil), m.ResultFormatCodes...),
		}
		for _, p := range m.Parameters {
			if p == nil {
				c.Parameters = append(c.Parameters, nil)
			} else {
				c.Parameters = append(c.Parameters, append([]byte{}, p...))
			}
		}
		return c
	case *pgproto3.Describe:
		return &pgproto3.Describe{ObjectType: m.ObjectType, Name: m.Name}
	case *pgproto3.Close:
		return &pgproto3.Close{ObjectType: m.ObjectType, Name: m.Name}
	case *pgproto3.Execute:
		return &pgproto3.Execute{Portal: m.Portal, MaxRows: m.MaxRows}
	}
	return msg
}

// frontendSize approximates how much memory holding a message costs.
func frontendSize(msg pgproto3.FrontendMessage) int {
	switch m := msg.(type) {
	case *pgproto3.Parse:
		return 64 + len(m.Name) + len(m.Query) + 4*len(m.ParameterOIDs)
	case *pgproto3.Bind:
		n := 64 + len(m.DestinationPortal) + len(m.PreparedStatement) + 2*(len(m.ParameterFormatCodes)+len(m.ResultFormatCodes))
		for _, p := range m.Parameters {
			n += 8 + len(p)
		}
		return n
	}
	return 64
}

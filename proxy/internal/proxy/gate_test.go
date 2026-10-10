package proxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// seenLog records what actually reached the "database".
type seenLog struct {
	mu   sync.Mutex
	list []string
}

func (s *seenLog) add(x string) { s.mu.Lock(); s.list = append(s.list, x); s.mu.Unlock() }
func (s *seenLog) all() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.list...)
}
func (s *seenLog) contains(sub string) bool {
	for _, x := range s.all() {
		if strings.Contains(x, sub) {
			return true
		}
	}
	return false
}

// fakeExtUpstream speaks enough of the extended protocol for pgx, tracks
// BEGIN/COMMIT, and records every statement it receives.
func fakeExtUpstream(t *testing.T) (string, *seenLog) {
	t.Helper()
	seen := &seenLog{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serveFakeExt(c, seen)
		}
	}()
	return ln.Addr().String(), seen
}

func serveFakeExt(c net.Conn, seen *seenLog) {
	defer c.Close()
	b := pgproto3.NewBackend(c, c)
	if _, err := b.ReceiveStartupMessage(); err != nil {
		return
	}
	tx := byte('I')
	b.Send(&pgproto3.AuthenticationOk{})
	b.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	b.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	b.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}})
	b.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
	if b.Flush() != nil {
		return
	}
	row := &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("v"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}}
	stmts, portals := map[string]string{}, map[string]string{}
	copyRows := 0
	for {
		msg, err := b.Receive()
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.CopyData:
			copyRows++
		case *pgproto3.CopyDone:
			b.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("COPY %d", copyRows))})
			b.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
			if b.Flush() != nil {
				return
			}
		case *pgproto3.Query:
			if strings.HasPrefix(strings.ToUpper(m.String), "COPY ") {
				copyRows = 0
				b.Send(&pgproto3.CopyInResponse{})
				if b.Flush() != nil {
					return
				}
				continue
			}
			seen.add("query:" + m.String)
			switch strings.ToUpper(strings.TrimSpace(m.String)) {
			case "BEGIN":
				tx = 'T'
				b.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
			case "COMMIT", "ROLLBACK":
				tx = 'I'
				b.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
			default:
				b.Send(row)
				b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("ok:" + m.String)}})
				b.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			}
			b.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
			if b.Flush() != nil {
				return
			}
		case *pgproto3.Parse:
			seen.add("parse:" + m.Query)
			stmts[m.Name] = m.Query
			b.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			seen.add("bind")
			portals[m.DestinationPortal] = m.PreparedStatement
			b.Send(&pgproto3.BindComplete{})
		case *pgproto3.Describe:
			if m.ObjectType == 'S' {
				n := strings.Count(stmts[m.Name], "$")
				oids := make([]uint32, n)
				for i := range oids {
					oids[i] = 25
				}
				b.Send(&pgproto3.ParameterDescription{ParameterOIDs: oids})
			}
			b.Send(row)
		case *pgproto3.Execute:
			sql := stmts[portals[m.Portal]]
			seen.add("execute:" + sql)
			b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("ok:" + sql)}})
			b.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		case *pgproto3.Close:
			b.Send(&pgproto3.CloseComplete{})
		case *pgproto3.Sync:
			b.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
			if b.Flush() != nil {
				return
			}
		case *pgproto3.Flush:
			if b.Flush() != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		}
	}
}

// blockGate rejects any statement mentioning BLOCKME.
type blockGate struct {
	NopObserver
	mu        sync.Mutex
	stmts     map[string]string // statement name -> sql
	portals   map[string]string // portal -> statement name
	rejected  int
	sawFrames []string
}

func newBlockGate() *blockGate {
	return &blockGate{stmts: map[string]string{}, portals: map[string]string{}}
}

func (g *blockGate) Gate(s *Session, msg pgproto3.FrontendMessage) *Rejection {
	g.mu.Lock()
	defer g.mu.Unlock()
	// Names are per connection: two sessions both have an unnamed statement.
	key := func(name string) string { return fmt.Sprintf("%d/%s", s.ID, name) }
	g.sawFrames = append(g.sawFrames, fmt.Sprintf("%T", msg))
	var sql string
	switch m := msg.(type) {
	case *pgproto3.Query:
		sql = m.String
	case *pgproto3.Parse:
		g.stmts[key(m.Name)] = m.Query
	case *pgproto3.Bind:
		g.portals[key(m.DestinationPortal)] = key(m.PreparedStatement)
	case *pgproto3.Execute:
		sql = g.stmts[g.portals[key(m.Portal)]]
	}
	if strings.Contains(sql, "BLOCKME") {
		g.rejected++
		return &Rejection{Code: "42501", Message: "tenantdb: blocked in test"}
	}
	return nil
}

func connectMode(t *testing.T, addr string, mode pgx.QueryExecMode) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://tester@%s/appdb?sslmode=disable", addr))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = mode
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect through proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func pgCode(err error) (code, severity string) {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code, pe.Severity
	}
	return "", ""
}

func query1(conn *pgx.Conn, sql string, args ...any) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out string
	err := conn.QueryRow(ctx, sql, args...).Scan(&out)
	return out, err
}

func TestGateRejectsSimpleQueryAndKeepsConnection(t *testing.T) {
	up, seen := fakeExtUpstream(t)
	addr := startProxy(t, up, newBlockGate())
	conn := connectMode(t, addr, pgx.QueryExecModeSimpleProtocol)

	_, err := query1(conn, "SELECT BLOCKME")
	if code, sev := pgCode(err); code != "42501" || sev != "ERROR" {
		t.Fatalf("err = %v (code %q severity %q), want ERROR 42501", err, code, sev)
	}
	if seen.contains("BLOCKME") {
		t.Fatal("rejected statement reached the database")
	}
	got, err := query1(conn, "SELECT fine")
	if err != nil || got != "ok:SELECT fine" {
		t.Fatalf("connection should stay usable: %q, %v", got, err)
	}
}

func TestGateRejectsExtendedBatchBeforeDatabaseSeesIt(t *testing.T) {
	up, seen := fakeExtUpstream(t)
	addr := startProxy(t, up, newBlockGate())
	conn := connectMode(t, addr, pgx.QueryExecModeExec) // Parse+Bind+Describe+Execute+Sync in one batch

	_, err := query1(conn, "SELECT BLOCKME, $1::text", "x")
	if code, sev := pgCode(err); code != "42501" || sev != "ERROR" {
		t.Fatalf("err = %v (code %q severity %q), want ERROR 42501", err, code, sev)
	}
	if seen.contains("BLOCKME") || seen.contains("bind") {
		t.Fatalf("part of a rejected batch reached the database: %v", seen.all())
	}
	got, err := query1(conn, "SELECT fine, $1::text", "y")
	if err != nil || got != "ok:SELECT fine, $1::text" {
		t.Fatalf("connection should stay usable after a rejected batch: %q, %v", got, err)
	}
}

func TestGatePassesPreparedStatementTraffic(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	gate := newBlockGate()
	addr := startProxy(t, up, gate)
	conn := connectMode(t, addr, pgx.QueryExecModeCacheStatement) // Parse+Describe+Sync, then Bind+Execute+Sync
	for i := 0; i < 3; i++ {
		got, err := query1(conn, "SELECT fine, $1::text", "z")
		if err != nil || got != "ok:SELECT fine, $1::text" {
			t.Fatalf("round %d: %q, %v", i, got, err)
		}
	}
	// A rejected statement through a cached-statement connection.
	_, err := query1(conn, "SELECT BLOCKME, $1::text", "z")
	if code, _ := pgCode(err); code != "42501" {
		t.Fatalf("err = %v", err)
	}
	if got, err := query1(conn, "SELECT fine, $1::text", "z"); err != nil || got == "" {
		t.Fatalf("after reject: %q, %v", got, err)
	}
}

func TestGateInsideTransactionIsFatalAndRollsBack(t *testing.T) {
	up, seen := fakeExtUpstream(t)
	addr := startProxy(t, up, newBlockGate())
	conn := connectMode(t, addr, pgx.QueryExecModeSimpleProtocol)

	if _, err := conn.Exec(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	_, err := query1(conn, "SELECT BLOCKME")
	if code, sev := pgCode(err); code != "42501" || sev != "FATAL" {
		t.Fatalf("err = %v (code %q severity %q), want FATAL 42501", err, code, sev)
	}
	if seen.contains("BLOCKME") {
		t.Fatal("rejected statement reached the database")
	}
	// The connection is gone, so the database rolls the transaction back.
	if _, err := query1(conn, "SELECT after"); err == nil {
		t.Fatal("connection should have been closed after a rejection inside a transaction")
	}
}

type panickyGate struct{ NopObserver }

func (panickyGate) Gate(*Session, pgproto3.FrontendMessage) *Rejection { panic("boom") }

func TestGatePanicFailsOpen(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxy(t, up, panickyGate{})
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec} {
		conn := connectMode(t, addr, mode)
		if got, err := query1(conn, "SELECT ok, $1::text", "a"); mode == pgx.QueryExecModeExec && (err != nil || got == "") {
			t.Fatalf("mode %v: %q, %v", mode, got, err)
		}
	}
}

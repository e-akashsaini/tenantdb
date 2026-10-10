package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// fakeUpstream is a minimal PostgreSQL server: no auth, answers every simple
// query with a single row containing "ok:<sql>".
func fakeUpstream(t *testing.T) string {
	t.Helper()
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
			go serveFake(c)
		}
	}()
	return ln.Addr().String()
}

func serveFake(c net.Conn) {
	defer c.Close()
	b := pgproto3.NewBackend(c, c)
	if _, err := b.ReceiveStartupMessage(); err != nil {
		return
	}
	b.Send(&pgproto3.AuthenticationOk{})
	b.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "16.0"})
	b.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	b.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}})
	b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if b.Flush() != nil {
		return
	}
	for {
		msg, err := b.Receive()
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			b.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("v"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}})
			b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("ok:" + m.String)}})
			b.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if b.Flush() != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		}
	}
}

type recorder struct {
	mu      sync.Mutex
	queries []string
	rows    int
}

func (r *recorder) OnFrontend(_ *Session, msg pgproto3.FrontendMessage) {
	if q, ok := msg.(*pgproto3.Query); ok {
		r.mu.Lock()
		r.queries = append(r.queries, q.String)
		r.mu.Unlock()
	}
}

func (r *recorder) OnBackend(_ *Session, msg pgproto3.BackendMessage) {
	if _, ok := msg.(*pgproto3.DataRow); ok {
		r.mu.Lock()
		r.rows++
		r.mu.Unlock()
	}
}

func startProxy(t *testing.T, upstream string, obs Observer) string {
	t.Helper()
	srv := New(Config{Listen: "127.0.0.1:0", Upstream: upstream, Observer: obs})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return srv.Addr().String()
}

func connect(t *testing.T, addr string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://tester@%s/appdb?sslmode=disable", addr))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect through proxy: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func TestRelaysQueriesAndObserverSeesThem(t *testing.T) {
	rec := &recorder{}
	addr := startProxy(t, fakeUpstream(t), rec)
	conn := connect(t, addr)

	var got string
	if err := conn.QueryRow(context.Background(), "SELECT 1 /* tenant=acme */").Scan(&got); err != nil {
		t.Fatalf("query: %v", err)
	}
	if want := "ok:SELECT 1 /* tenant=acme */"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.queries) != 1 || rec.queries[0] != "SELECT 1 /* tenant=acme */" {
		t.Fatalf("observer queries = %v", rec.queries)
	}
	if rec.rows != 1 {
		t.Fatalf("observer rows = %d, want 1", rec.rows)
	}
}

type panicky struct{ NopObserver }

func (panicky) OnFrontend(*Session, pgproto3.FrontendMessage) { panic("boom") }

func TestObserverPanicFailsOpen(t *testing.T) {
	addr := startProxy(t, fakeUpstream(t), panicky{})
	conn := connect(t, addr)

	var got string
	if err := conn.QueryRow(context.Background(), "SELECT 2").Scan(&got); err != nil {
		t.Fatalf("query should still succeed when the observer panics: %v", err)
	}
	if got != "ok:SELECT 2" {
		t.Fatalf("got %q", got)
	}
}

func TestSessionCarriesUserAndDatabase(t *testing.T) {
	seen := make(chan Session, 1)
	obs := sessionSpy{ch: seen}
	addr := startProxy(t, fakeUpstream(t), obs)
	conn := connect(t, addr)
	var s string
	if err := conn.QueryRow(context.Background(), "SELECT 3").Scan(&s); err != nil {
		t.Fatal(err)
	}
	select {
	case sess := <-seen:
		if sess.User != "tester" || sess.DB != "appdb" {
			t.Fatalf("session = %+v", sess)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("observer never saw a session")
	}
}

type sessionSpy struct {
	NopObserver
	ch chan Session
}

func (s sessionSpy) OnFrontend(sess *Session, msg pgproto3.FrontendMessage) {
	if _, ok := msg.(*pgproto3.Query); ok {
		select {
		case s.ch <- *sess:
		default:
		}
	}
}

func TestUpstreamDownReturnsErrorToClient(t *testing.T) {
	// Reserve a port, then close it so nothing is listening there.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()

	addr := startProxy(t, dead, nil)
	cfg, _ := pgx.ParseConfig(fmt.Sprintf("postgres://tester@%s/appdb?sslmode=disable", addr))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pgx.ConnectConfig(ctx, cfg); err == nil {
		t.Fatal("expected a connection error when upstream is down")
	}
}

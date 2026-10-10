package proxy

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// fakeTxUpstream is fakeUpstream except that BEGIN leaves the session in a
// transaction (ReadyForQuery 'T').
func fakeTxUpstream(t *testing.T) string {
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
			go func() {
				defer c.Close()
				b := pgproto3.NewBackend(c, c)
				if _, err := b.ReceiveStartupMessage(); err != nil {
					return
				}
				b.Send(&pgproto3.AuthenticationOk{})
				b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
				b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if b.Flush() != nil {
					return
				}
				for {
					msg, err := b.Receive()
					if err != nil {
						return
					}
					if q, ok := msg.(*pgproto3.Query); ok {
						b.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
						status := byte('I')
						if strings.EqualFold(q.String, "begin") {
							status = 'T'
						}
						b.Send(&pgproto3.ReadyForQuery{TxStatus: status})
						if b.Flush() != nil {
							return
						}
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func startServer(t *testing.T, cfg Config) (*Server, string, context.CancelFunc, chan struct{}) {
	t.Helper()
	cfg.Listen = "127.0.0.1:0"
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return srv, srv.Addr().String(), cancel, done
}

func TestDrainSendsIdleClientsAwayAndRefusesNewOnes(t *testing.T) {
	srv, addr, stop, done := startServer(t, Config{Upstream: fakeUpstream(t), DrainTimeout: 5 * time.Second})
	conn := connect(t, addr)
	if _, err := query1(conn, "select 1"); err != nil {
		t.Fatal(err)
	}
	if got := srv.Stats().ConnsActive.Load(); got != 1 {
		t.Fatalf("active = %d", got)
	}
	start := time.Now()
	stop()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("an idle client held the shutdown up")
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("an idle client should be sent away at once, not after the timeout: %v", time.Since(start))
	}
	_, err := query1(conn, "select 2")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "57P01" {
		// the client may see the closed socket first; either way it is no longer served
		if err == nil {
			t.Error("the connection survived the shutdown")
		}
	}
	if !srv.Draining() {
		t.Error("Draining() should say so")
	}
	if got := srv.Stats().ConnsDrained.Load(); got != 1 {
		t.Errorf("drained = %d", got)
	}
	if c, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		c.Close()
		t.Error("the listener should be closed")
	}
}

func TestDrainWaitsForABusyClientThenGivesUp(t *testing.T) {
	// the client sits inside a transaction: not idle, so it gets the whole timeout
	srv, addr, stop, done := startServer(t, Config{Upstream: fakeTxUpstream(t), DrainTimeout: 700 * time.Millisecond})
	cfg, _ := pgx.ParseConfig("postgres://tester@" + addr + "/appdb?sslmode=disable")
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), "begin"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the drain never ended")
	}
	el := time.Since(start)
	if el < 600*time.Millisecond {
		t.Errorf("a client in a transaction should have been given its time, got %v", el)
	}
	if got := srv.Stats().ConnsDrained.Load(); got != 0 {
		t.Errorf("a busy client is not 'drained' early: %d", got)
	}
}

func TestStatsCountWhatHappens(t *testing.T) {
	srv, addr, _, _ := startServer(t, Config{Upstream: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	cfg, _ := pgx.ParseConfig("postgres://tester@" + addr + "/appdb?sslmode=disable")
	if _, err := pgx.ConnectConfig(context.Background(), cfg); err == nil {
		t.Fatal("the upstream is not there")
	}
	// garbage instead of a startup packet
	c, _ := net.Dial("tcp", addr)
	c.Write([]byte("GET / HTTP/1.1\r\n\r\n"))
	c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (srv.Stats().UpstreamDialFailures.Load() == 0 || srv.Stats().StartupFailures.Load() == 0) {
		time.Sleep(10 * time.Millisecond)
	}
	st := srv.Stats()
	if st.ConnsTotal.Load() < 2 || st.UpstreamDialFailures.Load() != 1 || st.StartupFailures.Load() == 0 {
		t.Errorf("total=%d dial=%d startup=%d", st.ConnsTotal.Load(), st.UpstreamDialFailures.Load(), st.StartupFailures.Load())
	}
}

func TestStatsCountPanicsAndRejections(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	srv, addr, _, _ := startServer(t, Config{Upstream: up, Observer: newBlockGate()})
	conn := connectMode(t, addr, pgx.QueryExecModeSimpleProtocol)
	if _, err := query1(conn, "select 'BLOCKME'"); err == nil {
		t.Fatal("the test gate should have blocked this statement")
	}
	if srv.Stats().RejectedSoft.Load() != 1 {
		t.Errorf("soft rejections = %d", srv.Stats().RejectedSoft.Load())
	}
	srv2, addr2, _, _ := startServer(t, Config{Upstream: fakeUpstream(t), Observer: panicky{}})
	conn2 := connect(t, addr2)
	if _, err := query1(conn2, "select 1"); err != nil {
		t.Fatal(err)
	}
	if srv2.Stats().ObserverPanics.Load() == 0 {
		t.Error("panics in the observer must be counted")
	}
}

// fakeSlowUpstream answers "slow" queries after a delay.
func fakeSlowUpstream(t *testing.T, delay time.Duration) string {
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
			go func() {
				defer c.Close()
				b := pgproto3.NewBackend(c, c)
				if _, err := b.ReceiveStartupMessage(); err != nil {
					return
				}
				b.Send(&pgproto3.AuthenticationOk{})
				b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
				b.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
				b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if b.Flush() != nil {
					return
				}
				for {
					msg, err := b.Receive()
					if err != nil {
						return
					}
					if q, ok := msg.(*pgproto3.Query); ok {
						if strings.Contains(q.String, "slow") {
							time.Sleep(delay)
						}
						b.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("v"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}})
						b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("done")}})
						b.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
						b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
						if b.Flush() != nil {
							return
						}
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

// A query in flight is not idle, with or without a Gate: the drain must let it finish.
func TestDrainLetsARunningQueryFinish(t *testing.T) {
	for name, obs := range map[string]Observer{"observer only": NopObserver{}, "gate": newBlockGate()} {
		_, addr, stop, done := startServer(t, Config{Upstream: fakeSlowUpstream(t, 700*time.Millisecond), Observer: obs, DrainTimeout: 5 * time.Second})
		conn := connect(t, addr)
		type result struct {
			v   string
			err error
		}
		res := make(chan result, 1)
		go func() { v, err := query1(conn, "select 'slow'"); res <- result{v, err} }()
		time.Sleep(200 * time.Millisecond) // the query is on its way to the database
		stop()
		select {
		case r := <-res:
			if r.err != nil || r.v != "done" {
				t.Errorf("%s: the running query was cut off: %q %v", name, r.v, r.err)
			}
		case <-time.After(4 * time.Second):
			t.Fatalf("%s: the query never answered", name)
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("%s: the server did not stop once its clients were done", name)
		}
	}
}

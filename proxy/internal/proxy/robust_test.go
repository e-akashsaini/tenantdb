package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// What an untrusted client can do to the proxy at the protocol level.

// rawClient speaks the protocol to the proxy by hand, so a test can send what a
// well-behaved driver never would.
type rawClient struct {
	c net.Conn
	f *pgproto3.Frontend
}

func dialRaw(t *testing.T, addr string) *rawClient {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &rawClient{c: c, f: pgproto3.NewFrontend(c, c)}
}

// startup completes the handshake and reads up to the first ReadyForQuery.
func (r *rawClient) startup(t *testing.T) {
	t.Helper()
	r.f.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "tester", "database": "appdb"}})
	if err := r.f.Flush(); err != nil {
		t.Fatal(err)
	}
	r.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		m, err := r.f.Receive()
		if err != nil {
			t.Fatalf("startup: %v", err)
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			r.c.SetReadDeadline(time.Time{})
			return
		}
	}
}

// closedWithin reports whether the server closes the connection within d.
func (r *rawClient) closedWithin(d time.Duration) bool {
	r.c.SetReadDeadline(time.Now().Add(d))
	buf := make([]byte, 4096)
	for {
		if _, err := r.c.Read(buf); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return false
			}
			return true
		}
	}
}

func goroutines() int { return runtime.NumGoroutine() }

// settle waits for goroutines to drain back to near a baseline.
func settle(t *testing.T, base int, slack int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if goroutines() <= base+slack {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	t.Fatalf("goroutines did not drain: %d now, %d before\n%s", goroutines(), base, buf[:n])
}

func TestGarbageDoesNotCrashOrLeak(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up})
	base := goroutines()
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 200; i++ {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		junk := make([]byte, r.Intn(300))
		r.Read(junk)
		c.Write(junk)
		if i%3 == 0 {
			c.Write([]byte{0, 0, 0, 8, 4, 210, 22, 47}) // an SSLRequest, then more junk
			c.Write(junk)
		}
		c.Close()
	}
	// After a good handshake, then junk in place of messages.
	for i := 0; i < 50; i++ {
		rc := dialRaw(t, addr)
		rc.startup(t)
		junk := make([]byte, 1+r.Intn(500))
		r.Read(junk)
		rc.c.Write(junk)
		rc.c.Close()
	}
	settle(t, base, 5)
	conn := connect(t, addr)
	if got, err := query1(conn, "SELECT still_alive"); err != nil || got == "" {
		t.Fatalf("the proxy stopped serving after garbage: %q, %v", got, err)
	}
}

func TestTruncatedAndMalformedMessages(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up})
	base := goroutines()
	frames := map[string][]byte{
		"length smaller than the header": {'Q', 0, 0, 0, 2},
		"negative length":                {'Q', 0xff, 0xff, 0xff, 0xff},
		"truncated body":                 append([]byte{'Q', 0, 0, 0, 100}, bytes.Repeat([]byte{'x'}, 10)...),
		"unknown message type":           {'?', 0, 0, 0, 4},
		"string without terminator":      append([]byte{'Q', 0, 0, 0, 14}, []byte("SELECT 1 abc")...),
		"bind with absurd counts":        {'B', 0, 0, 0, 12, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff},
		"parse with absurd oid count":    {'P', 0, 0, 0, 10, 0, 0, 0xff, 0xff},
	}
	for name, frame := range frames {
		rc := dialRaw(t, addr)
		rc.startup(t)
		rc.c.Write(frame)
		if !rc.closedWithin(3 * time.Second) {
			// A server may also answer with an error; what matters is that it survives.
			t.Logf("%s: connection left open (fine, as long as the proxy survives)", name)
		}
		rc.c.Close()
	}
	settle(t, base, 5)
	if got, err := query1(connect(t, addr), "SELECT after_malformed"); err != nil || got == "" {
		t.Fatalf("the proxy stopped serving: %q, %v", got, err)
	}
}

func heapInUse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func TestAMessageThatClaimsToBeHugeIsRefusedWithoutBuffering(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up, MaxMessageBytes: 4 << 20})
	before := heapInUse()
	var clients []*rawClient
	for i := 0; i < 8; i++ { // eight connections each claiming a 1 GB message
		rc := dialRaw(t, addr)
		rc.startup(t)
		var hdr [5]byte
		hdr[0] = 'Q'
		binary.BigEndian.PutUint32(hdr[1:], 1<<30)
		rc.c.Write(hdr[:])
		clients = append(clients, rc)
	}
	for _, rc := range clients {
		if !rc.closedWithin(3 * time.Second) {
			t.Fatal("a connection claiming a 1 GB message was left open")
		}
	}
	if grew := int64(heapInUse()) - int64(before); grew > 64<<20 {
		t.Errorf("the proxy allocated %d MB for messages it should have refused", grew>>20)
	}
}

func TestMessagesUpToTheLimitStillPass(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up, MaxMessageBytes: 4 << 20})
	conn := connectMode(t, addr, pgx.QueryExecModeSimpleProtocol)
	big := "SELECT '" + string(bytes.Repeat([]byte{'a'}, 3<<20)) + "'"
	if _, err := query1(conn, big); err != nil {
		t.Fatalf("a 3 MB statement under a 4 MB limit: %v", err)
	}
}

func TestStartupMustArriveInTime(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up, StartupTimeout: 300 * time.Millisecond})
	base := goroutines()
	var conns []net.Conn
	for i := 0; i < 50; i++ { // slowloris: connect and say nothing, or half a startup packet
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			c.Write([]byte{0, 0, 0, 40, 0, 3})
		}
		conns = append(conns, c)
	}
	time.Sleep(1200 * time.Millisecond)
	for i, c := range conns {
		c.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := c.Read(make([]byte, 1)); err == nil {
			t.Fatalf("connection %d is still open", i)
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatalf("connection %d was never closed", i)
		}
		c.Close()
	}
	settle(t, base, 5)
}

func TestUnboundedPipeliningIsRefusedInBlockMode(t *testing.T) {
	up, seen := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up, Observer: newBlockGate(), MaxHeldBytes: 1 << 20})
	rc := dialRaw(t, addr)
	rc.startup(t)
	q := string(bytes.Repeat([]byte{'x'}, 100_000))
	for i := 0; i < 200; i++ { // never a Sync: the proxy has to hold all of it
		rc.f.Send(&pgproto3.Parse{Query: "SELECT '" + q + "'"})
	}
	rc.f.Flush()
	if !rc.closedWithin(5 * time.Second) {
		t.Fatal("the proxy kept buffering a pipeline that never syncs")
	}
	if len(seen.all()) != 0 {
		t.Errorf("held messages reached the database: %d", len(seen.all()))
	}
}

func TestConnectionStormLeavesNothingBehind(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up, Observer: newBlockGate()})
	base := goroutines()
	var wg sync.WaitGroup
	var failed atomic.Int32
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				failed.Add(1)
				return
			}
			defer c.Close()
			f := pgproto3.NewFrontend(c, c)
			f.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "u", "database": "d"}})
			f.Flush()
			c.SetReadDeadline(time.Now().Add(10 * time.Second))
			for {
				m, err := f.Receive()
				if err != nil {
					failed.Add(1)
					return
				}
				if _, ok := m.(*pgproto3.ReadyForQuery); ok {
					break
				}
			}
			switch i % 4 {
			case 0: // a query, then a clean goodbye
				f.Send(&pgproto3.Query{String: "SELECT 1"})
				f.Flush()
				for {
					if m, err := f.Receive(); err != nil || isRFQ(m) {
						break
					}
				}
				f.Send(&pgproto3.Terminate{})
				f.Flush()
			case 1: // vanish in the middle of a query
				f.Send(&pgproto3.Query{String: "SELECT 2"})
				f.Flush()
			case 2: // vanish in the middle of a batch the proxy is holding
				f.Send(&pgproto3.Parse{Query: "SELECT 3"})
				f.Send(&pgproto3.Bind{})
				f.Flush()
			case 3: // a refused statement, then vanish
				f.Send(&pgproto3.Query{String: "SELECT BLOCKME"})
				f.Flush()
			}
		}(i)
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Errorf("%d of 300 connections failed", failed.Load())
	}
	settle(t, base, 5)
	if got, err := query1(connect(t, addr), "SELECT after_the_storm"); err != nil || got == "" {
		t.Fatalf("%q, %v", got, err)
	}
}

func isRFQ(m pgproto3.BackendMessage) bool { _, ok := m.(*pgproto3.ReadyForQuery); return ok }

// A database that hangs up mid-result must not strand the client or a goroutine.
func TestUpstreamHangingUpMidQuery(t *testing.T) {
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
				b := pgproto3.NewBackend(c, c)
				if _, err := b.ReceiveStartupMessage(); err != nil {
					c.Close()
					return
				}
				b.Send(&pgproto3.AuthenticationOk{})
				b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				b.Flush()
				b.Receive() // the query
				c.Close()   // ... and drop dead
			}()
		}
	}()
	addr := startProxyCfg(t, Config{Upstream: ln.Addr().String(), Observer: newBlockGate()})
	base := goroutines()
	for i := 0; i < 20; i++ {
		rc := dialRaw(t, addr)
		rc.startup(t)
		rc.f.Send(&pgproto3.Query{String: "SELECT 1"})
		rc.f.Flush()
		if !rc.closedWithin(3 * time.Second) {
			t.Fatal("the client was left hanging after the database hung up")
		}
	}
	settle(t, base, 5)
}

// A client's cancel request has to reach the database.
func TestCancelRequestIsForwarded(t *testing.T) {
	cancels := make(chan pgproto3.CancelRequest, 4)
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
				m, err := b.ReceiveStartupMessage()
				if err != nil {
					return
				}
				if cr, ok := m.(*pgproto3.CancelRequest); ok {
					cancels <- *cr
				}
			}()
		}
	}()
	for _, gated := range []bool{false, true} {
		cfg := Config{Upstream: ln.Addr().String()}
		if gated {
			cfg.Observer = newBlockGate()
		}
		addr := startProxyCfg(t, cfg)
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		var buf bytes.Buffer
		binary.Write(&buf, binary.BigEndian, uint32(16))
		binary.Write(&buf, binary.BigEndian, uint32(80877102))
		binary.Write(&buf, binary.BigEndian, uint32(4242))
		binary.Write(&buf, binary.BigEndian, uint32(99))
		c.Write(buf.Bytes())
		c.Close()
		select {
		case cr := <-cancels:
			if cr.ProcessID != 4242 || !bytes.Equal(cr.SecretKey, []byte{0, 0, 0, 99}) {
				t.Errorf("gated=%v: cancel key changed on the way: %+v", gated, cr)
			}
		case <-time.After(3 * time.Second):
			t.Errorf("gated=%v: the cancel request never reached the database", gated)
		}
	}
}

func TestCopyInPassesThroughTheGate(t *testing.T) {
	up, seen := fakeExtUpstream(t)
	for _, gated := range []bool{false, true} {
		cfg := Config{Upstream: up}
		if gated {
			cfg.Observer = newBlockGate()
		}
		addr := startProxyCfg(t, cfg)
		rc := dialRaw(t, addr)
		rc.startup(t)
		rc.f.Send(&pgproto3.Query{String: "COPY t FROM STDIN"})
		rc.f.Flush()
		rc.c.SetReadDeadline(time.Now().Add(3 * time.Second))
		if m, err := rc.f.Receive(); err != nil {
			t.Fatalf("gated=%v: %v", gated, err)
		} else if _, ok := m.(*pgproto3.CopyInResponse); !ok {
			t.Fatalf("gated=%v: got %T, want CopyInResponse", gated, m)
		}
		for i := 0; i < 50; i++ {
			rc.f.Send(&pgproto3.CopyData{Data: []byte(fmt.Sprintf("row %d\n", i))})
		}
		rc.f.Send(&pgproto3.CopyDone{})
		rc.f.Flush()
		sawComplete := false
		for {
			m, err := rc.f.Receive()
			if err != nil {
				t.Fatalf("gated=%v: %v", gated, err)
			}
			if cc, ok := m.(*pgproto3.CommandComplete); ok && string(cc.CommandTag) == "COPY 50" {
				sawComplete = true
			}
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				break
			}
		}
		if !sawComplete {
			t.Errorf("gated=%v: the 50 rows did not all arrive", gated)
		}
		rc.c.Close()
	}
	_ = seen
}

// Many sessions at once, under -race.
func TestManyConcurrentSessionsThroughTheGate(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up, Observer: newBlockGate()})
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cfg, _ := pgx.ParseConfig(fmt.Sprintf("postgres://tester@%s/appdb?sslmode=disable", addr))
			cfg.DefaultQueryExecMode = []pgx.QueryExecMode{pgx.QueryExecModeSimpleProtocol, pgx.QueryExecModeExec, pgx.QueryExecModeCacheStatement}[i%3]
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				errs <- err
				return
			}
			defer conn.Close(ctx)
			for j := 0; j < 40; j++ {
				sql := fmt.Sprintf("SELECT %d, $1::text", j)
				if cfg.DefaultQueryExecMode == pgx.QueryExecModeSimpleProtocol {
					sql = fmt.Sprintf("SELECT %d", j)
				}
				var out string
				var qerr error
				if cfg.DefaultQueryExecMode == pgx.QueryExecModeSimpleProtocol {
					qerr = conn.QueryRow(ctx, sql).Scan(&out)
				} else {
					qerr = conn.QueryRow(ctx, sql, "x").Scan(&out)
				}
				if qerr != nil {
					errs <- fmt.Errorf("session %d query %d: %w", i, j, qerr)
					return
				}
				if j%7 == 0 { // a refusal in the middle must not disturb the session
					var o2 string
					if err := conn.QueryRow(ctx, "SELECT BLOCKME"+fmt.Sprint(j)).Scan(&o2); err == nil {
						errs <- fmt.Errorf("session %d: BLOCKME was not refused", i)
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	_ = io.EOF
}

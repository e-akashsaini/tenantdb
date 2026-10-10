// Package proxy is a transparent PostgreSQL wire-protocol proxy.
//
// It relays messages between client and upstream unchanged (authentication
// included) and shows each message to an Observer. It fails open: an observer
// that panics is ignored, and a query is only ever rejected when the Observer
// is also a Gate and says so explicitly (see Gate).
package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Config configures a Server.
type Config struct {
	Listen      string // address to accept clients on, e.g. ":6432"
	Upstream    string // host:port of the real PostgreSQL server
	UpstreamTLS bool   // dial the upstream with TLS (required by most hosted Postgres)
	// UpstreamTLSInsecure skips certificate verification. For local testing only.
	UpstreamTLSInsecure bool
	DialTimeout         time.Duration
	Observer            Observer
	Logger              *slog.Logger

	// TLS, if set, lets clients encrypt their connection to the proxy (the
	// server certificate and key). Without it the proxy answers a client's TLS
	// request with "no" and the client falls back to plaintext.
	TLS *tls.Config
	// RequireTLS refuses clients that do not encrypt. Needs TLS.
	RequireTLS bool
	// MaxMessageBytes caps the size of one message from a client (default 128 MiB).
	// The protocol allows a message to claim a length of a gigabyte before sending
	// a byte of it; without a cap a handful of connections could make the proxy
	// allocate memory for messages that never arrive.
	MaxMessageBytes int
	// MaxHeldBytes caps what a gated session may leave unsynced (default 64 MiB).
	// A Gate has to see a whole batch before any of it is forwarded, so the proxy
	// holds extended-protocol messages until Sync; a client that never syncs would
	// otherwise grow that without bound.
	MaxHeldBytes int
	// StartupTimeout is how long a client has to send its startup packet (default
	// 30s), so connections that say nothing cannot pile up.
	StartupTimeout time.Duration
	// GateFailClosed makes a Gate that panics reject the statement instead of
	// letting it through. For block mode: a statement nobody could check is not
	// one anybody should run.
	GateFailClosed bool
	// DrainTimeout is how long a stopping server waits for clients to finish before
	// closing their connections (default 30s; a negative value closes at once).
	// Idle clients are told to reconnect as soon as the drain starts.
	DrainTimeout time.Duration
}

// Stats are counters the server keeps while it runs.
type Stats struct {
	ConnsActive          atomic.Int64
	ConnsTotal           atomic.Uint64
	ConnsDrained         atomic.Uint64 // idle clients sent away when the server was stopping
	UpstreamDialFailures atomic.Uint64
	TLSHandshakeFailures atomic.Uint64
	StartupFailures      atomic.Uint64 // clients that never completed their startup (garbage, timeout)
	ObserverPanics       atomic.Uint64
	RejectedSoft         atomic.Uint64 // statements refused; the connection stayed usable
	RejectedHard         atomic.Uint64 // statements refused by closing the connection
}

// liveSession is what a draining server needs to send an idle client away.
type liveSession struct {
	gs     *gateState
	notify func()
}

// Server accepts client connections and relays them to the upstream.
type Server struct {
	cfg    Config
	log    *slog.Logger
	obs    Observer
	nextID atomic.Uint64

	mu       sync.Mutex
	ln       net.Listener
	conns    map[net.Conn]struct{}
	live     map[uint64]*liveSession
	wg       sync.WaitGroup
	stats    Stats
	draining atomic.Bool
}

// Stats returns the server's counters.
func (s *Server) Stats() *Stats { return &s.stats }

// Draining reports whether the server has been told to stop and is waiting for
// its clients to finish.
func (s *Server) Draining() bool { return s.draining.Load() }

// New returns a Server with defaults filled in.
func New(cfg Config) *Server {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = 128 << 20
	}
	if cfg.MaxHeldBytes <= 0 {
		cfg.MaxHeldBytes = 64 << 20
	}
	if cfg.StartupTimeout <= 0 {
		cfg.StartupTimeout = 30 * time.Second
	}
	if cfg.Observer == nil {
		cfg.Observer = NopObserver{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	return &Server{cfg: cfg, log: cfg.Logger, obs: cfg.Observer, conns: map[net.Conn]struct{}{}, live: map[uint64]*liveSession{}}
}

// Listen binds the listening socket. Use Addr to find the bound address.
func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	return nil
}

// Addr returns the listening address, or nil before Listen.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve accepts connections until ctx is cancelled or the listener fails.
func (s *Server) Serve(ctx context.Context) error {
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln == nil {
		return errors.New("proxy: Serve called before Listen")
	}
	go func() {
		<-ctx.Done()
		s.drain()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				s.wg.Wait()
				return nil
			}
			return err
		}
		s.track(c, true)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.track(c, false)
			s.handle(c)
		}()
	}
}

// Close stops accepting and closes every connection at once.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
}

// drain stops accepting, sends idle clients away with the error Postgres itself
// gives when an administrator stops it (they reconnect, to a new instance behind
// a load balancer), and gives the others DrainTimeout to finish before closing
// whatever is left.
func (s *Server) drain() {
	s.draining.Store(true)
	s.mu.Lock()
	if s.ln != nil {
		s.ln.Close()
	}
	s.mu.Unlock()
	deadline := time.Now().Add(s.cfg.DrainTimeout)
	for s.cfg.DrainTimeout > 0 && time.Now().Before(deadline) {
		s.mu.Lock()
		var idle []*liveSession
		for _, l := range s.live {
			if l.gs.idle() {
				idle = append(idle, l)
			}
		}
		active := len(s.live)
		s.mu.Unlock()
		for _, l := range idle {
			s.stats.ConnsDrained.Add(1)
			l.notify()
		}
		if active == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.Close()
}

func (s *Server) track(c net.Conn, add bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if add {
		s.conns[c] = struct{}{}
	} else {
		delete(s.conns, c)
		c.Close()
	}
}

func (s *Server) handle(client net.Conn) {
	s.stats.ConnsTotal.Add(1)
	s.stats.ConnsActive.Add(1)
	defer s.stats.ConnsActive.Add(-1)
	id := s.nextID.Add(1)
	log := s.log.With("session", id, "client", client.RemoteAddr().String())
	backend := s.newBackend(client)
	// A client must get on with its startup packet.
	_ = client.SetReadDeadline(time.Now().Add(s.cfg.StartupTimeout))

	var startup *pgproto3.StartupMessage
	secure := false
	for startup == nil {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.stats.StartupFailures.Add(1)
				log.Debug("startup failed", "err", err)
			}
			return
		}
		switch m := msg.(type) {
		case *pgproto3.StartupMessage:
			startup = m
		case *pgproto3.SSLRequest:
			if s.cfg.TLS == nil || secure {
				// No certificate configured (or already encrypted): the client falls back.
				if _, err := client.Write([]byte{'N'}); err != nil {
					return
				}
				continue
			}
			if _, err := client.Write([]byte{'S'}); err != nil {
				return
			}
			tc := tls.Server(client, s.cfg.TLS)
			_ = tc.SetDeadline(time.Now().Add(s.cfg.DialTimeout))
			if err := tc.Handshake(); err != nil {
				s.stats.TLSHandshakeFailures.Add(1)
				log.Debug("client TLS handshake failed", "err", err)
				return
			}
			_ = tc.SetDeadline(time.Time{})
			client, secure = tc, true
			backend = s.newBackend(tc)
		case *pgproto3.GSSEncRequest:
			if _, err := client.Write([]byte{'N'}); err != nil {
				return
			}
		case *pgproto3.CancelRequest:
			s.forwardCancel(log, m)
			return
		default:
			log.Debug("unexpected startup message", "type", fmt.Sprintf("%T", msg))
			return
		}
	}

	_ = client.SetReadDeadline(time.Time{})
	if s.cfg.RequireTLS && !secure {
		sendFatal(backend, "28000", "tenantdb: this server requires an encrypted (TLS) connection")
		return
	}

	up, err := s.dialUpstream()
	if err != nil {
		s.stats.UpstreamDialFailures.Add(1)
		log.Error("upstream dial failed", "err", err)
		sendFatal(backend, "08006", "tenantdb: cannot reach upstream database")
		return
	}
	s.track(up, true)
	defer s.track(up, false)

	sess := &Session{ID: id, User: startup.Parameters["user"], DB: startup.Parameters["database"], Params: startup.Parameters, gs: &gateState{}}
	log = log.With("user", sess.User, "db", sess.DB)
	log.Debug("session open")
	defer log.Debug("session closed")
	s.mu.Lock()
	s.live[id] = &liveSession{gs: sess.gs, notify: func() {
		_ = writeClient(sess, backend, &pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL",
			Code: "57P01", Message: "terminating connection due to administrator command"})
		client.Close()
	}}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.live, id)
		s.mu.Unlock()
	}()

	frontend := pgproto3.NewFrontend(up, up)
	frontend.Send(startup)
	if err := frontend.Flush(); err != nil {
		log.Error("upstream startup failed", "err", err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // client -> upstream
		defer wg.Done()
		defer up.Close()
		if gate, ok := s.obs.(Gate); ok {
			s.pumpClientGated(log, sess, gate, backend, frontend)
			return
		}
		s.pumpClient(log, sess, backend, frontend)
	}()
	go func() { // upstream -> client
		defer wg.Done()
		defer client.Close()
		s.pumpUpstream(log, sess, backend, frontend)
	}()
	wg.Wait()
}

func (s *Server) pumpClient(log *slog.Logger, sess *Session, backend *pgproto3.Backend, frontend *pgproto3.Frontend) {
	for {
		msg, err := backend.Receive()
		if err != nil {
			logRelayErr(log, "client read", err)
			return
		}
		switch msg.(type) {
		case *pgproto3.Query, *pgproto3.Sync, *pgproto3.FunctionCall:
			sess.gs.ready.Store(false) // work is in flight: not idle until the reply says so
		}
		s.safeObserve(log, func() { s.obs.OnFrontend(sess, msg) })
		frontend.Send(msg)
		if err := frontend.Flush(); err != nil {
			logRelayErr(log, "upstream write", err)
			return
		}
		if _, ok := msg.(*pgproto3.Terminate); ok {
			return
		}
	}
}

func (s *Server) pumpUpstream(log *slog.Logger, sess *Session, backend *pgproto3.Backend, frontend *pgproto3.Frontend) {
	for {
		msg, err := frontend.Receive()
		if err != nil {
			logRelayErr(log, "upstream read", err)
			return
		}
		// The client's 'p' messages are ambiguous (password, SASL initial, SASL
		// response); the backend decoder needs to know which one to expect next.
		switch msg.(type) {
		case *pgproto3.AuthenticationCleartextPassword:
			backend.SetAuthType(pgproto3.AuthTypeCleartextPassword)
		case *pgproto3.AuthenticationMD5Password:
			backend.SetAuthType(pgproto3.AuthTypeMD5Password)
		case *pgproto3.AuthenticationSASL:
			backend.SetAuthType(pgproto3.AuthTypeSASL)
		case *pgproto3.AuthenticationSASLContinue:
			backend.SetAuthType(pgproto3.AuthTypeSASLContinue)
		case *pgproto3.AuthenticationSASLFinal:
			backend.SetAuthType(pgproto3.AuthTypeSASLFinal)
		}
		if rfq, ok := msg.(*pgproto3.ReadyForQuery); ok {
			sess.gs.tx.Store(uint32(rfq.TxStatus))
			sess.gs.ready.Store(true)
		}
		s.safeObserve(log, func() { s.obs.OnBackend(sess, msg) })
		if err := writeClient(sess, backend, msg); err != nil {
			logRelayErr(log, "client write", err)
			return
		}
	}
}

// safeObserve keeps a panicking observer from killing the connection (fail open).
func (s *Server) safeObserve(log *slog.Logger, fn func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			s.stats.ObserverPanics.Add(1)
			log.Error("observer panic", "panic", r)
			panicked = true
		}
	}()
	fn()
	return false
}

func (s *Server) dialUpstream() (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", s.cfg.Upstream, s.cfg.DialTimeout)
	if err != nil {
		return nil, err
	}
	if !s.cfg.UpstreamTLS {
		return conn, nil
	}
	f := pgproto3.NewFrontend(conn, conn)
	f.Send(&pgproto3.SSLRequest{})
	if err := f.Flush(); err != nil {
		conn.Close()
		return nil, err
	}
	resp := make([]byte, 1)
	if _, err := io.ReadFull(conn, resp); err != nil {
		conn.Close()
		return nil, err
	}
	if resp[0] != 'S' {
		conn.Close()
		return nil, errors.New("upstream does not support TLS")
	}
	host, _, err := net.SplitHostPort(s.cfg.Upstream)
	if err != nil {
		conn.Close()
		return nil, err
	}
	tc := tls.Client(conn, &tls.Config{ServerName: host, InsecureSkipVerify: s.cfg.UpstreamTLSInsecure})
	if err := tc.Handshake(); err != nil {
		conn.Close()
		return nil, err
	}
	return tc, nil
}

// forwardCancel relays a query-cancel request. Cancel keys pass through
// untouched (BackendKeyData is relayed as-is), so the upstream recognises them.
func (s *Server) forwardCancel(log *slog.Logger, req *pgproto3.CancelRequest) {
	up, err := s.dialUpstream()
	if err != nil {
		log.Debug("cancel dial failed", "err", err)
		return
	}
	defer up.Close()
	f := pgproto3.NewFrontend(up, up)
	f.Send(req)
	if err := f.Flush(); err != nil {
		log.Debug("cancel write failed", "err", err)
		return
	}
	io.Copy(io.Discard, up) // upstream closes when done
}

func sendFatal(b *pgproto3.Backend, code, msg string) {
	b.Send(&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: msg})
	_ = b.Flush()
}

func logRelayErr(log *slog.Logger, what string, err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return
	}
	log.Debug(what, "err", err)
}

// newBackend reads client messages, refusing any longer than MaxMessageBytes.
func (s *Server) newBackend(c net.Conn) *pgproto3.Backend {
	b := pgproto3.NewBackend(c, c)
	b.SetMaxBodyLen(s.cfg.MaxMessageBytes)
	return b
}

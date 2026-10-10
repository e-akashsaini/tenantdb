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
}

// Server accepts client connections and relays them to the upstream.
type Server struct {
	cfg    Config
	log    *slog.Logger
	obs    Observer
	nextID atomic.Uint64

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// New returns a Server with defaults filled in.
func New(cfg Config) *Server {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.Observer == nil {
		cfg.Observer = NopObserver{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Server{cfg: cfg, log: cfg.Logger, obs: cfg.Observer, conns: map[net.Conn]struct{}{}}
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
		s.shutdown()
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

func (s *Server) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
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
	id := s.nextID.Add(1)
	log := s.log.With("session", id, "client", client.RemoteAddr().String())
	backend := pgproto3.NewBackend(client, client)

	var startup *pgproto3.StartupMessage
	for startup == nil {
		msg, err := backend.ReceiveStartupMessage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Debug("startup failed", "err", err)
			}
			return
		}
		switch m := msg.(type) {
		case *pgproto3.StartupMessage:
			startup = m
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			// The client-facing side is plaintext for now; the client falls back.
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

	up, err := s.dialUpstream()
	if err != nil {
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
func (s *Server) safeObserve(log *slog.Logger, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("observer panic", "panic", r)
		}
	}()
	fn()
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

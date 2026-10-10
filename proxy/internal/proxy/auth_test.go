package proxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// ---- SCRAM channel binding ----

// fakeSASLUpstream offers both SCRAM mechanisms, as a TLS-enabled PostgreSQL does,
// and then, if failWith is set, refuses the client's channel binding.
func fakeSASLUpstream(t *testing.T, failWith string) string {
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
				b.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"}})
				if b.Flush() != nil {
					return
				}
				if failWith != "" {
					b.SetAuthType(pgproto3.AuthTypeSASL)
					if _, err := b.Receive(); err != nil {
						return
					}
					b.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "08P01", Message: failWith})
					b.Flush()
					return
				}
				time.Sleep(300 * time.Millisecond)
			}()
		}
	}()
	return ln.Addr().String()
}

// offeredMechanisms connects like a client, over TLS or not, and returns the
// authentication methods the proxy passes on.
func offeredMechanisms(t *testing.T, addr string, useTLS bool) []string {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var conn net.Conn = c
	if useTLS {
		f := pgproto3.NewFrontend(c, c)
		f.Send(&pgproto3.SSLRequest{})
		if err := f.Flush(); err != nil {
			t.Fatal(err)
		}
		resp := make([]byte, 1)
		if _, err := c.Read(resp); err != nil || resp[0] != 'S' {
			t.Fatalf("TLS refused: %v %q", err, resp)
		}
		tc := tls.Client(c, &tls.Config{InsecureSkipVerify: true})
		if err := tc.Handshake(); err != nil {
			t.Fatal(err)
		}
		conn = tc
	}
	f := pgproto3.NewFrontend(conn, conn)
	f.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "u", "database": "d"}})
	if err := f.Flush(); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	msg, err := f.Receive()
	if err != nil {
		t.Fatal(err)
	}
	sasl, ok := msg.(*pgproto3.AuthenticationSASL)
	if !ok {
		t.Fatalf("got %T", msg)
	}
	return append([]string(nil), sasl.AuthMechanisms...)
}

func TestChannelBindingIsOfferedOnlyWhereItCanWork(t *testing.T) {
	both := []string{"SCRAM-SHA-256-PLUS", "SCRAM-SHA-256"}
	plain := []string{"SCRAM-SHA-256"}
	for _, c := range []struct {
		mode      string
		tlsClient bool
		want      []string
	}{
		{"auto", false, plain}, // libpq refuses PLUS on a connection it does not see as TLS
		{"auto", true, both},   // works when the proxy presents the database's certificate
		{"", true, both},       // auto is the default
		{"strip", true, plain},
		{"strip", false, plain},
		{"passthrough", false, both},
		{"passthrough", true, both},
	} {
		cfg := Config{Upstream: fakeSASLUpstream(t, "")}
		cfg.SCRAMChannelBinding = c.mode
		cfg.TLS = testTLSConfig(t)
		addr := startProxyCfg(t, cfg)
		got := offeredMechanisms(t, addr, c.tlsClient)
		if fmt.Sprint(got) != fmt.Sprint(c.want) {
			t.Errorf("mode %q, TLS client %v: offered %v, want %v", c.mode, c.tlsClient, got, c.want)
		}
	}
}

func TestRefusedChannelBindingIsCountedAndExplained(t *testing.T) {
	for _, msg := range []string{"SCRAM channel binding check failed", "SCRAM channel binding negotiation error"} {
		srv := New(Config{Listen: "127.0.0.1:0", Upstream: fakeSASLUpstream(t, msg), TLS: testTLSConfig(t)})
		if err := srv.Listen(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { srv.Serve(ctx); close(done) }()
		addr := srv.Addr().String()

		c, _ := net.Dial("tcp", addr)
		f := pgproto3.NewFrontend(c, c)
		f.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "u"}})
		f.Flush()
		f.Receive() // the SASL offer
		f.Send(&pgproto3.SASLInitialResponse{AuthMechanism: "SCRAM-SHA-256", Data: []byte("n,,n=u,r=abc")})
		f.Flush()
		c.SetReadDeadline(time.Now().Add(3 * time.Second))
		m, err := f.Receive()
		c.Close()
		if err != nil {
			t.Fatal(err)
		}
		if e, ok := m.(*pgproto3.ErrorResponse); !ok || e.Message != msg {
			t.Errorf("the database's error must reach the client unchanged: %+v", m)
		}
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && srv.Stats().ChannelBindingFailed.Load() == 0 {
			time.Sleep(10 * time.Millisecond)
		}
		if srv.Stats().ChannelBindingFailed.Load() != 1 {
			t.Errorf("%q was not counted", msg)
		}
		cancel()
		<-done
	}
}

// ---- mutual TLS ----

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert, key, pool}
}

func (ca *testCA) issue(t *testing.T, cn string, usage x509.ExtKeyUsage) tls.Certificate {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{usage}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

type certSpy struct {
	NopObserver
	mu    sync.Mutex
	certs []string
}

func (s *certSpy) OnFrontend(sess *Session, msg pgproto3.FrontendMessage) {
	if _, ok := msg.(*pgproto3.Query); ok {
		s.mu.Lock()
		s.certs = append(s.certs, sess.ClientCert)
		s.mu.Unlock()
	}
}

func connectWithCert(addr string, serverCA *testCA, cert *tls.Certificate) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://tester@%s/appdb?sslmode=require", addr))
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.TLSConfig = &tls.Config{RootCAs: serverCA.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12}
	if cert != nil {
		// send it whatever CAs the server says it accepts, as a hostile client would
		cfg.TLSConfig.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return cert, nil }
	}
	cfg.Fallbacks = nil
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return pgx.ConnectConfig(ctx, cfg)
}

func TestMutualTLS(t *testing.T) {
	serverCA, clientCA, otherCA := newTestCA(t, "server-ca"), newTestCA(t, "client-ca"), newTestCA(t, "other-ca")
	serverCert := serverCA.issue(t, "tenantdb", x509.ExtKeyUsageServerAuth)
	good := clientCA.issue(t, "billing-service", x509.ExtKeyUsageClientAuth)
	stranger := otherCA.issue(t, "intruder", x509.ExtKeyUsageClientAuth)

	for name, mode := range map[string]tls.ClientAuthType{"require": tls.RequireAndVerifyClientCert, "verify-if-given": tls.VerifyClientCertIfGiven} {
		spy := &certSpy{}
		srv := New(Config{Listen: "127.0.0.1:0", Upstream: fakeUpstream(t), Observer: spy,
			TLS: &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: mode, ClientCAs: clientCA.pool, MinVersion: tls.VersionTLS12}})
		if err := srv.Listen(); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { srv.Serve(ctx); close(done) }()
		addr := srv.Addr().String()

		conn, err := connectWithCert(addr, serverCA, &good)
		if err != nil {
			t.Fatalf("%s: a certificate from the client CA must be accepted: %v", name, err)
		}
		if _, err := query1(conn, "select 1"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		conn.Close(context.Background())
		spy.mu.Lock()
		if len(spy.certs) != 1 || spy.certs[0] != "CN=billing-service" {
			t.Errorf("%s: the session should carry the certificate's subject: %v", name, spy.certs)
		}
		spy.mu.Unlock()

		if c, err := connectWithCert(addr, serverCA, &stranger); err == nil {
			c.Close(context.Background())
			t.Errorf("%s: a certificate from another CA must be refused", name)
		}
		c, err := connectWithCert(addr, serverCA, nil)
		switch name {
		case "require":
			if err == nil {
				c.Close(context.Background())
				t.Error("require: a client with no certificate must be refused")
			}
		default:
			if err != nil {
				t.Errorf("verify-if-given: no certificate is allowed: %v", err)
			} else {
				c.Close(context.Background())
			}
		}
		if srv.Stats().TLSHandshakeFailures.Load() == 0 {
			t.Errorf("%s: refused handshakes should be counted", name)
		}
		cancel()
		<-done
	}
}

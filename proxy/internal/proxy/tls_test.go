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
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// testTLSConfig returns a server config with a throwaway self-signed certificate.
func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "tenantdb-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}
}

func startProxyCfg(t *testing.T, cfg Config) string {
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
	return srv.Addr().String()
}

func connectSSL(t *testing.T, addr, sslmode string) (*pgx.Conn, error) {
	t.Helper()
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://tester@%s/appdb?sslmode=%s", addr, sslmode))
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err == nil {
		t.Cleanup(func() { conn.Close(context.Background()) })
	}
	return conn, err
}

func isTLS(conn *pgx.Conn) bool {
	_, ok := conn.PgConn().Conn().(*tls.Conn)
	return ok
}

func TestClientsCanEncryptTheirConnection(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up, TLS: testTLSConfig(t)})

	conn, err := connectSSL(t, addr, "require")
	if err != nil {
		t.Fatalf("sslmode=require: %v", err)
	}
	if !isTLS(conn) {
		t.Fatal("connection is not encrypted")
	}
	if got, err := query1(conn, "SELECT secret"); err != nil || got != "ok:SELECT secret" {
		t.Fatalf("query over TLS: %q, %v", got, err)
	}

	if c, err := connectSSL(t, addr, "prefer"); err != nil || !isTLS(c) {
		t.Fatalf("sslmode=prefer should pick TLS when offered: %v", err)
	}
	// Not required, so plaintext clients still work.
	if c, err := connectSSL(t, addr, "disable"); err != nil || isTLS(c) {
		t.Fatalf("sslmode=disable: %v", err)
	}
}

func TestRequireTLSRefusesPlaintextClients(t *testing.T) {
	up, seen := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up, TLS: testTLSConfig(t), RequireTLS: true})

	_, err := connectSSL(t, addr, "disable")
	if err == nil {
		t.Fatal("a plaintext client must be refused")
	}
	if code, sev := pgCode(err); code != "28000" || sev != "FATAL" {
		t.Errorf("err = %v (code %q severity %q)", err, code, sev)
	}
	if len(seen.all()) != 0 {
		t.Errorf("a refused client reached the database: %v", seen.all())
	}
	conn, err := connectSSL(t, addr, "require")
	if err != nil {
		t.Fatalf("an encrypted client must be accepted: %v", err)
	}
	if got, err := query1(conn, "SELECT 1"); err != nil || got == "" {
		t.Fatalf("%q, %v", got, err)
	}
}

func TestWithoutACertificateTLSIsDeclined(t *testing.T) {
	up, _ := fakeExtUpstream(t)
	addr := startProxyCfg(t, Config{Upstream: up})
	if c, err := connectSSL(t, addr, "prefer"); err != nil || isTLS(c) {
		t.Fatalf("prefer should fall back to plaintext: %v", err)
	}
	if _, err := connectSSL(t, addr, "require"); err == nil {
		t.Fatal("require cannot be satisfied without a certificate")
	}
}

func TestGateThatPanicsFailsClosedWhenAsked(t *testing.T) {
	up, seen := fakeExtUpstream(t)
	closed := startProxyCfg(t, Config{Upstream: up, Observer: panickyGate{}, GateFailClosed: true})

	conn := connectMode(t, closed, pgx.QueryExecModeSimpleProtocol)
	_, err := query1(conn, "SELECT must_not_run")
	if code, _ := pgCode(err); code != "XX000" {
		t.Fatalf("err = %v, want XX000", err)
	}
	if seen.contains("must_not_run") {
		t.Fatal("a statement nobody could check reached the database")
	}

	ext := connectMode(t, closed, pgx.QueryExecModeExec)
	if _, err := query1(ext, "SELECT must_not_run, $1::text", "x"); err == nil {
		t.Fatal("extended-protocol statement should be refused too")
	}
	if seen.contains("must_not_run") {
		t.Fatal("a statement nobody could check reached the database")
	}
}

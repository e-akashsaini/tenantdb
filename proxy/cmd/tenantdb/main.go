// Command tenantdb runs the tenant-aware PostgreSQL proxy.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/akashsaini/tenantdb/internal/attrib"
	"github.com/akashsaini/tenantdb/internal/proxy"
)

func main() {
	var (
		listen       = flag.String("listen", env("TENANTDB_LISTEN", ":6432"), "address to accept clients on")
		upstream     = flag.String("upstream", env("TENANTDB_UPSTREAM", "127.0.0.1:5432"), "host:port of the PostgreSQL server")
		upTLS        = flag.Bool("upstream-tls", env("TENANTDB_UPSTREAM_TLS", "") == "1", "connect to the upstream with TLS")
		upTLSOff     = flag.Bool("upstream-tls-insecure", false, "skip upstream certificate verification (testing only)")
		tenantCols   = flag.String("tenant-columns", env("TENANTDB_TENANT_COLUMNS", "tenant_id"), "comma-separated column names that identify the tenant")
		keyCols      = flag.String("key-columns", env("TENANTDB_KEY_COLUMNS", "id"), "comma-separated column names that identify a row, used to learn which tenant owns it (empty disables the row cache)")
		cacheSize    = flag.Int("cache-size", 100000, "maximum rows remembered by the row-to-tenant cache")
		schemaDSN    = flag.String("schema-dsn", env("TENANTDB_SCHEMA_DSN", ""), "connection string of a read-only role used to read the schema and pre-warm the cache (prefer the TENANTDB_SCHEMA_DSN env var: flags show up in process lists)")
		schemaEvery  = flag.Duration("schema-refresh", 5*time.Minute, "how often to re-read the schema")
		tagVar       = flag.String("tag-var", env("TENANTDB_TAG_VAR", "tenantdb.tenant"), "session variable a connection sets to declare its tenant (SET tenantdb.tenant = 'acme', or startup option -c tenantdb.tenant=acme)")
		lookupRows   = flag.Bool("lookup", true, "resolve rows missing from the cache by asking the database (needs -schema-dsn)")
		strictFlag   = flag.String("strict", env("TENANTDB_STRICT", "off"), "what to do with statements that cannot be pinned to a tenant: off (just report sources), alert (flag violations in the log), block (refuse to forward them; needs -schema-dsn)")
		strictExempt = flag.String("strict-exempt", env("TENANTDB_STRICT_EXEMPT", ""), "comma-separated roles never held to the policy (migrations, admin tools)")
		strictScan   = flag.String("strict-allow-scan", env("TENANTDB_STRICT_ALLOW_SCAN", ""), "comma-separated roles allowed to read across all tenants (reports, analytics)")
		cacheTTL     = flag.Duration("cache-ttl", time.Hour, "how long a cached row owner is trusted (0: until evicted); bounds staleness after deletes the proxy did not see")
		tlsCert      = flag.String("tls-cert", env("TENANTDB_TLS_CERT", ""), "server certificate (PEM) so clients can encrypt their connection to the proxy; re-read when the file changes")
		tlsKey       = flag.String("tls-key", env("TENANTDB_TLS_KEY", ""), "private key (PEM) for -tls-cert")
		requireTLS   = flag.Bool("require-tls", env("TENANTDB_REQUIRE_TLS", "") == "1", "refuse clients that do not use TLS (needs -tls-cert)")
		onError      = flag.String("strict-on-error", env("TENANTDB_STRICT_ON_ERROR", "block"), "in -strict=block, what to do with a statement the policy check itself failed on (a bug): block it, or allow it")
		prewarm      = flag.Bool("prewarm", true, "fill the row cache from the database at startup (needs -schema-dsn)")
		debug        = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	mode, err := attrib.ParseStrictMode(*strictFlag)
	if err != nil {
		log.Error("bad -strict", "err", err)
		os.Exit(2)
	}
	if mode != attrib.StrictOff && *schemaDSN == "" {
		// Without the schema nothing can be told apart from "unknown", so the
		// policy would flag (or block) everything.
		log.Error("-strict needs -schema-dsn: the policy cannot tell tenant tables from shared ones without the schema")
		os.Exit(2)
	}

	if *onError != "block" && *onError != "allow" {
		log.Error("bad -strict-on-error (want block or allow)", "value", *onError)
		os.Exit(2)
	}
	var tlsCfg *tls.Config
	if *tlsCert != "" || *tlsKey != "" {
		if *tlsCert == "" || *tlsKey == "" {
			log.Error("-tls-cert and -tls-key go together")
			os.Exit(2)
		}
		rl := &certReloader{certFile: *tlsCert, keyFile: *tlsKey}
		if _, err := rl.get(nil); err != nil {
			log.Error("cannot load TLS certificate", "err", err)
			os.Exit(2)
		}
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: rl.get}
	}
	if *requireTLS && tlsCfg == nil {
		log.Error("-require-tls needs -tls-cert and -tls-key")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var catalog *attrib.Catalog
	if *schemaDSN != "" {
		catalog = attrib.NewCatalog(*schemaDSN)
		lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if err := catalog.Load(lctx); err != nil {
			// Fail open: the proxy still relays and attributes without the schema.
			if mode == attrib.StrictBlock {
				// Failing open here would silently allow everything; failing
				// closed would block everything. Neither is a decision to make
				// for the operator.
				log.Error("schema load failed and -strict=block needs it; refusing to start", "err", err)
				os.Exit(1)
			}
			log.Error("schema load failed; running without catalog", "err", err)
		} else {
			log.Info("schema loaded")
			if blind := catalog.BlindTables(); len(blind) > 0 {
				log.Warn("row-level security hides rows from the schema role: row lookups and cache pre-warming skip these tables, so their statements fall back to unknown; give the role BYPASSRLS (read-only) to fix",
					"tables", blind)
			}
		}
		cancel()
		go catalog.Refresh(ctx, *schemaEvery, func(err error) { log.Warn("schema refresh failed", "err", err) })
	}

	var lookup *attrib.Lookup
	if catalog != nil && *lookupRows {
		var err error
		if lookup, err = attrib.NewLookup(ctx, *schemaDSN); err != nil {
			log.Error("lookup disabled", "err", err)
		} else {
			defer lookup.Close()
		}
	}

	attributor := attrib.New(attrib.Options{
		Catalog: catalog,
		Lookup:  lookup,
		TagVar:  *tagVar,
		Strict: attrib.StrictOptions{
			Mode:      mode,
			Exempt:    strings.Split(*strictExempt, ","),
			AllowScan: strings.Split(*strictScan, ","),
		},
		TenantColumns: strings.Split(*tenantCols, ","),
		KeyColumns:    strings.Split(*keyCols, ","),
		CacheSize:     *cacheSize,
		CacheTTL:      *cacheTTL,
	}, func(e attrib.Event) {
		attrs := []any{"session", e.SessionID, "user", e.User, "db", e.DB,
			"tenants", e.Tenants, "source", e.Source, "scan", e.Scan, "conflict", e.Conflict, "sql", e.SQL}
		switch {
		case e.Blocked:
			log.Warn("blocked", append(attrs, "violation", e.Violation)...)
		case e.Violation != "":
			log.Warn("violation", append(attrs, "violation", e.Violation)...)
		default:
			log.Info("query", attrs...)
		}
	})

	srv := proxy.New(proxy.Config{
		Observer:            attributor.Observer(),
		TLS:                 tlsCfg,
		RequireTLS:          *requireTLS,
		GateFailClosed:      mode == attrib.StrictBlock && *onError == "block",
		Listen:              *listen,
		Upstream:            *upstream,
		UpstreamTLS:         *upTLS,
		UpstreamTLSInsecure: *upTLSOff,
		Logger:              log,
	})
	if err := srv.Listen(); err != nil {
		log.Error("listen failed", "err", err)
		os.Exit(1)
	}
	log.Info("tenantdb proxy listening", "addr", srv.Addr().String(), "upstream", *upstream, "upstream_tls", *upTLS)

	if catalog != nil && *prewarm {
		// Background: clients are served immediately while the cache fills.
		go func() {
			if err := attributor.Warm(ctx, *schemaDSN, *cacheSize, log); err != nil {
				log.Error("cache warm failed", "err", err)
			}
		}()
	}
	if err := srv.Serve(ctx); err != nil {
		log.Error("serve failed", "err", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// certReloader serves a certificate and picks up a renewed one without a
// restart: the files are re-read when their modification time changes.
type certReloader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cert    *tls.Certificate
	modTime time.Time
	checked time.Time
}

func (r *certReloader) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cert != nil && time.Since(r.checked) < 30*time.Second {
		return r.cert, nil
	}
	r.checked = time.Now()
	fi, err := os.Stat(r.certFile)
	if err == nil && r.cert != nil && fi.ModTime().Equal(r.modTime) {
		return r.cert, nil
	}
	c, lerr := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if lerr != nil {
		if r.cert != nil {
			return r.cert, nil // keep serving the last good certificate
		}
		return nil, lerr
	}
	r.cert = &c
	if fi != nil {
		r.modTime = fi.ModTime()
	}
	return r.cert, nil
}

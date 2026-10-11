// Command tenantdb runs the tenant-aware PostgreSQL proxy.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/e-akashsaini/tenantdb/internal/attrib"
	"github.com/e-akashsaini/tenantdb/internal/proxy"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "analyze" {
		os.Exit(runAnalyze(os.Args[2:]))
	}
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck(os.Args[2:]))
	}
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
		clientCA     = flag.String("tls-client-ca", env("TENANTDB_TLS_CLIENT_CA", ""), "CA bundle (PEM) to verify client certificates against (mutual TLS); clients must then present a certificate it signed. Re-read when the file changes")
		clientAuth   = flag.String("tls-client-auth", env("TENANTDB_TLS_CLIENT_AUTH", "require"), "with -tls-client-ca: require (a certificate is mandatory) or verify-if-given (one is checked only if the client sends it)")
		requireTLS   = flag.Bool("require-tls", env("TENANTDB_REQUIRE_TLS", "") == "1", "refuse clients that do not use TLS (needs -tls-cert)")
		onError      = flag.String("strict-on-error", env("TENANTDB_STRICT_ON_ERROR", "block"), "in -strict=block, what to do with a statement the policy check itself failed on (a bug): block it, or allow it")
		prewarm      = flag.Bool("prewarm", true, "fill the row cache from the database at startup (needs -schema-dsn)")
		debug        = flag.Bool("debug", false, "verbose logging (same as -log-level=debug)")
		logFormat    = flag.String("log-format", env("TENANTDB_LOG_FORMAT", "text"), "log format: text or json")
		logLevel     = flag.String("log-level", env("TENANTDB_LOG_LEVEL", "info"), "log level: debug, info, warn or error")
		logSQL       = flag.String("log-sql", env("TENANTDB_LOG_SQL", "normalized"), "how statements appear in the log: normalized (literals replaced by $1, $2: no data values in the log), full (as sent), none")
		adminAddr    = flag.String("admin-addr", env("TENANTDB_ADMIN_ADDR", ""), "address for /metrics, /healthz, /readyz and /version, e.g. 127.0.0.1:9187 (off if empty; it has no authentication: keep it on an internal interface)")
		adminPprof   = flag.Bool("admin-pprof", false, "also serve /debug/pprof on the admin address")
		drainTimeout = flag.Duration("shutdown-timeout", 30*time.Second, "on SIGTERM/SIGINT: how long clients get to finish before their connections are closed (idle ones are sent away at once; a second signal stops immediately)")
		configFile   = flag.String("config", env("TENANTDB_CONFIG", ""), "file of `name: value` settings, one per flag; the command line overrides it, it overrides environment variables")
		cacheVerify  = flag.Float64("cache-verify", 0, "fraction (0 to 1) of cache hits re-checked against the database in the background; a cached owner that no longer matches is evicted and reported. Bounds how long a missed delete can mislead (needs -schema-dsn; 0 is off)")
		scramCB      = flag.String("scram-channel-binding", env("TENANTDB_SCRAM_CHANNEL_BINDING", "auto"), "SCRAM-SHA-256-PLUS (channel binding) cannot pass through a proxy that ends TLS. auto: hide it from clients not using TLS to the proxy, offer it to those that are (works if the proxy has the database's certificate); strip: never offer it; passthrough: always offer it")
		showVersion  = flag.Bool("version", false, "print the version and exit")
	)
	flag.Parse()

	ver, commit, goVer := buildInfo()
	if *showVersion {
		fmt.Println("tenantdb", ver, "(commit "+orNone(commit)+", "+goVer+")")
		return
	}
	var fromFile map[string]string
	if *configFile != "" {
		vals, order, err := parseConfig(*configFile)
		if err == nil {
			err = applyConfig(flag.CommandLine, vals, order)
		}
		fromFile = vals
		if err != nil {
			fmt.Fprintln(os.Stderr, "tenantdb: config:", err)
			os.Exit(2)
		}
	}
	log, err := newLogger(os.Stderr, *logFormat, *logLevel, *debug)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenantdb:", err)
		os.Exit(2)
	}
	if *logSQL != "normalized" && *logSQL != "full" && *logSQL != "none" {
		log.Error("bad -log-sql (want normalized, full or none)", "value", *logSQL)
		os.Exit(2)
	}
	if *configFile != "" {
		log.Info("config file applied", "file", *configFile)
		if fi, err := os.Stat(*configFile); err == nil && fi.Mode().Perm()&0o007 != 0 && fromFile["schema-dsn"] != "" {
			log.Warn("the config file is readable by everyone and holds a database connection string", "file", *configFile)
		}
	}

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

	if *cacheVerify < 0 || *cacheVerify > 1 {
		log.Error("bad -cache-verify (want a fraction from 0 to 1)", "value", *cacheVerify)
		os.Exit(2)
	}
	if *scramCB != "auto" && *scramCB != "strip" && *scramCB != "passthrough" {
		log.Error("bad -scram-channel-binding (want auto, strip or passthrough)", "value", *scramCB)
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
		if *clientCA != "" {
			if *clientAuth != "require" && *clientAuth != "verify-if-given" {
				log.Error("bad -tls-client-auth (want require or verify-if-given)", "value", *clientAuth)
				os.Exit(2)
			}
			cr := &caReloader{file: *clientCA}
			if _, err := cr.pool(); err != nil {
				log.Error("cannot load the client CA bundle", "err", err)
				os.Exit(2)
			}
			mode := tls.RequireAndVerifyClientCert
			if *clientAuth == "verify-if-given" {
				mode = tls.VerifyClientCertIfGiven
			}
			base := tlsCfg
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12, GetCertificate: rl.get,
				GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
					pool, err := cr.pool()
					if err != nil {
						return nil, err
					}
					c := base.Clone()
					c.ClientAuth, c.ClientCAs = mode, pool
					return c, nil
				}}
			log.Info("mutual TLS: client certificates are verified", "ca", *clientCA, "mode", *clientAuth)
		}
	} else if *clientCA != "" {
		log.Error("-tls-client-ca needs -tls-cert and -tls-key")
		os.Exit(2)
	}
	if *requireTLS && tlsCfg == nil {
		log.Error("-require-tls needs -tls-cert and -tls-key")
		os.Exit(2)
	}

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)

	var catalog *attrib.Catalog
	if *schemaDSN != "" {
		catalog = attrib.NewCatalog(*schemaDSN)
		catalog.TenantColumns = strings.Split(*tenantCols, ",") // foreign keys are verified against it
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

	o := newObservability(ver, commit, goVer, string(mode))
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
		CacheVerify:   *cacheVerify,
		OnStale: func(table, col, cached string, actual []string) {
			log.Warn("a cached row owner no longer matches the database and was evicted", "table", table, "column", col, "cached", cached, "actual", actual)
		},
	}, func(e attrib.Event) {
		o.event(e)
		attrs := []any{"session", e.SessionID, "user", e.User, "db", e.DB,
			"tenants", e.Tenants, "source", e.Source, "scan", e.Scan, "conflict", e.Conflict}
		if q := sqlForLog(*logSQL, e.SQL); q != "" {
			attrs = append(attrs, "sql", q)
		}
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
		Observer:            o.wrap(attributor.Observer()),
		DrainTimeout:        *drainTimeout,
		SCRAMChannelBinding: *scramCB,
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
	o.bind(srv, attributor, catalog)
	log.Info("tenantdb proxy listening", "version", ver, "addr", srv.Addr().String(), "upstream", *upstream,
		"upstream_tls", *upTLS, "strict", mode, "log_sql", *logSQL)
	if *adminAddr != "" {
		adm := &adminServer{obs: o, srv: srv, catalog: catalog, upstream: *upstream, needs: mode != attrib.StrictOff,
			ver: map[string]string{"version": ver, "commit": commit, "go": goVer}}
		addr, err := adm.serve(ctx, *adminAddr, *adminPprof)
		if err != nil {
			log.Error("admin listener failed", "err", err)
			os.Exit(1)
		}
		log.Info("admin endpoints listening", "addr", addr.String())
		if !isLoopback(addr) {
			log.Warn("the admin endpoints have no authentication and are not on a loopback address: restrict who can reach them", "addr", addr.String())
		}
	}
	go func() {
		stopping := false
		for sig := range sigs {
			switch sig {
			case syscall.SIGHUP:
				if catalog != nil {
					log.Info("SIGHUP: re-reading the schema")
					catalog.ReloadAsync()
				}
			default:
				if stopping {
					log.Warn("second signal: closing every connection now")
					srv.Close()
					return
				}
				stopping = true
				log.Info("stopping: no new connections; idle clients are sent away, the others have time to finish",
					"signal", sig.String(), "timeout", *drainTimeout)
				stop()
			}
		}
	}()

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
	log.Info("stopped")
}

func orNone(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
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

// caReloader serves the CA pool clients' certificates are checked against, and
// picks up a changed bundle without a restart.
type caReloader struct {
	file string

	mu      sync.Mutex
	p       *x509.CertPool
	modTime time.Time
	checked time.Time
}

func (r *caReloader) pool() (*x509.CertPool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.p != nil && time.Since(r.checked) < 10*time.Second {
		return r.p, nil
	}
	r.checked = time.Now()
	fi, err := os.Stat(r.file)
	if err == nil && r.p != nil && fi.ModTime().Equal(r.modTime) {
		return r.p, nil
	}
	pem, err := os.ReadFile(r.file)
	if err == nil {
		p := x509.NewCertPool()
		if p.AppendCertsFromPEM(pem) {
			r.p = p
			if fi != nil {
				r.modTime = fi.ModTime()
			}
			return r.p, nil
		}
		err = fmt.Errorf("%s holds no PEM certificates", r.file)
	}
	if r.p != nil {
		return r.p, nil // keep the last good bundle
	}
	return nil, err
}

// Command tenantdb runs the tenant-aware PostgreSQL proxy.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
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

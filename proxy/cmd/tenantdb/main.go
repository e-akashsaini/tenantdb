// Command tenantdb runs the tenant-aware PostgreSQL proxy.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	"github.com/akashsaini/tenantdb/internal/attrib"
	"github.com/akashsaini/tenantdb/internal/proxy"
)

func main() {
	var (
		listen     = flag.String("listen", env("TENANTDB_LISTEN", ":6432"), "address to accept clients on")
		upstream   = flag.String("upstream", env("TENANTDB_UPSTREAM", "127.0.0.1:5432"), "host:port of the PostgreSQL server")
		upTLS      = flag.Bool("upstream-tls", env("TENANTDB_UPSTREAM_TLS", "") == "1", "connect to the upstream with TLS")
		upTLSOff   = flag.Bool("upstream-tls-insecure", false, "skip upstream certificate verification (testing only)")
		tenantCols = flag.String("tenant-columns", env("TENANTDB_TENANT_COLUMNS", "tenant_id"), "comma-separated column names that identify the tenant")
		keyCols    = flag.String("key-columns", env("TENANTDB_KEY_COLUMNS", "id"), "comma-separated column names that identify a row, used to learn which tenant owns it (empty disables the row cache)")
		cacheSize  = flag.Int("cache-size", 100000, "maximum rows remembered by the row-to-tenant cache")
		debug      = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	attributor := attrib.New(attrib.Options{
		TenantColumns: strings.Split(*tenantCols, ","),
		KeyColumns:    strings.Split(*keyCols, ","),
		CacheSize:     *cacheSize,
	}, func(e attrib.Event) {
		log.Info("query", "session", e.SessionID, "user", e.User, "db", e.DB,
			"tenants", e.Tenants, "source", e.Source, "conflict", e.Conflict, "sql", e.SQL)
	})

	srv := proxy.New(proxy.Config{
		Observer:            attributor,
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
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

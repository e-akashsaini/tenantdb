package main

import (
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/e-akashsaini/tenantdb/internal/attrib"
	"github.com/e-akashsaini/tenantdb/internal/metrics"
	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// version is set at build time: go build -ldflags "-X main.version=v1.2.3".
var version = "dev"

// commitStamp is the commit, set at build time when git metadata is not available:
// go build -ldflags "-X main.commitStamp=$SHA".
var commitStamp string

// buildInfo describes this binary: its version, the commit it was built from and
// the Go toolchain.
func buildInfo() (ver, commit, goVer string) {
	ver, goVer = version, runtime.Version()
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				commit = s.Value
				if len(commit) > 12 {
					commit = commit[:12]
				}
			case "vcs.modified":
				if s.Value == "true" && commit != "" {
					commit += "-dirty"
				}
			}
		}
	}
	if commit == "" && commitStamp != "" {
		commit = commitStamp // a build without git metadata (the container image)
		if len(commit) > 12 {
			commit = commit[:12]
		}
	}
	return
}

// newLogger builds the logger: text or JSON, at the chosen level.
func newLogger(w io.Writer, format, level string, debugFlag bool) (*slog.Logger, error) {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "info", "":
		lv = slog.LevelInfo
	case "warn", "warning":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		return nil, fmt.Errorf("bad -log-level %q (want debug, info, warn or error)", level)
	}
	if debugFlag {
		lv = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: lv}
	switch strings.ToLower(format) {
	case "text", "":
		return slog.New(slog.NewTextHandler(w, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(w, opts)), nil
	}
	return nil, fmt.Errorf("bad -log-format %q (want text or json)", format)
}

const maxLoggedSQL = 8192

// sqlForLog prepares a statement for the log. Statements carry the values the
// application sends: names, emails, tokens. By default they are logged with
// every literal replaced by a placeholder (the tenants the proxy found are
// logged separately); "full" logs them as sent, "none" leaves them out.
func sqlForLog(mode, sql string) string {
	switch mode {
	case "none":
		return ""
	case "full":
	default:
		n, err := attrib.Normalize(sql)
		if err != nil {
			return "<statement could not be normalized>"
		}
		sql = n
	}
	if len(sql) > maxLoggedSQL {
		sql = sql[:maxLoggedSQL] + "…"
	}
	return sql
}

// observability holds the metrics and the event logger.
type observability struct {
	reg        *metrics.Registry
	statements *metrics.CounterVec
	violations *metrics.CounterVec
	conflicts  *metrics.Counter
	unparsed   *metrics.Counter
	scans      *metrics.Counter
	analysis   *metrics.Histogram
}

func newObservability(ver, commit, goVer, mode string) *observability {
	r := metrics.New()
	o := &observability{reg: r}
	r.NewInfo("tenantdb_build_info", "The version of this binary.", []string{"version", "commit", "go", "strict"}, []string{ver, commit, goVer, mode})
	r.NewGaugeFunc("tenantdb_start_time_seconds", "When the process started (unix time).", func() float64 { return float64(started.Unix()) })
	o.statements = r.NewCounterVec("tenantdb_statements_total", "Statements attributed, by how their tenant was determined.", "source")
	o.violations = r.NewCounterVec("tenantdb_violations_total", "Statements the strict policy objected to, by kind and what was done.", "violation", "action")
	o.conflicts = r.NewCounter("tenantdb_conflicts_total", "Statements that belong to a different tenant than their connection or transaction declared.")
	o.unparsed = r.NewCounter("tenantdb_unparsed_statements_total", "Statements that could not be analysed.")
	o.scans = r.NewCounter("tenantdb_scans_total", "Statements that read tenant data with nothing restricting them.")
	o.analysis = r.NewHistogram("tenantdb_analysis_seconds", "Time spent analysing one client message before it is forwarded.",
		[]float64{0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.05, 0.25, 1})
	return o
}

var started = time.Now()

// event counts one attributed statement.
func (o *observability) event(e attrib.Event) {
	o.statements.With(string(e.Source)).Inc()
	if e.Conflict {
		o.conflicts.Inc()
	}
	if e.Unparsed {
		o.unparsed.Inc()
	}
	if e.Scan {
		o.scans.Inc()
	}
	if e.Violation != "" {
		action := "alerted"
		if e.Blocked {
			action = "blocked"
		}
		o.violations.With(e.Violation, action).Inc()
	}
}

// bind registers the metrics that read other components when scraped.
func (o *observability) bind(srv *proxy.Server, a *attrib.Attributor, cat *attrib.Catalog) {
	r, st := o.reg, srv.Stats()
	f := func(n interface{ Load() uint64 }) func() float64 { return func() float64 { return float64(n.Load()) } }
	r.NewGaugeFunc("tenantdb_connections_active", "Client connections open now.", func() float64 { return float64(st.ConnsActive.Load()) })
	r.NewCounterFunc("tenantdb_connections_total", "Client connections accepted.", f(&st.ConnsTotal))
	r.NewCounterFunc("tenantdb_connections_drained_total", "Idle clients sent away because the server was stopping.", f(&st.ConnsDrained))
	r.NewCounterFunc("tenantdb_upstream_dial_failures_total", "Times the database could not be reached for a client.", f(&st.UpstreamDialFailures))
	r.NewCounterFunc("tenantdb_tls_handshake_failures_total", "Client TLS handshakes that failed.", f(&st.TLSHandshakeFailures))
	r.NewCounterFunc("tenantdb_startup_failures_total", "Clients that sent garbage or never finished their startup.", f(&st.StartupFailures))
	r.NewCounterFunc("tenantdb_scram_channel_binding_failures_total", "Clients whose SCRAM channel binding the database refused (it cannot pass through a proxy that ends TLS).", f(&st.ChannelBindingFailed))
	r.NewCounterFunc("tenantdb_observer_panics_total", "Panics in statement analysis (the proxy fails open unless block mode says otherwise).", f(&st.ObserverPanics))
	r.NewCounterFunc("tenantdb_rejected_statements_soft_total", "Statements refused with an error; the connection stayed usable.", f(&st.RejectedSoft))
	r.NewCounterFunc("tenantdb_rejected_statements_hard_total", "Statements refused by closing the connection (it was not safe to fake an abort).", f(&st.RejectedHard))
	r.NewGaugeFunc("tenantdb_draining", "1 while the server is stopping and waiting for clients to finish.", func() float64 {
		if srv.Draining() {
			return 1
		}
		return 0
	})
	r.NewGaugeFunc("tenantdb_cache_entries", "Rows in the row-to-tenant cache.", func() float64 { return float64(a.Stats().CacheEntries) })
	r.NewGaugeFunc("tenantdb_credential_cache_entries", "Entries in the credential-lookup cache.", func() float64 { return float64(a.Stats().BridgeEntries) })
	r.NewCounterFunc("tenantdb_cache_hits_total", "Row owners answered from the cache.", func() float64 { return float64(a.Stats().CacheHits) })
	r.NewCounterFunc("tenantdb_cache_misses_total", "Row owners the cache did not know.", func() float64 { return float64(a.Stats().CacheMisses) })
	r.NewCounterFunc("tenantdb_lookups_total", "Row owners asked of the database.", func() float64 { return float64(a.Stats().Lookups) })
	r.NewCounterFunc("tenantdb_cache_verifications_total", "Cache hits re-checked against the database (-cache-verify).", func() float64 { return float64(a.Stats().Verifications) })
	r.NewCounterFunc("tenantdb_cache_stale_total", "Cached row owners found to disagree with the database and evicted.", func() float64 { return float64(a.Stats().StaleFound) })
	r.NewCounterFunc("tenantdb_lookup_failures_total", "Row-owner lookups that failed.", func() float64 { return float64(a.Stats().LookupFailures) })
	if cat != nil {
		r.NewGaugeFunc("tenantdb_schema_loaded", "1 once the schema has been read.", func() float64 {
			if cat.Loaded() {
				return 1
			}
			return 0
		})
		r.NewCounterFunc("tenantdb_schema_loads_total", "Successful schema reads.", func() float64 { return float64(cat.Stats().Loads) })
		r.NewCounterFunc("tenantdb_schema_load_failures_total", "Failed schema reads.", func() float64 { return float64(cat.Stats().LoadFailures) })
		r.NewGaugeFunc("tenantdb_schema_last_load_timestamp_seconds", "When the schema was last read (unix time).", func() float64 {
			if t := cat.Stats().LastLoad; !t.IsZero() {
				return float64(t.Unix())
			}
			return 0
		})
		r.NewGaugeFunc("tenantdb_schema_tables", "Tables and partitions in the schema snapshot.", func() float64 { return float64(cat.Stats().Tables) })
		r.NewGaugeFunc("tenantdb_schema_views", "Views in the schema snapshot.", func() float64 { return float64(cat.Stats().Views) })
	}
	r.NewGaugeFunc("go_goroutines", "Goroutines.", func() float64 { return float64(runtime.NumGoroutine()) })
	r.NewGaugeFunc("go_memstats_heap_inuse_bytes", "Heap in use.", func() float64 {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return float64(m.HeapInuse)
	})
}

// timed wraps the attributor's observer so the time it takes is measured.
type timedObserver struct {
	proxy.Observer
	h *metrics.Histogram
}

func (t timedObserver) OnFrontend(s *proxy.Session, m pgproto3.FrontendMessage) {
	start := time.Now()
	t.Observer.OnFrontend(s, m)
	t.h.Observe(time.Since(start).Seconds())
}

// timedGate additionally keeps the Gate interface.
type timedGate struct {
	timedObserver
	g proxy.Gate
}

func (t timedGate) Gate(s *proxy.Session, m pgproto3.FrontendMessage) *proxy.Rejection {
	start := time.Now()
	r := t.g.Gate(s, m)
	t.h.Observe(time.Since(start).Seconds())
	return r
}

func (o *observability) wrap(obs proxy.Observer) proxy.Observer {
	if g, ok := obs.(proxy.Gate); ok {
		return timedGate{timedObserver{obs, o.analysis}, g}
	}
	return timedObserver{obs, o.analysis}
}

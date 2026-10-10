package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/akashsaini/tenantdb/internal/attrib"
	"github.com/akashsaini/tenantdb/internal/proxy"
)

// adminServer serves the operational endpoints:
//
//	/metrics   Prometheus metrics
//	/healthz   200 while the process is running (liveness)
//	/readyz    200 when it can serve clients, 503 with the reasons when it cannot (readiness)
//	/version   what is running
//
// It has no authentication: bind it to a loopback or internal address.
type adminServer struct {
	obs      *observability
	srv      *proxy.Server
	catalog  *attrib.Catalog
	upstream string
	needs    bool // the schema is required to serve (strict mode)
	ver      map[string]string

	mu        sync.Mutex
	probedAt  time.Time
	probedErr string
}

func (a *adminServer) handler(pprofOn bool) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", a.obs.reg.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", a.ready)
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(a.ver)
	})
	if pprofOn {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

// ready reports whether the proxy can serve a client now.
func (a *adminServer) ready(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{}
	ok := true
	fail := func(name, why string) { checks[name] = why; ok = false }
	if a.srv.Draining() {
		fail("serving", "shutting down")
	} else {
		checks["serving"] = "ok"
	}
	if a.catalog != nil {
		switch {
		case a.catalog.Loaded():
			checks["schema"] = "ok"
		case a.needs:
			fail("schema", "not loaded")
		default:
			checks["schema"] = "not loaded (running without it)"
		}
	}
	if err := a.upstreamReachable(); err != "" {
		fail("upstream", err)
	} else {
		checks["upstream"] = "ok"
	}
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(map[string]any{"ready": ok, "checks": checks})
}

// upstreamReachable dials the database, at most once every few seconds however
// often it is asked.
func (a *adminServer) upstreamReachable() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if time.Since(a.probedAt) < 3*time.Second {
		return a.probedErr
	}
	a.probedAt = time.Now()
	c, err := net.DialTimeout("tcp", a.upstream, 2*time.Second)
	if err != nil {
		a.probedErr = err.Error()
		return a.probedErr
	}
	c.Close()
	a.probedErr = ""
	return ""
}

// serve runs the admin server until ctx ends.
func (a *adminServer) serve(ctx context.Context, addr string, pprofOn bool) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	hs := &http.Server{Handler: a.handler(pprofOn), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 60 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		hs.Shutdown(sctx)
	}()
	go hs.Serve(ln)
	return ln.Addr(), nil
}

// isLoopback reports whether addr only accepts connections from this machine.
func isLoopback(a net.Addr) bool {
	t, ok := a.(*net.TCPAddr)
	return ok && t.IP.IsLoopback()
}

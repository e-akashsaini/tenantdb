// Package metrics is a small Prometheus-compatible registry: counters with
// labels, gauges and counters read from a function, and histograms, exposed in
// the text exposition format. It has no dependencies on purpose; the proxy needs a
// dozen metrics, not a client library.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metrics and writes them out.
type Registry struct {
	mu      sync.Mutex
	metrics []metric
}

type metric interface {
	write(w io.Writer)
	name() string
}

// New returns an empty registry.
func New() *Registry { return &Registry{} }

func (r *Registry) add(m metric) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, o := range r.metrics {
		if o.name() == m.name() {
			panic("metrics: duplicate metric " + m.name())
		}
	}
	r.metrics = append(r.metrics, m)
}

// Write writes every metric in the Prometheus text format.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	ms := append([]metric(nil), r.metrics...)
	r.mu.Unlock()
	for _, m := range ms {
		m.write(w)
	}
}

// Handler serves the registry.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.Write(w)
	})
}

func header(w io.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, escapeHelp(help), name, typ)
}

func escapeHelp(s string) string { return strings.NewReplacer(`\`, `\\`, "\n", `\n`).Replace(s) }

func escapeLabel(s string) string {
	return strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(s)
}

func labelPairs(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, n, escapeLabel(values[i]))
	}
	b.WriteByte('}')
	return b.String()
}

func formatFloat(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "+Inf"
	case math.IsInf(f, -1):
		return "-Inf"
	case math.IsNaN(f):
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// Counter only goes up.
type Counter struct{ v atomic.Uint64 }

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// CounterVec is a counter per combination of label values. Label values should
// come from a small fixed set: every distinct combination is kept for good.
type CounterVec struct {
	nm, help string
	labels   []string
	mu       sync.Mutex
	m        map[string]*Counter
	vals     map[string][]string
}

// NewCounterVec registers a counter with the given label names.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	v := &CounterVec{nm: name, help: help, labels: labels, m: map[string]*Counter{}, vals: map[string][]string{}}
	r.add(v)
	return v
}

// With returns the counter for these label values.
func (v *CounterVec) With(values ...string) *Counter {
	if len(values) != len(v.labels) {
		panic("metrics: wrong number of label values for " + v.nm)
	}
	key := strings.Join(values, "\xff")
	v.mu.Lock()
	defer v.mu.Unlock()
	c := v.m[key]
	if c == nil {
		c = &Counter{}
		v.m[key] = c
		v.vals[key] = append([]string(nil), values...)
	}
	return c
}

func (v *CounterVec) name() string { return v.nm }

func (v *CounterVec) write(w io.Writer) {
	header(w, v.nm, v.help, "counter")
	v.mu.Lock()
	keys := make([]string, 0, len(v.m))
	for k := range v.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	type row struct {
		labels string
		n      uint64
	}
	rows := make([]row, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, row{labelPairs(v.labels, v.vals[k]), v.m[k].v.Load()})
	}
	v.mu.Unlock()
	if len(rows) == 0 && len(v.labels) == 0 {
		rows = append(rows, row{"", 0})
	}
	for _, r := range rows {
		fmt.Fprintf(w, "%s%s %d\n", v.nm, r.labels, r.n)
	}
}

// NewCounter registers a counter without labels.
func (r *Registry) NewCounter(name, help string) *Counter {
	v := r.NewCounterVec(name, help)
	return v.With()
}

type funcMetric struct {
	nm, help, typ string
	labels        []string
	values        []string
	fn            func() float64
}

func (f *funcMetric) name() string { return f.nm }
func (f *funcMetric) write(w io.Writer) {
	header(w, f.nm, f.help, f.typ)
	fmt.Fprintf(w, "%s%s %s\n", f.nm, labelPairs(f.labels, f.values), formatFloat(f.fn()))
}

// NewGaugeFunc registers a gauge whose value is read when scraped.
func (r *Registry) NewGaugeFunc(name, help string, fn func() float64) {
	r.add(&funcMetric{nm: name, help: help, typ: "gauge", fn: fn})
}

// NewCounterFunc registers a counter whose value is read when scraped (for
// counts kept elsewhere, as atomics).
func (r *Registry) NewCounterFunc(name, help string, fn func() float64) {
	r.add(&funcMetric{nm: name, help: help, typ: "counter", fn: fn})
}

// NewInfo registers a constant gauge of 1 with labels, such as build information.
func (r *Registry) NewInfo(name, help string, labels, values []string) {
	r.add(&funcMetric{nm: name, help: help, typ: "gauge", labels: labels, values: values, fn: func() float64 { return 1 }})
}

// Histogram counts observations into buckets.
type Histogram struct {
	nm, help string
	bounds   []float64
	mu       sync.Mutex
	counts   []uint64
	sum      float64
	n        uint64
}

// NewHistogram registers a histogram with the given upper bounds (ascending).
func (r *Registry) NewHistogram(name, help string, bounds []float64) *Histogram {
	h := &Histogram{nm: name, help: help, bounds: append([]float64(nil), bounds...), counts: make([]uint64, len(bounds))}
	r.add(h)
	return h
}

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	for i, b := range h.bounds {
		if v <= b {
			h.counts[i]++
		}
	}
	h.sum += v
	h.n++
	h.mu.Unlock()
}

func (h *Histogram) name() string { return h.nm }

func (h *Histogram) write(w io.Writer) {
	header(w, h.nm, h.help, "histogram")
	h.mu.Lock()
	counts, sum, n := append([]uint64(nil), h.counts...), h.sum, h.n
	h.mu.Unlock()
	for i, b := range h.bounds {
		fmt.Fprintf(w, "%s_bucket{le=\"%s\"} %d\n", h.nm, formatFloat(b), counts[i])
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %s\n%s_count %d\n", h.nm, n, h.nm, formatFloat(sum), h.nm, n)
}

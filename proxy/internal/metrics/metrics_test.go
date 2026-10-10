package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func scrape(r *Registry) string {
	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	return rec.Body.String()
}

func TestExpositionFormat(t *testing.T) {
	r := New()
	c := r.NewCounterVec("t_statements_total", "Statements by source.", "source")
	c.With("explicit").Inc()
	c.With("explicit").Add(2)
	c.With(`we"ird\` + "\n").Inc()
	plain := r.NewCounter("t_plain_total", "A counter.")
	plain.Inc()
	r.NewGaugeFunc("t_gauge", "A gauge.", func() float64 { return 2.5 })
	r.NewCounterFunc("t_func_total", "From elsewhere.", func() float64 { return 7 })
	r.NewInfo("t_build_info", "Build.", []string{"version"}, []string{"1.2.3"})
	h := r.NewHistogram("t_seconds", "Latency.", []float64{0.1, 1})
	h.Observe(0.05)
	h.Observe(0.5)
	h.Observe(5)
	out := scrape(r)
	for _, want := range []string{
		"# HELP t_statements_total Statements by source.\n# TYPE t_statements_total counter\n",
		`t_statements_total{source="explicit"} 3` + "\n",
		`t_statements_total{source="we\"ird\\\n"} 1` + "\n",
		"t_plain_total 1\n",
		"# TYPE t_gauge gauge\nt_gauge 2.5\n",
		"# TYPE t_func_total counter\nt_func_total 7\n",
		`t_build_info{version="1.2.3"} 1` + "\n",
		`t_seconds_bucket{le="0.1"} 1` + "\n",
		`t_seconds_bucket{le="1"} 2` + "\n",
		`t_seconds_bucket{le="+Inf"} 3` + "\n",
		"t_seconds_sum 5.55\n",
		"t_seconds_count 3\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestAnUntouchedCounterIsStillExposed(t *testing.T) {
	r := New()
	r.NewCounter("t_zero_total", "Never incremented.")
	if out := scrape(r); !strings.Contains(out, "t_zero_total 0\n") {
		t.Errorf("a counter that has not happened yet must read 0, not be absent:\n%s", out)
	}
}

func TestConcurrentUse(t *testing.T) {
	r := New()
	c := r.NewCounterVec("t_c_total", "c", "k")
	h := r.NewHistogram("t_h", "h", []float64{1})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				c.With(string(rune('a' + i%3))).Inc()
				h.Observe(0.5)
				if j%100 == 0 {
					_ = scrape(r)
				}
			}
		}(i)
	}
	wg.Wait()
	out := scrape(r)
	if !strings.Contains(out, "t_h_count 8000\n") {
		t.Errorf("lost observations:\n%s", out)
	}
}

func TestDuplicateAndMislabelledMetricsPanic(t *testing.T) {
	r := New()
	r.NewCounter("t_dup_total", "x")
	func() {
		defer func() {
			if recover() == nil {
				t.Error("registering a name twice is a bug that should be loud")
			}
		}()
		r.NewCounter("t_dup_total", "x")
	}()
	v := r.NewCounterVec("t_l_total", "x", "a", "b")
	defer func() {
		if recover() == nil {
			t.Error("the wrong number of label values should panic")
		}
	}()
	v.With("only-one")
}

package attrib

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/akashsaini/tenantdb/internal/proxy"
)

// answerLookup says who owns a row, and counts how often it was asked.
type answerLookup struct {
	mu     sync.Mutex
	owners map[string][]string // key value -> tenants
	asked  int
}

func (l *answerLookup) tenants(_ context.Context, _ *TableInfo, _ string, _, val string) ([]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.asked++
	return l.owners[val], nil
}

func (l *answerLookup) count() int { l.mu.Lock(); defer l.mu.Unlock(); return l.asked }

type staleReport struct {
	table, col, cached string
	actual             []string
}

func verifyHarness(rate float64, lk *answerLookup) (*harness, *[]staleReport, *sync.Mutex) {
	var mu sync.Mutex
	var reports []staleReport
	sp := soundSpec()
	h := &harness{s: &proxy.Session{ID: 1, User: "app", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: sp.build(),
		CacheVerify: rate, OnStale: func(table, col, cached string, actual []string) {
			mu.Lock()
			reports = append(reports, staleReport{table, col, cached, actual})
			mu.Unlock()
		}}, h.record)
	h.a.lk = lk
	return h, &reports, &mu
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestACachedOwnerThatNoLongerMatchesIsEvictedAndReported(t *testing.T) {
	lk := &answerLookup{owners: map[string][]string{"7": {"globex"}}} // the row now belongs to another tenant
	h, reports, mu := verifyHarness(1, lk)
	h.a.Cache().Put("public.orders", "id", "7", "acme")

	e := h.attributed(t, `SELECT * FROM orders WHERE id = 7`)
	if e.Source != SourceCache || e.Tenants[0] != "acme" {
		t.Fatalf("the statement itself is answered from the cache, without waiting: %+v", e)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(*reports) == 1 }, "the stale report")
	if r := (*reports)[0]; r.table != "public.orders" || r.col != "id" || r.cached != "acme" || len(r.actual) != 1 || r.actual[0] != "globex" {
		t.Errorf("report = %+v", r)
	}
	if _, ok := h.a.Cache().Get("public.orders", "id", "7"); ok {
		t.Error("the stale entry must be gone")
	}
	if st := h.a.Stats(); st.Verifications != 1 || st.StaleFound != 1 {
		t.Errorf("stats = %+v", st)
	}
	// ... and the next statement asks the database instead of trusting the old answer
	h.query(`SELECT * FROM orders WHERE id = 7`)
	h.waitFor(t, 2)
	if e = h.last(t); e.Source != SourceLookup || len(e.Tenants) != 1 || e.Tenants[0] != "globex" {
		t.Errorf("after eviction the database is asked and its answer is used: %+v", e)
	}
}

func TestADeletedRowIsStaleToo(t *testing.T) {
	lk := &answerLookup{owners: map[string][]string{}} // no such row any more
	h, reports, mu := verifyHarness(1, lk)
	h.a.Cache().Put("public.orders", "id", "7", "acme")
	h.attributed(t, `SELECT * FROM orders WHERE id = 7`)
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(*reports) == 1 }, "the stale report")
}

func TestACachedOwnerThatStillMatchesIsLeftAlone(t *testing.T) {
	lk := &answerLookup{owners: map[string][]string{"7": {"acme"}}}
	h, reports, mu := verifyHarness(1, lk)
	h.a.Cache().Put("public.orders", "id", "7", "acme")
	h.attributed(t, `SELECT * FROM orders WHERE id = 7`)
	waitFor(t, func() bool { return h.a.Stats().Verifications == 1 }, "the check")
	mu.Lock()
	defer mu.Unlock()
	if len(*reports) != 0 || h.a.Stats().StaleFound != 0 {
		t.Errorf("nothing was stale: %v", *reports)
	}
	if _, ok := h.a.Cache().Get("public.orders", "id", "7"); !ok {
		t.Error("a verified entry stays")
	}
}

func TestVerificationIsOffByDefaultAndSampled(t *testing.T) {
	lk := &answerLookup{owners: map[string][]string{"7": {"acme"}}}
	h, _, _ := verifyHarness(0, lk)
	h.a.Cache().Put("public.orders", "id", "7", "acme")
	for i := 0; i < 20; i++ {
		h.attributed(t, `SELECT * FROM orders WHERE id = 7`)
	}
	time.Sleep(50 * time.Millisecond)
	if lk.count() != 0 {
		t.Errorf("rate 0 must never ask the database: %d", lk.count())
	}

	lk2 := &answerLookup{owners: map[string][]string{"7": {"acme"}}}
	h2, _, _ := verifyHarness(0.1, lk2)
	h2.a.Cache().Put("public.orders", "id", "7", "acme")
	for i := 0; i < 1000; i++ {
		h2.attributed(t, `SELECT * FROM orders WHERE id = 7`)
	}
	waitFor(t, func() bool { return lk2.count() >= 90 }, "about a tenth of the hits to be checked")
	time.Sleep(50 * time.Millisecond)
	if n := lk2.count(); n < 90 || n > 110 {
		t.Errorf("a rate of 0.1 over 1000 hits checked %d", n)
	}
}

func TestVerificationFollowsForeignKeysToTheParentRow(t *testing.T) {
	// items.order_id is a foreign key: the owner that was cached is the parent order's
	lk := &answerLookup{owners: map[string][]string{"9": {"globex"}}}
	h, reports, mu := verifyHarness(1, lk)
	h.a.Cache().Put("public.orders", "id", "9", "acme")
	e := h.attributed(t, `SELECT * FROM items WHERE order_id = 9`)
	if e.Source != SourceCache {
		t.Fatalf("%+v", e)
	}
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(*reports) == 1 }, "the stale report")
	if (*reports)[0].table != "public.orders" {
		t.Errorf("the parent's row is what was checked: %+v", (*reports)[0])
	}
}

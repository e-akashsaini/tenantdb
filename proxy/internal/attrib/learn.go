package attrib

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// Learning relationships from the queries themselves.
//
// Declared foreign keys and the <thing>_id naming rule miss plenty of real
// schemas (templates.folder_id pointing at template_folders, a column called
// `ord`, a legacy `owner`). But an application that joins A.x = B.y, with B.y
// unique, is telling us A.x probably points at B. The attributor reports such
// joins here; they are verified against the data exactly like inferred foreign
// keys (every sampled A row must have the tenant of the B row it points at) in
// the background, and a relationship that passes is used from then on. A wrong
// guess costs one cheap query and is forgotten.

const suggestQueue = 256

// SuggestFK offers a candidate foreign key child.ccol -> parent.pcol, as seen in
// a join condition. It returns at once; the check runs in the background.
func (c *Catalog) SuggestFK(child *TableInfo, ccol string, parent *TableInfo, pcol string) {
	if c == nil || c.dsn == "" || len(c.TenantColumns) == 0 || child == nil || parent == nil || child == parent ||
		child.Schema == "pg_temp" || parent.Schema == "pg_temp" || !parent.IsUnique(pcol) {
		return
	}
	tset := lowerSet(c.TenantColumns)
	if tset[ccol] || c.tenantColOf(child, tset) == "" || c.tenantColOf(parent, tset) == "" {
		return
	}
	f := fkCand{child: child, ccol: ccol, parent: parent, pcol: pcol, inferred: true}
	id := f.id()

	c.mu.RLock()
	_, known := c.fks[child.Key()][ccol]
	v, memo := c.fkMemo[id]
	c.mu.RUnlock()
	if known {
		return
	}
	ttl := fkMemoTTL
	if memo && v.inconclusive {
		ttl = fkInconclusiveTTL
	}
	if memo && time.Since(v.at) < ttl {
		return
	}

	c.sugMu.Lock()
	if c.sugSeen == nil {
		c.sugSeen = map[string]time.Time{}
	}
	if at, seen := c.sugSeen[id]; seen && time.Since(at) < fkInconclusiveTTL {
		c.sugMu.Unlock()
		return
	}
	c.sugSeen[id] = time.Now()
	if c.sugQ == nil {
		c.sugQ = make(chan fkCand, suggestQueue)
		go c.suggestWorker()
	}
	q := c.sugQ
	c.sugMu.Unlock()

	c.sugWG.Add(1)
	select {
	case q <- f:
	default:
		c.sugWG.Done() // queue full: it will be suggested again
	}
}

func (c *Catalog) tenantColOf(t *TableInfo, tset map[string]bool) string {
	for _, col := range t.Cols {
		if tset[col] {
			return col
		}
	}
	return ""
}

func (c *Catalog) suggestWorker() {
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			conn.Close(context.Background())
		}
	}()
	for f := range c.sugQ {
		func() {
			defer c.sugWG.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if conn == nil || conn.IsClosed() {
				var err error
				if conn, err = pgx.Connect(ctx, c.dsn); err != nil {
					conn = nil
					return
				}
			}
			tset := lowerSet(c.TenantColumns)
			ok, inconclusive := verifyFK(ctx, conn, f, c.tenantColOf(f.child, tset), c.tenantColOf(f.parent, tset))
			c.mu.Lock()
			if c.fkMemo == nil {
				c.fkMemo = map[string]fkVerdict{}
			}
			c.fkMemo[f.id()] = fkVerdict{ok: ok, inconclusive: inconclusive, at: time.Now()}
			if ok {
				c.learnLocked(f)
			}
			c.mu.Unlock()
		}()
	}
}

// learnLocked records a verified relationship. c.mu must be held.
func (c *Catalog) learnLocked(f fkCand) {
	ref := FKRef{ParentKey: f.parent.Key(), ParentCol: f.pcol}
	if c.learned == nil {
		c.learned = map[string]map[string]FKRef{}
	}
	if c.learned[f.child.Key()] == nil {
		c.learned[f.child.Key()] = map[string]FKRef{}
	}
	c.learned[f.child.Key()][f.ccol] = ref
	// copy-on-write, so readers holding the old map are unaffected
	next := make(map[string]map[string]FKRef, len(c.fks)+1)
	for k, m := range c.fks {
		next[k] = m
	}
	cols := map[string]FKRef{}
	for k, v := range c.fks[f.child.Key()] {
		cols[k] = v
	}
	cols[f.ccol] = ref
	next[f.child.Key()] = cols
	c.fks = next
}

// mergeLearned adds the learned relationships to a freshly computed set. c.mu must be held.
func (c *Catalog) mergeLearned(fks map[string]map[string]FKRef) map[string]map[string]FKRef {
	for child, cols := range c.learned {
		for col, ref := range cols {
			if _, still := c.tables[child]; !still {
				continue
			}
			if _, parent := c.tables[ref.ParentKey]; !parent {
				continue
			}
			if fks[child] == nil {
				fks[child] = map[string]FKRef{}
			}
			if _, have := fks[child][col]; !have {
				fks[child][col] = ref
			}
		}
	}
	return fks
}

// DrainSuggestions waits until every suggested relationship has been checked (or
// the timeout passes). For tests and for replaying a recorded workload in two passes.
func (c *Catalog) DrainSuggestions(timeout time.Duration) {
	done := make(chan struct{})
	go func() { c.sugWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

var _ = strings.ToLower
var _ sync.Mutex

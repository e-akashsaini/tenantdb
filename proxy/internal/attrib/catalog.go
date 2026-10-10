package attrib

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// TableInfo describes one table as the catalog sees it.
type TableInfo struct {
	Schema string
	Name   string
	Cols   []string // lower-cased, in physical order, dropped columns excluded
	raw    []string // original spelling of Cols, for quoting
	// Ambiguous is set when the same table name exists in more than one
	// schema. The proxy keys everything by bare table name, so it refuses to
	// reason about such tables rather than guess.
	Ambiguous bool
}

// Catalog is a snapshot of the database schema, loaded over its own
// connection (a read-only role is enough). It answers "which tables carry a
// tenant column" and "what is the column order" without touching live traffic.
type Catalog struct {
	dsn       string
	mu        sync.RWMutex
	tables    map[string]*TableInfo
	loaded    bool
	reloading atomic.Bool
}

func NewCatalog(dsn string) *Catalog { return &Catalog{dsn: dsn} }

// pg_catalog rather than information_schema: the latter hides columns the role
// cannot read, which would make shared and tenant tables look the same.
const catalogSQL = `
SELECT n.nspname, c.relname, a.attname
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','v','m','f')
  AND a.attnum > 0 AND NOT a.attisdropped
  AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
ORDER BY n.nspname, c.relname, a.attnum`

type colRow struct{ schema, table, col string }

// Load replaces the snapshot with the current schema.
func (c *Catalog) Load(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, c.dsn)
	if err != nil {
		return fmt.Errorf("catalog connect: %w", err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, catalogSQL)
	if err != nil {
		return fmt.Errorf("catalog query: %w", err)
	}
	defer rows.Close()
	var all []colRow
	for rows.Next() {
		var r colRow
		if err := rows.Scan(&r.schema, &r.table, &r.col); err != nil {
			return err
		}
		all = append(all, r)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.set(all)
	return nil
}

func (c *Catalog) set(rows []colRow) {
	m := map[string]*TableInfo{}
	for _, r := range rows {
		key := strings.ToLower(r.table)
		ti := m[key]
		switch {
		case ti == nil:
			ti = &TableInfo{Schema: r.schema, Name: r.table}
			m[key] = ti
		case ti.Schema != r.schema:
			ti.Ambiguous = true
			continue
		}
		ti.Cols = append(ti.Cols, strings.ToLower(r.col))
		ti.raw = append(ti.raw, r.col)
	}
	c.mu.Lock()
	c.tables, c.loaded = m, true
	c.mu.Unlock()
}

// ReloadAsync refreshes the snapshot in the background, e.g. after DDL. At
// most one reload runs at a time.
func (c *Catalog) ReloadAsync() {
	if c.dsn == "" || !c.reloading.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.reloading.Store(false)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.Load(ctx) // keep the previous snapshot on failure
	}()
}

// Refresh reloads the snapshot every interval until ctx ends.
func (c *Catalog) Refresh(ctx context.Context, every time.Duration, onErr func(error)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := c.Load(cctx); err != nil && onErr != nil {
				onErr(err)
			}
			cancel()
		}
	}
}

func (c *Catalog) Loaded() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.loaded
}

// Table returns the table with the given lower-cased name.
func (c *Catalog) Table(name string) (*TableInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ti, ok := c.tables[name]
	return ti, ok
}

// all returns the tables in a stable order.
func (c *Catalog) all() []*TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*TableInfo, 0, len(c.tables))
	for _, t := range c.tables {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Schema+"."+out[i].Name < out[j].Schema+"."+out[j].Name })
	return out
}

// quoted returns the column's original spelling, safe to use in SQL.
func (t *TableInfo) quoted(lower string) string {
	for i, c := range t.Cols {
		if c == lower && i < len(t.raw) {
			return pgx.Identifier{t.raw[i]}.Sanitize()
		}
	}
	return pgx.Identifier{lower}.Sanitize()
}

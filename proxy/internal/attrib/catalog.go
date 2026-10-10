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
	Kind   byte     // pg_class.relkind: r table, p partitioned, v view, m matview, f foreign
	Cols   []string // lower-cased, in physical order, dropped columns excluded
	raw    []string // original spelling of Cols, for quoting
}

// Key is the name the row cache uses for this table.
func (t *TableInfo) Key() string { return strings.ToLower(t.Schema + "." + t.Name) }

// quoted returns the column's original spelling, safe to use in SQL.
func (t *TableInfo) quoted(lower string) string {
	for i, c := range t.Cols {
		if c == lower && i < len(t.raw) {
			return pgx.Identifier{t.raw[i]}.Sanitize()
		}
	}
	return pgx.Identifier{lower}.Sanitize()
}

func (t *TableInfo) qualified() string { return pgx.Identifier{t.Schema, t.Name}.Sanitize() }

// isSystemSchema holds relations that never contain tenant data.
func isSystemSchema(s string) bool { return s == "pg_catalog" || s == "information_schema" }

// defaultSearchPath is what Postgres uses until something changes it.
var defaultSearchPath = []string{"$user", "public"}

// Catalog is a snapshot of the database schema, loaded over its own
// connection (a read-only role is enough). It answers "which tables carry a
// tenant column", "what is the column order" and "which functions are user
// code" without touching live traffic.
type Catalog struct {
	dsn       string
	mu        sync.RWMutex
	tables    map[string]*TableInfo // "schema.name", lower-cased
	funcs     map[string]bool       // "schema.name" of user-defined functions
	loaded    bool
	reloading atomic.Bool
}

func NewCatalog(dsn string) *Catalog { return &Catalog{dsn: dsn} }

// pg_catalog rather than information_schema: the latter hides columns the role
// cannot read, which would make shared and tenant tables look the same.
const catalogSQL = `
SELECT n.nspname, c.relname, c.relkind::text, a.attname
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','v','m','f')
  AND a.attnum > 0 AND NOT a.attisdropped
  AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
ORDER BY n.nspname, c.relname, a.attnum`

// Functions that belong to an extension (deptype 'e') are not user code.
const funcSQL = `
SELECT n.nspname, p.proname
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')`

type colRow struct {
	schema, table string
	kind          byte // 0 means an ordinary table
	col           string
}

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
	var all []colRow
	for rows.Next() {
		var r colRow
		var kind string
		if err := rows.Scan(&r.schema, &r.table, &kind, &r.col); err != nil {
			rows.Close()
			return err
		}
		if kind != "" {
			r.kind = kind[0]
		}
		all = append(all, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	frows, err := conn.Query(ctx, funcSQL)
	if err != nil {
		return fmt.Errorf("catalog functions: %w", err)
	}
	funcs := map[string]bool{}
	for frows.Next() {
		var s, n string
		if err := frows.Scan(&s, &n); err != nil {
			frows.Close()
			return err
		}
		funcs[strings.ToLower(s+"."+n)] = true
	}
	frows.Close()
	if err := frows.Err(); err != nil {
		return err
	}
	c.set(all, funcs)
	return nil
}

func (c *Catalog) set(rows []colRow, funcs map[string]bool) {
	m := map[string]*TableInfo{}
	for _, r := range rows {
		key := strings.ToLower(r.schema + "." + r.table)
		ti := m[key]
		if ti == nil {
			kind := r.kind
			if kind == 0 {
				kind = 'r'
			}
			ti = &TableInfo{Schema: r.schema, Name: r.table, Kind: kind}
			m[key] = ti
		}
		ti.Cols = append(ti.Cols, strings.ToLower(r.col))
		ti.raw = append(ti.raw, r.col)
	}
	if funcs == nil {
		funcs = map[string]bool{}
	}
	c.mu.Lock()
	c.tables, c.funcs, c.loaded = m, funcs, true
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

// Table finds the table ref names, the way Postgres would: a schema-qualified
// name is looked up directly; an unqualified one is searched for along
// searchPath (with pg_catalog implicitly first), "$user" standing for user.
func (c *Catalog) Table(ref TableRef, searchPath []string, user string) (*TableInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ref.Schema != "" {
		ti, ok := c.tables[ref.Schema+"."+ref.Name]
		return ti, ok
	}
	if ti, ok := c.tables["pg_catalog."+ref.Name]; ok {
		return ti, true
	}
	for _, s := range searchPath {
		if s == "$user" {
			s = strings.ToLower(user)
		}
		if ti, ok := c.tables[s+"."+ref.Name]; ok {
			return ti, true
		}
	}
	return nil, false
}

// UserFunc reports whether ref is a function written by the application (not
// built in, not from an extension), resolved like Table.
func (c *Catalog) UserFunc(ref TableRef, searchPath []string, user string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ref.Schema != "" {
		return c.funcs[ref.Schema+"."+ref.Name]
	}
	for _, s := range searchPath {
		if s == "$user" {
			s = strings.ToLower(user)
		}
		if c.funcs[s+"."+ref.Name] {
			return true
		}
	}
	return false
}

// all returns the tables in a stable order.
func (c *Catalog) all() []*TableInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]*TableInfo, 0, len(c.tables))
	for _, t := range c.tables {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}

package attrib

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Warm fills the row cache from the database before live traffic has to teach
// it. For every tenant-scoped table that has a key column it reads
// (key, tenant) pairs over its own connection, so client queries are never
// slowed down by it. Reading stops after maxRows rows in total; the cache's own
// size limit still applies on top.
func (a *Attributor) Warm(ctx context.Context, dsn string, maxRows int, log *slog.Logger) error {
	if a.cat == nil || !a.cat.Loaded() {
		return fmt.Errorf("warm: schema catalog not loaded")
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("warm connect: %w", err)
	}
	defer conn.Close(ctx)

	type job struct {
		ti   *TableInfo
		tcol string
		keys []string
	}
	var jobs []job
	for _, ti := range a.cat.all() {
		// A table the role cannot fully read (row-level security) would warm a
		// partial picture; leave it to be resolved row by row.
		if !a.cat.CanRead(ti) {
			continue
		}
		if tcol, keys := a.scopeOf(ti); tcol != "" && len(keys) > 0 {
			jobs = append(jobs, job{ti, tcol, keys})
		}
	}

	// Each table gets an equal share of what is left of the budget, so one huge
	// table cannot starve the rest. Rows a small table leaves unused flow on.
	total := 0
	for i, j := range jobs {
		share := (maxRows - total) / (len(jobs) - i)
		if share <= 0 {
			log.Warn("cache warm stopped at row limit", "limit", maxRows)
			break
		}
		n, err := a.warmTable(ctx, conn, j.ti, j.tcol, j.keys, share)
		total += n
		log.Info("cache warm", "table", j.ti.Key(), "rows", n, "share", share)
		if err != nil {
			return fmt.Errorf("warm %s: %w", j.ti.Key(), err)
		}
	}
	log.Info("cache warm done", "rows", total, "cached", a.cache.Len())
	return nil
}

func (a *Attributor) warmTable(ctx context.Context, conn *pgx.Conn, ti *TableInfo, tcol string, keys []string, limit int) (int, error) {
	// Quote with the catalog's original spelling; the cache uses lower-case names.
	sel := []string{ti.quoted(tcol) + "::text"}
	for _, k := range keys {
		sel = append(sel, ti.quoted(k)+"::text")
	}
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s IS NOT NULL LIMIT %d",
		strings.Join(sel, ", "), ti.qualified(), ti.quoted(tcol), limit)
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	table := ti.Key()
	n := 0
	vals := make([]*string, len(sel))
	dest := make([]any, len(sel))
	for i := range vals {
		dest[i] = &vals[i]
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return n, err
		}
		n++
		if vals[0] == nil {
			continue
		}
		for i, k := range keys {
			if v := vals[i+1]; v != nil {
				a.cache.Put(table, k, *v, *vals[0])
			}
		}
	}
	return n, rows.Err()
}

// scopeOf returns the table's tenant column (empty if it has none) and the
// configured key columns it contains.
func (a *Attributor) scopeOf(ti *TableInfo) (tenantCol string, keys []string) {
	for _, c := range ti.Cols {
		switch {
		case tenantCol == "" && a.tenants[c]:
			tenantCol = c
		case !a.tenants[c] && ti.IsUnique(c):
			keys = append(keys, c)
		}
	}
	return tenantCol, keys
}

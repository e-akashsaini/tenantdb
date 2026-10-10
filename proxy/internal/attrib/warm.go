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

	total := 0
	for _, ti := range a.cat.all() {
		if ti.Ambiguous {
			continue
		}
		tcol, keys := a.scopeOf(ti)
		if tcol == "" || len(keys) == 0 {
			continue
		}
		n, err := a.warmTable(ctx, conn, ti, tcol, keys, maxRows-total)
		total += n
		log.Info("cache warm", "table", ti.Name, "rows", n)
		if err != nil {
			return fmt.Errorf("warm %s: %w", ti.Name, err)
		}
		if total >= maxRows {
			log.Warn("cache warm stopped at row limit", "limit", maxRows)
			break
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
		strings.Join(sel, ", "), pgx.Identifier{ti.Schema, ti.Name}.Sanitize(), ti.quoted(tcol), limit)
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	table := strings.ToLower(ti.Name)
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
		case a.ex.keys[c]:
			keys = append(keys, c)
		}
	}
	return tenantCol, keys
}

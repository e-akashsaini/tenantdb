package attrib

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// rowLookup asks the database which tenant owns a row. It is the authority the
// row cache is a shortcut for.
type rowLookup interface {
	tenants(ctx context.Context, ti *TableInfo, tenantCol, keyCol, val string) ([]string, error)
}

// bridgeLookup is the optional second kind of question: who owns the tenant rows
// that the row of a credential table (an access token) points at.
type bridgeLookup interface {
	viaTenants(ctx context.Context, s bridgeSpec, val string) ([]string, error)
}

// Lookup resolves rows over a small pool of read-only connections. Concurrent
// requests for the same row share one query.
type Lookup struct {
	pool *pgxpool.Pool

	mu       sync.Mutex
	inflight map[lookupKey]*lookupCall
}

type lookupKey struct{ table, col, val string }

type lookupCall struct {
	done    chan struct{}
	tenants []string
	err     error
}

// DefaultLookupConns is how many connections the lookup pool may open.
const DefaultLookupConns = 8

// NewLookup connects lazily; the pool opens connections as lookups need them.
func NewLookup(ctx context.Context, dsn string) (*Lookup, error) {
	return NewLookupSize(ctx, dsn, DefaultLookupConns)
}

// NewLookupSize is NewLookup with a pool of at most conns connections. Lookups
// that find every connection busy wait for one, up to the lookup timeout.
func NewLookupSize(ctx context.Context, dsn string, conns int) (*Lookup, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if conns < 1 {
		conns = DefaultLookupConns
	}
	cfg.MaxConns = int32(conns)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &Lookup{pool: pool, inflight: map[lookupKey]*lookupCall{}}, nil
}

func (l *Lookup) Close() { l.pool.Close() }

func (l *Lookup) tenants(ctx context.Context, ti *TableInfo, tenantCol, keyCol, val string) ([]string, error) {
	key := lookupKey{ti.Key(), keyCol, normVal(val)}
	l.mu.Lock()
	if c, ok := l.inflight[key]; ok {
		l.mu.Unlock()
		select {
		case <-c.done:
			return c.tenants, c.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c := &lookupCall{done: make(chan struct{})}
	l.inflight[key] = c
	l.mu.Unlock()

	c.tenants, c.err = l.query(ctx, ti, tenantCol, keyCol, val)

	l.mu.Lock()
	delete(l.inflight, key)
	l.mu.Unlock()
	close(c.done)
	return c.tenants, c.err
}

func (l *Lookup) query(ctx context.Context, ti *TableInfo, tenantCol, keyCol, val string) ([]string, error) {
	q := fmt.Sprintf("SELECT DISTINCT %s::text FROM %s WHERE %s = $1 LIMIT 50",
		ti.quoted(tenantCol), ti.qualified(), ti.quoted(keyCol))
	rows, err := l.pool.Query(ctx, q, val)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t *string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		if t != nil {
			out = append(out, *t)
		}
	}
	return out, rows.Err()
}

func (l *Lookup) viaTenants(ctx context.Context, s bridgeSpec, val string) ([]string, error) {
	key := lookupKey{s.table(), s.bk, bridgeVal(normVal(val))}
	l.mu.Lock()
	if c, ok := l.inflight[key]; ok {
		l.mu.Unlock()
		select {
		case <-c.done:
			return c.tenants, c.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	c := &lookupCall{done: make(chan struct{})}
	l.inflight[key] = c
	l.mu.Unlock()

	c.tenants, c.err = l.viaQuery(ctx, s, val)

	l.mu.Lock()
	delete(l.inflight, key)
	l.mu.Unlock()
	close(c.done)
	return c.tenants, c.err
}

func (l *Lookup) viaQuery(ctx context.Context, s bridgeSpec, val string) ([]string, error) {
	// FROM b JOIN m1 ON m1.join = b.bfk JOIN m2 ON m2.join = m1.next ... JOIN t ON t.tjoin = mN.next
	from := s.b.qualified() + " b"
	prev, prevCol := "b", s.b.quoted(s.bfk)
	for i, m := range s.mids {
		alias := fmt.Sprintf("m%d", i+1)
		from += fmt.Sprintf(" JOIN %s %s ON %s.%s = %s.%s", m.ti.qualified(), alias, alias, m.ti.quoted(m.join), prev, prevCol)
		prev, prevCol = alias, m.ti.quoted(m.next)
	}
	from += fmt.Sprintf(" JOIN %s t ON t.%s = %s.%s", s.t.qualified(), s.t.quoted(s.tjoin), prev, prevCol)
	q := fmt.Sprintf("SELECT DISTINCT t.%s::text FROM %s WHERE b.%s = $1 LIMIT 50", s.t.quoted(s.ttenant), from, s.b.quoted(s.bk))
	rows, err := l.pool.Query(ctx, q, val)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t *string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		if t != nil {
			out = append(out, *t)
		}
	}
	return out, rows.Err()
}

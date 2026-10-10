package attrib

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

type cacheKey struct{ table, col, val string }

type cacheEntry struct {
	key     cacheKey
	tenant  string
	expires time.Time // zero: never
}

// RowCache remembers which tenant owns a row, keyed by (table, key column,
// value). It is shared by every connection and bounded by LRU eviction.
type RowCache struct {
	mu  sync.Mutex
	max int
	ttl time.Duration
	now func() time.Time
	ll  *list.List
	m   map[cacheKey]*list.Element
}

func NewRowCache(max int) *RowCache { return NewRowCacheTTL(max, 0) }

// NewRowCacheTTL returns a cache whose entries are trusted for ttl after they
// were last written (0: until evicted). A row's tenant almost never changes,
// but the proxy cannot see every delete; the TTL bounds how long a stale entry
// can survive.
func NewRowCacheTTL(max int, ttl time.Duration) *RowCache {
	if max <= 0 {
		max = 100_000
	}
	return &RowCache{max: max, ttl: ttl, now: time.Now, ll: list.New(), m: map[cacheKey]*list.Element{}}
}

func (c *RowCache) deadline() time.Time {
	if c.ttl <= 0 {
		return time.Time{}
	}
	return c.now().Add(c.ttl)
}

// normVal makes equal values compare equal: UUIDs are case-insensitive.
func normVal(v string) string {
	if len(v) == 36 && v[8] == '-' && v[13] == '-' && v[18] == '-' && v[23] == '-' {
		return strings.ToLower(v)
	}
	return v
}

func (c *RowCache) Get(table, col, val string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey{table, col, normVal(val)}
	el, ok := c.m[k]
	if !ok {
		return "", false
	}
	e := el.Value.(*cacheEntry)
	if !e.expires.IsZero() && c.now().After(e.expires) {
		delete(c.m, k)
		c.ll.Remove(el)
		return "", false
	}
	c.ll.MoveToFront(el)
	return e.tenant, true
}

func (c *RowCache) Put(table, col, val, tenant string) {
	k := cacheKey{table, col, normVal(val)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[k]; ok {
		e := el.Value.(*cacheEntry)
		e.tenant, e.expires = tenant, c.deadline()
		c.ll.MoveToFront(el)
		return
	}
	c.m[k] = c.ll.PushFront(&cacheEntry{key: k, tenant: tenant, expires: c.deadline()})
	for c.ll.Len() > c.max {
		last := c.ll.Back()
		delete(c.m, last.Value.(*cacheEntry).key)
		c.ll.Remove(last)
	}
}

func (c *RowCache) Delete(table, col, val string) {
	k := cacheKey{table, col, normVal(val)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[k]; ok {
		delete(c.m, k)
		c.ll.Remove(el)
	}
}

// DropTable forgets everything learned about a table.
func (c *RowCache) DropTable(table string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, el := range c.m {
		if k.table == table {
			delete(c.m, k)
			c.ll.Remove(el)
		}
	}
}

func (c *RowCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Clear forgets everything.
func (c *RowCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	c.m = map[cacheKey]*list.Element{}
}

// bridgeTTL: what a credential maps to is trusted only briefly, whatever the
// row cache is set to. Tokens are revoked and sessions end.
func bridgeTTL(rowTTL time.Duration) time.Duration {
	const max = time.Minute
	if rowTTL > 0 && rowTTL < max {
		return rowTTL
	}
	return max
}

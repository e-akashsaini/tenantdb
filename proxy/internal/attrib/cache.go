package attrib

import (
	"container/list"
	"strings"
	"sync"
)

type cacheKey struct{ table, col, val string }

type cacheEntry struct {
	key    cacheKey
	tenant string
}

// RowCache remembers which tenant owns a row, keyed by (table, key column,
// value). It is shared by every connection and bounded by LRU eviction.
type RowCache struct {
	mu  sync.Mutex
	max int
	ll  *list.List
	m   map[cacheKey]*list.Element
}

func NewRowCache(max int) *RowCache {
	if max <= 0 {
		max = 100_000
	}
	return &RowCache{max: max, ll: list.New(), m: map[cacheKey]*list.Element{}}
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
	el, ok := c.m[cacheKey{table, col, normVal(val)}]
	if !ok {
		return "", false
	}
	c.ll.MoveToFront(el)
	return el.Value.(*cacheEntry).tenant, true
}

func (c *RowCache) Put(table, col, val, tenant string) {
	k := cacheKey{table, col, normVal(val)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[k]; ok {
		el.Value.(*cacheEntry).tenant = tenant
		c.ll.MoveToFront(el)
		return
	}
	c.m[k] = c.ll.PushFront(&cacheEntry{k, tenant})
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

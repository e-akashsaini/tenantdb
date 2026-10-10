package attrib

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Polymorphic associations, the Rails way: a child row says which parent it
// belongs to with two columns, X_type (a class name such as "Template") and X_id.
// The table the id refers to is only known from the type value, so a lookup like
//
//	WHERE record_type = 'Template' AND record_id = 5
//
// is resolved by mapping the type to a table and asking that table's row 5 which
// tenant it belongs to. The mapping (class name to table name) follows Rails'
// naming rules and, like an inferred foreign key, is used only after checking a
// sample of real rows: a guess that attributed a query to the wrong tenant would
// be worse than no attribution.

var (
	acronymBoundary = regexp.MustCompile(`([A-Z]+)([A-Z][a-z])`)
	wordBoundary    = regexp.MustCompile(`([a-z\d])([A-Z])`)
)

// railsTableNames lists the table names a Rails class name might be stored in:
// "Template" -> templates, "ActiveStorage::Blob" -> active_storage_blobs.
func railsTableNames(class string) []string {
	s := strings.ReplaceAll(class, "::", "_")
	s = acronymBoundary.ReplaceAllString(s, "${1}_${2}")
	s = wordBoundary.ReplaceAllString(s, "${1}_${2}")
	s = strings.ToLower(s)
	if s == "" {
		return nil
	}
	return []string{pluralize(s), s, s + "s", s + "es"}
}

func pluralize(w string) string {
	switch {
	case strings.HasSuffix(w, "s"), strings.HasSuffix(w, "x"), strings.HasSuffix(w, "z"),
		strings.HasSuffix(w, "ch"), strings.HasSuffix(w, "sh"):
		return w + "es"
	case strings.HasSuffix(w, "y") && len(w) > 1 && !strings.ContainsRune("aeiou", rune(w[len(w)-2])):
		return strings.TrimSuffix(w, "y") + "ies"
	}
	return w + "s"
}

// HasPolyID reports whether col is the id half of a polymorphic pair in ti: it
// ends in _id and the table also has the matching _type column.
func (t *TableInfo) HasPolyID(col string) bool {
	base, ok := strings.CutSuffix(col, "_id")
	if !ok || base == "" {
		return false
	}
	for _, c := range t.Cols {
		if c == base+"_type" {
			return true
		}
	}
	return false
}

// Poly returns the verified parent table a polymorphic pair points at when the
// type column holds typeValue.
func (c *Catalog) Poly(ti *TableInfo, base, typeValue string) (FKRef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ref, ok := c.polys[ti.Key()][base][typeValue]
	return ref, ok
}

// verifyPolys finds the polymorphic pairs of tables that carry a tenant and, for
// each type value in use, the table it names, keeping those that check out.
func (c *Catalog) verifyPolys(ctx context.Context, conn *pgx.Conn, tables map[string]*TableInfo, tcol func(*TableInfo) string) map[string]map[string]map[string]FKRef {
	out := map[string]map[string]map[string]FKRef{}
	now := time.Now()
	for _, child := range tables {
		if child.Schema == "pg_temp" || tcol(child) == "" {
			continue
		}
		for _, typeCol := range child.Cols {
			base, ok := strings.CutSuffix(typeCol, "_type")
			if !ok || base == "" || !child.HasPolyID(base+"_id") {
				continue
			}
			for _, v := range distinctValues(ctx, conn, child, typeCol) {
				parent := railsTable(tables, child.Schema, v)
				if parent == nil || parent == child || tcol(parent) == "" || !parent.IsUnique("id") {
					continue
				}
				id := child.Key() + "." + base + "#" + v + "->" + parent.Key()
				c.mu.RLock()
				verdict, memo := c.fkMemo[id]
				c.mu.RUnlock()
				ttl := fkMemoTTL
				if memo && verdict.inconclusive {
					ttl = fkInconclusiveTTL
				}
				if !memo || now.Sub(verdict.at) > ttl {
					good, inconclusive := verifyPoly(ctx, conn, child, base, v, parent, tcol(child), tcol(parent))
					verdict = fkVerdict{ok: good, inconclusive: inconclusive, at: now}
					c.mu.Lock()
					if c.fkMemo == nil {
						c.fkMemo = map[string]fkVerdict{}
					}
					c.fkMemo[id] = verdict
					c.mu.Unlock()
				}
				if !verdict.ok {
					continue
				}
				if out[child.Key()] == nil {
					out[child.Key()] = map[string]map[string]FKRef{}
				}
				if out[child.Key()][base] == nil {
					out[child.Key()][base] = map[string]FKRef{}
				}
				out[child.Key()][base][v] = FKRef{ParentKey: parent.Key(), ParentCol: "id"}
			}
		}
	}
	return out
}

func railsTable(tables map[string]*TableInfo, schema, class string) *TableInfo {
	for _, name := range railsTableNames(class) {
		if t := tables[strings.ToLower(schema)+"."+name]; t != nil {
			return t
		}
	}
	return nil
}

// distinctValues samples the values of a type column.
func distinctValues(ctx context.Context, conn *pgx.Conn, t *TableInfo, col string) []string {
	q := fmt.Sprintf("SELECT DISTINCT %[1]s::text FROM (SELECT %[1]s FROM %[2]s WHERE %[1]s IS NOT NULL LIMIT %[3]d) s LIMIT 100",
		t.quoted(col), t.qualified(), fkSampleRows)
	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if rows.Scan(&v) == nil {
			out = append(out, v)
		}
	}
	return out
}

// verifyPoly is verifyFK for the rows of child whose type column holds typeValue.
func verifyPoly(ctx context.Context, conn *pgx.Conn, child *TableInfo, base, typeValue string, parent *TableInfo, childTenant, parentTenant string) (ok, inconclusive bool) {
	q := fmt.Sprintf(`SELECT count(*) FILTER (WHERE c.t IS NOT NULL AND p.%[1]s IS NOT NULL),
       count(*) FILTER (WHERE c.t IS DISTINCT FROM p.%[1]s)
FROM (SELECT %[2]s AS k, %[3]s AS t FROM %[4]s WHERE %[5]s = $1 AND %[2]s IS NOT NULL LIMIT %[6]d) c
JOIN %[7]s p ON p.%[8]s = c.k`,
		parent.quoted(parentTenant), child.quoted(base+"_id"), child.quoted(childTenant), child.qualified(),
		child.quoted(base+"_type"), fkSampleRows, parent.qualified(), parent.quoted("id"))
	var matched, mismatched int64
	if err := conn.QueryRow(ctx, q, typeValue).Scan(&matched, &mismatched); err != nil {
		return false, false
	}
	if matched == 0 && mismatched == 0 {
		return false, true
	}
	return matched > 0 && mismatched == 0, false
}

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

	unique map[string]bool // columns with a single-column, non-partial unique index
	rls    bool            // row-level security is enabled

	viewDef string    // a view's definition (schema-qualified), if it could be read
	vp      viewParse // ... parsed once (views.go)
}

// Key is the name the row cache uses for this table.
func (t *TableInfo) Key() string { return strings.ToLower(t.Schema + "." + t.Name) }

// IsUnique reports whether col is provably unique in this table: it has a
// single-column unique index (a primary key or unique constraint) that is not
// partial. Only such a column can identify a row.
func (t *TableInfo) IsUnique(col string) bool { return t.unique[col] }

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

// FuncDef is an application-defined function or procedure as stored in pg_proc.
type FuncDef struct {
	Schema, Name string
	Lang         string
	Kind         byte // f function, p procedure, a aggregate, w window
	Src          string
	ArgNames     []string
	NArgs        int
	HasModes     bool   // some arguments are OUT/INOUT/VARIADIC/TABLE
	Ret          string // what it returns: "void", "trigger", "set" (SETOF / TABLE) or "scalar"; "" is read as void
}

// isSystemSchema holds relations that never contain tenant data.
func isSystemSchema(s string) bool { return s == "pg_catalog" || s == "information_schema" }

// defaultSearchPath is what Postgres uses until something changes it.
var defaultSearchPath = []string{"$user", "public"}

// Catalog is a snapshot of the database schema, loaded over its own
// connection (a read-only role is enough). It answers "which tables carry a
// tenant column", "which columns are unique", "what is the column order",
// "which functions are user code" and "what search_path does this role get"
// without touching live traffic.
type Catalog struct {
	dsn       string
	mu        sync.RWMutex
	tables    map[string]*TableInfo // "schema.name", lower-cased
	funcs     map[string][]*FuncDef // "schema.name" of user-defined functions
	triggers  map[string][]trigRow  // table key -> its enabled, non-internal triggers
	ruled     map[string]bool       // tables with rewrite rules (CREATE RULE): their actions are not read
	rls       *rlsData              // roles, table ownership and policies (rls.go)
	gen       uint64
	loaded    bool
	reloading atomic.Bool

	// TenantColumns names the columns that hold the tenant. Set it before Load:
	// foreign keys can only be verified when the tenant column is known.
	TenantColumns []string
	fks           map[string]map[string]FKRef            // table key -> column -> the verified parent row it points at
	fkMemo        map[string]fkVerdict                   // verdicts kept across reloads
	polys         map[string]map[string]map[string]FKRef // table -> association -> type value -> parent

	// relationships learned from joins in traffic, checked in the background (learn.go)
	learned map[string]map[string]FKRef
	sugMu   sync.Mutex
	sugSeen map[string]time.Time
	sugQ    chan fkCand
	sugWG   sync.WaitGroup

	bypassRLS  bool                   // the loading role is not subject to row-level security
	roleSearch map[[2]string][]string // (role, database) -> search_path from ALTER ROLE/DATABASE ... SET
	baseSearch []string
}

func NewCatalog(dsn string) *Catalog { return &Catalog{dsn: dsn, fkMemo: map[string]fkVerdict{}} }

// FKRef says a column points at a column of a parent table.
type FKRef struct{ ParentKey, ParentCol string }

type fkVerdict struct {
	ok           bool
	inconclusive bool // no rows to judge by yet: look again soon
	at           time.Time
}

const (
	fkMemoTTL         = time.Hour
	fkInconclusiveTTL = 30 * time.Second
	fkSampleRows      = 5000
)

// pg_catalog rather than information_schema: the latter hides columns the role
// cannot read, which would make shared and tenant tables look the same.
const catalogSQL = `
SELECT n.nspname, c.relname, c.relkind::text, a.attname, c.relrowsecurity
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','p','v','m','f')
  AND a.attnum > 0 AND NOT a.attisdropped
  AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'
ORDER BY n.nspname, c.relname, a.attnum`

// Single-column unique indexes, excluding partial and expression indexes: only
// those make a column's value identify exactly one row.
const uniqueSQL = `
SELECT n.nspname, c.relname, a.attname
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
WHERE i.indisunique AND i.indisvalid AND i.indnatts = 1 AND i.indpred IS NULL AND i.indexprs IS NULL
  AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%'`

// Functions that belong to an extension (deptype 'e') are not user code.
const funcSQL = `
SELECT n.nspname, p.proname, l.lanname, p.prokind::text, p.prosrc,
       COALESCE(p.proargnames, '{}'::text[]), p.pronargs::int, p.proargmodes IS NOT NULL,
       CASE WHEN p.prorettype = 'void'::regtype THEN 'void' WHEN p.prorettype = 'trigger'::regtype THEN 'trigger'
            WHEN p.proretset THEN 'set' ELSE 'scalar' END
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
JOIN pg_language l ON l.oid = p.prolang
WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
  AND NOT EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e')`

// View definitions, with every name schema-qualified (an empty search path makes
// pg_get_viewdef spell out the schema), so they mean the same to every session.
const viewDefSQL = `
SELECT n.nspname, c.relname, pg_get_viewdef(c.oid, false)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v' AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'
  AND n.nspname NOT LIKE 'pg_temp%'`

// Triggers a statement on a table can fire. Internal ones (foreign keys) are not
// application code.
const triggerSQL = `
SELECT n.nspname, c.relname, t.tgtype::int, pn.nspname, p.proname
FROM pg_trigger t
JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_namespace pn ON pn.oid = p.pronamespace
WHERE NOT t.tgisinternal AND t.tgenabled IN ('O', 'A')
  AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'`

// CREATE RULE ... ON INSERT/UPDATE/DELETE rewrites a statement into others. pg_rules
// leaves out the rules that make a view a view.
const rulesSQL = `
SELECT DISTINCT schemaname, tablename FROM pg_rules
WHERE schemaname NOT IN ('pg_catalog','information_schema')`

const roleSettingSQL = `
SELECT COALESCE(r.rolname, ''), COALESCE(d.datname, ''), s.setconfig
FROM pg_db_role_setting s
LEFT JOIN pg_roles r ON r.oid = s.setrole
LEFT JOIN pg_database d ON d.oid = s.setdatabase`

// Declared single-column foreign keys.
const fkSQL = `
SELECT cn.nspname, c.relname, a.attname, pn.nspname, p.relname, pa.attname
FROM pg_constraint k
JOIN pg_class c ON c.oid = k.conrelid JOIN pg_namespace cn ON cn.oid = c.relnamespace
JOIN pg_class p ON p.oid = k.confrelid JOIN pg_namespace pn ON pn.oid = p.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.conkey[1]
JOIN pg_attribute pa ON pa.attrelid = p.oid AND pa.attnum = k.confkey[1]
WHERE k.contype = 'f' AND array_length(k.conkey, 1) = 1`

type colRow struct {
	schema, table string
	kind          byte // 0 means an ordinary table
	col           string
	rls           bool
}

// snapshotData is everything one Load reads.
type snapshotData struct {
	cols       []colRow
	unique     map[string]map[string]bool // table key -> unique columns
	funcs      []*FuncDef
	bypassRLS  bool
	roleSearch map[[2]string][]string
	baseSearch []string
	fks        map[string]map[string]FKRef // tests only; Load computes them itself
	polys      map[string]map[string]map[string]FKRef
	viewdefs   map[string]string // table key -> definition
	triggers   map[string][]trigRow
	ruled      map[string]bool
	rls        *rlsData
}

// trigRow is one trigger: tgtype's bits say when it fires (insert 4, delete 8,
// update 16, truncate 32) and fn names its function as schema.name.
type trigRow struct {
	typ int
	fn  string
}

// Trigger event bits, as in pg_trigger.tgtype.
const (
	trigInsert   = 4
	trigDelete   = 8
	trigUpdate   = 16
	trigTruncate = 32
)

// Load replaces the snapshot with the current schema.
func (c *Catalog) Load(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, c.dsn)
	if err != nil {
		return fmt.Errorf("catalog connect: %w", err)
	}
	defer conn.Close(ctx)
	d := snapshotData{unique: map[string]map[string]bool{}, roleSearch: map[[2]string][]string{}}

	rows, err := conn.Query(ctx, catalogSQL)
	if err != nil {
		return fmt.Errorf("catalog query: %w", err)
	}
	for rows.Next() {
		var r colRow
		var kind string
		if err := rows.Scan(&r.schema, &r.table, &kind, &r.col, &r.rls); err != nil {
			rows.Close()
			return err
		}
		if kind != "" {
			r.kind = kind[0]
		}
		d.cols = append(d.cols, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	urows, err := conn.Query(ctx, uniqueSQL)
	if err != nil {
		return fmt.Errorf("catalog unique indexes: %w", err)
	}
	for urows.Next() {
		var s, t, col string
		if err := urows.Scan(&s, &t, &col); err != nil {
			urows.Close()
			return err
		}
		k := strings.ToLower(s + "." + t)
		if d.unique[k] == nil {
			d.unique[k] = map[string]bool{}
		}
		d.unique[k][strings.ToLower(col)] = true
	}
	urows.Close()
	if err := urows.Err(); err != nil {
		return err
	}

	frows, err := conn.Query(ctx, funcSQL)
	if err != nil {
		return fmt.Errorf("catalog functions: %w", err)
	}
	for frows.Next() {
		f := &FuncDef{}
		var kind string
		if err := frows.Scan(&f.Schema, &f.Name, &f.Lang, &kind, &f.Src, &f.ArgNames, &f.NArgs, &f.HasModes, &f.Ret); err != nil {
			frows.Close()
			return err
		}
		if kind != "" {
			f.Kind = kind[0]
		}
		d.funcs = append(d.funcs, f)
	}
	frows.Close()
	if err := frows.Err(); err != nil {
		return err
	}

	d.viewdefs = loadViewDefs(ctx, conn)
	d.triggers = loadTriggers(ctx, conn)
	d.ruled = loadRuled(ctx, conn)
	d.rls = loadRLS(ctx, conn)

	// Row-level security can blind the lookup role; know whether it is subject to it.
	if err := conn.QueryRow(ctx, `SELECT COALESCE((SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user), false)`).Scan(&d.bypassRLS); err != nil {
		return fmt.Errorf("catalog rls: %w", err)
	}

	srows, err := conn.Query(ctx, roleSettingSQL)
	if err != nil {
		return fmt.Errorf("catalog role settings: %w", err)
	}
	for srows.Next() {
		var role, db string
		var conf []string
		if err := srows.Scan(&role, &db, &conf); err != nil {
			srows.Close()
			return err
		}
		for _, kv := range conf {
			if k, v, ok := strings.Cut(kv, "="); ok && strings.EqualFold(k, "search_path") {
				d.roleSearch[[2]string{role, db}] = splitPath(v)
			}
		}
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return err
	}
	var base string
	if err := conn.QueryRow(ctx, `SELECT reset_val FROM pg_settings WHERE name = 'search_path'`).Scan(&base); err == nil {
		d.baseSearch = splitPath(base)
	}

	c.apply(d)
	c.loadFKs(ctx, conn)
	return nil
}

// loadTriggers reads the application's triggers. If the role cannot see them the
// map is nil and statements are analysed as if there were none.
func loadTriggers(ctx context.Context, conn *pgx.Conn) map[string][]trigRow {
	rows, err := conn.Query(ctx, triggerSQL)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := map[string][]trigRow{}
	for rows.Next() {
		var s, t, fs, fn string
		var typ int
		if err := rows.Scan(&s, &t, &typ, &fs, &fn); err != nil {
			return out
		}
		k := strings.ToLower(s + "." + t)
		out[k] = append(out[k], trigRow{typ: typ, fn: strings.ToLower(fs + "." + fn)})
	}
	return out
}

func loadRuled(ctx context.Context, conn *pgx.Conn) map[string]bool {
	rows, err := conn.Query(ctx, rulesSQL)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var s, t string
		if err := rows.Scan(&s, &t); err != nil {
			return out
		}
		out[strings.ToLower(s+"."+t)] = true
	}
	return out
}

// HasRules reports whether a rewrite rule is defined on ti.
func (c *Catalog) HasRules(ti *TableInfo) bool {
	if c == nil || ti == nil {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ruled[ti.Key()]
}

// TrigDef is a trigger a statement may fire.
type TrigDef struct {
	Funcs []*FuncDef // the function(s) it could be; none if it cannot be found
}

// Triggers returns the triggers on ti that fire for the given event bits
// (trigInsert | trigUpdate | ...).
func (c *Catalog) Triggers(ti *TableInfo, events int) []TrigDef {
	if c == nil || ti == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []TrigDef
	for _, t := range c.triggers[ti.Key()] {
		if t.typ&events != 0 {
			out = append(out, TrigDef{Funcs: c.funcs[t.fn]})
		}
	}
	return out
}

// loadViewDefs reads every view's definition. A role that cannot read some of
// them simply gets none for those: the views stay opaque, as before.
func loadViewDefs(ctx context.Context, conn *pgx.Conn) map[string]string {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL search_path = pg_catalog`); err != nil {
		return nil
	}
	rows, err := tx.Query(ctx, viewDefSQL)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var s, n string
		var def *string
		if err := rows.Scan(&s, &n, &def); err != nil {
			return out
		}
		if def != nil && len(*def) <= 1<<20 {
			out[strings.ToLower(s+"."+n)] = *def
		}
	}
	return out
}

func (c *Catalog) apply(d snapshotData) {
	m := map[string]*TableInfo{}
	for _, r := range d.cols {
		key := strings.ToLower(r.schema + "." + r.table)
		ti := m[key]
		if ti == nil {
			kind := r.kind
			if kind == 0 {
				kind = 'r'
			}
			ti = &TableInfo{Schema: r.schema, Name: r.table, Kind: kind, rls: r.rls, unique: d.unique[key], viewDef: d.viewdefs[key]}
			m[key] = ti
		}
		ti.Cols = append(ti.Cols, strings.ToLower(r.col))
		ti.raw = append(ti.raw, r.col)
	}
	funcs := map[string][]*FuncDef{}
	for _, f := range d.funcs {
		k := strings.ToLower(f.Schema + "." + f.Name)
		funcs[k] = append(funcs[k], f)
	}
	c.mu.Lock()
	c.tables, c.funcs, c.triggers, c.ruled, c.rls, c.loaded = m, funcs, d.triggers, d.ruled, d.rls, true
	c.bypassRLS, c.roleSearch, c.baseSearch = d.bypassRLS, d.roleSearch, d.baseSearch
	if d.fks != nil {
		c.fks = d.fks
	}
	if d.polys != nil {
		c.polys = d.polys
	}
	c.gen++
	c.mu.Unlock()
}

type fkCand struct {
	child    *TableInfo
	ccol     string
	parent   *TableInfo
	pcol     string
	inferred bool
}

func (f fkCand) id() string {
	return f.child.Key() + "." + f.ccol + "->" + f.parent.Key() + "." + f.pcol
}

// loadFKs works out which columns point at a row of a tenant table, so a lookup
// by that column (WHERE submission_id = 5) can be attributed through the parent
// row's tenant. Candidates are the declared foreign keys plus, for the many
// schemas (Rails among them) that declare none, columns named <thing>_id next
// to a table called <thing> or <thing>s. A candidate is used only if both tables
// carry the tenant column, the parent column is unique, and a sample of child
// rows always has the same tenant as the parent row it points at: a wrong guess
// would attribute queries to the wrong tenant, which is worse than none.
func (c *Catalog) loadFKs(ctx context.Context, conn *pgx.Conn) {
	if len(c.TenantColumns) == 0 {
		return
	}
	tset := lowerSet(c.TenantColumns)
	tcol := func(t *TableInfo) string {
		for _, col := range t.Cols {
			if tset[col] {
				return col
			}
		}
		return ""
	}
	c.mu.RLock()
	tables := make(map[string]*TableInfo, len(c.tables))
	for k, v := range c.tables {
		tables[k] = v
	}
	c.mu.RUnlock()

	var cands []fkCand
	seen := map[string]bool{}
	add := func(child *TableInfo, ccol string, parent *TableInfo, pcol string, inferred bool) {
		if child == nil || parent == nil || child == parent || tset[ccol] || child.Schema == "pg_temp" ||
			tcol(child) == "" || tcol(parent) == "" || !parent.IsUnique(pcol) {
			return
		}
		f := fkCand{child, ccol, parent, pcol, inferred}
		if !seen[f.id()] {
			seen[f.id()] = true
			cands = append(cands, f)
		}
	}
	declared := map[string]bool{}
	if rows, err := conn.Query(ctx, fkSQL); err == nil {
		for rows.Next() {
			var cs, ct, cc, ps, pt, pc string
			if rows.Scan(&cs, &ct, &cc, &ps, &pt, &pc) != nil {
				continue
			}
			child, parent := tables[strings.ToLower(cs+"."+ct)], tables[strings.ToLower(ps+"."+pt)]
			declared[strings.ToLower(cs+"."+ct+"."+cc)] = true
			add(child, strings.ToLower(cc), parent, strings.ToLower(pc), false)
		}
		rows.Close()
	}
	for _, child := range tables {
		for _, col := range child.Cols {
			if !strings.HasSuffix(col, "_id") || col == "_id" || declared[strings.ToLower(child.Key())+"."+col] {
				continue
			}
			base := strings.TrimSuffix(col, "_id")
			for _, name := range pluralForms(base) {
				if parent := tables[strings.ToLower(child.Schema)+"."+name]; parent != nil && parent != child {
					add(child, col, parent, "id", true)
					break
				}
			}
		}
	}

	out := map[string]map[string]FKRef{}
	now := time.Now()
	for _, f := range cands {
		c.mu.RLock()
		v, memo := c.fkMemo[f.id()]
		c.mu.RUnlock()
		ttl := fkMemoTTL
		if memo && v.inconclusive {
			ttl = fkInconclusiveTTL
		}
		if !memo || now.Sub(v.at) > ttl {
			ok, inconclusive := verifyFK(ctx, conn, f, tcol(f.child), tcol(f.parent))
			v = fkVerdict{ok: ok, inconclusive: inconclusive, at: now}
			c.mu.Lock()
			if c.fkMemo == nil {
				c.fkMemo = map[string]fkVerdict{}
			}
			c.fkMemo[f.id()] = v
			c.mu.Unlock()
		}
		if !v.ok {
			continue
		}
		k := f.child.Key()
		if out[k] == nil {
			out[k] = map[string]FKRef{}
		}
		out[k][f.ccol] = FKRef{ParentKey: f.parent.Key(), ParentCol: f.pcol}
	}
	polys := c.verifyPolys(ctx, conn, tables, tcol)
	c.mu.Lock()
	c.fks, c.polys = c.mergeLearned(out), polys
	c.mu.Unlock()
}

// verifyFK samples child rows and checks every one that points at a parent row
// has that parent's tenant. At least one row must match, and none may differ.
// inconclusive means there was nothing to judge by (empty tables), not that the
// guess is wrong.
func verifyFK(ctx context.Context, conn *pgx.Conn, f fkCand, childTenant, parentTenant string) (ok, inconclusive bool) {
	q := fmt.Sprintf(`SELECT count(*) FILTER (WHERE c.t IS NOT NULL AND p.%[1]s IS NOT NULL),
       count(*) FILTER (WHERE c.t IS DISTINCT FROM p.%[1]s)
FROM (SELECT %[2]s AS k, %[3]s AS t FROM %[4]s WHERE %[2]s IS NOT NULL LIMIT %[5]d) c
JOIN %[6]s p ON p.%[7]s = c.k`,
		f.parent.quoted(parentTenant), f.child.quoted(f.ccol), f.child.quoted(childTenant), f.child.qualified(), fkSampleRows,
		f.parent.qualified(), f.parent.quoted(f.pcol))
	var matched, mismatched int64
	if err := conn.QueryRow(ctx, q).Scan(&matched, &mismatched); err != nil {
		return false, false // e.g. the column types do not even compare
	}
	if matched == 0 && mismatched == 0 {
		return false, true
	}
	return matched > 0 && mismatched == 0, false
}

// pluralForms lists the table names a column "<base>_id" might point at.
func pluralForms(base string) []string {
	out := []string{base, base + "s", base + "es"}
	if strings.HasSuffix(base, "y") {
		out = append(out, strings.TrimSuffix(base, "y")+"ies")
	}
	return out
}

// FK returns the verified parent row a column of ti points at.
func (c *Catalog) FK(ti *TableInfo, col string) (FKRef, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ref, ok := c.fks[ti.Key()][col]
	return ref, ok
}

// TableByKey finds a table by its schema.name key.
func (c *Catalog) TableByKey(key string) (*TableInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ti, ok := c.tables[key]
	return ti, ok
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

// Generation changes every time the snapshot is replaced.
func (c *Catalog) Generation() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.gen
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

// Funcs returns the application-defined functions ref could name (every
// overload), resolved like Table.
func (c *Catalog) Funcs(ref TableRef, searchPath []string, user string) []*FuncDef {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ref.Schema != "" {
		return c.funcs[ref.Schema+"."+ref.Name]
	}
	var out []*FuncDef
	for _, s := range searchPath {
		if s == "$user" {
			s = strings.ToLower(user)
		}
		out = append(out, c.funcs[s+"."+ref.Name]...)
	}
	return out
}

// UserFunc reports whether ref is a function written by the application (not
// built in, not from an extension).
func (c *Catalog) UserFunc(ref TableRef, searchPath []string, user string) bool {
	return len(c.Funcs(ref, searchPath, user)) > 0
}

// DefaultSearchPath is the search_path a new connection of user to db starts
// with: ALTER ROLE ... IN DATABASE ... SET, then ALTER ROLE, then ALTER
// DATABASE, then the server-wide ALTER ... ALL SET, then the server default.
func (c *Catalog) DefaultSearchPath(user, db string) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, k := range [][2]string{{user, db}, {user, ""}, {"", db}, {"", ""}} {
		if p, ok := c.roleSearch[k]; ok {
			return p
		}
	}
	if len(c.baseSearch) > 0 {
		return c.baseSearch
	}
	return defaultSearchPath
}

// CanRead reports whether the catalog's role sees every row of ti. Row-level
// security hides rows from a role that does not bypass it, so a lookup or a
// warm-up against such a table would silently miss rows.
func (c *Catalog) CanRead(ti *TableInfo) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !ti.rls || c.bypassRLS
}

// BlindTables lists tables the catalog's role cannot fully read because of
// row-level security, for a start-up warning.
func (c *Catalog) BlindTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.bypassRLS {
		return nil
	}
	var out []string
	for _, t := range c.tables {
		if t.rls {
			out = append(out, t.Key())
		}
	}
	sort.Strings(out)
	return out
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

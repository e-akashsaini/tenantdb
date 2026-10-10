package attrib

import (
	"reflect"
	"testing"
)

func testExtractor() *Extractor { return NewExtractor([]string{"tenant_id"}, []string{"id"}) }

func TestSQLFunctionBodyBindsNamedParameters(t *testing.T) {
	def := &FuncDef{Schema: "public", Name: "orders_of", Lang: "sql", Kind: 'f', NArgs: 2,
		ArgNames: []string{"p_tenant", "p_min"},
		Src:      `SELECT * FROM orders WHERE tenant_id = p_tenant AND amount > p_min`}
	b := testExtractor().bodyOf(def)
	if b.Opaque || len(b.Stmts) != 1 {
		t.Fatalf("body = %+v", b)
	}
	if got := b.Stmts[0].Params; !reflect.DeepEqual(got, []int{1}) {
		t.Errorf("tenant comes from argument %v, want [1]", got)
	}
	if got := b.Stmts[0].Tables; len(got) != 1 || got[0].Name != "orders" {
		t.Errorf("tables = %v", got)
	}
}

func TestSQLFunctionBodyPositionalAndQualifiedParameters(t *testing.T) {
	pos := &FuncDef{Name: "f", Lang: "sql", Kind: 'f', NArgs: 1, Src: `SELECT 1 FROM orders WHERE tenant_id = $1`}
	if b := testExtractor().bodyOf(pos); !reflect.DeepEqual(b.Stmts[0].Params, []int{1}) {
		t.Errorf("positional: %+v", b.Stmts[0].Params)
	}
	q := &FuncDef{Name: "myfn", Lang: "sql", Kind: 'f', NArgs: 1, ArgNames: []string{"t"}, Src: `SELECT 1 FROM orders WHERE tenant_id = myfn.t`}
	if b := testExtractor().bodyOf(q); !reflect.DeepEqual(b.Stmts[0].Params, []int{1}) {
		t.Errorf("function-qualified: %+v", b.Stmts[0].Params)
	}
}

func TestPlpgsqlBodyStatementsAreExtracted(t *testing.T) {
	def := &FuncDef{Name: "work", Lang: "plpgsql", Kind: 'f', NArgs: 1, ArgNames: []string{"p_tenant"}, Src: `
DECLARE n int;
BEGIN
  SELECT count(*) INTO n FROM orders WHERE tenant_id = p_tenant;
  IF n > 10 THEN
    UPDATE orders SET amount = 0 WHERE tenant_id = p_tenant;
  END IF;
  PERFORM 1 FROM countries WHERE code = 'IE';
  RETURN;
END`}
	b := testExtractor().bodyOf(def)
	if b.Opaque {
		t.Fatalf("body should be readable: %+v", b)
	}
	tables := map[string]bool{}
	var tenantArgs int
	for _, s := range b.Stmts {
		for _, tb := range s.Tables {
			tables[tb.Name] = true
		}
		if reflect.DeepEqual(s.Params, []int{1}) {
			tenantArgs++
		}
	}
	if !tables["orders"] || !tables["countries"] {
		t.Errorf("tables = %v", tables)
	}
	if tenantArgs < 2 {
		t.Errorf("both orders statements filter on argument 1, found %d", tenantArgs)
	}
}

func TestDynamicSQLAndOtherLanguagesAreOpaque(t *testing.T) {
	e := testExtractor()
	for name, def := range map[string]*FuncDef{
		"execute":     {Name: "a", Lang: "plpgsql", Kind: 'f', Src: `BEGIN EXECUTE 'SELECT * FROM ' || quote_ident('orders'); END`},
		"for exec":    {Name: "b", Lang: "plpgsql", Kind: 'f', Src: `DECLARE r record; BEGIN FOR r IN EXECUTE 'SELECT 1' LOOP NULL; END LOOP; END`},
		"python":      {Name: "c", Lang: "plpython3u", Kind: 'f', Src: `return 1`},
		"c":           {Name: "d", Lang: "c", Kind: 'f', Src: `x`},
		"empty":       {Name: "e", Lang: "sql", Kind: 'f', Src: ``},
		"aggregate":   {Name: "f", Lang: "sql", Kind: 'a', Src: `SELECT 1`},
		"garbage":     {Name: "g", Lang: "sql", Kind: 'f', Src: `SELEKT nonsense`},
		"bad plpgsql": {Name: "h", Lang: "plpgsql", Kind: 'f', Src: `BEGIN NOT VALID`},
	} {
		if b := e.bodyOf(def); !b.Opaque {
			t.Errorf("%s: should be opaque, got %+v", name, b)
		}
	}
}

func TestOutParametersMakeArgumentPositionsUntrustworthy(t *testing.T) {
	def := &FuncDef{Name: "f", Lang: "sql", Kind: 'f', NArgs: 2, HasModes: true, ArgNames: []string{"p_tenant", "result"},
		Src: `SELECT 1 FROM orders WHERE tenant_id = p_tenant`}
	b := testExtractor().bodyOf(def)
	if !b.NoArgs || len(b.Stmts[0].Params) != 0 {
		t.Errorf("must not read argument positions: %+v params=%v", b, b.Stmts[0].Params)
	}
}

func TestDoBlockIsReadLikeAFunctionBody(t *testing.T) {
	stmts, err := testExtractor().Parse(`DO $$ BEGIN PERFORM 1 FROM orders WHERE tenant_id = 'acme'; END $$`)
	if err != nil || len(stmts) != 1 {
		t.Fatalf("%v", err)
	}
	st := stmts[0]
	if !st.DML || st.Body == nil || st.Body.Opaque || len(st.Body.Stmts) == 0 || st.Body.Stmts[0].Tenants[0] != "acme" {
		t.Errorf("%+v / %+v", st, st.Body)
	}
	dyn, _ := testExtractor().Parse(`DO $$ DECLARE t text := 'x'; BEGIN EXECUTE 'DROP TABLE ' || t; END $$`)
	if !dyn[0].Body.Opaque {
		t.Error("dynamic DO block must be opaque")
	}
}

func TestCallCarriesArguments(t *testing.T) {
	stmts, _ := testExtractor().Parse(`SELECT app.orders_of('acme', 5), lower('X')`)
	var found bool
	for _, c := range stmts[0].Calls {
		if c.Ref.Name == "orders_of" && c.Ref.Schema == "app" {
			found = len(c.Args) == 2 && c.Args[0].Lit == "acme" && c.Args[1].Lit == "5"
		}
	}
	if !found {
		t.Errorf("calls = %+v", stmts[0].Calls)
	}
	call, _ := testExtractor().Parse(`CALL archive($1, 'x')`)
	if !call[0].DML || len(call[0].Calls) != 1 || call[0].Calls[0].Args[0].Param != 1 {
		t.Errorf("CALL: %+v", call[0])
	}
}

func TestUtilityStatementsRunTheirQuery(t *testing.T) {
	e := testExtractor()
	for sql, wantTable := range map[string]string{
		`EXPLAIN ANALYZE SELECT * FROM orders`:                 "orders",
		`EXPLAIN (ANALYZE true, BUFFERS) SELECT * FROM orders`: "orders",
		`DECLARE c CURSOR FOR SELECT * FROM orders`:            "orders",
		`CREATE TABLE copy AS SELECT * FROM orders`:            "orders",
		`CREATE TEMP TABLE copy AS SELECT * FROM orders`:       "orders",
		`SELECT * INTO TEMP copy FROM orders`:                  "orders",
		`COPY orders TO STDOUT`:                                "orders",
		`COPY (SELECT * FROM orders) TO STDOUT`:                "orders",
		`REFRESH MATERIALIZED VIEW mv`:                         "mv",
	} {
		stmts, err := e.Parse(sql)
		if err != nil || len(stmts) != 1 {
			t.Fatalf("%s: %v", sql, err)
		}
		st := stmts[0]
		hit := false
		for _, tb := range st.Tables {
			hit = hit || tb.Name == wantTable
		}
		if !st.DML || !hit {
			t.Errorf("%s: DML=%v tables=%v", sql, st.DML, st.Tables)
		}
	}
	for _, sql := range []string{`EXPLAIN SELECT * FROM orders`, `EXPLAIN (ANALYZE false) SELECT * FROM orders`} {
		stmts, _ := e.Parse(sql)
		if stmts[0].DML {
			t.Errorf("%s: a plain EXPLAIN reads no data and must not be attributed", sql)
		}
	}
	cp, _ := e.Parse(`COPY orders FROM STDIN`)
	if !cp[0].Insert {
		t.Error("COPY FROM is a write")
	}
}

func TestPrepareExecuteDeallocateAndDrops(t *testing.T) {
	e := testExtractor()
	p, _ := e.Parse(`PREPARE q (text) AS SELECT * FROM orders WHERE tenant_id = $1`)
	if p[0].Prepare == nil || p[0].Prepare.Name != "q" || !p[0].Prepare.Stmt.DML || !reflect.DeepEqual(p[0].Prepare.Stmt.Params, []int{1}) {
		t.Errorf("prepare: %+v", p[0].Prepare)
	}
	x, _ := e.Parse(`EXECUTE q('acme')`)
	if x[0].Exec == nil || x[0].Exec.Name != "q" || x[0].Exec.Args[0].Lit != "acme" {
		t.Errorf("execute: %+v", x[0].Exec)
	}
	d, _ := e.Parse(`DEALLOCATE q`)
	if d[0].Dealloc == nil || *d[0].Dealloc != "q" {
		t.Errorf("dealloc: %+v", d[0].Dealloc)
	}
	da, _ := e.Parse(`DEALLOCATE ALL`)
	if da[0].Dealloc == nil || *da[0].Dealloc != "" {
		t.Errorf("dealloc all: %+v", da[0].Dealloc)
	}
	dr, _ := e.Parse(`DROP TABLE a, s.b; TRUNCATE c, d`)
	if len(dr[0].Drops) != 2 || dr[0].Drops[1] != (TableRef{"s", "b"}) || len(dr[1].Drops) != 2 {
		t.Errorf("drops: %+v %+v", dr[0].Drops, dr[1].Drops)
	}
}

func TestTempTableDefinitions(t *testing.T) {
	e := testExtractor()
	c, _ := e.Parse(`CREATE TEMP TABLE t (id int, tenant_id text, LIKE other)`)
	if c[0].Temp == nil || c[0].Temp.Name != "t" || !reflect.DeepEqual(c[0].Temp.Cols, []string{"id", "tenant_id"}) || len(c[0].Temp.Like) != 1 {
		t.Errorf("create: %+v", c[0].Temp)
	}
	a, _ := e.Parse(`CREATE TEMPORARY TABLE u AS SELECT * FROM orders`)
	if a[0].Temp == nil || !a[0].Temp.FromQuery || !a[0].DML {
		t.Errorf("ctas: %+v", a[0])
	}
	if p, _ := e.Parse(`CREATE TABLE real_t (a int)`); p[0].Temp != nil {
		t.Error("a normal table is not temporary")
	}
}

func TestExecuteOfAKnownStringIsRead(t *testing.T) {
	e := testExtractor()
	for name, c := range map[string]struct {
		src    string
		params []int
	}{
		"a literal":                {`BEGIN EXECUTE 'SELECT 1 FROM orders WHERE tenant_id = ''acme'''; END`, nil},
		"USING":                    {`BEGIN EXECUTE 'SELECT 1 FROM orders WHERE tenant_id = $1' USING p_tenant; END`, []int{1}},
		"format with %L":           {`BEGIN EXECUTE format('SELECT 1 FROM orders WHERE tenant_id = %L', p_tenant); END`, []int{1}},
		"format with %s of a name": {`BEGIN EXECUTE format('SELECT 1 FROM orders WHERE tenant_id = %s', p_tenant); END`, []int{1}},
		"format with a constant":   {`BEGIN EXECUTE format('SELECT 1 FROM orders WHERE tenant_id = %L AND amount > %s', p_tenant, 5); END`, []int{1}},
	} {
		def := &FuncDef{Name: "f", Lang: "plpgsql", Kind: 'f', NArgs: 1, ArgNames: []string{"p_tenant"}, Src: c.src}
		b := e.bodyOf(def)
		if b.Opaque || len(b.Stmts) == 0 {
			t.Errorf("%s: should be readable: %+v", name, b)
			continue
		}
		var got []int
		for _, s := range b.Stmts {
			if len(s.Params) > 0 {
				got = s.Params
			}
		}
		if !reflect.DeepEqual(got, c.params) && !(len(got) == 0 && len(c.params) == 0) {
			t.Errorf("%s: params %v, want %v", name, got, c.params)
		}
	}
	if b := e.bodyOf(&FuncDef{Name: "g", Lang: "plpgsql", Kind: 'f', Src: `BEGIN EXECUTE 'SELECT 1 FROM orders WHERE tenant_id = ''acme'''; END`}); len(b.Stmts) == 0 || len(b.Stmts[0].Tenants) != 1 || b.Stmts[0].Tenants[0] != "acme" {
		t.Errorf("the literal's tenant should be seen: %+v", b)
	}
}

func TestExecuteOfAComputedStringStaysOpaque(t *testing.T) {
	e := testExtractor()
	for name, src := range map[string]string{
		"concatenation":     `BEGIN EXECUTE 'SELECT 1 FROM orders WHERE tenant_id = ' || quote_literal(p_tenant); END`,
		"%I":                `BEGIN EXECUTE format('SELECT 1 FROM %I', p_tenant); END`,
		"position":          `BEGIN EXECUTE format('SELECT 1 FROM orders WHERE tenant_id = %1$L', p_tenant); END`,
		"too few arguments": `BEGIN EXECUTE format('SELECT %L, %L', p_tenant); END`,
		"too many":          `BEGIN EXECUTE format('SELECT %L', p_tenant, p_tenant); END`,
		"USING expression":  `BEGIN EXECUTE 'SELECT 1 FROM orders WHERE tenant_id = $1' USING lower(p_tenant); END`,
		"missing USING":     `BEGIN EXECUTE 'SELECT 1 FROM orders WHERE tenant_id = $2' USING p_tenant; END`,
		"a column":          `BEGIN EXECUTE format('SELECT 1 FROM orders WHERE tenant_id = %L', (SELECT 1)); END`,
		"unquoted text":     `BEGIN EXECUTE format('SELECT 1 FROM orders WHERE tenant_id = %s', 'x'); END`,
		"into a variable":   `DECLARE n int; BEGIN EXECUTE 'SELECT ' || n::text; END`,
	} {
		def := &FuncDef{Name: "f", Lang: "plpgsql", Kind: 'f', NArgs: 1, ArgNames: []string{"p_tenant"}, Src: src}
		if b := e.bodyOf(def); !b.Opaque {
			t.Errorf("%s: should stay opaque: %+v", name, b)
		}
	}
}

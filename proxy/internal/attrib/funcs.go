package attrib

import (
	"encoding/json"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// FuncBody is the SQL a function (or DO block) runs, read statically.
type FuncBody struct {
	Stmts []Stmt
	// Opaque means the code cannot be read: another language, dynamic SQL
	// (EXECUTE of a computed string), an aggregate, or a body that does not
	// parse. What it touches is then unknown, by construction.
	Opaque bool
	// NoArgs means argument positions are unreliable (OUT/INOUT/VARIADIC
	// parameters shift them), so no argument can be trusted to name a tenant.
	NoArgs bool
}

// forFunc returns an extractor that reads references to the function's named
// parameters as "argument n".
func (e *Extractor) forFunc(def *FuncDef) *Extractor {
	fe := *e
	fe.fname = strings.ToLower(def.Name)
	fe.args = nil
	if def.HasModes {
		return &fe
	}
	for i, n := range def.ArgNames {
		if n = strings.ToLower(n); n != "" {
			if fe.args == nil {
				fe.args = map[string]int{}
			}
			fe.args[n] = i + 1
		}
	}
	return &fe
}

// bodyOf reads a function's code.
func (e *Extractor) bodyOf(def *FuncDef) *FuncBody {
	b := &FuncBody{NoArgs: def.HasModes}
	if def.Kind == 'a' || def.Kind == 'w' || strings.TrimSpace(def.Src) == "" {
		b.Opaque = true
		return b
	}
	fe := e.forFunc(def)
	switch strings.ToLower(def.Lang) {
	case "sql":
		stmts, ok := fe.parseAll(def.Src)
		b.Stmts, b.Opaque = stmts, !ok
	case "plpgsql":
		queries, ok := plpgsqlQueries(def)
		if !ok {
			b.Opaque = true
			return b
		}
		for _, q := range queries {
			stmts, ok := fe.parseAll(q)
			if !ok {
				// PL/pgSQL hands over bare expressions ("n > 10") as well as statements.
				stmts, ok = fe.parseAll("SELECT " + q)
			}
			if !ok {
				b.Opaque = true
				return b
			}
			b.Stmts = append(b.Stmts, stmts...)
		}
	default:
		b.Opaque = true
	}
	return b
}

// doBody reads an anonymous DO block.
func (e *Extractor) doBody(d *pg_query.DoStmt) *FuncBody {
	def := &FuncDef{Name: "do", Lang: "plpgsql", Kind: 'p'}
	for _, a := range d.Args {
		el := a.GetDefElem()
		if el == nil {
			continue
		}
		switch strings.ToLower(el.Defname) {
		case "as":
			def.Src = operandText(el.Arg)
		case "language":
			def.Lang = operandText(el.Arg)
		}
	}
	return e.bodyOf(def)
}

func (e *Extractor) parseAll(sql string) ([]Stmt, bool) {
	tree, err := pg_query.Parse(sql)
	if err != nil {
		return nil, false
	}
	var out []Stmt
	for _, raw := range tree.Stmts {
		out = append(out, e.stmtFor(stmtText(sql, raw), raw.Stmt))
	}
	return out, true
}

// plpgsqlQueries returns every SQL statement embedded in a PL/pgSQL body. ok
// is false if the body does not parse or runs dynamic SQL.
func plpgsqlQueries(def *FuncDef) (queries []string, ok bool) {
	tag := "$tdb_body$"
	if strings.Contains(def.Src, tag) {
		return nil, false
	}
	var params []string
	if len(def.ArgNames) == def.NArgs && !def.HasModes {
		for i, n := range def.ArgNames {
			if n == "" {
				n = "tdb_arg" + string(rune('a'+i%26))
			}
			params = append(params, pgIdent(n)+" text")
		}
	}
	create := "CREATE FUNCTION tdb_probe(" + strings.Join(params, ", ") + ") RETURNS void LANGUAGE plpgsql AS " + tag + def.Src + tag
	js, err := pg_query.ParsePlPgSqlToJSON(create)
	if err != nil {
		return nil, false
	}
	var doc any
	if err := json.Unmarshal([]byte(js), &doc); err != nil {
		return nil, false
	}
	ok = true
	var walk func(v any)
	walk = func(v any) {
		switch t := v.(type) {
		case map[string]any:
			for k, child := range t {
				switch k {
				case "PLpgSQL_stmt_dynexecute", "PLpgSQL_stmt_dynfors", "dynquery":
					ok = false
				case "PLpgSQL_expr":
					if m, isMap := child.(map[string]any); isMap {
						if q, isStr := m["query"].(string); isStr {
							queries = append(queries, q)
						}
					}
				}
				walk(child)
			}
		case []any:
			for _, child := range t {
				walk(child)
			}
		}
	}
	walk(doc)
	return queries, ok
}

func pgIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

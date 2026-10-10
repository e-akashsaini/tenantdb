package attrib

import (
	"encoding/json"
	"strconv"
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

// forTrigger returns an extractor that reads NEW.<tenant> and OLD.<tenant> as
// "argument 1": the tenant of the row that fired the trigger.
func (e *Extractor) forTrigger(def *FuncDef) *Extractor {
	fe := *e
	fe.fname = strings.ToLower(def.Name)
	fe.args = nil
	fe.trig = true
	return &fe
}

// bodyOf reads a function's code.
func (e *Extractor) bodyOf(def *FuncDef) *FuncBody { return e.readBody(def, e.forFunc(def)) }

// triggerBodyOf reads a trigger function's code.
func (e *Extractor) triggerBodyOf(def *FuncDef) *FuncBody { return e.readBody(def, e.forTrigger(def)) }

func (e *Extractor) readBody(def *FuncDef, fe *Extractor) *FuncBody {
	b := &FuncBody{NoArgs: def.HasModes}
	if def.Kind == 'a' || def.Kind == 'w' || strings.TrimSpace(def.Src) == "" {
		b.Opaque = true
		return b
	}
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
				// PL/pgSQL hands over bare expressions ("n > 10") as well as statements,
				// and assignments ("n := n + 1"), of which only the value is a query.
				if _, rhs, isAssign := strings.Cut(q, ":="); isAssign {
					q = rhs
				}
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
	tree, err := Parse(sql)
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
	if strings.Contains(def.Src, tag) || checkParseable(def.Src) != nil {
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
	// The declared return type decides which RETURN forms the PL/pgSQL parser accepts.
	ret := "void"
	switch def.Ret {
	case "trigger":
		ret, params = "trigger", nil
	case "set":
		ret = "SETOF int"
	case "scalar":
		ret = "int"
	}
	create := "CREATE FUNCTION tdb_probe(" + strings.Join(params, ", ") + ") RETURNS " + ret + " LANGUAGE plpgsql AS " + tag + def.Src + tag
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
				case "PLpgSQL_stmt_dynexecute":
					// EXECUTE of a string that is known when the function is read: a
					// literal, or format() of a literal with plain arguments.
					if q, readable := literalExecute(child); readable {
						queries = append(queries, q)
						continue
					}
					ok = false
				case "PLpgSQL_stmt_dynfors", "dynquery":
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

// literalExecute reads `EXECUTE <string> [USING args]` when the string is a
// constant, or format() of a constant template whose arguments are plain
// identifiers (the function's variables and parameters) or constants. The
// result is the SQL it runs, with each argument spelled where it is used, so
// that `tenant_id = p_tenant` is read like any other statement of the body.
func literalExecute(node any) (string, bool) {
	m, isMap := node.(map[string]any)
	if !isMap {
		return "", false
	}
	exprText := func(v any) (string, bool) {
		em, ok := v.(map[string]any)
		if !ok {
			return "", false
		}
		inner, ok := em["PLpgSQL_expr"].(map[string]any)
		if !ok {
			return "", false
		}
		q, ok := inner["query"].(string)
		return q, ok
	}
	q, ok := exprText(m["query"])
	if !ok {
		return "", false
	}
	var using []string
	if ps, ok := m["params"].([]any); ok {
		for _, p := range ps {
			u, ok := exprText(p)
			if !ok || !plainOperand(u) {
				return "", false
			}
			using = append(using, u)
		}
	}
	res, err := Parse("SELECT " + q)
	if err != nil || len(res.Stmts) != 1 {
		return "", false
	}
	sel := res.Stmts[0].Stmt.GetSelectStmt()
	if sel == nil || len(sel.TargetList) != 1 || sel.FromClause != nil {
		return "", false
	}
	val := unwrap(sel.TargetList[0].GetResTarget().GetVal())
	text, isConst := constString(val)
	if !isConst {
		fc := val.GetFuncCall()
		if fc == nil || len(fc.Funcname) != 1 || strings.ToLower(fc.Funcname[0].GetString_().GetSval()) != "format" || len(fc.Args) < 1 {
			return "", false
		}
		tmpl, isConst := constString(unwrap(fc.Args[0]))
		if !isConst {
			return "", false
		}
		var args []string
		for _, a := range fc.Args[1:] {
			s, ok := formatArg(unwrap(a))
			if !ok {
				return "", false
			}
			args = append(args, s)
		}
		if text, ok = expandFormat(tmpl, args); !ok {
			return "", false
		}
	}
	return bindUsing(text, using)
}

// plainOperand: a variable or parameter name, a number, or a string constant.
func plainOperand(s string) bool {
	if s == "" {
		return false
	}
	if s[0] == '\'' {
		res, err := Parse("SELECT " + s)
		if err != nil || len(res.Stmts) != 1 {
			return false
		}
		sel := res.Stmts[0].Stmt.GetSelectStmt()
		if sel == nil || len(sel.TargetList) != 1 {
			return false
		}
		_, ok := constString(unwrap(sel.TargetList[0].GetResTarget().GetVal()))
		return ok
	}
	for i, r := range s {
		switch {
		case r >= '0' && r <= '9' && i > 0:
		case r >= '0' && r <= '9':
			for _, r2 := range s {
				if r2 < '0' || r2 > '9' {
					return false
				}
			}
			return true
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		default:
			return false
		}
	}
	return true
}

func constString(n *pg_query.Node) (string, bool) {
	if c := n.GetAConst(); c != nil && c.GetSval() != nil {
		return c.GetSval().Sval, true
	}
	return "", false
}

// formatArg spells a format() argument as it would appear in SQL: a name as
// itself, a number as itself, a string as a quoted literal. A %I or %L of the
// argument is then the same text, except that %s of a string drops the quotes.
func formatArg(n *pg_query.Node) (string, bool) {
	if cr := n.GetColumnRef(); cr != nil && len(cr.Fields) == 1 && cr.Fields[0].GetString_() != nil {
		s := cr.Fields[0].GetString_().Sval
		if plainOperand(s) {
			return s, true
		}
		return "", false
	}
	if c := n.GetAConst(); c != nil {
		switch {
		case c.GetIval() != nil:
			return strconv.Itoa(int(c.GetIval().Ival)), true
		case c.GetSval() != nil:
			return "'" + strings.ReplaceAll(c.GetSval().Sval, "'", "''") + "'", true
		}
	}
	return "", false
}

// expandFormat does format(): %L and %s take the next argument, %% is a percent.
// Anything else (%I, positions, widths) is not read.
func expandFormat(tmpl string, args []string) (string, bool) {
	var b strings.Builder
	next := 0
	for i := 0; i < len(tmpl); i++ {
		if tmpl[i] != '%' {
			b.WriteByte(tmpl[i])
			continue
		}
		i++
		if i >= len(tmpl) {
			return "", false
		}
		switch tmpl[i] {
		case '%':
			b.WriteByte('%')
		case 'L', 's':
			if next >= len(args) {
				return "", false
			}
			a := args[next]
			next++
			if tmpl[i] == 's' && len(a) >= 2 && a[0] == '\'' {
				return "", false // an unquoted splice of text: not a value
			}
			b.WriteString(a)
		default:
			return "", false
		}
	}
	return b.String(), next == len(args)
}

// bindUsing replaces $1, $2, ... outside string literals with the USING
// arguments. A statement that refers to an argument it was not given is not read.
func bindUsing(sql string, using []string) (string, bool) {
	var b strings.Builder
	inStr := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		switch {
		case c == '\'':
			inStr = !inStr
			b.WriteByte(c)
		case c == '$' && !inStr && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9':
			j := i + 1
			n := 0
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				n = n*10 + int(sql[j]-'0')
				j++
			}
			if n < 1 || n > len(using) {
				return "", false
			}
			b.WriteString(using[n-1])
			i = j - 1
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), true
}

package attrib

import (
	"errors"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// The SQL parser is C code (libpg_query) reached through cgo. A C stack overflow
// is not a panic Go can recover from: it kills the whole process. Nesting
// (parentheses, subqueries, CASE, NOT, ...) is stopped cleanly by the parser's
// own limit, but a long left-leaning chain of binary operators, casts, JOINs or
// UNIONs builds a tree as deep as the chain is long, and overflows the stack
// while the tree is converted: measured at about 40,000 terms. Chains past
// roughly 5,000 are rejected anyway (the Go side refuses to decode a tree that
// deep), so refusing them up front costs nothing a real statement needs.
//
// Parse is the only way the rest of the package reaches the parser.

const (
	// MaxStatementBytes is the largest statement that is analysed at all.
	MaxStatementBytes = 16 << 20
	maxOperatorChain  = 3000 // binary operators, casts, ... between separators
	maxStructChain    = 3000 // JOIN / UNION / INTERSECT / EXCEPT in one statement
)

// ErrTooComplex says a statement was not handed to the parser because doing so
// could crash the process.
var ErrTooComplex = errors.New("statement too large or too deeply chained to analyse")

// Parse is pg_query.Parse behind a guard against statements that crash the parser.
func Parse(sql string) (*pg_query.ParseResult, error) {
	if err := checkParseable(sql); err != nil {
		return nil, err
	}
	return pg_query.Parse(sql)
}

// Normalize is pg_query.Normalize behind the same guard.
func Normalize(sql string) (string, error) {
	if err := checkParseable(sql); err != nil {
		return sql, err
	}
	return pg_query.Normalize(sql)
}

func isOpChar(c byte) bool { return strings.IndexByte("+-*/<>=~!@#%^&|`?", c) >= 0 }

func isWordChar(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// checkParseable scans the statement once, skipping strings, quoted identifiers
// and comments, and counts the tokens that make the parse tree deep.
func checkParseable(sql string) error {
	if len(sql) > MaxStatementBytes {
		return ErrTooComplex
	}
	var opChain, structChain int
	operand := false // was the previous token something a binary operator can follow?
	n := len(sql)
	for i := 0; i < n; {
		c := sql[i]
		switch {
		case c == '\'': // 'string' ('' escapes; E'..\'..' backslashes)
			backslash := i > 0 && (sql[i-1] == 'e' || sql[i-1] == 'E')
			i++
			for i < n {
				if backslash && sql[i] == '\\' {
					i += 2
					continue
				}
				if sql[i] == '\'' {
					if i+1 < n && sql[i+1] == '\'' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			i++
			operand = true
		case c == '"': // "identifier"
			i++
			for i < n {
				if sql[i] == '"' {
					if i+1 < n && sql[i+1] == '"' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			i++
			operand = true
		case c == '$' && i+1 < n && (sql[i+1] == '$' || sql[i+1] == '_' || sql[i+1] >= 'a' && sql[i+1] <= 'z' || sql[i+1] >= 'A' && sql[i+1] <= 'Z'):
			// $tag$ ... $tag$ (a parameter like $1 is handled below)
			j := i + 1
			for j < n && isWordChar(sql[j]) && sql[j] != '$' {
				j++
			}
			if j < n && sql[j] == '$' {
				tag := sql[i : j+1]
				end := strings.Index(sql[j+1:], tag)
				if end < 0 {
					return nil // unterminated: the parser reports it
				}
				i = j + 1 + end + len(tag)
				operand = true
			} else {
				i = j
				operand = true
			}
		case c == '-' && i+1 < n && sql[i+1] == '-': // -- comment
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*': // /* nested /* comments */ */
			depth := 0
			for i < n {
				if sql[i] == '/' && i+1 < n && sql[i+1] == '*' {
					depth++
					i += 2
				} else if sql[i] == '*' && i+1 < n && sql[i+1] == '/' {
					depth--
					i += 2
					if depth == 0 {
						break
					}
				} else {
					i++
				}
			}
		case c == ':' && i+1 < n && sql[i+1] == ':': // cast
			opChain++
			i += 2
			operand = false
		case c == ',' || c == '(' || c == ')' || c == ';' || c == '[' || c == ']':
			opChain = 0
			i++
			operand = c == ')' || c == ']'
		case isOpChar(c):
			j := i
			for j < n && isOpChar(sql[j]) && !(j > i && (sql[j] == '-' && j+1 < n && sql[j+1] == '-' || sql[j] == '/' && j+1 < n && sql[j+1] == '*')) {
				j++
			}
			op := sql[i:j]
			i = j
			if operand && !nonAssociative(op) {
				opChain++
			}
			operand = false
		case isWordChar(c) || c == '.':
			j := i
			for j < n && (isWordChar(sql[j]) || sql[j] == '.') {
				j++
			}
			w := sql[i:j]
			i = j
			switch strings.ToUpper(w) {
			case "JOIN", "UNION", "INTERSECT", "EXCEPT":
				structChain++
				operand = false
			case "IS", "ISNULL", "NOTNULL", "COLLATE", "ZONE": // postfix chains build left-deep trees too
				opChain++
				operand = false
			case "AND", "OR", "NOT", "SELECT", "FROM", "WHERE", "ON", "THEN", "ELSE", "WHEN", "IN", "LIKE", "BETWEEN", "AS", "BY", "SET", "VALUES", "RETURNING", "HAVING", "USING", "LIMIT", "OFFSET", "WITH", "ALL", "DISTINCT":
				operand = false
			default:
				operand = true
			}
		default:
			i++ // whitespace, '.', '\\', ...
		}
		if opChain > maxOperatorChain || structChain > maxStructChain {
			return ErrTooComplex
		}
	}
	return nil
}

// nonAssociative operators cannot be chained (a = b = c is a syntax error).
func nonAssociative(op string) bool {
	switch op {
	case "=", "<", ">", "<=", ">=", "<>", "!=":
		return true
	}
	return false
}

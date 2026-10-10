// Package analyze runs the attribution engine over a recorded workload (a SQL
// file, a pg_stat_statements export or a Postgres log) instead of live
// traffic, and reports which statements would be flagged.
package analyze

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Entry is one recorded statement.
type Entry struct {
	SQL     string
	Calls   int64    // how many times it ran (1 unless the source says otherwise)
	Session string   // groups statements of one connection ("" if unknown)
	User    string   // database role, if recorded
	DB      string   // database, if recorded
	Params  []string // bound parameter values, when the log recorded them
	// HasParams is true when Params were recorded (a nil element is NULL).
	HasParams bool
}

// Format names a workload format.
type Format string

const (
	FormatAuto Format = "auto"
	// FormatSQL is plain SQL text: statements separated by semicolons.
	FormatSQL Format = "sql"
	// FormatStatements is a CSV export of pg_stat_statements with a header that
	// has a "query" column and optionally "calls".
	FormatStatements Format = "csv"
	// FormatLog is a PostgreSQL server log (stderr format) recorded with
	// log_statement = 'all' or log_min_duration_statement.
	FormatLog Format = "log"
)

// ParseFormat validates a flag value.
func ParseFormat(s string) (Format, error) {
	switch f := Format(strings.ToLower(strings.TrimSpace(s))); f {
	case "", FormatAuto:
		return FormatAuto, nil
	case FormatSQL, FormatStatements, FormatLog:
		return f, nil
	}
	return "", fmt.Errorf("unknown format %q (want auto, sql, csv or log)", s)
}

// Read parses a workload. With FormatAuto the format is guessed from the first
// few kilobytes.
func Read(r io.Reader, format Format) ([]Entry, error) {
	br := bufio.NewReaderSize(r, 1<<20)
	if format == FormatAuto || format == "" {
		head, _ := br.Peek(8192)
		format = sniff(string(head))
	}
	switch format {
	case FormatLog:
		return readLog(br)
	case FormatStatements:
		return readStatements(br)
	default:
		return readSQL(br)
	}
}

func sniff(head string) Format {
	switch {
	case strings.Contains(head, "LOG:  statement:") || strings.Contains(head, "LOG:  execute ") || strings.Contains(head, "ms  statement:"):
		return FormatLog
	}
	first := strings.ToLower(strings.SplitN(head, "\n", 2)[0])
	if strings.Contains(first, "query") && strings.Contains(first, ",") && !strings.Contains(first, ";") {
		return FormatStatements
	}
	return FormatSQL
}

func readSQL(r io.Reader) ([]Entry, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	parts, err := pg_query.SplitWithScanner(string(b), true)
	if err != nil {
		return nil, fmt.Errorf("splitting SQL: %w", err)
	}
	var out []Entry
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" && p != ";" {
			out = append(out, Entry{SQL: p, Calls: 1})
		}
	}
	return out, nil
}

func readStatements(r io.Reader) ([]Entry, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	head, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("reading CSV header: %w", err)
	}
	qi, ci := -1, -1
	for i, h := range head {
		switch strings.ToLower(strings.TrimSpace(h)) {
		case "query":
			qi = i
		case "calls":
			ci = i
		}
	}
	if qi < 0 {
		return nil, fmt.Errorf("CSV has no \"query\" column (header: %v)", head)
	}
	var out []Entry
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		if qi >= len(rec) || strings.TrimSpace(rec[qi]) == "" {
			continue
		}
		e := Entry{SQL: rec[qi], Calls: 1}
		if ci >= 0 && ci < len(rec) {
			if n, err := strconv.ParseInt(strings.TrimSpace(rec[ci]), 10, 64); err == nil && n > 0 {
				e.Calls = n
			}
		}
		out = append(out, e)
	}
}

var (
	logHead  = regexp.MustCompile(`LOG:\s+(?:duration: [\d.]+ ms\s+)?(statement|execute [^:]*): (.*)$`)
	logOther = regexp.MustCompile(`^\S.*?(?:LOG|ERROR|WARNING|DETAIL|STATEMENT|HINT|CONTEXT|FATAL|NOTICE):\s`)
	logPID   = regexp.MustCompile(`\[(\d+)\]`)
	logUser  = regexp.MustCompile(`\]\s+(?:\[[^\]]*\]\s+)?([^\s@\[]+)@([^\s\[]+)`)
	logParam = regexp.MustCompile(`DETAIL:\s+parameters:\s+(.*)$`)
)

// readLog reads a PostgreSQL stderr log. A statement may continue on following
// lines that start with whitespace; an "execute" line may be followed by a
// DETAIL line carrying the bound parameters.
func readLog(r io.Reader) ([]Entry, error) {
	var out []Entry
	var cur *Entry
	flush := func() {
		if cur != nil {
			cur.SQL = strings.TrimSpace(cur.SQL)
			if cur.SQL != "" {
				out = append(out, *cur)
			}
			cur = nil
		}
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		if m := logHead.FindStringSubmatchIndex(line); m != nil {
			flush()
			e := &Entry{SQL: line[m[4]:m[5]], Calls: 1}
			if pm := logPID.FindStringSubmatch(line[:m[0]]); pm != nil {
				e.Session = pm[1]
			}
			if um := logUser.FindStringSubmatch(line[:m[0]]); um != nil {
				e.User, e.DB = um[1], um[2]
			}
			cur = e
			continue
		}
		if pm := logParam.FindStringSubmatch(line); pm != nil && cur != nil {
			cur.Params, cur.HasParams = parseLogParams(pm[1]), true
			continue
		}
		if line[0] == ' ' || line[0] == '\t' {
			if cur != nil {
				cur.SQL += "\n" + strings.TrimLeft(line, " \t")
			}
			continue
		}
		if logOther.MatchString(line) {
			flush()
		}
	}
	flush()
	return out, sc.Err()
}

// parseLogParams reads "$1 = 'a”b', $2 = NULL, $3 = '5'". A nil element is NULL.
func parseLogParams(s string) []string {
	var out []string
	i := 0
	for i < len(s) {
		j := strings.Index(s[i:], "= ")
		if j < 0 {
			break
		}
		i += j + 2
		if strings.HasPrefix(s[i:], "NULL") {
			out = append(out, NullParam)
			i += 4
		} else if i < len(s) && s[i] == '\'' {
			i++
			var sb strings.Builder
			for i < len(s) {
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						sb.WriteByte('\'')
						i += 2
						continue
					}
					i++
					break
				}
				sb.WriteByte(s[i])
				i++
			}
			out = append(out, sb.String())
		} else {
			break
		}
		if k := strings.Index(s[i:], "$"); k >= 0 {
			i += k
		} else {
			break
		}
	}
	return out
}

// NullParam marks a NULL in Entry.Params as read from a log.
const NullParam = "\x00null"

package analyze

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/akashsaini/tenantdb/internal/attrib"
)

// Report is the result of analysing a workload. Counts are weighted by how many
// times each statement ran, when the source says.
type Report struct {
	Entries  int64 `json:"statements"` // recorded statements, weighted
	Unparsed int64 `json:"unparsed"`   // could not be parsed
	Other    int64 `json:"other"`      // SET, BEGIN, DDL, ...: nothing to attribute
	Events   int64 `json:"attributed"` // statements the engine attributed

	BySource   map[string]int64 `json:"by_source"`
	Violations map[string]int64 `json:"violations"` // what -strict would flag, by kind
	Scans      int64            `json:"scans"`      // reads of tenant data with no tenant or key filter

	Shapes []*Shape    `json:"shapes"` // query shapes that violate, worst first
	Tables []*TableRow `json:"tables"` // per table

	shapes map[string]*Shape
	tables map[string]*TableRow
}

// Shape is a group of violating statements that differ only in their literals.
type Shape struct {
	Violation string   `json:"violation"`
	Source    string   `json:"source"`
	Calls     int64    `json:"calls"`
	Tables    []string `json:"tables"`
	Query     string   `json:"query"`   // normalised
	Example   string   `json:"example"` // as recorded
	Users     []string `json:"users,omitempty"`
	users     map[string]bool
}

// TableRow is what happened to one table.
type TableRow struct {
	Table      string           `json:"table"`
	Statements int64            `json:"statements"`
	BySource   map[string]int64 `json:"by_source"`
	Violating  int64            `json:"violating"`
}

func newReport() *Report {
	return &Report{BySource: map[string]int64{}, Violations: map[string]int64{},
		shapes: map[string]*Shape{}, tables: map[string]*TableRow{}}
}

func (r *Report) add(ev attrib.Event, w int64) {
	r.Events += w
	r.BySource[string(ev.Source)] += w
	if ev.Scan {
		r.Scans += w
	}
	for _, t := range ev.Tables {
		tr := r.tables[t]
		if tr == nil {
			tr = &TableRow{Table: t, BySource: map[string]int64{}}
			r.tables[t] = tr
		}
		tr.Statements += w
		tr.BySource[string(ev.Source)] += w
		if ev.Violation != "" {
			tr.Violating += w
		}
	}
	if ev.Violation == "" {
		return
	}
	r.Violations[ev.Violation] += w
	norm, err := attrib.Normalize(ev.SQL)
	if err != nil {
		norm = ev.SQL
	}
	key := ev.Violation + "\x00" + norm
	sh := r.shapes[key]
	if sh == nil {
		sh = &Shape{Violation: ev.Violation, Source: string(ev.Source), Tables: ev.Tables, Query: norm,
			Example: ev.SQL, users: map[string]bool{}}
		r.shapes[key] = sh
	}
	sh.Calls += w
	if ev.User != "" {
		sh.users[ev.User] = true
	}
}

func (r *Report) finish() {
	r.Shapes = r.Shapes[:0]
	for _, s := range r.shapes {
		s.Users = sortedKeys(s.users)
		r.Shapes = append(r.Shapes, s)
	}
	sort.Slice(r.Shapes, func(i, j int) bool {
		if r.Shapes[i].Calls != r.Shapes[j].Calls {
			return r.Shapes[i].Calls > r.Shapes[j].Calls
		}
		return r.Shapes[i].Query < r.Shapes[j].Query
	})
	r.Tables = r.Tables[:0]
	for _, t := range r.tables {
		r.Tables = append(r.Tables, t)
	}
	sort.Slice(r.Tables, func(i, j int) bool {
		if r.Tables[i].Violating != r.Tables[j].Violating {
			return r.Tables[i].Violating > r.Tables[j].Violating
		}
		return r.Tables[i].Table < r.Tables[j].Table
	})
}

// JSON writes the report as indented JSON.
func (r *Report) JSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func pct(n, total int64) string {
	if total == 0 {
		return "0.0"
	}
	return fmt.Sprintf("%.1f", float64(n)*100/float64(total))
}

func squash(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		s = s[:n-1] + "…"
	}
	return s
}

// Text writes a human-readable summary. top limits the number of query shapes.
func (r *Report) Text(w io.Writer, top int) {
	r.finish()
	fmt.Fprintf(w, "tenantdb analysis\n=================\n")
	fmt.Fprintf(w, "%d statements read", r.Entries)
	if r.Unparsed > 0 {
		fmt.Fprintf(w, " (%d could not be parsed)", r.Unparsed)
	}
	fmt.Fprintf(w, "; %d attributed, %d need no attribution (SET, BEGIN, DDL, ...)\n\n", r.Events, r.Other)

	fmt.Fprintf(w, "How tenants were determined\n")
	order := []attrib.Source{attrib.SourceExplicit, attrib.SourceSession, attrib.SourceTransaction, attrib.SourceCache,
		attrib.SourceDerived, attrib.SourceUntenanted, attrib.SourceNoMatch, attrib.SourceScan, attrib.SourceUnknown}
	for _, s := range order {
		if n := r.BySource[string(s)]; n > 0 {
			fmt.Fprintf(w, "  %-12s %9d  %5s%%\n", s, n, pct(n, r.Events))
		}
	}

	var viol int64
	for _, n := range r.Violations {
		viol += n
	}
	fmt.Fprintf(w, "\nWhat strict mode would flag: %d of %d attributed statements (%s%%)\n", viol, r.Events, pct(viol, r.Events))
	for _, k := range []string{"unknown", "scan", "conflict"} {
		if n := r.Violations[k]; n > 0 {
			fmt.Fprintf(w, "  %-10s %9d\n", k, n)
		}
	}
	if viol == 0 {
		fmt.Fprintf(w, "  nothing: every statement that touches tenant data could be tied to a tenant\n")
		return
	}

	fmt.Fprintf(w, "\nWorst query shapes (statements that differ only in their literals are grouped)\n")
	for i, s := range r.Shapes {
		if i >= top {
			fmt.Fprintf(w, "  ... and %d more shapes (use -top)\n", len(r.Shapes)-top)
			break
		}
		fmt.Fprintf(w, "  %2d. [%s] x%d  %s\n      %s\n", i+1, s.Violation, s.Calls, strings.Join(s.Tables, ", "), squash(s.Query, 160))
	}

	fmt.Fprintf(w, "\nTables with flagged statements\n")
	n := 0
	for _, t := range r.Tables {
		if t.Violating == 0 {
			continue
		}
		n++
		fmt.Fprintf(w, "  %-34s %7d flagged of %d\n", t.Table, t.Violating, t.Statements)
	}
}

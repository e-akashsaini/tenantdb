package attrib

import (
	"strconv"
	"strings"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// COPY ... FROM STDIN.
//
// The statement names a table, not the tenants of the rows that follow. The rows
// arrive afterwards as CopyData messages, so the event for the statement is held
// until the stream ends and then carries the tenant column's values: every
// distinct tenant the rows named. A stream that cannot be read (binary format, a
// row too long, a tenant column left to a default) stays "unknown", as before.

// CopySpec describes a COPY ... FROM STDIN.
type CopySpec struct {
	Table  TableRef
	Cols   []string // the column list, lower-cased; empty means every column in table order
	Format string   // text, csv or binary
	Delim  byte
	Quote  byte
	Escape byte
	Null   string
	Header bool
}

// copySpecOf reads the COPY options. It returns nil for anything but a load
// from the client.
func copySpecOf(c *pg_query.CopyStmt) *CopySpec {
	if !c.IsFrom || c.IsProgram || c.Filename != "" || c.Relation == nil {
		return nil
	}
	sp := &CopySpec{Table: relRef(c.Relation), Format: "text"}
	for _, a := range c.Attlist {
		sp.Cols = append(sp.Cols, strings.ToLower(a.GetString_().GetSval()))
	}
	var nullSet bool
	for _, o := range c.Options {
		d := o.GetDefElem()
		if d == nil {
			continue
		}
		arg := operandText(d.Arg)
		switch strings.ToLower(d.Defname) {
		case "format":
			sp.Format = strings.ToLower(arg)
		case "csv":
			sp.Format = "csv"
		case "binary":
			sp.Format = "binary"
		case "delimiter":
			if len(arg) == 1 {
				sp.Delim = arg[0]
			} else {
				return nil
			}
		case "null":
			sp.Null, nullSet = arg, true
		case "quote":
			if len(arg) == 1 {
				sp.Quote = arg[0]
			} else {
				return nil
			}
		case "escape":
			if len(arg) == 1 {
				sp.Escape = arg[0]
			} else {
				return nil
			}
		case "header":
			sp.Header = arg == "" || strings.EqualFold(arg, "true") || strings.EqualFold(arg, "on") ||
				strings.EqualFold(arg, "match") || arg == "1"
		case "encoding", "freeze", "force_not_null", "force_null", "force_quote", "on_error", "log_verbosity", "reject_limit":
			// do not change where a field starts or ends
		default:
			return nil // an option this reader does not know
		}
	}
	if sp.Delim == 0 {
		sp.Delim = '\t'
		if sp.Format == "csv" {
			sp.Delim = ','
		}
	}
	if !nullSet {
		sp.Null = `\N`
		if sp.Format == "csv" {
			sp.Null = ""
		}
	}
	if sp.Quote == 0 {
		sp.Quote = '"'
	}
	if sp.Escape == 0 {
		sp.Escape = sp.Quote
	}
	return sp
}

const (
	copyMaxField   = 4096    // a tenant value longer than this is not a tenant
	copyMaxTenants = 1000    // distinct tenants kept
	copyMaxBytes   = 1 << 31 // stop reading after this much data
)

// copyParser reads COPY data (text or CSV) as it arrives, in chunks cut anywhere,
// and reports the value of one column per row.
type copyParser struct {
	csv                bool
	delim, quote, esc  byte
	null               string
	tidx               int
	header             bool
	row                func(val string, null, ok bool)
	field              int
	raw                []byte
	over               bool // the target field is too long
	inQuote            bool
	quoted             bool // the field had a quote: never NULL
	pendEsc, pendQuote bool
	skipLF             bool
	atStart            bool // nothing of the line has been read
	endCand            bool // the line so far is `\.`
	escStart           bool // the backslash being read began the line
	done               bool
	headerLeft         bool
	rowStarted         bool
	read               int64
}

func newCopyParser(sp *CopySpec, tidx int, row func(val string, null, ok bool)) *copyParser {
	return &copyParser{csv: sp.Format == "csv", delim: sp.Delim, quote: sp.Quote, esc: sp.Escape, null: sp.Null,
		tidx: tidx, header: sp.Header, headerLeft: sp.Header, row: row, atStart: true}
}

func (p *copyParser) endField() {
	if p.field == p.tidx && !p.headerLeft {
		isNull := !p.quoted && string(p.raw) == p.null
		switch {
		case p.over:
			p.row("", false, false)
		case isNull:
			p.row("", true, true)
		case p.csv:
			p.row(string(p.raw), false, true)
		default:
			p.row(unescapeCopyText(p.raw), false, true)
		}
	}
	p.raw, p.over, p.quoted = p.raw[:0], false, false
	p.field++
}

func (p *copyParser) endRow() {
	p.endField()
	if p.field <= p.tidx && !p.headerLeft {
		p.row("", false, false) // too few fields
	}
	p.headerLeft = false
	p.field, p.atStart, p.rowStarted, p.endCand = 0, true, false, false
}

func (p *copyParser) keep(b byte) {
	if p.field == p.tidx {
		if len(p.raw) >= copyMaxField {
			p.over = true
			return
		}
		p.raw = append(p.raw, b)
	}
}

// Write feeds the next chunk of CopyData.
func (p *copyParser) Write(data []byte) {
	for _, c := range data {
		if p.done {
			return
		}
		p.read++
		if p.read > copyMaxBytes {
			p.row("", false, false)
			p.done = true
			return
		}
		if p.skipLF {
			p.skipLF = false
			if c == '\n' {
				continue
			}
		}
		if p.csv {
			p.csvByte(c)
		} else {
			p.textByte(c)
		}
	}
}

func (p *copyParser) textByte(c byte) {
	if p.pendEsc {
		p.pendEsc = false
		if p.escStart && c == '.' {
			p.endCand = true // `\.` alone on a line ends the data
		}
		p.atStart = false
		p.keep(c)
		return
	}
	switch c {
	case '\\':
		p.escStart = p.atStart
		p.pendEsc, p.rowStarted = true, true
		p.keep(c)
		return
	case p.delim:
		p.atStart, p.rowStarted = false, true
		p.endField()
	case '\n', '\r':
		if p.endCand {
			p.done = true
			return
		}
		if c == '\r' {
			p.skipLF = true
		}
		if p.atStart && !p.rowStarted {
			return // an empty line
		}
		p.endRow()
	default:
		p.atStart, p.rowStarted = false, true
		p.keep(c)
	}
}

func (p *copyParser) csvByte(c byte) {
	if p.inQuote {
		switch {
		case p.pendQuote:
			p.pendQuote = false
			if c == p.quote {
				p.keep(c)
				return
			}
			p.inQuote = false
			// the quote closed the value: handle c as an ordinary character
		case p.pendEsc:
			p.pendEsc = false
			p.keep(c)
			return
		case c == p.esc && p.esc != p.quote:
			p.pendEsc = true
			return
		case c == p.quote:
			p.pendQuote = true
			return
		default:
			p.keep(c)
			return
		}
	}
	switch c {
	case p.quote:
		if len(p.raw) == 0 && !p.quoted {
			p.inQuote, p.quoted, p.rowStarted, p.atStart = true, true, true, false
			return
		}
		p.keep(c)
	case p.delim:
		p.rowStarted, p.atStart = true, false
		p.endField()
	case '\n', '\r':
		if c == '\r' {
			p.skipLF = true
		}
		if !p.rowStarted {
			return
		}
		p.endRow()
	default:
		p.rowStarted, p.atStart = true, false
		p.keep(c)
	}
}

// Close ends the stream: a last row without a newline still counts.
func (p *copyParser) Close() {
	if p.done {
		return
	}
	if p.inQuote && !p.pendQuote {
		p.row("", false, false) // an unterminated quote
		return
	}
	if p.rowStarted {
		p.endRow()
	}
}

// unescapeCopyText undoes the backslash sequences of COPY's text format.
func unescapeCopyText(b []byte) string {
	if !strings.Contains(string(b), `\`) {
		return string(b)
	}
	var out []byte
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 >= len(b) {
			out = append(out, b[i])
			continue
		}
		i++
		switch c := b[i]; c {
		case 'b':
			out = append(out, '\b')
		case 'f':
			out = append(out, '\f')
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case 'v':
			out = append(out, '\v')
		case 'x':
			j := i + 1
			for j < len(b) && j < i+3 && isHex(b[j]) {
				j++
			}
			if j > i+1 {
				v, _ := strconv.ParseUint(string(b[i+1:j]), 16, 8)
				out = append(out, byte(v))
				i = j - 1
			} else {
				out = append(out, 'x')
			}
		case '0', '1', '2', '3', '4', '5', '6', '7':
			j := i
			for j < len(b) && j < i+3 && b[j] >= '0' && b[j] <= '7' {
				j++
			}
			v, _ := strconv.ParseUint(string(b[i:j]), 8, 16)
			out = append(out, byte(v))
			i = j - 1
		default:
			out = append(out, c)
		}
	}
	return string(out)
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// copyIn is a COPY FROM STDIN whose event is waiting for the data.
type copyIn struct {
	ev      Event
	parser  *copyParser
	tenants map[string]bool
	rows    int
	bad     bool // some row did not name a tenant, or could not be read
	over    bool
	gen     uint64
}

// beginCopy holds the event of a COPY FROM STDIN until its data has been read. It
// reports whether it took over: only when the format can be read and the tenant
// column's position in the rows is certain.
func (a *Attributor) beginCopy(st *state, ev Event, x Stmt) bool {
	sp := x.Copy
	if sp == nil || (sp.Format != "csv" && sp.Format != "text") || a.cat == nil || !a.cat.Loaded() {
		return false
	}
	ti, tmp, ok := a.tableFor(st, sp.Table)
	if !ok || tmp != nil || ti == nil || ti.Kind == 'v' || ti.Kind == 'm' {
		return false
	}
	tcol, _ := a.scopeOf(ti)
	if tcol == "" {
		return false
	}
	cols := sp.Cols
	if len(cols) == 0 {
		if ti.hasGenerated {
			return false // generated columns are not in the data: positions would be a guess
		}
		cols = ti.Cols
	}
	tidx := -1
	for i, c := range cols {
		if c == tcol {
			tidx = i
		}
	}
	if tidx < 0 {
		return false // the tenant comes from a default
	}
	ci := &copyIn{ev: ev, tenants: map[string]bool{}, gen: st.txnGen}
	ci.parser = newCopyParser(sp, tidx, func(val string, null, ok bool) {
		ci.rows++
		switch {
		case !ok || null:
			ci.bad = true
		case len(ci.tenants) >= copyMaxTenants && !ci.tenants[normVal(val)]:
			ci.over = true
		default:
			ci.tenants[normVal(val)] = true
		}
	})
	st.copyIn = ci
	return true
}

// copyData feeds a chunk of the stream.
func (st *state) copyData(data []byte) {
	if st.copyIn != nil {
		st.copyIn.parser.Write(data)
	}
}

// endCopy completes the held event with what the stream held.
func (a *Attributor) endCopy(st *state, failed bool) {
	ci := st.copyIn
	if ci == nil {
		return
	}
	st.copyIn = nil
	if !failed {
		ci.parser.Close()
	}
	ev := ci.ev
	switch {
	case failed || ci.bad || ci.over:
		// leave the event as it was: unknown
	case ci.rows == 0:
		ev.Source = SourceNoMatch
	default:
		ev.Tenants = sortedKeys(ci.tenants)
		ev.Source = SourceExplicit
	}
	a.settle(st, &ev, ci.gen == st.txnGen)
}

// onCopyFrontend handles the messages of a COPY stream.
func (a *Attributor) onCopyFrontend(s *proxy.Session, msg pgproto3.FrontendMessage) {
	switch m := msg.(type) {
	case *pgproto3.CopyData:
		st := a.st(s)
		st.mu.Lock()
		st.copyData(m.Data)
		st.mu.Unlock()
	case *pgproto3.CopyDone:
		st := a.st(s)
		st.mu.Lock()
		a.endCopy(st, false)
		st.mu.Unlock()
	case *pgproto3.CopyFail:
		st := a.st(s)
		st.mu.Lock()
		a.endCopy(st, true)
		st.mu.Unlock()
	}
}

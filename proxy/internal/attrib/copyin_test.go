package attrib

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

type copyRow struct {
	val  string
	null bool
	ok   bool
}

// parseCopy runs data through the parser cut into chunks of the given size.
func parseCopy(sp *CopySpec, tidx int, data string, chunk int) []copyRow {
	var out []copyRow
	p := newCopyParser(sp, tidx, func(v string, null, ok bool) { out = append(out, copyRow{v, null, ok}) })
	for i := 0; i < len(data); i += chunk {
		end := i + chunk
		if end > len(data) {
			end = len(data)
		}
		p.Write([]byte(data[i:end]))
	}
	p.Close()
	return out
}

func specOf(t *testing.T, sql string) *CopySpec {
	t.Helper()
	stmts, err := testExtractor().Parse(sql)
	if err != nil || len(stmts) != 1 || stmts[0].Copy == nil {
		t.Fatalf("%s: %v %+v", sql, err, stmts)
	}
	return stmts[0].Copy
}

func TestCopyOptionsAreRead(t *testing.T) {
	for sql, want := range map[string]CopySpec{
		`COPY orders FROM STDIN`: {Format: "text", Delim: '\t', Null: `\N`, Quote: '"', Escape: '"'},
		`COPY orders (a, b) FROM STDIN WITH (FORMAT csv, HEADER true)`:               {Format: "csv", Delim: ',', Null: "", Quote: '"', Escape: '"', Header: true, Cols: []string{"a", "b"}},
		`COPY orders FROM STDIN CSV DELIMITER ';' NULL 'NULL' QUOTE '''' ESCAPE '\'`: {Format: "csv", Delim: ';', Null: "NULL", Quote: '\'', Escape: '\\'},
		`COPY orders FROM STDIN WITH (FORMAT text, DELIMITER '|')`:                   {Format: "text", Delim: '|', Null: `\N`, Quote: '"', Escape: '"'},
		`COPY orders FROM STDIN WITH (FORMAT binary)`:                                {Format: "binary", Delim: '\t', Null: `\N`, Quote: '"', Escape: '"'},
	} {
		got := specOf(t, sql)
		got.Table = TableRef{}
		if !reflect.DeepEqual(*got, want) {
			t.Errorf("%s:\n  got  %+v\n  want %+v", sql, *got, want)
		}
	}
	for _, sql := range []string{`COPY orders TO STDOUT`, `COPY orders FROM '/tmp/x'`, `COPY orders FROM PROGRAM 'cat x'`, `COPY (SELECT 1) TO STDOUT`,
		`COPY orders FROM STDIN WITH (FORMAT csv, SURPRISE 1)`} {
		stmts, err := testExtractor().Parse(sql)
		if err != nil || len(stmts) != 1 || stmts[0].Copy != nil {
			t.Errorf("%s should not be inspected: %v %+v", sql, err, stmts)
		}
	}
}

func TestCopyParserReadsTheTenantColumn(t *testing.T) {
	text := CopySpec{Format: "text", Delim: '\t', Null: `\N`, Quote: '"', Escape: '"'}
	csv := CopySpec{Format: "csv", Delim: ',', Null: "", Quote: '"', Escape: '"'}
	csvHdr := csv
	csvHdr.Header = true
	pipe := text
	pipe.Delim = '|'
	for _, c := range []struct {
		name string
		sp   CopySpec
		tidx int
		data string
		want []copyRow
	}{
		{"text rows", text, 1, "1\tacme\tx\n2\tglobex\ty\n", []copyRow{{"acme", false, true}, {"globex", false, true}}},
		{"no trailing newline", text, 1, "1\tacme\tx", []copyRow{{"acme", false, true}}},
		{"CRLF", text, 1, "1\tacme\r\n2\tglobex\r\n", []copyRow{{"acme", false, true}, {"globex", false, true}}},
		{"NULL", text, 1, "1\t\\N\tx\n", []copyRow{{"", true, true}}},
		{"an escaped tab and backslash", text, 1, "1\ta\\tb\\\\c\tx\n", []copyRow{{"a\tb\\c", false, true}}},
		{"an escaped delimiter in another field", text, 1, "a\\\tb\tacme\n", []copyRow{{"acme", false, true}}},
		{"the escaped N is not NULL", text, 1, "1\t\\\\N\n", []copyRow{{"\\N", false, true}}},
		{"octal and hex", text, 0, "\\101\\x42\n", []copyRow{{"AB", false, true}}},
		{"the end marker", text, 0, "a\nb\n\\.\nc\n", []copyRow{{"a", false, true}, {"b", false, true}}},
		{"a row that is too short", text, 2, "1\tx\n", []copyRow{{"", false, false}}},
		{"another delimiter", pipe, 1, "1|acme|x\n", []copyRow{{"acme", false, true}}},
		{"csv rows", csv, 1, "1,acme,x\n2,globex,y\n", []copyRow{{"acme", false, true}, {"globex", false, true}}},
		{"a quoted comma", csv, 1, `1,"a,b",x` + "\n", []copyRow{{"a,b", false, true}}},
		{"a doubled quote", csv, 1, `1,"say ""hi""",x` + "\n", []copyRow{{`say "hi"`, false, true}}},
		{"a newline inside quotes", csv, 1, "1,\"a\nb\",x\n2,c,y\n", []copyRow{{"a\nb", false, true}, {"c", false, true}}},
		{"an unquoted empty value is NULL", csv, 1, "1,,x\n", []copyRow{{"", true, true}}},
		{"a quoted empty value is not", csv, 1, "1,\"\",x\n", []copyRow{{"", false, true}}},
		{"a header", csvHdr, 1, "id,tenant,note\n1,acme,x\n", []copyRow{{"acme", false, true}}},
		{"a quote inside an unquoted value", csv, 1, "1,a\"b,x\n", []copyRow{{`a"b`, false, true}}},
		{"an unterminated quote", csv, 0, "\"abc\n", []copyRow{{"", false, false}}},
		{"blank lines", csv, 0, "a\n\nb\n", []copyRow{{"a", false, true}, {"b", false, true}}},
	} {
		sp := c.sp
		for _, chunk := range []int{len(c.data) + 1, 1, 2, 3, 5} {
			got := parseCopy(&sp, c.tidx, c.data, chunk)
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("%s (chunks of %d): got %+v, want %+v", c.name, chunk, got, c.want)
				break
			}
		}
	}
	// a value too long to be a tenant is not read
	long := strings.Repeat("x", copyMaxField+10)
	if got := parseCopy(&text, 0, long+"\n", 1000); len(got) != 1 || got[0].ok {
		t.Errorf("an enormous value: %+v", got[0])
	}
}

func copyHarness() *harness { return copyHarnessWith(StrictOptions{}) }

func copyHarnessWith(opt StrictOptions) *harness {
	sp := soundSpec()
	sp.rows = append(sp.rows, tbl("public", "gen", 'r', "id", "tenant_id", "g")...)
	for i := range sp.rows {
		if sp.rows[i].table == "gen" && sp.rows[i].col == "g" {
			sp.rows[i].generated = true
		}
	}
	return specHarness(sp, opt)
}

func (h *harness) copyIn(chunks ...string) {
	for _, c := range chunks {
		h.front(&pgproto3.CopyData{Data: []byte(c)})
	}
	h.front(&pgproto3.CopyDone{})
}

func TestCopyFromStdinIsAttributedToTheTenantsOfItsRows(t *testing.T) {
	h := copyHarness()
	h.query(`COPY orders (id, tenant_id, status) FROM STDIN`)
	if len(h.events) != 0 {
		t.Fatalf("the event waits for the rows: %+v", h.events)
	}
	h.copyIn("1\tacme\topen\n2\tglob", "ex\topen\n3\tacme\tpaid\n")
	e := h.last(t)
	if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme", "globex"}) {
		t.Errorf("rows for two tenants: %+v", e)
	}

	h = copyHarness()
	h.query(`COPY orders FROM STDIN WITH (FORMAT csv)`) // table order: id, tenant_id, customer_id, status, amount
	h.copyIn("1,acme,5,open,1\n", "2,acme,6,open,2\n")
	if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("csv in table order: %+v", e)
	}

	for name, c := range map[string]struct{ sql, data string }{
		"a row without a tenant":            {`COPY orders (id, tenant_id) FROM STDIN`, "1\t\\N\n"},
		"a row too short":                   {`COPY orders (id, tenant_id) FROM STDIN`, "1\n"},
		"the tenant left to a default":      {`COPY orders (id, status) FROM STDIN`, "1\topen\n"},
		"binary":                            {`COPY orders (id, tenant_id) FROM STDIN WITH (FORMAT binary)`, "PGCOPY\n"},
		"generated columns shift positions": {`COPY gen FROM STDIN`, "1\tacme\n"},
	} {
		h := copyHarness()
		h.query(c.sql)
		h.copyIn(c.data)
		if len(h.events) == 0 {
			h.rfq('I')
		}
		if e := h.last(t); e.Source == SourceExplicit {
			t.Errorf("%s: claimed %v", name, e.Tenants)
		}
	}

	h = copyHarness()
	h.query(`COPY orders (id, tenant_id) FROM STDIN`)
	h.copyIn()
	if e := h.last(t); e.Source != SourceNoMatch {
		t.Errorf("no rows: %+v", e)
	}

	// the server stopped the load: what it had named is what it addressed
	h = copyHarness()
	h.query(`COPY orders (id, tenant_id) FROM STDIN`)
	h.front(&pgproto3.CopyData{Data: []byte("1\tacme\n")})
	h.a.OnBackend(h.s, &pgproto3.ErrorResponse{Severity: "ERROR", Code: "23505", Message: "duplicate"})
	if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("aborted by the server: %+v", e)
	}
	// the client gave up: nothing it named is vouched for
	h = copyHarness()
	h.query(`COPY orders (id, tenant_id) FROM STDIN`)
	h.front(&pgproto3.CopyData{Data: []byte("1\tacme\n")})
	h.front(&pgproto3.CopyFail{Message: "no"})
	if e := h.last(t); e.Source == SourceExplicit {
		t.Errorf("CopyFail: %+v", e)
	}
}

func TestCopyOfAHugeNumberOfTenantsIsNotRemembered(t *testing.T) {
	h := copyHarness()
	h.query(`COPY orders (id, tenant_id) FROM STDIN`)
	var b strings.Builder
	for i := 0; i < copyMaxTenants+5; i++ {
		fmt.Fprintf(&b, "%d\tt%d\n", i, i)
	}
	h.copyIn(b.String())
	if e := h.last(t); e.Source == SourceExplicit {
		t.Errorf("too many tenants to list: %d", len(e.Tenants))
	}
}

// Postgres is the judge of where a field starts and ends: load random hostile
// data with a real COPY and compare the tenants it stored with what the parser read.
func TestCopyParserAgreesWithPostgres(t *testing.T) {
	dsn := os.Getenv("TENANTDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TENANTDB_TEST_DSN not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `DROP TABLE IF EXISTS tdb_copy; CREATE TABLE tdb_copy (id int, tenant_id text, note text)`); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, `DROP TABLE IF EXISTS tdb_copy`)

	seed := int64(1)
	if v, err := strconv.ParseInt(os.Getenv("TENANTDB_FUZZ_SEED"), 10, 64); err == nil {
		seed = v
	}
	iters := 400
	if v, err := strconv.Atoi(os.Getenv("TENANTDB_FUZZ_N")); err == nil && v > 0 {
		iters = v / 8
	}
	r := rand.New(rand.NewSource(seed))
	alphabet := []string{"a", "B", "7", " ", ",", ";", "|", "\"", "'", "\\", "\t", "\n", "\r", "é", "世", ".", "N", "%", "x", "0", "-"}
	special := []string{"", `\N`, "NULL", `\.`, `""`, "a,b", "t1", "T2", "x\ny"}
	value := func() string {
		if r.Intn(6) == 0 {
			return special[r.Intn(len(special))]
		}
		var b strings.Builder
		for i := r.Intn(8); i > 0; i-- {
			b.WriteString(alphabet[r.Intn(len(alphabet))])
		}
		return b.String()
	}
	compared, rowsSeen := 0, 0
	for it := 0; it < iters; it++ {
		csv := r.Intn(2) == 0
		delim := byte(',')
		if !csv {
			delim = '\t'
		}
		if r.Intn(3) == 0 {
			delim = []byte{'|', ';', ':'}[r.Intn(3)]
		}
		quote, esc := byte('"'), byte('"')
		if csv && r.Intn(4) == 0 {
			quote = '\''
			esc = quote
			if r.Intn(2) == 0 {
				esc = '\\'
			}
		}
		nullStr := `\N`
		if csv {
			nullStr = ""
		}
		if r.Intn(4) == 0 {
			nullStr = "NULL"
		}
		header := csv && r.Intn(3) == 0
		colList := r.Intn(2) == 0 // (note, tenant_id, id) instead of the table's order
		var data bytes.Buffer
		if header {
			data.WriteString("h1" + string(delim) + "h2" + string(delim) + "h3\n")
		}
		nrows := r.Intn(6)
		for i := 0; i < nrows; i++ {
			vals := []string{strconv.Itoa(i), value(), value()}
			if colList {
				vals = []string{vals[2], vals[1], vals[0]}
			}
			for j, v := range vals {
				if j > 0 {
					data.WriteByte(delim)
				}
				switch {
				case csv:
					e := v
					if esc != quote {
						e = strings.ReplaceAll(e, string(esc), string(esc)+string(esc))
					}
					e = strings.ReplaceAll(e, string(quote), string(esc)+string(quote))
					if v == "" || e != v || strings.ContainsAny(v, string([]byte{delim, '\n', '\r'})) || v == `\.` || r.Intn(5) == 0 {
						e = string(quote) + e + string(quote)
					}
					data.WriteString(e)
				default:
					data.WriteString(strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`, string(delim), `\`+string(delim)).Replace(v))
				}
			}
			data.WriteByte('\n')
		}
		cols := "(id, tenant_id, note)"
		if colList {
			cols = "(note, tenant_id, id)"
		}
		lit := func(v string) string {
			return "E'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), "'", `\'`) + "'"
		}
		opts := fmt.Sprintf("FORMAT text, DELIMITER %s, NULL %s", lit(string(delim)), lit(nullStr))
		if csv {
			opts = fmt.Sprintf("FORMAT csv, DELIMITER %s, NULL %s, QUOTE %s, ESCAPE %s", lit(string(delim)), lit(nullStr), lit(string(quote)), lit(string(esc)))
			if header {
				opts += ", HEADER true"
			}
		}
		sql := fmt.Sprintf("COPY tdb_copy %s FROM STDIN WITH (%s)", cols, opts)

		if _, err := conn.Exec(ctx, `TRUNCATE tdb_copy`); err != nil {
			t.Fatal(err)
		}
		_, err := conn.PgConn().CopyFrom(ctx, bytes.NewReader(data.Bytes()), sql)
		if err != nil {
			continue // data the writer got wrong for this option mix: Postgres refused it, nothing to compare
		}
		compared++
		rows, err := conn.Query(ctx, `SELECT tenant_id FROM tdb_copy`)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]int{}
		wantNull := 0
		for rows.Next() {
			var tv *string
			if err := rows.Scan(&tv); err != nil {
				t.Fatal(err)
			}
			rowsSeen++
			if tv == nil {
				wantNull++
			} else {
				want[*tv]++
			}
		}
		rows.Close()

		stmts, perr := testExtractor().Parse(sql)
		if perr != nil || len(stmts) != 1 || stmts[0].Copy == nil {
			t.Fatalf("%s: %v", sql, perr)
		}
		got := map[string]int{}
		gotNull, gotBad := 0, 0
		p := newCopyParser(stmts[0].Copy, 1, func(v string, null, ok bool) {
			switch {
			case !ok:
				gotBad++
			case null:
				gotNull++
			default:
				got[v]++
			}
		})
		b := data.Bytes()
		for len(b) > 0 {
			n := r.Intn(7) + 1
			if n > len(b) {
				n = len(b)
			}
			p.Write(b[:n])
			b = b[n:]
		}
		p.Close()
		if gotBad != 0 || gotNull != wantNull || !reflect.DeepEqual(got, want) {
			keys := func(m map[string]int) []string {
				var out []string
				for k, n := range m {
					out = append(out, fmt.Sprintf("%q×%d", k, n))
				}
				sort.Strings(out)
				return out
			}
			t.Errorf("seed %d iteration %d:\n  %s\n  data %q\n  parser:   %v null=%d bad=%d\n  postgres: %v null=%d", seed, it, sql, data.String(),
				keys(got), gotNull, gotBad, keys(want), wantNull)
		}
	}
	t.Logf("seed %d: %d of %d random loads were accepted by Postgres (%d rows compared)", seed, compared, iters, rowsSeen)
	if compared < iters/4 {
		t.Errorf("only %d of %d loads were accepted: the generator is mostly producing refused data", compared, iters)
	}
}

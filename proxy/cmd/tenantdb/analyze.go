package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/akashsaini/tenantdb/internal/analyze"
	"github.com/akashsaini/tenantdb/internal/attrib"
)

// runAnalyze implements `tenantdb analyze`: attribute a recorded workload
// instead of live traffic. Returns the process exit code.
func runAnalyze(args []string) int {
	fs := flag.NewFlagSet("analyze", flag.ContinueOnError)
	var (
		schemaDSN  = fs.String("schema-dsn", os.Getenv("TENANTDB_SCHEMA_DSN"), "connection string of a (read-only) role on a database with the same schema as the one the workload ran against")
		tenantCols = fs.String("tenant-columns", env("TENANTDB_TENANT_COLUMNS", "tenant_id"), "comma-separated column names that identify the tenant")
		keyCols    = fs.String("key-columns", env("TENANTDB_KEY_COLUMNS", "id"), "comma-separated column names that identify a row")
		format     = fs.String("format", "auto", "input format: auto, sql, csv (pg_stat_statements export) or log (PostgreSQL stderr log)")
		user       = fs.String("user", "", "database role to assume for statements that do not record one")
		db         = fs.String("db", "", "database name to assume for statements that do not record one")
		exempt     = fs.String("strict-exempt", "", "comma-separated roles never held to the policy")
		allowScan  = fs.String("strict-allow-scan", "", "comma-separated roles allowed to read across all tenants")
		tagVar     = fs.String("tag-var", "tenantdb.tenant", "session variable a connection sets to declare its tenant")
		warm       = fs.Bool("warm", true, "fill the row cache from the database before replaying, so statements that name a row by id can be attributed")
		learn      = fs.Bool("learn", true, "replay the workload twice: first to learn which joins are foreign keys the schema does not declare (checked against the data), then to report")
		lookup     = fs.Bool("lookup", true, "resolve rows missing from the cache by asking the database")
		asJSON     = fs.Bool("json", false, "write the full report as JSON")
		top        = fs.Int("top", 20, "how many query shapes to list")
	)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: tenantdb analyze [flags] FILE...   (FILE "-" reads standard input)

Runs the same tenant attribution the proxy does over a recorded workload and
reports what -strict would flag, without putting anything in the path of a live
database. Needs the schema (-schema-dsn) to tell tenant tables from shared ones.

Workloads:
  .sql   statements separated by semicolons
  .csv   \copy (SELECT query, calls FROM pg_stat_statements) TO 'q.csv' CSV HEADER
  .log   PostgreSQL log with log_statement = 'all' (or log_min_duration_statement = 0)

flags:`)
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	files := fs.Args()
	if len(files) == 0 {
		fs.Usage()
		return 2
	}
	if *schemaDSN == "" {
		fmt.Fprintln(os.Stderr, "tenantdb analyze: -schema-dsn (or TENANTDB_SCHEMA_DSN) is required: without the schema nothing can be told apart from \"unknown\"")
		return 2
	}
	f, err := analyze.ParseFormat(*format)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenantdb analyze:", err)
		return 2
	}

	cat := attrib.NewCatalog(*schemaDSN)
	cat.TenantColumns = strings.Split(*tenantCols, ",") // foreign keys are verified against it
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err = cat.Load(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenantdb analyze: loading the schema:", err)
		return 1
	}

	var lk *attrib.Lookup
	if *lookup {
		if lk, err = attrib.NewLookup(context.Background(), *schemaDSN); err != nil {
			fmt.Fprintln(os.Stderr, "tenantdb analyze: lookups disabled:", err)
			lk = nil
		} else {
			defer lk.Close()
		}
	}
	newRun := func(lk *attrib.Lookup) *analyze.Runner {
		return analyze.NewRunner(analyze.Options{
			User: *user, DB: *db, Lookup: lk,
			Attrib: attrib.Options{
				TenantColumns: strings.Split(*tenantCols, ","),
				KeyColumns:    strings.Split(*keyCols, ","),
				Catalog:       cat,
				TagVar:        *tagVar,
				Strict: attrib.StrictOptions{
					Mode:      attrib.StrictAlert,
					Exempt:    strings.Split(*exempt, ","),
					AllowScan: strings.Split(*allowScan, ","),
				},
			},
		})
	}
	var all []analyze.Entry
	for _, name := range files {
		entries, code := readWorkload(name, f)
		if code != 0 {
			return code
		}
		all = append(all, entries...)
	}
	if *learn {
		// First pass: the joins the application runs reveal relationships (a.x = b.y
		// with b.y unique) that the schema does not declare. They are checked against
		// the data in the background; the second pass uses the ones that hold.
		first := newRun(nil)
		for _, e := range all {
			first.Feed(e)
		}
		cat.DrainSuggestions(2 * time.Minute)
	}
	run := newRun(lk)
	if *warm {
		wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Minute)
		quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
		if err := run.Warm(wctx, *schemaDSN, 1_000_000, quiet); err != nil {
			fmt.Fprintln(os.Stderr, "tenantdb analyze: warming the cache failed (row lookups by id will rely on the log):", err)
		}
		wcancel()
	}
	for _, e := range all {
		run.Feed(e)
	}
	rep := run.Report()
	if *asJSON {
		if err := rep.JSON(os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "tenantdb analyze:", err)
			return 1
		}
		return 0
	}
	rep.Text(os.Stdout, *top)
	return 0
}

// readWorkload reads one workload file ("-" is standard input).
func readWorkload(name string, f analyze.Format) ([]analyze.Entry, int) {
	var r io.Reader = os.Stdin
	if name != "-" {
		fh, err := os.Open(name)
		if err != nil {
			fmt.Fprintln(os.Stderr, "tenantdb analyze:", err)
			return nil, 1
		}
		defer fh.Close()
		r = fh
	}
	ff := f
	if ff == analyze.FormatAuto {
		switch {
		case strings.HasSuffix(name, ".sql"):
			ff = analyze.FormatSQL
		case strings.HasSuffix(name, ".csv"):
			ff = analyze.FormatStatements
		case strings.HasSuffix(name, ".log"):
			ff = analyze.FormatLog
		}
	}
	entries, err := analyze.Read(r, ff)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tenantdb analyze:", name+":", err)
		return nil, 1
	}
	return entries, 0
}

package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
)

// runAnalyzeJSON runs `tenantdb analyze -json` and returns its report.
func runAnalyzeJSON(t *testing.T, args ...string) map[string]any {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan []byte)
	go func() { b, _ := io.ReadAll(r); done <- b }()
	rc := runAnalyze(append([]string{"-json"}, args...))
	w.Close()
	os.Stdout = old
	out := <-done
	if rc != 0 {
		t.Fatalf("analyze exited %d: %s", rc, out)
	}
	var rep map[string]any
	if err := json.Unmarshal(out, &rep); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	return rep
}

func TestAnalyzeLearnsRelationshipsFromTheWorkload(t *testing.T) {
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
	cleanup := `DROP TABLE IF EXISTS an_boxes, an_folders CASCADE`
	for _, q := range []string{
		cleanup,
		`CREATE TABLE an_folders (id bigint PRIMARY KEY, tenant_id text)`,
		`CREATE TABLE an_boxes (id bigint PRIMARY KEY, tenant_id text, folder bigint)`,
		`INSERT INTO an_folders SELECT g, 't' || (g % 3) FROM generate_series(1, 30) g`,
		`INSERT INTO an_boxes SELECT g, 't' || ((g % 30 + 1) % 3), (g % 30) + 1 FROM generate_series(1, 90) g`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer conn.Exec(ctx, cleanup)

	sql := filepath.Join(t.TempDir(), "w.sql")
	// `folder` is not named after the table it points at: only the joins say so
	os.WriteFile(sql, []byte(`SELECT b.* FROM an_boxes b JOIN an_folders f ON b.folder = f.id WHERE f.tenant_id = 't1';
SELECT b.* FROM an_boxes b JOIN an_folders f ON b.folder = f.id WHERE f.tenant_id = 't2';
`), 0o600)

	violations := func(rep map[string]any) float64 {
		var n float64
		if v, ok := rep["violations"].(map[string]any); ok {
			for _, c := range v {
				n += c.(float64)
			}
		}
		return n
	}
	base := []string{"-schema-dsn", dsn, "-tenant-columns", "tenant_id", "-warm=false"}
	without := runAnalyzeJSON(t, append(base, "-learn=false", sql)...)
	with := runAnalyzeJSON(t, append(base, sql)...)
	if violations(without) != 2 {
		t.Errorf("without learning, the join through an unknown relationship is not attributed: %v", without["violations"])
	}
	if violations(with) != 0 {
		t.Errorf("with learning the relationship is found and verified, and nothing is flagged: %v", with["violations"])
	}
}

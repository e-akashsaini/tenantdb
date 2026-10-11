package attrib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Block mode against a real server, through the real proxy: a COPY whose rows
// cannot be attributed is aborted with CopyFail, so Postgres rolls it back and
// the session stays usable; a clean one lands.
func TestBlockModeCopyThroughTheProxy(t *testing.T) {
	dsn := os.Getenv("TENANTDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TENANTDB_TEST_DSN not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	for _, q := range []string{
		`DROP TABLE IF EXISTS tdb_blockcopy`,
		`CREATE TABLE tdb_blockcopy (id bigint PRIMARY KEY, tenant_id text, note text)`,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	defer admin.Exec(ctx, `DROP TABLE IF EXISTS tdb_blockcopy`)

	cat := NewCatalog(dsn)
	cat.TenantColumns = []string{"tenant_id"}
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	a := New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat,
		Strict: StrictOptions{Mode: StrictBlock}}, func(Event) {})

	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	srv := proxy.New(proxy.Config{Listen: "127.0.0.1:0", Upstream: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Observer: a.Observer(), Logger: discardLogger()})
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go srv.Serve(sctx)
	defer srv.Close()

	pcfg := cfg.Copy()
	pcfg.Host, pcfg.Port = "127.0.0.1", uint16(srv.Addr().(*net.TCPAddr).Port)
	pcfg.TLSConfig, pcfg.Fallbacks = nil, nil
	conn, err := pgx.ConnectConfig(ctx, pcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	load := func(data string) error {
		_, err := conn.PgConn().CopyFrom(ctx, strings.NewReader(data), "COPY tdb_blockcopy (id, tenant_id, note) FROM STDIN")
		return err
	}

	if err := load("1\tacme\ta\n2\tacme\tb\n"); err != nil {
		t.Fatalf("a load naming one tenant: %v", err)
	}
	if n := countRows(t, admin, ""); n != 2 {
		t.Fatalf("rows after a clean load: %d", n)
	}

	// A row with no tenant: aborted, rolled back, the session still works.
	err = load("3\tacme\tc\n4\t\\N\td\n")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || !strings.Contains(pe.Message, "tenantdb: statement blocked") {
		t.Fatalf("expected the tenantdb refusal, got %v", err)
	}
	if n := countRows(t, admin, ""); n != 2 {
		t.Fatalf("a refused load must leave nothing behind: %d rows", n)
	}
	var one int
	if err := conn.QueryRow(ctx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
		t.Fatalf("the session is not usable after the refusal: %v", err)
	}

	// Inside a transaction the abort is Postgres's own: the transaction fails.
	if _, err := conn.Exec(ctx, `BEGIN`); err != nil {
		t.Fatal(err)
	}
	if err := load("5\t\\N\te\n"); err == nil {
		t.Fatal("expected a refusal inside a transaction")
	}
	if _, err := conn.Exec(ctx, `ROLLBACK`); err != nil {
		t.Fatalf("rollback after a refused copy: %v", err)
	}

	// And it can load again afterwards, many rows, many chunks.
	var b strings.Builder
	for i := 10; i < 20010; i++ {
		fmt.Fprintf(&b, "%d\tglobex\tn%d\n", i, i)
	}
	if err := load(b.String()); err != nil {
		t.Fatalf("a big clean load: %v", err)
	}
	if n := countRows(t, admin, "WHERE tenant_id = 'globex'"); n != 20000 {
		t.Fatalf("big load: %d rows", n)
	}
}

func countRows(t *testing.T, conn *pgx.Conn, where string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM tdb_blockcopy `+where).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

package attrib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Prepared statements through the real proxy in block mode: a driver that caches
// named statements keeps working across a rejected batch, and the database's own
// statement cannot be used to get past the policy.
func TestBlockModePreparedStatementsThroughTheProxy(t *testing.T) {
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
		`DROP TABLE IF EXISTS tdb_prep`,
		`CREATE TABLE tdb_prep (id bigint PRIMARY KEY, tenant_id text NOT NULL, n int)`,
		`INSERT INTO tdb_prep VALUES (1,'acme',10),(2,'acme',20),(3,'globex',30)`,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	defer admin.Exec(ctx, `DROP TABLE IF EXISTS tdb_prep`)

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
	conn, err := pgx.ConnectConfig(ctx, pcfg) // default mode: named, cached prepared statements
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	sum := func(tenant string) (int, error) {
		var n int
		err := conn.QueryRow(ctx, `SELECT sum(n) FROM tdb_prep WHERE tenant_id = $1`, tenant).Scan(&n)
		return n, err
	}
	for i := 0; i < 3; i++ { // the first call prepares, the next ones reuse the cached name
		if n, err := sum("acme"); err != nil || n != 30 {
			t.Fatalf("call %d: %d, %v", i, n, err)
		}
	}

	// A scan is refused whichever way it is sent ...
	refused := func(err error) bool {
		var pe *pgconn.PgError
		return errors.As(err, &pe) && strings.Contains(pe.Message, "tenantdb: statement blocked")
	}
	var n int
	if err := conn.QueryRow(ctx, `SELECT sum(n) FROM tdb_prep`).Scan(&n); !refused(err) {
		t.Fatalf("the scan should have been refused, got %d, %v", n, err)
	}
	// ... as a batch that mixes an allowed statement with it.
	b := &pgx.Batch{}
	b.Queue(`SELECT sum(n) FROM tdb_prep WHERE tenant_id = $1`, "acme")
	b.Queue(`SELECT sum(n) FROM tdb_prep`)
	if err := conn.SendBatch(ctx, b).Close(); !refused(err) {
		t.Fatalf("the batch should have been refused, got %v", err)
	}

	// The cached statement still works afterwards (whether or not the connection survived).
	for i := 0; i < 3; i++ {
		if conn.IsClosed() {
			if conn, err = pgx.ConnectConfig(ctx, pcfg); err != nil {
				t.Fatal(err)
			}
		}
		if n, err := sum("globex"); err != nil || n != 30 {
			t.Fatalf("after a refusal, call %d: %d, %v", i, n, err)
		}
	}

	// SQL-level PREPARE shares the namespace with the protocol: running it through
	// Bind/Execute is checked like anything else.
	if _, err := conn.Exec(ctx, `PREPARE tdb_scan AS SELECT sum(n) FROM tdb_prep`, pgx.QueryExecModeSimpleProtocol); err != nil {
		t.Fatalf("PREPARE alone reads nothing: %v", err)
	}
	if err := conn.QueryRow(ctx, `EXECUTE tdb_scan`).Scan(&n); !refused(err) {
		t.Fatalf("EXECUTE of a prepared scan should be refused, got %d, %v", n, err)
	}
}

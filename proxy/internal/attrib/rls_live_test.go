package attrib

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5"
)

// Policies against a real server: roles, ownership, FORCE, SET ROLE and the
// policy text pg_get_expr produces. For random statements run as random roles the
// tenants Postgres really returns must be inside what the attributor claims.
func TestDifferentialRowLevelSecurity(t *testing.T) {
	dsn := os.Getenv("TENANTDB_TEST_DSN")
	if dsn == "" {
		t.Skip("TENANTDB_TEST_DSN not set")
	}
	n := 600
	if v, err := strconv.Atoi(os.Getenv("TENANTDB_FUZZ_N")); err == nil && v > 0 {
		n = v / 4
	}
	seed := int64(1)
	if v, err := strconv.ParseInt(os.Getenv("TENANTDB_FUZZ_SEED"), 10, 64); err == nil {
		seed = v
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, q := range []string{
		`DROP SCHEMA IF EXISTS tdbrls CASCADE`,
		`DROP ROLE IF EXISTS tdb_app`, `DROP ROLE IF EXISTS tdb_owner`, `DROP ROLE IF EXISTS tdb_member`,
		`CREATE ROLE tdb_owner`, `CREATE ROLE tdb_app`, `CREATE ROLE tdb_member IN ROLE tdb_owner`,
		`CREATE SCHEMA tdbrls AUTHORIZATION tdb_owner`,
		`GRANT USAGE ON SCHEMA tdbrls TO tdb_app, tdb_member`,
		`SET ROLE tdb_owner`,
		`CREATE TABLE tdbrls.plain (id bigint PRIMARY KEY, tenant_id text, amount int)`,
		`CREATE TABLE tdbrls.forced (id bigint PRIMARY KEY, tenant_id text, amount int)`,
		`CREATE TABLE tdbrls.multi (id bigint PRIMARY KEY, tenant_id text, amount int)`,
		`CREATE TABLE tdbrls.restricted (id bigint PRIMARY KEY, tenant_id text, amount int)`,
		`CREATE TABLE tdbrls.byrole (id bigint PRIMARY KEY, tenant_id text, amount int)`,
		`ALTER TABLE tdbrls.plain ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE tdbrls.forced ENABLE ROW LEVEL SECURITY`, `ALTER TABLE tdbrls.forced FORCE ROW LEVEL SECURITY`,
		`ALTER TABLE tdbrls.multi ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE tdbrls.restricted ENABLE ROW LEVEL SECURITY`,
		`ALTER TABLE tdbrls.byrole ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY p ON tdbrls.plain USING (tenant_id = current_setting('app.tenant'))`,
		`CREATE POLICY p ON tdbrls.forced USING (tenant_id = current_setting('app.tenant', true))`,
		`CREATE POLICY a ON tdbrls.multi USING (tenant_id = current_setting('app.tenant'))`,
		`CREATE POLICY b ON tdbrls.multi USING (tenant_id = 't3')`,
		`CREATE POLICY p ON tdbrls.restricted USING (tenant_id IN ('t0','t1','t2'))`,
		`CREATE POLICY r ON tdbrls.restricted AS RESTRICTIVE USING (tenant_id = current_setting('app.tenant'))`,
		`CREATE POLICY p ON tdbrls.byrole USING (tenant_id = current_user)`,
		`GRANT SELECT, UPDATE ON ALL TABLES IN SCHEMA tdbrls TO tdb_app, tdb_member`,
		`RESET ROLE`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, tb := range []string{"plain", "forced", "multi", "restricted", "byrole"} {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO tdbrls.%s SELECT g, 't' || (g %% 4), g FROM generate_series(1, 80) g`, tb)); err != nil {
			t.Fatal(err)
		}
	}
	// a role named like a tenant, to try the current_user policy
	for _, q := range []string{`DROP ROLE IF EXISTS t1`, `CREATE ROLE t1`, `GRANT USAGE ON SCHEMA tdbrls TO t1`, `GRANT SELECT ON tdbrls.byrole TO t1`} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	defer func() {
		conn.Exec(ctx, `RESET ROLE`)
		conn.Exec(ctx, `DROP SCHEMA IF EXISTS tdbrls CASCADE`)
		for _, r := range []string{"tdb_app", "tdb_owner", "tdb_member", "t1"} {
			conn.Exec(ctx, `DROP ROLE IF EXISTS `+r)
		}
	}()

	cat := NewCatalog(dsn)
	cat.TenantColumns = []string{"tenant_id"}
	if err := cat.Load(ctx); err != nil {
		t.Fatal(err)
	}
	h := &harness{s: &proxy.Session{ID: 1, User: "postgres", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: cat}, h.record)

	r := rand.New(rand.NewSource(seed))
	tables := []string{"plain", "forced", "multi", "restricted", "byrole"}
	roles := []string{"tdb_app", "tdb_owner", "tdb_member", "t1", "postgres"}
	credited, unsound := 0, 0
	for i := 0; i < n; i++ {
		role := roles[r.Intn(len(roles))]
		tb := tables[r.Intn(len(tables))]
		tenant := fmt.Sprintf("t%d", r.Intn(4))
		setVar := r.Intn(8) != 0 // sometimes leave app.tenant to the previous value
		pred := []string{"true", "amount > " + strconv.Itoa(r.Intn(80)), "id = " + strconv.Itoa(r.Intn(80)+1),
			"tenant_id <> 't" + strconv.Itoa(r.Intn(4)) + "'", "tenant_id = 't" + strconv.Itoa(r.Intn(4)) + "'",
			"tenant_id = 't" + strconv.Itoa(r.Intn(4)) + "' OR amount > 70"}[r.Intn(6)]

		h.set("RESET ROLE")
		h.set("SET ROLE " + role)
		if role == "postgres" {
			h.set("RESET ROLE")
		}
		if _, err := conn.Exec(ctx, `RESET ROLE`); err != nil {
			t.Fatal(err)
		}
		if setVar {
			if _, err := conn.Exec(ctx, fmt.Sprintf(`SET app.tenant = '%s'`, tenant)); err != nil {
				t.Fatal(err)
			}
			h.set(fmt.Sprintf(`SET app.tenant = '%s'`, tenant))
		}
		if role != "postgres" {
			if _, err := conn.Exec(ctx, `SET ROLE `+role); err != nil {
				t.Fatal(err)
			}
		}
		// the policy's current_setting without a prior SET may fail: skip those
		sql := fmt.Sprintf(`SELECT * FROM tdbrls.%s WHERE %s`, tb, pred)
		truthSQL := fmt.Sprintf(`SELECT DISTINCT tenant_id FROM tdbrls.%s WHERE %s`, tb, pred)
		if role == "t1" && tb != "byrole" {
			continue // no privileges
		}
		rows, err := conn.Query(ctx, truthSQL)
		if err != nil {
			conn.Exec(ctx, `RESET ROLE`)
			continue
		}
		actual := map[string]bool{}
		for rows.Next() {
			var tv *string
			if err := rows.Scan(&tv); err != nil {
				t.Fatal(err)
			}
			if tv != nil {
				actual[*tv] = true
			}
		}
		rows.Close()
		if rows.Err() != nil {
			conn.Exec(ctx, `RESET ROLE`)
			continue
		}
		e := h.attributed(t, sql)
		switch e.Source {
		case SourceExplicit, SourceCache, SourceLookup:
			credited++
			claim := map[string]bool{}
			for _, c := range e.Tenants {
				claim[c] = true
			}
			for tv := range actual {
				if !claim[tv] {
					unsound++
					t.Errorf("UNSOUND as %s, app.tenant=%s:\n  %s\n  claim:  %v\n  actual: %v", role, tenant, sql, e.Tenants, sortedSet(actual))
				}
			}
		case SourceNoMatch:
			credited++
			if len(actual) != 0 {
				unsound++
				t.Errorf("UNSOUND (nomatch) as %s:\n  %s\n  actual: %v", role, sql, sortedSet(actual))
			}
		}
	}
	t.Logf("seed=%d: %d statements, %d credited with a tenant, %d unsound", seed, n, credited, unsound)
	if credited == 0 {
		t.Error("no statement was ever credited with a policy: the test is not exercising anything")
	}
}

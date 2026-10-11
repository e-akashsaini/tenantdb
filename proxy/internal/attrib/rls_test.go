package attrib

import (
	"reflect"
	"testing"

	"github.com/e-akashsaini/tenantdb/internal/proxy"
)

func rlsSpec(policies ...policy) catSpec {
	sp := soundSpec()
	sp.rls = []string{"public.orders"}
	if len(policies) == 0 {
		policies = []policy{{cmd: '*', permissive: true, qual: "(tenant_id = current_setting('app.tenant'::text))"}}
	}
	sp.rlsd = &rlsData{
		roles: map[string]roleInfo{
			"app": {}, "acme": {}, "other": {}, "owner": {}, "admin": {super: true}, "bypasser": {bypass: true},
			"member": {},
		},
		members:  map[string][]string{"member": {"owner"}},
		tables:   map[string]rlsTable{"public.orders": {owner: "owner"}},
		policies: map[string][]policy{"public.orders": policies},
	}
	return sp
}

func rlsHarnessAs(sp catSpec, user string) *harness {
	h := &harness{s: &proxy.Session{ID: 1, User: user, DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: sp.build()}, h.record)
	return h
}

func (h *harness) set(sql string) { h.query(sql); h.rfq('I') }

func TestPoliciesConfineEveryStatementOnTheTable(t *testing.T) {
	h := rlsHarnessAs(rlsSpec(), "app")
	if e := h.attributed(t, `SELECT * FROM orders`); e.Source == SourceExplicit {
		t.Fatalf("app.tenant has not been set: nothing says whose rows these are: %+v", e)
	}
	h.set(`SET app.tenant = 'acme'`)
	for _, c := range []struct {
		name, sql string
		tenants   []string
	}{
		{"an unfiltered read", `SELECT * FROM orders`, []string{"acme"}},
		{"with other conditions", `SELECT * FROM orders WHERE status = 'open'`, []string{"acme"}},
		{"an update", `UPDATE orders SET status = 'x' WHERE id = 5 OR status = 'y'`, []string{"acme"}},
		{"a delete", `DELETE FROM orders WHERE amount > 5`, []string{"acme"}},
		{"through a join, to the tables the policy's table is tied to", `SELECT i.* FROM items i JOIN orders o ON i.order_id = o.id`, []string{"acme"}},
		{"in a subquery", `SELECT * FROM items WHERE order_id IN (SELECT id FROM orders)`, []string{"acme"}},
		{"alongside the statement's own tenant", `SELECT * FROM orders WHERE tenant_id = 'acme'`, []string{"acme"}},
	} {
		e := h.attributed(t, c.sql)
		if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s:\n  %s\n  got %s %v (%s), want %v", c.name, c.sql, e.Source, e.Tenants, e.Violation, c.tenants)
		}
	}
	// the statement's own tenant and the policy's cannot both hold
	if e := h.attributed(t, `SELECT * FROM orders WHERE tenant_id = 'globex'`); e.Source != SourceNoMatch {
		t.Errorf("the policy hides every other tenant's rows: %+v", e)
	}
	// a table without a policy is as before
	if e := h.attributed(t, `SELECT * FROM customers`); e.Source == SourceExplicit {
		t.Errorf("customers has no policy: %+v", e)
	}
}

func TestPoliciesAreOnlyCreditedWhereTheyReallyApply(t *testing.T) {
	notCredited := func(name string, h *harness, sql string) {
		t.Helper()
		if e := h.attributed(t, sql); e.Source == SourceExplicit {
			t.Errorf("%s: %+v", name, e)
		}
	}
	set := `SET app.tenant = 'acme'`
	for user, why := range map[string]string{
		"admin": "a superuser", "bypasser": "BYPASSRLS", "owner": "the table's owner", "member": "a member of the owner",
		"nobody": "a role the catalog does not know",
	} {
		h := rlsHarnessAs(rlsSpec(), user)
		h.set(set)
		notCredited(why, h, `SELECT * FROM orders`)
	}
	// FORCE ROW LEVEL SECURITY brings the owner under the policies
	sp := rlsSpec()
	sp.rlsd.tables["public.orders"] = rlsTable{owner: "owner", force: true}
	h := rlsHarnessAs(sp, "owner")
	h.set(set)
	if e := h.attributed(t, `SELECT * FROM orders`); e.Source != SourceExplicit {
		t.Errorf("forced: %+v", e)
	}

	// SET ROLE changes who the policies are applied to
	h = rlsHarnessAs(rlsSpec(), "app")
	h.set(set)
	h.set(`SET ROLE admin`)
	notCredited("after SET ROLE to a superuser", h, `SELECT * FROM orders`)
	h.set(`RESET ROLE`)
	if e := h.attributed(t, `SELECT * FROM orders`); e.Source != SourceExplicit {
		t.Errorf("after RESET ROLE: %+v", e)
	}
	h.set(`SET ROLE owner`)
	notCredited("after SET ROLE to the owner", h, `SELECT * FROM orders`)
	h.set(`SET ROLE NONE`)
	h.query(`BEGIN`)
	h.rfq('T')
	h.query(`SET LOCAL ROLE admin`)
	h.rfq('T')
	h.query(`SELECT * FROM orders`)
	if e := h.last(t); e.Source == SourceExplicit {
		t.Errorf("after SET LOCAL ROLE: %+v", e)
	}
	h.rfq('T')
	h.query(`COMMIT`)
	h.rfq('I')
	if e := h.attributed(t, `SELECT * FROM orders`); e.Source != SourceExplicit {
		t.Errorf("the local role ended with the transaction: %+v", e)
	}
	h.set(`SELECT set_config('role', 'admin', false)`)
	notCredited("set_config('role')", h, `SELECT * FROM orders`)
	h.set(`DISCARD ALL`)
	h.set(set)
	if e := h.attributed(t, `SELECT * FROM orders`); e.Source != SourceExplicit {
		t.Errorf("after DISCARD ALL: %+v", e)
	}
	h.set(`SET SESSION AUTHORIZATION admin`)
	notCredited("after SET SESSION AUTHORIZATION", h, `SELECT * FROM orders`)

	// policies for other commands or other roles do not apply
	h = rlsHarnessAs(rlsSpec(policy{cmd: 'a', permissive: true, qual: "(tenant_id = current_setting('app.tenant'::text))"}), "app")
	h.set(set)
	notCredited("a policy for INSERT only", h, `SELECT * FROM orders`)
	h = rlsHarnessAs(rlsSpec(policy{cmd: '*', permissive: true, roles: []string{"other"}, qual: "(tenant_id = current_setting('app.tenant'::text))"}), "app")
	h.set(set)
	notCredited("a policy for another role", h, `SELECT * FROM orders`)
	h = rlsHarnessAs(rlsSpec(policy{cmd: '*', permissive: true, roles: []string{"other"}, qual: "(tenant_id = current_setting('app.tenant'::text))"}), "other")
	h.set(set)
	if e := h.attributed(t, `SELECT * FROM orders`); e.Source != SourceExplicit {
		t.Errorf("a policy for this role: %+v", e)
	}
}

func TestPolicyShapes(t *testing.T) {
	cs := "(tenant_id = current_setting('app.tenant'::text))"
	for _, c := range []struct {
		name     string
		policies []policy
		user     string
		tenants  []string // nil: must not be claimed
	}{
		{"two permissive policies are alternatives", []policy{
			{cmd: '*', permissive: true, qual: "(tenant_id = 't1'::text)"}, {cmd: '*', permissive: true, qual: "(tenant_id = 't2'::text)"}},
			"app", []string{"t1", "t2"}},
		{"... and one of them may allow everything", []policy{
			{cmd: '*', permissive: true, qual: "(tenant_id = 't1'::text)"}, {cmd: '*', permissive: true, qual: "true"}},
			"app", nil},
		{"a restrictive policy narrows", []policy{
			{cmd: '*', permissive: true, qual: "(tenant_id IN ('t1'::text, 't2'::text))"}, {cmd: '*', permissive: false, qual: "(tenant_id = 't2'::text)"}},
			"app", []string{"t2"}},
		{"a restrictive policy on its own does not allow anything", []policy{
			{cmd: '*', permissive: false, qual: cs}},
			"app", nil},
		{"the policy names the role", []policy{
			{cmd: '*', permissive: true, qual: "(tenant_id = CURRENT_USER)"}},
			"acme", []string{"acme"}},
		{"a policy the proxy cannot read confines nothing", []policy{
			{cmd: '*', permissive: true, qual: "(tenant_id = ANY (string_to_array(current_setting('app.tenant'::text), ','::text)))"}},
			"app", nil},
		{"a policy that consults another table confines nothing", []policy{
			{cmd: '*', permissive: true, qual: "(tenant_id IN ( SELECT m.tenant_id FROM memberships m WHERE (m.user_name = CURRENT_USER)))"}},
			"app", nil},
		{"qualified column names", []policy{
			{cmd: '*', permissive: true, qual: "(orders.tenant_id = 't7'::text)"}},
			"app", []string{"t7"}},
	} {
		h := rlsHarnessAs(rlsSpec(c.policies...), c.user)
		h.set(`SET app.tenant = 'acme'`)
		e := h.attributed(t, `SELECT * FROM orders`)
		if c.tenants == nil {
			if e.Source == SourceExplicit {
				t.Errorf("%s: claimed %v", c.name, e.Tenants)
			}
			continue
		}
		if e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, c.tenants) {
			t.Errorf("%s: got %s %v, want %v", c.name, e.Source, e.Tenants, c.tenants)
		}
	}
}

func TestPoliciesAreNotCreditedToCodeThatRunsWithItsOwnersRights(t *testing.T) {
	sp := rlsSpec()
	sp.rows = append(sp.rows, tbl("public", "orders_view", 'v', "id")...)
	sp.views = map[string]string{"public.orders_view": `SELECT orders.id FROM public.orders`}
	h := rlsHarnessAs(sp, "app")
	h.set(`SET app.tenant = 'acme'`)
	// the view's owner reads orders, and owners are exempt: the policy may not apply
	if e := h.attributed(t, `SELECT * FROM orders_view`); e.Source == SourceExplicit {
		t.Errorf("view: %+v", e)
	}
}

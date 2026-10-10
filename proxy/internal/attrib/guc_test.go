package attrib

import (
	"reflect"
	"testing"

	"github.com/akashsaini/tenantdb/internal/proxy"
	"github.com/jackc/pgx/v5/pgproto3"
)

// The row-level-security style of tenancy: the application sets a session
// variable once and every query compares the tenant column to it.

func gucHarness() *harness { return specHarness(soundSpec(), StrictOptions{}) }

func TestCurrentSettingResolvesToWhatTheConnectionSet(t *testing.T) {
	h := gucHarness()
	h.query(`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant')`)
	if e := h.last(t); e.Source == SourceExplicit {
		t.Errorf("nothing has set app.tenant yet: %+v", e)
	}
	h.rfq('I')
	h.query(`SET app.tenant = 'acme'`)
	h.rfq('I')
	for _, sql := range []string{
		`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant')`,
		`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant', true)`,
		`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant')::text`,
		`SELECT * FROM orders WHERE current_setting('app.tenant') = tenant_id AND status = 'open'`,
		`SELECT * FROM orders WHERE tenant_id = CURRENT_SETTING('APP.TENANT')`,
		`SELECT o.* FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.tenant_id = current_setting('app.tenant')`,
	} {
		h.query(sql)
		if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
			t.Errorf("%s: %+v", sql, e)
		}
		h.rfq('I')
	}
	// the value follows the connection
	h.query(`SET app.tenant = 'globex'`)
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant')`)
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"globex"}) {
		t.Errorf("after changing it: %+v", e)
	}
	// ... and a variable of another name says nothing
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE tenant_id = current_setting('app.other')`)
	if e := h.last(t); e.Source == SourceExplicit {
		t.Errorf("a different variable: %+v", e)
	}
}

func TestSessionVariableLifetime(t *testing.T) {
	h := gucHarness()
	read := func() Event {
		h.rfq('I')
		h.query(`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant', true)`)
		return h.last(t)
	}
	h.query(`SET app.tenant = 'a'`)
	if e := read(); !reflect.DeepEqual(e.Tenants, []string{"a"}) {
		t.Fatalf("SET: %+v", e)
	}
	h.query(`BEGIN`)
	h.query(`SET LOCAL app.tenant = 'b'`)
	h.query(`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant')`)
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"b"}) {
		t.Errorf("SET LOCAL inside the transaction: %+v", e)
	}
	h.query(`COMMIT`)
	if e := read(); !reflect.DeepEqual(e.Tenants, []string{"a"}) {
		t.Errorf("after COMMIT the session value is back: %+v", e)
	}
	h.query(`SELECT set_config('app.tenant', 'c', false)`)
	if e := read(); !reflect.DeepEqual(e.Tenants, []string{"c"}) {
		t.Errorf("set_config: %+v", e)
	}
	h.query(`RESET app.tenant`)
	if e := read(); e.Source == SourceExplicit {
		t.Errorf("after RESET: %+v", e)
	}
	h.query(`SET app.tenant = 'd'`)
	h.query(`DISCARD ALL`)
	if e := read(); e.Source == SourceExplicit {
		t.Errorf("after DISCARD ALL: %+v", e)
	}
}

func TestSetConfigWithABoundValue(t *testing.T) {
	// What most ORMs and drivers do: SELECT set_config('app.tenant', $1, false)
	h := gucHarness()
	h.front(&pgproto3.Parse{Name: "s", Query: `SELECT set_config('app.tenant', $1, false)`})
	h.front(&pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte("acme")}})
	h.front(&pgproto3.Execute{})
	h.front(&pgproto3.Sync{})
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant')`)
	if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("%+v", e)
	}
}

func TestSessionVariableFromTheStartupOptions(t *testing.T) {
	h := gucHarness()
	h.s = &proxy.Session{ID: 1, User: "app", DB: "d", Params: map[string]string{"options": "-c app.tenant=acme"}}
	h.query(`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant')`)
	if e := h.last(t); !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("%+v", e)
	}
}

func TestSessionVariablesInInsertsAndFunctionArguments(t *testing.T) {
	spec := soundSpec()
	h := specHarness(spec, StrictOptions{})
	h.query(`SET app.tenant = 'acme'`)
	h.rfq('I')
	h.query(`INSERT INTO orders (id, tenant_id) VALUES (1, current_setting('app.tenant'))`)
	if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("INSERT: %+v", e)
	}
	h.reply(nil, nil, "INSERT 0 1")
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE id = 1`)
	if e := h.last(t); e.Source != SourceCache {
		t.Errorf("learned the row's owner from the INSERT: %+v", e)
	}
	h.rfq('I')
	h.query(`SELECT * FROM orders_of(current_setting('app.tenant'))`)
	if e := h.last(t); e.Source != SourceExplicit || !reflect.DeepEqual(e.Tenants, []string{"acme"}) {
		t.Errorf("function argument: %+v", e)
	}
}

func TestSessionVariableDoesNotConfineAnOrAlternative(t *testing.T) {
	h := gucHarness()
	h.query(`SET app.tenant = 'acme'`)
	h.rfq('I')
	h.query(`SELECT * FROM orders WHERE tenant_id = current_setting('app.tenant') OR status = 'open'`)
	if e := h.last(t); e.Source == SourceExplicit {
		t.Errorf("%+v", e)
	}
}

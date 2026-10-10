package attrib

import (
	"context"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Row-level security.
//
// A table with RLS enabled shows a role only the rows its policies allow. When
// the policy is `tenant_id = current_setting('app.tenant')`, every statement the
// application sends is confined to that tenant whether or not it says so: the
// policy is one more condition on the table, one the statement cannot drop. So
// it is evaluated like the WHERE clause of a view: as an extra condition on the
// occurrence, and the usual rules decide what it confines.
//
// It is only credited when the role the statement runs as is known and really is
// subject to the policies: not a superuser, not BYPASSRLS, not the table's owner
// (unless FORCE ROW LEVEL SECURITY). It is not credited inside views or function
// bodies, which run with their owner's rights, nor after a SET ROLE it cannot read.

type roleInfo struct{ super, bypass bool }

type rlsTable struct {
	owner string
	force bool
}

// policy is one CREATE POLICY. cmd is pg_policy.polcmd: '*', 'r' (SELECT), 'a'
// (INSERT), 'w' (UPDATE), 'd' (DELETE). roles is empty for PUBLIC.
type policy struct {
	cmd        byte
	permissive bool
	roles      []string
	qual       string // USING
}

type rlsData struct {
	roles    map[string]roleInfo
	members  map[string][]string // role -> the roles it is a member of
	tables   map[string]rlsTable // table key -> owner and FORCE
	policies map[string][]policy // table key -> its policies
}

const rolesSQL = `SELECT rolname, rolsuper, rolbypassrls FROM pg_roles`

const membersSQL = `
SELECT m.rolname, r.rolname FROM pg_auth_members am
JOIN pg_roles m ON m.oid = am.member JOIN pg_roles r ON r.oid = am.roleid`

const rlsTablesSQL = `
SELECT n.nspname, c.relname, pg_get_userbyid(c.relowner), c.relforcerowsecurity
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relrowsecurity AND n.nspname NOT IN ('pg_catalog','information_schema')`

const policySQL = `
SELECT n.nspname, c.relname, p.polcmd::text, p.polpermissive,
       COALESCE((SELECT array_agg(r.rolname) FROM pg_roles r WHERE r.oid = ANY(p.polroles)), '{}'::name[])::text[],
       p.polroles = '{0}'::oid[], COALESCE(pg_get_expr(p.polqual, p.polrelid), '')
FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid JOIN pg_namespace n ON n.oid = c.relnamespace`

// loadRLS reads roles, ownership and policies. A role that cannot read them
// gets nothing, and no statement is then credited with a policy.
func loadRLS(ctx context.Context, conn *pgx.Conn) *rlsData {
	d := &rlsData{roles: map[string]roleInfo{}, members: map[string][]string{},
		tables: map[string]rlsTable{}, policies: map[string][]policy{}}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SET LOCAL search_path = pg_catalog`); err != nil {
		return nil
	}
	rows, err := tx.Query(ctx, rolesSQL)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var n string
		var ri roleInfo
		if err := rows.Scan(&n, &ri.super, &ri.bypass); err != nil {
			rows.Close()
			return nil
		}
		d.roles[n] = ri
	}
	rows.Close()
	rows, err = tx.Query(ctx, membersSQL)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var m, r string
		if err := rows.Scan(&m, &r); err != nil {
			rows.Close()
			return nil
		}
		d.members[m] = append(d.members[m], r)
	}
	rows.Close()
	rows, err = tx.Query(ctx, rlsTablesSQL)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var s, t, o string
		var f bool
		if err := rows.Scan(&s, &t, &o, &f); err != nil {
			rows.Close()
			return nil
		}
		d.tables[strings.ToLower(s+"."+t)] = rlsTable{owner: o, force: f}
	}
	rows.Close()
	rows, err = tx.Query(ctx, policySQL)
	if err != nil {
		return nil
	}
	for rows.Next() {
		var s, t, cmd, qual string
		var perm, public bool
		var roles []string
		if err := rows.Scan(&s, &t, &cmd, &perm, &roles, &public, &qual); err != nil {
			rows.Close()
			return nil
		}
		p := policy{permissive: perm, qual: qual}
		if cmd != "" {
			p.cmd = cmd[0]
		}
		if !public {
			p.roles = roles
		}
		k := strings.ToLower(s + "." + t)
		d.policies[k] = append(d.policies[k], p)
	}
	rows.Close()
	return d
}

// closure is role and every role it is (transitively) a member of.
func (d *rlsData) closure(role string) map[string]bool {
	out := map[string]bool{role: true}
	queue := []string{role}
	for len(queue) > 0 {
		r := queue[0]
		queue = queue[1:]
		for _, m := range d.members[r] {
			if !out[m] {
				out[m] = true
				queue = append(queue, m)
			}
		}
	}
	return out
}

// RLSFor returns the USING expressions of the policies that apply to role for a
// statement of kind cmd ('r' read, 'w' update, 'd' delete) on ti: the permissive
// ones (any may allow a row) and the restrictive ones (all must). applies is
// false when the role is not subject to row-level security on ti, or when it
// cannot be known, and no condition may then be assumed.
func (c *Catalog) RLSFor(ti *TableInfo, role string, cmd byte) (permissive, restrictive []string, applies bool) {
	if c == nil || ti == nil || !ti.rls || role == "" {
		return nil, nil, false
	}
	c.mu.RLock()
	d := c.rls
	c.mu.RUnlock()
	if d == nil {
		return nil, nil, false
	}
	ri, known := d.roles[role]
	tab, haveTab := d.tables[ti.Key()]
	if !known || !haveTab || ri.super || ri.bypass {
		return nil, nil, false
	}
	member := d.closure(role)
	if member[tab.owner] && !tab.force {
		return nil, nil, false // the owner is exempt unless the table is forced
	}
	for _, p := range d.policies[ti.Key()] {
		if p.cmd != '*' && p.cmd != cmd {
			continue
		}
		if len(p.roles) > 0 {
			hit := false
			for _, r := range p.roles {
				if member[r] {
					hit = true
					break
				}
			}
			if !hit {
				continue
			}
		}
		if p.permissive {
			permissive = append(permissive, p.qual)
		} else {
			restrictive = append(restrictive, p.qual)
		}
	}
	if len(permissive) == 0 {
		return nil, nil, false // default deny: the role sees nothing; nothing to claim
	}
	return permissive, restrictive, true
}

// qualCache holds parsed policy expressions.
var qualCache sync.Map // string -> *pg_query.Node (nil: unparseable)

func parseQual(qual string) *pg_query.Node {
	if v, ok := qualCache.Load(qual); ok {
		n, _ := v.(*pg_query.Node)
		return n
	}
	var node *pg_query.Node
	if strings.TrimSpace(qual) != "" {
		if res, err := Parse("SELECT 1 WHERE " + qual); err == nil && len(res.Stmts) == 1 {
			if sel := res.Stmts[0].Stmt.GetSelectStmt(); sel != nil {
				node = sel.WhereClause
			}
		}
	}
	qualCache.Store(qual, node)
	return node
}

// policyCond turns the policies that apply to an occurrence into a condition on
// it: (permissive1 OR permissive2 ...) AND restrictive1 AND .... A policy that
// cannot be read is a condition that confines nothing, which is the right
// reading for an alternative and harmless for a conjunct.
func (ev *confEval) policyCond(occID string, ti *TableInfo, permissive, restrictive []string) *Cond {
	b := &builder{e: ev.a.ex, prefix: "rls:", subSeen: map[*pg_query.SubLink]bool{}, subNode: map[*pg_query.SubLink]*QueryNode{}}
	sc := &bscope{names: map[string]string{strings.ToLower(ti.Name): occID}, occs: []string{occID}}
	dummy := &QueryNode{}
	build := func(qual string) *Cond {
		n := parseQual(qual)
		if n == nil {
			return &Cond{Kind: CondOther}
		}
		return b.expr(dummy, sc, n)
	}
	var perm *Cond
	if len(permissive) == 1 {
		perm = build(permissive[0])
	} else {
		perm = &Cond{Kind: CondOr}
		for _, q := range permissive {
			perm.Kids = append(perm.Kids, build(q))
		}
	}
	if len(restrictive) == 0 {
		return perm
	}
	and := &Cond{Kind: CondAnd, Kids: []*Cond{perm}}
	for _, q := range restrictive {
		and.Kids = append(and.Kids, build(q))
	}
	return and
}

// roleState is the role a session runs as, as far as the proxy can tell.
type roleState struct {
	set     bool   // SET ROLE / SET SESSION AUTHORIZATION was seen
	name    string // ... to this role ("" with set: back to the login user)
	unknown bool   // ... to something that could not be read
}

// effRole is the role the session's statements run as. ok is false if it cannot
// be known.
func (st *state) effRole() (string, bool) {
	r := st.roleS
	if st.roleL.set {
		r = st.roleL
	}
	switch {
	case r.unknown:
		return "", false
	case r.set && r.name != "":
		return r.name, true
	}
	return st.user, st.user != ""
}

// applyRole records SET ROLE / SET SESSION AUTHORIZATION.
func (st *state) applyRole(sv *SetVar) {
	r := roleState{set: true}
	switch {
	case sv.Reset:
	case len(sv.Values) == 1 && strings.EqualFold(sv.Values[0], "none"), len(sv.Values) == 1 && strings.EqualFold(sv.Values[0], "default"):
	case len(sv.Values) == 1:
		r.name = sv.Values[0]
	default:
		r.unknown = true
	}
	if sv.Name == "session_authorization" {
		// it changes the login user too: not tracked beyond "unknown" unless reset
		if !sv.Reset && !(len(sv.Values) == 1 && strings.EqualFold(sv.Values[0], "default")) {
			r.unknown = true
		}
	}
	if sv.Local {
		st.roleL = r
	} else {
		st.roleS = r
	}
}

package attrib

import (
	"strings"

	"github.com/akashsaini/tenantdb/internal/proxy"
)

// catSpec describes a schema for tests.
type catSpec struct {
	rows       []colRow
	unique     map[string][]string // "schema.table" -> unique columns
	funcs      []*FuncDef
	rls        []string // "schema.table"
	subjectRLS bool     // the loading role is subject to row-level security
	roleSearch map[[2]string][]string
	baseSearch []string
	fks        map[string]map[string]FKRef // "schema.table" -> column -> parent
	polys      map[string]map[string]map[string]FKRef
}

func (sp catSpec) snapshot() snapshotData {
	d := snapshotData{cols: sp.rows, unique: map[string]map[string]bool{}, funcs: sp.funcs,
		bypassRLS: !sp.subjectRLS, roleSearch: sp.roleSearch, baseSearch: sp.baseSearch, fks: sp.fks, polys: sp.polys}
	for k, cols := range sp.unique {
		d.unique[k] = map[string]bool{}
		for _, c := range cols {
			d.unique[k][c] = true
		}
	}
	rls := map[string]bool{}
	for _, k := range sp.rls {
		rls[k] = true
	}
	for i := range d.cols {
		if rls[strings.ToLower(d.cols[i].schema+"."+d.cols[i].table)] {
			d.cols[i].rls = true
		}
	}
	return d
}

func (sp catSpec) build() *Catalog {
	c := NewCatalog("")
	c.apply(sp.snapshot())
	return c
}

// set loads a snapshot where every "id" and "code" column is a primary key and
// the listed functions exist as opaque (C) functions. Tests that care about
// uniqueness or function bodies use catSpec directly.
func (c *Catalog) set(rows []colRow, funcs map[string]bool) {
	sp := catSpec{rows: rows, unique: map[string][]string{}}
	for _, r := range rows {
		if r.col == "id" || r.col == "code" {
			k := strings.ToLower(r.schema + "." + r.table)
			sp.unique[k] = append(sp.unique[k], r.col)
		}
	}
	for k := range funcs {
		s, n, _ := strings.Cut(k, ".")
		sp.funcs = append(sp.funcs, &FuncDef{Schema: s, Name: n, Lang: "c", Kind: 'f'})
	}
	c.apply(sp.snapshot())
}

func specHarness(sp catSpec, opt StrictOptions) *harness {
	h := &harness{s: &proxy.Session{ID: 1, User: "app", DB: "d"}}
	h.a = New(Options{TenantColumns: []string{"tenant_id"}, KeyColumns: []string{"id"}, Catalog: sp.build(), Strict: opt}, h.record)
	return h
}

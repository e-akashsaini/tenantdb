package attrib

// tempReg is a temporary table created by a statement still awaiting its reply.
type tempReg struct {
	def     *TempDef
	kind    tempKind
	tenants []string
}

type tempKind int

const (
	// tempTainted: filled from data the proxy could not attribute; reading it is unknown.
	tempTainted tempKind = iota
	// tempUntenanted: filled from data that belongs to no tenant.
	tempUntenanted
	// tempDerived: filled from one set of tenants' data, which it inherits.
	tempDerived
	// tempTenant: it has a tenant column of its own, so it is filtered like any tenant table.
	tempTenant
)

// tempTable is a temporary table of one session.
type tempTable struct {
	dropOnCommit bool
	kind         tempKind
	tenants      []string
	info         *TableInfo // columns, for tempTenant
}

// registerTemp remembers a temporary table once its CREATE succeeded.
func (a *Attributor) registerTemp(st *state, reg *tempReg) {
	def := reg.def
	t := &tempTable{kind: reg.kind, tenants: reg.tenants}
	cols := append([]string(nil), def.Cols...)
	complete := true
	for _, l := range def.Like {
		li, ltmp, ok := a.tableFor(st, l)
		switch {
		case !ok:
			complete = false
		case ltmp != nil && ltmp.info != nil:
			cols = append(cols, ltmp.info.Cols...)
		case ltmp != nil:
			complete = false
		default:
			cols = append(cols, li.Cols...)
		}
	}
	hasTenant := false
	for _, c := range cols {
		hasTenant = hasTenant || a.tenants[c]
	}
	switch {
	case hasTenant && (!def.FromQuery || len(def.Cols) > 0):
		// It has a tenant column of its own, so it is filtered like any tenant table.
		t.kind = tempTenant
		t.info = &TableInfo{Schema: "pg_temp", Name: def.Name, Kind: 'r', Cols: cols, raw: cols}
	case !def.FromQuery && complete:
		t.kind = tempUntenanted
	case !def.FromQuery:
		t.kind = tempTainted
	}
	t.dropOnCommit = def.OnCommitDrop
	st.temps[def.Name] = t
	st.txnTemps = append(st.txnTemps, def.Name)
}

// provenance says what a temporary table created from a query inherits.
func (a *Attributor) provenance(st *state, r analysis, tenants []string) (tempKind, []string) {
	switch {
	case r.unresolvedFn || r.scan || r.unsure:
		return tempTainted, nil
	case len(tenants) > 0 || len(r.derived) > 0:
		return tempDerived, dedupe(append(append([]string(nil), tenants...), r.derived...))
	case !r.tenantTouched:
		return tempUntenanted, nil
	case st.curTag() != "":
		return tempDerived, []string{st.curTag()}
	}
	return tempTainted, nil
}

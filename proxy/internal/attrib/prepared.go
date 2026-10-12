package attrib

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// bindEnv is what a statement's names were resolved against when it was prepared.
type bindEnv struct {
	temps map[string]*tempTable
	path  []string
}

func (a *Attributor) captureEnv(st *state) *bindEnv {
	e := &bindEnv{path: a.pathOf(st)}
	if len(st.temps) > 0 {
		e.temps = make(map[string]*tempTable, len(st.temps))
		for k, v := range st.temps {
			e.temps[k] = v
		}
	}
	return e
}

type prepared struct {
	stmts    []Stmt
	unparsed string // the SQL, if it could not be analysed
}

type portal struct {
	unparsed string
	stmts    []Stmt
	params   []string // decoded bound values; valid[i] is false for NULL
	valid    []bool
}

// resolve turns an operand into its value, using bound parameters if any.
func (p *portal) resolve(o Operand) (string, bool) {
	if !o.Set {
		return "", false
	}
	if o.Param == 0 {
		return o.Lit, true
	}
	if p == nil || o.Param > len(p.params) || !p.valid[o.Param-1] {
		return "", false
	}
	return p.params[o.Param-1], true
}

func formatFor(codes []int16, i int) int16 {
	switch len(codes) {
	case 0:
		return 0
	case 1:
		return codes[0]
	}
	if i < len(codes) {
		return codes[i]
	}
	return 0
}

// decodeParam turns a bound parameter into the string form a tenant id would
// have in SQL. Binary values are decoded by size: 16 bytes as a UUID, 4 or 8
// as an integer, anything else as text.
func decodeParam(b []byte, format int16) string {
	if format == 0 {
		return string(b)
	}
	switch len(b) {
	case 16:
		h := hex.EncodeToString(b)
		return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:])
	case 8:
		return fmt.Sprint(int64(binary.BigEndian.Uint64(b)))
	case 4:
		return fmt.Sprint(int32(binary.BigEndian.Uint32(b)))
	}
	return string(b)
}

package attrib

import "strings"

func isDDL(tag string) bool {
	return strings.HasPrefix(tag, "CREATE ") || strings.HasPrefix(tag, "ALTER ") || strings.HasPrefix(tag, "DROP ")
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]bool{}
	for _, v := range a {
		m[v] = true
	}
	for _, v := range b {
		if !m[v] {
			return false
		}
	}
	return true
}

func dedupe(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

// subsetOf: is every element of a in b?
func subsetOf(a, b []string) bool {
	in := make(map[string]bool, len(b))
	for _, v := range b {
		in[v] = true
	}
	for _, v := range a {
		if !in[v] {
			return false
		}
	}
	return true
}

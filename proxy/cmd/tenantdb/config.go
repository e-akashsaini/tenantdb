package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

// A config file is a list of `name: value` (or `name = value`) lines, one per
// command-line flag, with the flag's name and `#` comments:
//
//	listen: ":6432"
//	upstream: db.internal:5432
//	strict: alert
//	# schema-dsn is better in the environment: this file would then hold a password
//
// Values may be quoted. Precedence, highest first: command line, config file,
// environment variables, built-in defaults.

// parseConfig reads the file into name -> value, in order of appearance.
func parseConfig(path string) (map[string]string, []string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	out := map[string]string{}
	var order []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		i := strings.IndexAny(line, ":=")
		if i <= 0 {
			return nil, nil, fmt.Errorf("%s:%d: want `name: value`, got %q", path, n, line)
		}
		name := strings.ToLower(strings.TrimSpace(line[:i]))
		name = strings.ReplaceAll(strings.TrimLeft(name, "-"), "_", "-")
		val := strings.TrimSpace(line[i+1:])
		val = unquoteConfig(stripComment(val))
		if _, dup := out[name]; dup {
			return nil, nil, fmt.Errorf("%s:%d: %s is set twice", path, n, name)
		}
		out[name] = val
		order = append(order, name)
	}
	return out, order, sc.Err()
}

// stripComment drops a trailing ` # comment` that is not inside quotes.
func stripComment(v string) string {
	var q byte
	for i := 0; i < len(v); i++ {
		switch c := v[i]; {
		case q != 0:
			if c == q {
				q = 0
			}
		case c == '"' || c == '\'':
			q = c
		case c == '#' && (i == 0 || v[i-1] == ' ' || v[i-1] == '\t'):
			return strings.TrimSpace(v[:i])
		}
	}
	return v
}

func unquoteConfig(v string) string {
	if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
		return v[1 : len(v)-1]
	}
	return v
}

// applyConfig sets every flag the file names that was not given on the command
// line. A name that is not a flag is an error: a typo in a security setting must
// not be silently ignored.
func applyConfig(fs *flag.FlagSet, values map[string]string, order []string) error {
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	var unknown []string
	for _, name := range order {
		f := fs.Lookup(name)
		if f == nil || name == "config" || name == "version" {
			unknown = append(unknown, name)
			continue
		}
		if given[name] {
			continue
		}
		if err := fs.Set(name, values[name]); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("unknown setting(s) in the config file: %s", strings.Join(unknown, ", "))
	}
	return nil
}

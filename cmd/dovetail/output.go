package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

// outputFormat is set by -o/--output: "plain" (default) or "json".
var outputFormat = "plain"

// outputFlag registers -o and --output on the flag set.
func outputFlag(fs *flag.FlagSet) *string {
	var f string
	fs.StringVar(&f, "output", "plain", "output format: plain or json")
	fs.StringVar(&f, "o", "plain", "shorthand for --output")
	return &f
}

func setOutput(f string) error {
	switch f {
	case "plain", "json":
		outputFormat = f
		return nil
	}
	return fmt.Errorf("unknown output format %q (plain or json)", f)
}

// emit prints v. In json mode it is encoded as JSON. In plain mode, plain
// is called when given, otherwise a generic key/value rendering is printed.
func emit(v any, plain func()) error {
	if outputFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	}
	if plain != nil {
		plain()
		return nil
	}
	// Generic rendering via the JSON shape so json tags name the fields.
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	var any_ any
	if err := json.Unmarshal(b, &any_); err != nil {
		return err
	}
	renderPlain(any_, "")
	return nil
}

func renderPlain(v any, indent string) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			switch child := x[k].(type) {
			case map[string]any:
				if len(child) == 0 {
					fmt.Printf("%s%s: {}\n", indent, k)
					continue
				}
				fmt.Printf("%s%s:\n", indent, k)
				renderPlain(child, indent+"  ")
			case []any:
				if len(child) == 0 {
					fmt.Printf("%s%s: []\n", indent, k)
					continue
				}
				fmt.Printf("%s%s:\n", indent, k)
				renderPlain(child, indent+"  ")
			default:
				fmt.Printf("%s%s: %s\n", indent, k, scalar(child))
			}
		}
	case []any:
		for _, item := range x {
			switch child := item.(type) {
			case map[string]any, []any:
				fmt.Printf("%s-\n", indent)
				renderPlain(child, indent+"  ")
			default:
				fmt.Printf("%s- %s\n", indent, scalar(child))
			}
		}
	default:
		fmt.Printf("%s%s\n", indent, scalar(v))
	}
}

func scalar(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	}
	return fmt.Sprint(v)
}

// table prints aligned columns in plain mode.
func table(header []string, rows [][]string) {
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = len(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if i < len(widths) && len(c) > widths[i] {
				widths[i] = len(c)
			}
		}
	}
	line := func(cells []string) {
		var b strings.Builder
		for i, c := range cells {
			if i == len(cells)-1 {
				b.WriteString(c)
			} else {
				b.WriteString(fmt.Sprintf("%-*s  ", widths[i], c))
			}
		}
		fmt.Println(strings.TrimRight(b.String(), " "))
	}
	line(header)
	for _, r := range rows {
		line(r)
	}
}

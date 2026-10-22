package index

import "testing"

func TestEscapeComponent(t *testing.T) {
	for in, want := range map[string]string{
		"Sent":        "Sent",
		"A/B Ltd":     "A\\2fB Ltd",
		"back\\slash": "back\\5cslash",
		"x/y\\z":      "x\\2fy\\5cz",
	} {
		if got := EscapeComponent(in); got != want {
			t.Errorf("escape %q: got %q want %q", in, got, want)
		}
		if back := UnescapeComponent(EscapeComponent(in)); back != in {
			t.Errorf("round trip %q: got %q", in, back)
		}
	}
	if got := UnescapeComponent("broken\\zz"); got != "broken\\zz" {
		t.Errorf("malformed escape changed: %q", got)
	}
	if got := UnescapeComponent("Release 1\\2e0"); got != "Release 1.0" {
		t.Errorf("hex escape of a dot: %q", got)
	}
}

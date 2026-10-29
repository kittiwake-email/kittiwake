package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolvePrecedence(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "admin.yaml")
	_ = os.WriteFile(file, []byte("url: http://file:1\ntoken: file-token\n"), 0o600)
	noEnv := func(string) string { return "" }
	env := func(k string) string {
		return map[string]string{"KITTIWAKE_ADMIN_URL": "http://env:2", "KITTIWAKE_ADMIN_TOKEN": "env-token"}[k]
	}
	s, err := resolve("", "", noEnv, file)
	if err != nil || s.URL != "http://file:1" || s.Token != "file-token" {
		t.Fatalf("file: %+v %v", s, err)
	}
	s, _ = resolve("", "", env, file)
	if s.URL != "http://env:2" || s.Token != "env-token" {
		t.Fatalf("env over file: %+v", s)
	}
	s, _ = resolve("http://flag:3", "flag-token", env, file)
	if s.URL != "http://flag:3" || s.Token != "flag-token" {
		t.Fatalf("flag over env: %+v", s)
	}
	s, _ = resolve("", "", noEnv, filepath.Join(dir, "missing.yaml"))
	if s.URL != "http://[::1]:8081" || s.Token != "" {
		t.Fatalf("defaults: %+v", s)
	}
	_ = os.WriteFile(file, []byte("url: [broken"), 0o600)
	if _, err := resolve("", "", noEnv, file); err == nil {
		t.Fatal("broken yaml accepted")
	}
}

func TestSplitArgsAndHelpers(t *testing.T) {
	pos, rest, err := splitArgs([]string{"id", "--x", "1"}, 1, "u")
	if err != nil || pos[0] != "id" || len(rest) != 2 {
		t.Fatal(pos, rest, err)
	}
	if _, _, err := splitArgs([]string{"--x"}, 1, "u"); err == nil {
		t.Fatal("flag as positional")
	}
	if _, err := enabledFrom(true, true); err == nil {
		t.Fatal("exclusive flags")
	}
	if ds := parseDestinations("a@b.c, 6b1e0e0e-0000-0000-0000-000000000000"); len(ds) != 2 || ds[0].Address != "a@b.c" || ds[1].MailboxID == "" {
		t.Fatalf("destinations: %+v", ds)
	}
	if q := quotaFrom(-1, -1, -1, -1); q != nil {
		t.Fatal("no quota flags should be nil")
	}
	if q := quotaFrom(-1, 5, -1, -1); q == nil || q.HardBytes != 5 {
		t.Fatal("quota")
	}
}

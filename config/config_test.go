package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse([]byte("modes: [lmtp, api]\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.HasMode("lmtp") || cfg.HasMode("imap") {
		t.Fatal("modes")
	}
	if cfg.Ceph.Pools.Meta != "dovetail.meta" || cfg.Mail.TrashRetention != 30*24*time.Hour {
		t.Fatal("defaults not applied")
	}
}

func TestParseOverridesAndValidation(t *testing.T) {
	y := `
modes: [worker]
ceph:
  monitors: ["[2001:db8::1]:6789"]
  user: mail
  key: ${DOVETAIL_TEST_KEY}
  pools: {messages: m, attachments: a, index: i, meta: x}
mail:
  trash_retention: 72h
  spam_header: X-Spam
  spam_value: "Yes"
listen:
  lmtp: {address: "[::1]:2424", proxy_protocol: required}
log: {level: debug, format: text}
`
	t.Setenv("DOVETAIL_TEST_KEY", "sekrit")
	cfg, err := Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	k, _ := cfg.Ceph.Key.Resolve()
	if k != "sekrit" || cfg.Mail.TrashRetention != 72*time.Hour || cfg.Listen.LMTP.ProxyProtocol != "required" || cfg.Ceph.Pools.Meta != "x" {
		t.Fatalf("overrides: %+v", cfg)
	}
	if _, err := Parse([]byte("modes: [bogus]\n")); err == nil || !strings.Contains(err.Error(), "unknown mode") {
		t.Fatalf("expected mode error, got %v", err)
	}
	if _, err := Parse([]byte("unknown_key: 1\n")); err == nil {
		t.Fatal("expected unknown field error")
	}
	if _, err := Parse([]byte("log: {level: loud}\n")); err == nil {
		t.Fatal("expected level error")
	}
}

func TestSecretFile(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "k")
	_, _ = f.WriteString("abc\n")
	_ = f.Close()
	v, err := Secret("file:" + f.Name()).Resolve()
	if err != nil || v != "abc" {
		t.Fatalf("%q %v", v, err)
	}
}

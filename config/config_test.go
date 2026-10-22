package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse([]byte("modes: [lmtp, jmap]\ndatabase: {dsn: \"dovetail:x@tcp(db:3306)/dovetail\"}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.HasMode("lmtp") || cfg.HasMode("imap") {
		t.Fatal("modes")
	}
	if cfg.Ceph.Pools.Meta != "dovetail.meta" || cfg.Mail.TrashRetention != 30*24*time.Hour || cfg.Cache.Credentials != 5*time.Second || cfg.Cache.Identity != 5*time.Second || cfg.Cache.Root != 5*time.Second {
		t.Fatal("defaults not applied")
	}
}

func TestParseOverridesAndValidation(t *testing.T) {
	y := `
modes: [worker]
ceph:
  backend: memory
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
	cfg, err = Parse([]byte("ceph: {backend: memory}\ncache: {credentials: 0s, identity: 1s}\n"))
	if err != nil || cfg.Cache.Credentials != 0 || cfg.Cache.Identity != time.Second || cfg.Cache.Root != 5*time.Second {
		t.Fatalf("cache overrides: %+v %v", cfg.Cache, err)
	}
	if _, err := Parse([]byte("cache: {root: -1s}\n")); err == nil {
		t.Fatal("expected negative cache TTL error")
	}
	// librados needs a database; memory does not.
	if _, err := Parse([]byte("modes: [imap]\n")); err == nil || !strings.Contains(err.Error(), "database.dsn") {
		t.Fatalf("expected dsn error, got %v", err)
	}
	// With librados the clusters come from the registry, not the config.
	if _, err := Parse([]byte("modes: [imap]\ndatabase: {dsn: x}\nceph: {monitors: [\"[::1]:6789\"]}\n")); err == nil || !strings.Contains(err.Error(), "registered in the database") {
		t.Fatalf("expected cluster-in-config error, got %v", err)
	}
	if _, err := Parse([]byte("modes: [imap]\ndatabase: {dsn: x}\nceph: {config_file: /etc/ceph/ceph.conf, options: {ms_bind_ipv6: \"true\"}}\n")); err != nil {
		t.Fatalf("librados with registry: %v", err)
	}
	if _, err := Parse([]byte("modes: [imap]\nceph: {backend: memory}\n")); err != nil {
		t.Fatalf("memory backend without dsn: %v", err)
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

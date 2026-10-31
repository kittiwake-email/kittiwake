package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse([]byte("modes: [lmtp, jmap]\ndatabase: {dsn: \"kittiwake:x@tcp(db:3306)/kittiwake\"}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.HasMode("lmtp") || cfg.HasMode("imap") {
		t.Fatal("modes")
	}
	if cfg.Ceph.Pools.Meta != "kittiwake.meta" || cfg.Mail.TrashRetention != 30*24*time.Hour || cfg.Cache.Credentials != 5*time.Second || cfg.Cache.Identity != 5*time.Second || cfg.Cache.Root != 5*time.Second {
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
  key: ${KITTIWAKE_TEST_KEY}
  pools: {messages: m, attachments: a, index: i, meta: x}
mail:
  trash_retention: 72h
  spam_header: X-Spam
  spam_value: "Yes"
listen:
  lmtp: {address: "[::1]:2424", proxy_protocol: required}
log: {level: debug, format: text}
`
	t.Setenv("KITTIWAKE_TEST_KEY", "sekrit")
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
	if _, err := Parse([]byte("modes: [proxy, imap]\ndatabase: {dsn: x}\n")); err == nil || !strings.Contains(err.Error(), "proxy mode runs in a process of its own") {
		t.Fatalf("proxy with another mode: %v", err)
	}
	if _, err := Parse([]byte("modes: [proxy]\nceph: {backend: memory}\n")); err == nil || !strings.Contains(err.Error(), "database.dsn") {
		t.Fatalf("proxy without a database: %v", err)
	}
	if _, err := Parse([]byte("modes: [proxy]\ndatabase: {dsn: x}\n")); err == nil || !strings.Contains(err.Error(), "handoff_key") {
		t.Fatalf("local auth without a key: %v", err)
	}
	if _, err := Parse([]byte("modes: [proxy]\ndatabase: {dsn: x}\nproxy: {handoff_key: bogus}\n")); err == nil || !strings.Contains(err.Error(), "handoff_key") {
		t.Fatalf("bad key: %v", err)
	}
	if _, err := Parse([]byte("proxy: {auth: maybe}\n")); err == nil || !strings.Contains(err.Error(), "proxy.auth") {
		t.Fatalf("bad auth: %v", err)
	}
	if cfg, err := Parse([]byte("modes: [proxy]\ndatabase: {dsn: x}\nproxy: {auth: passthrough}\n")); err != nil || cfg.Proxy.Auth != "passthrough" {
		t.Fatalf("passthrough: %v", err)
	}
	if _, err := Parse([]byte("proxy: {ingress_tls: maybe}\n")); err == nil || !strings.Contains(err.Error(), "ingress_tls") {
		t.Fatalf("ingress_tls: %v", err)
	}
	if _, err := Parse([]byte("proxy: {ingress_ports: {smtp: 25}}\n")); err == nil || !strings.Contains(err.Error(), "ingress_ports") {
		t.Fatalf("ingress_ports: %v", err)
	}
	if cfg, err := Parse([]byte("modes: [proxy]\ndatabase: {proxy_dsn: ro}\nproxy: {auth: passthrough}\n")); err != nil || cfg.Database.ProxyDSN != "ro" {
		t.Fatalf("proxy_dsn alone: %v", err)
	}
	if cfg, err := Parse([]byte("modes: [proxy]\ndatabase: {dsn: x}\nproxy: {ingress_ports: {imap: 1993}, handoff_key: " + testKey + "}\n")); err != nil || cfg.Proxy.IngressPorts["imap"] != 1993 || cfg.Proxy.IngressPorts["pop3"] != 995 || cfg.Proxy.IngressTLS != "strict" || cfg.Listen.ByMode("proxy").Management != "[::1]:9100" {
		t.Fatalf("proxy defaults: %+v %v", cfg.Proxy, err)
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
	// Management address: a lone mode uses its own, several modes the shared one.
	if cfg, _ := Parse([]byte("ceph: {backend: memory}\n")); cfg.Listen.ManagementAddress([]string{"imap"}) != "[::1]:9092" || cfg.Listen.ManagementAddress([]string{"worker"}) != "[::1]:9097" || cfg.Listen.ManagementAddress([]string{"imap", "pop3"}) != "[::1]:9090" {
		t.Fatalf("management addresses: %+v", cfg.Listen)
	}
	if _, err := Parse([]byte("modes: [postfix]\nceph: {backend: memory}\n")); err == nil {
		t.Fatal("old mode name postfix accepted")
	}
	// modes is optional (systemd instances pass --modes).
	if cfg, err := Parse([]byte("database: {dsn: x}\n")); err != nil || len(cfg.Modes) != 0 {
		t.Fatalf("config without modes: %+v %v", cfg.Modes, err)
	}
	// librados needs a database; memory does not.
	if _, err := Parse([]byte("modes: [imap]\n")); err == nil || !strings.Contains(err.Error(), "database.dsn") {
		t.Fatalf("expected dsn error, got %v", err)
	}
	// With librados the storage entries come from the registry, not the config.
	if _, err := Parse([]byte("modes: [imap]\ndatabase: {dsn: x}\nceph: {monitors: [\"[::1]:6789\"]}\n")); err == nil || !strings.Contains(err.Error(), "registered in the database") {
		t.Fatalf("expected storage-in-config error, got %v", err)
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

func TestSubmissionConfig(t *testing.T) {
	cfg, err := Parse([]byte("modes: [submission]\nceph: {backend: memory}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Submission.RequireTLSProxy || cfg.Submission.SaveToSent || cfg.Listen.ManagementAddress([]string{"submission"}) != "[::1]:9098" {
		t.Fatalf("submission defaults: %+v %+v", cfg.Submission, cfg.Listen.Submission)
	}
	if _, err := Parse([]byte("ceph: {backend: memory}\nsmtp: {xclient: true, username: u}\n")); err == nil {
		t.Fatal("xclient with SASL credentials accepted")
	}
	if _, err := Parse([]byte("ceph: {backend: memory}\nlisten: {submission: {proxy_protocol: bogus}}\n")); err == nil {
		t.Fatal("bad submission proxy_protocol accepted")
	}
}

// testKey is a base64 Ed25519 seed (not used anywhere real).
const testKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="

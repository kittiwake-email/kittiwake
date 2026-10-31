package config

import (
	"strings"
	"testing"
)

func TestEnvOverrides(t *testing.T) {
	env := map[string]string{
		"KITTIWAKE_MODES":                        "imap, pop3",
		"KITTIWAKE_NAMESPACES":                   "default,acme",
		"KITTIWAKE_MANAGEMENT":                   "[::]:9090",
		"KITTIWAKE_DB_DSN":                       "u:p@tcp(db:3306)/k",
		"KITTIWAKE_CEPH_BACKEND":                 "memory",
		"KITTIWAKE_NODE_ADVERTISE":               "2001:db8::10",
		"KITTIWAKE_PROXY_PROTOCOL":               "required",
		"KITTIWAKE_PROXY_TRUSTED_ADDRESSES":      "2001:db8::1, 2001:db8::/64",
		"KITTIWAKE_SMTP_RELAY_HOST":              "[2001:db8::25]:25",
		"KITTIWAKE_SUBMISSION_REQUIRE_TLS_PROXY": "false",
		"KITTIWAKE_SESSION_ISSUER":               "https://mail.example.org",
		"KITTIWAKE_BACKUP_TARGET":                "filesystem",
		"KITTIWAKE_BACKUP_DIRECTORY":             "/var/lib/kittiwake/backups",
		"KITTIWAKE_BACKUP_KEY":                   strings.Repeat("ab", 32),
		"KITTIWAKE_LOG_FORMAT":                   "text",
		"KITTIWAKE_LOG_LEVEL":                    "",    // empty: ignored
		"KITTIWAKE_PROXY_HANDOFF_KEY":            "   ", // blank: ignored
	}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	cfg, err := ParseEnv([]byte("listen: {imap: {address: \"[::]:1143\", proxy_protocol: off, management: \"[::1]:9092\"}}\nlog: {level: warn}\n"), lookup)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(cfg.Modes, ",") != "imap,pop3" || strings.Join(cfg.Namespaces, ",") != "default,acme" {
		t.Fatalf("modes/namespaces: %v %v", cfg.Modes, cfg.Namespaces)
	}
	if cfg.Listen.ManagementAddress(cfg.Modes) != "[::]:9090" || cfg.Listen.ManagementAddress([]string{"imap"}) != "[::]:9090" {
		t.Fatalf("management: %+v", cfg.Listen)
	}
	if cfg.Database.DSN != "u:p@tcp(db:3306)/k" || cfg.Ceph.Backend != "memory" || cfg.Node.Advertise != "2001:db8::10" {
		t.Fatalf("db/ceph/node: %+v", cfg)
	}
	if cfg.Listen.IMAP.Address != "[::]:1143" || cfg.Listen.IMAP.ProxyProtocol != "required" || cfg.Listen.Proxy.JMAP.ProxyProtocol != "required" || cfg.Listen.Admin.ProxyProtocol != "off" {
		t.Fatalf("proxy protocol: %+v", cfg.Listen)
	}
	if len(cfg.Proxy.TrustedAddresses) != 2 || cfg.Proxy.TrustedAddresses[1] != "2001:db8::/64" {
		t.Fatalf("trusted: %v", cfg.Proxy.TrustedAddresses)
	}
	if cfg.SMTP.RelayHost != "[2001:db8::25]:25" || cfg.Submission.RequireTLSProxy || cfg.Session.Issuer != "https://mail.example.org" {
		t.Fatalf("smtp/submission/session: %+v %+v %+v", cfg.SMTP, cfg.Submission, cfg.Session)
	}
	if cfg.Backup.Target != "filesystem" || cfg.Backup.Directory == "" || len(cfg.Backup.Keys) != 1 || cfg.Backup.Keys[0].ID != 1 || cfg.Backup.CurrentKey != 1 {
		t.Fatalf("backup: %+v", cfg.Backup.BackupTarget)
	}
	if cfg.Log.Format != "text" || cfg.Log.Level != "warn" || cfg.Proxy.HandoffKey != "" {
		t.Fatalf("log/handoff: %+v %q", cfg.Log, cfg.Proxy.HandoffKey)
	}
	// Overrides are validated like the file.
	if _, err := ParseEnv(nil, func(k string) (string, bool) { return "maybe", k == "KITTIWAKE_SMTP_XCLIENT" }); err == nil || !strings.Contains(err.Error(), "KITTIWAKE_SMTP_XCLIENT") {
		t.Fatalf("bad bool: %v", err)
	}
	if _, err := ParseEnv(nil, func(k string) (string, bool) { return "bogus", k == "KITTIWAKE_MODES" }); err == nil || !strings.Contains(err.Error(), "unknown mode") {
		t.Fatalf("bad mode: %v", err)
	}
	// Parse ignores the process environment.
	t.Setenv("KITTIWAKE_LOG_FORMAT", "text")
	if cfg, err := Parse([]byte("ceph: {backend: memory}\n")); err != nil || cfg.Log.Format != "json" {
		t.Fatalf("Parse reads the environment: %+v %v", cfg.Log, err)
	}
	if o := EnvOverrides(); len(o) != 1 || o[0] != "KITTIWAKE_LOG_FORMAT" {
		t.Fatalf("overrides: %v", o)
	}
}

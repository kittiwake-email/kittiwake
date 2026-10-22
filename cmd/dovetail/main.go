// Command dovetail is the Dovetail mail platform daemon and admin CLI.
//
//	dovetail serve --config /etc/dovetail/dovetail.yaml
//	dovetail check-config --config ...
//	dovetail admin --config ... <subcommand>
//	dovetail version
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dovetail/app"
	"dovetail/config"
	"dovetail/service/actor"
	"dovetail/service/admin"
	"dovetail/store/identity"
	"dovetail/store/mariadb"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "check-config":
		err = runCheckConfig(os.Args[2:])
	case "migrate":
		err = runMigrate(os.Args[2:])
	case "admin":
		err = runAdmin(os.Args[2:])
	case "version", "--version", "-v":
		err = runVersion(os.Args[2:])
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: dovetail <command> [flags]

commands:
  serve         run the daemon in the modes listed in the config file
  check-config  parse and validate the config file
  migrate       create or upgrade the metadata database schema (database.dsn)
  admin         administrative operations against RADOS (see: dovetail admin help)
  version       print the version (also: -v, --version)

flags:
  -c, --config <file>   config file (serve, check-config, admin; default $DOVETAIL_CONFIG
                        or /etc/dovetail/dovetail.yaml)
  -o, --output <fmt>    plain (default) or json (check-config, admin, version)`)
}

func loadConfig(path string) (config.Config, error) {
	if path == "" {
		path = os.Getenv("DOVETAIL_CONFIG")
	}
	if path == "" {
		path = "/etc/dovetail/dovetail.yaml"
	}
	return config.Load(path)
}

// seedAccount creates domain, mailbox and a full-access account for
// development runs on the volatile memory backend.
func seedAccount(a *app.App, spec string) error {
	user, pw, ok := strings.Cut(spec, ":")
	if !ok || len(pw) < 8 {
		return fmt.Errorf("--seed-account must be user@domain:password (password >= 8 chars)")
	}
	ctx := actor.With(context.Background(), actor.System("cli"))
	adm := a.Services.Admin
	_, domain, err := identity.NormalizeAddress(user)
	if err != nil {
		return err
	}
	if _, err := adm.CreateDomain(ctx, identity.Domain{Name: domain, Enabled: true}); err != nil {
		return err
	}
	mb, err := adm.CreateMailbox(ctx, identity.Mailbox{})
	if err != nil {
		return err
	}
	acct, err := adm.CreateAccount(ctx, admin.CreateAccountRequest{Username: user, MailboxID: mb.ID, Roles: []string{"read", "write", "send"}, Password: pw})
	if err != nil {
		return err
	}
	tok, secret, err := adm.Bootstrap(ctx, "dev", 24*time.Hour)
	if err != nil {
		return err
	}
	a.Log.Warn("development seed created", "account", acct.Username, "mailbox", mb.ID.String(), "admin_token_id", tok.ID, "admin_token", secret)
	return nil
}

func runVersion(args []string) error {
	fs := newFlagSet("version")
	of := outputFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := setOutput(*of); err != nil {
		return err
	}
	return emit(map[string]string{"version": app.Version}, func() { fmt.Println("dovetail", app.Version) })
}

func runCheckConfig(args []string) error {
	fs := newFlagSet("check-config")
	cfgPath := configFlag(fs)
	of := outputFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := setOutput(*of); err != nil {
		return err
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	return emit(map[string]any{"ok": true, "modes": cfg.Modes, "backend": cfg.Ceph.Backend, "pools": cfg.Ceph.Pools},
		func() {
			fmt.Printf("config ok: modes=%v backend=%s pools=%+v\n", cfg.Modes, cfg.Ceph.Backend, cfg.Ceph.Pools)
		})
}

func runServe(args []string) error {
	fs := newFlagSet("serve")
	cfgPath := configFlag(fs)
	seed := fs.String("seed-account", "", "development only (memory backend): create user@domain:password with a mailbox at startup")
	modes := fs.String("modes", "", "comma-separated modes to run, overriding the config (e.g. imap or lmtp,postfix)")
	mgmt := fs.String("management", "", "management listener address, overriding the config (metrics/health)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	if *modes != "" {
		cfg.Modes = nil
		for _, m := range strings.Split(*modes, ",") {
			if m = strings.TrimSpace(m); m != "" {
				cfg.Modes = append(cfg.Modes, m)
			}
		}
		if err := cfg.Validate(); err != nil {
			return err
		}
	}
	if *mgmt != "" {
		cfg.Listen.Management.Address = *mgmt
	}
	if len(cfg.Modes) == 0 {
		return fmt.Errorf("no modes configured")
	}
	log := app.NewLogger(cfg.Log)
	client, err := app.Connect(cfg.Ceph)
	if err != nil {
		return err
	}
	db, err := app.OpenDatabase(context.Background(), cfg.Database)
	if err != nil {
		return err
	}
	a, err := app.New(cfg, client, db, log)
	if err != nil {
		return err
	}
	defer a.Close()
	if *seed != "" {
		if cfg.Ceph.Backend != "memory" {
			return fmt.Errorf("--seed-account is only allowed with ceph.backend: memory")
		}
		if err := seedAccount(a, *seed); err != nil {
			return err
		}
	}
	if cfg.HasMode("jmap") {
		a.JMAPHandler = buildJMAP(a)
	}
	if cfg.HasMode("admin") {
		a.AdminHandler = buildAdmin(a)
	}
	a.ExtraModes = protocolModes()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("dovetail starting", "version", app.Version, "modes", cfg.Modes, "backend", cfg.Ceph.Backend)
	return a.Run(ctx)
}

// runMigrate creates or upgrades the metadata schema.
func runMigrate(args []string) error {
	fs := newFlagSet("migrate")
	cfgPath := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	dsn, err := cfg.Database.DSN.Resolve()
	if err != nil {
		return err
	}
	if dsn == "" {
		return fmt.Errorf("database.dsn is not configured")
	}
	db, err := mariadb.Open(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx := context.Background()
	before, err := db.SchemaVersion(ctx)
	if err != nil {
		return err
	}
	after, err := db.Migrate(ctx)
	if err != nil {
		return err
	}
	if after == before {
		fmt.Printf("schema version %d, up to date\n", after)
	} else {
		fmt.Printf("schema version %d -> %d\n", before, after)
	}
	return nil
}

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
	case "admin":
		err = runAdmin(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Println("dovetail", app.Version)
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
  admin         administrative operations against RADOS (see: dovetail admin help)
  version       print the version`)
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
	mb, err := adm.CreateMailbox(ctx, identity.Mailbox{Enabled: true})
	if err != nil {
		return err
	}
	acct, err := adm.CreateAccount(ctx, admin.CreateAccountRequest{Username: user, MailboxID: mb.ID, Roles: []string{"read", "write", "send"}, Password: pw})
	if err != nil {
		return err
	}
	res, err := adm.Bootstrap(ctx, "admin@"+domain, pw, "dev", 24*time.Hour)
	if err != nil {
		return err
	}
	a.Log.Warn("development seed created", "account", acct.Username, "mailbox", mb.ID.String(), "admin", res.Account.Username, "admin_token", res.Secret)
	return nil
}

func runCheckConfig(args []string) error {
	fs := newFlagSet("check-config")
	cfgPath := fs.String("config", "", "path to the YAML config (default $DOVETAIL_CONFIG or /etc/dovetail/dovetail.yaml)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	fmt.Printf("config ok: modes=%v backend=%s pools=%+v\n", cfg.Modes, cfg.Ceph.Backend, cfg.Ceph.Pools)
	return nil
}

func runServe(args []string) error {
	fs := newFlagSet("serve")
	cfgPath := fs.String("config", "", "path to the YAML config")
	seed := fs.String("seed-account", "", "development only (memory backend): create user@domain:password with a mailbox at startup")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	if len(cfg.Modes) == 0 {
		return fmt.Errorf("no modes configured")
	}
	log := app.NewLogger(cfg.Log)
	client, err := app.Connect(cfg.Ceph)
	if err != nil {
		return err
	}
	a, err := app.New(cfg, client, log)
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
	if cfg.HasMode("api") {
		a.APIHandler = buildAPI(a)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("dovetail starting", "version", app.Version, "modes", cfg.Modes, "backend", cfg.Ceph.Backend)
	return a.Run(ctx)
}

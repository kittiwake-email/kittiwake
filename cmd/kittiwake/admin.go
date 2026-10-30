package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"kittiwake/app"
	"kittiwake/service/actor"
	"kittiwake/service/admin"
	"kittiwake/store/identity"
)

// The local admin subcommand is deliberately minimal: it only exists to get
// an admin API token when there is none (first install, lost tokens).
// Everything else is done through the admin API with kittiwake-admin.

func adminUsage() {
	fmt.Fprintln(os.Stderr, `usage: kittiwake admin -c <file> [-o plain|json] <subcommand> [flags]

subcommands:
  bootstrap  [--comment text] [--token-lifetime 24h]
             mint the first admin token; refuses when admin tokens already exist
  token      [--comment text] [--expires YYYY-MM-DD|RFC3339] [--read-only]
             mint an admin token (recovery when all tokens are lost, or a read-only token for jobs)

Admins are not accounts: an admin is an API token with a comment, an expiry
and a role (admin, or admin-ro for read-only use). The CLI talks to RADOS
directly with the daemon's config file and acts as the system actor; both
actions are audited in the admin log. All other administration is done over
the admin API: see kittiwake-admin.`)
}

func runAdmin(args []string) error {
	fs := newFlagSet("admin")
	cfgPath := configFlag(fs)
	of := outputFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := setOutput(*of); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 || rest[0] == "help" {
		adminUsage()
		return nil
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		return err
	}
	// The CLI is interactive: only warnings and errors on stderr (audit
	// records still land in RADOS; the structured log stream is for daemons).
	if cfg.Log.Level == "info" || cfg.Log.Level == "debug" {
		cfg.Log.Level = "warn"
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
	ctx := actor.With(context.Background(), actor.System("cli"))
	switch rest[0] {
	case "bootstrap":
		return cmdBootstrap(ctx, a.Services.Admin, rest[1:])
	case "token":
		return cmdToken(ctx, a.Services.Admin, rest[1:])
	}
	adminUsage()
	return fmt.Errorf("unknown admin subcommand %q", rest[0])
}

func cmdBootstrap(ctx context.Context, adm *admin.Service, args []string) error {
	fs := newFlagSet("bootstrap")
	lifetime := fs.Duration("token-lifetime", 24*time.Hour, "lifetime of the bootstrap admin token")
	comment := fs.String("comment", "bootstrap", "token comment")
	if err := fs.Parse(args); err != nil {
		return err
	}
	t, secret, err := adm.Bootstrap(ctx, *comment, *lifetime)
	if err != nil {
		return err
	}
	return printToken(t, secret)
}

func cmdToken(ctx context.Context, adm *admin.Service, args []string) error {
	fs := newFlagSet("token")
	expires := fs.String("expires", "", "expiry date (YYYY-MM-DD) or RFC3339; default 24h from now")
	comment := fs.String("comment", "", "purpose of the token (required)")
	readOnly := fs.Bool("read-only", false, "mint a read-only (admin-ro) token")
	namespace := fs.String("namespace", "", "bind the token to one namespace (a tenant admin); default: the whole platform")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *comment == "" {
		return fmt.Errorf("--comment is required")
	}
	var exp time.Time
	if *expires != "" {
		var err error
		if exp, err = time.Parse("2006-01-02", *expires); err != nil {
			if exp, err = time.Parse(time.RFC3339, *expires); err != nil {
				return fmt.Errorf("--expires: %w", err)
			}
		}
	}
	t, secret, err := adm.CreateAdminToken(ctx, *readOnly, *namespace, exp, *comment)
	if err != nil {
		return err
	}
	return printToken(t, secret)
}

func printToken(t identity.Token, secret string) error {
	return emit(map[string]any{"token_id": t.ID, "roles": t.Roles, "namespace": t.Namespace, "expires_at": t.ExpiresAt, "comment": t.Comment, "token_secret": secret}, func() {
		scope := "platform"
		if t.Namespace != "" {
			scope = "namespace " + t.Namespace
		}
		fmt.Printf("token id:     %s (%s, %s, expires %s)\n", t.ID, strings.Join(t.Roles, ","), scope, t.ExpiresAt.Format(time.RFC3339))
		fmt.Printf("token secret: %s\n", secret)
		fmt.Println("The secret is shown once. Use it as: kittiwake-admin --token <secret> ...")
	})
}

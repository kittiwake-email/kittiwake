package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"dovetail/app"
	"dovetail/service/actor"
	"dovetail/service/admin"
)

// The local admin subcommand is deliberately minimal: it only exists to get
// an admin API token when there is none (first install, lost tokens).
// Everything else is done through the admin API with dovetail-admin.

func adminUsage() {
	fmt.Fprintln(os.Stderr, `usage: dovetail admin -c <file> [-o plain|json] <subcommand> [flags]

subcommands:
  bootstrap  --username root@example.org [--password-stdin] [--token-lifetime 24h] [--comment text]
             create the first admin account (and its domain) and print an API token once
  token      --account <uuid|username> [--expires YYYY-MM-DD|RFC3339] [--comment text] [--roles r,...]
             mint an API token for an existing account (recovery when all tokens are lost)

The CLI talks to RADOS directly with the daemon's config file and acts as the
system actor; both actions are audited in the admin log. All other
administration is done over the admin API: see dovetail-admin.`)
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
	a, err := app.New(cfg, client, log)
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

func readPasswordStdin() (string, error) {
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func cmdBootstrap(ctx context.Context, adm *admin.Service, args []string) error {
	fs := newFlagSet("bootstrap")
	username := fs.String("username", "", "admin username (an e-mail address)")
	pwStdin := fs.Bool("password-stdin", false, "read the admin password from stdin")
	lifetime := fs.Duration("token-lifetime", 24*time.Hour, "lifetime of the bootstrap API token")
	comment := fs.String("comment", "bootstrap", "token comment")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *username == "" {
		return fmt.Errorf("--username is required")
	}
	pw := ""
	if *pwStdin {
		var err error
		if pw, err = readPasswordStdin(); err != nil {
			return err
		}
	}
	res, err := adm.Bootstrap(ctx, *username, pw, *comment, *lifetime)
	if err != nil {
		return err
	}
	return emit(map[string]any{
		"account": res.Account.Username, "account_id": res.Account.ID, "token_id": res.Token.ID,
		"expires_at": res.Token.ExpiresAt, "token_secret": res.Secret,
	}, func() {
		fmt.Printf("admin account: %s (%s)\n", res.Account.Username, res.Account.ID)
		fmt.Printf("token id:      %s (expires %s)\n", res.Token.ID, res.Token.ExpiresAt.Format(time.RFC3339))
		fmt.Printf("token secret:  %s\n", res.Secret)
		fmt.Println("The secret is shown once. Use it as: dovetail-admin --token <secret> ...")
	})
}

func cmdToken(ctx context.Context, adm *admin.Service, args []string) error {
	fs := newFlagSet("token")
	acct := fs.String("account", "", "account uuid or username")
	expires := fs.String("expires", "", "expiry date (YYYY-MM-DD) or RFC3339; default 24h from now")
	comment := fs.String("comment", "cli-token", "purpose of the token")
	roles := fs.String("roles", "", "comma-separated roles (default: the account's)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *acct == "" {
		return fmt.Errorf("--account is required")
	}
	id, err := uuid.Parse(*acct)
	if err != nil {
		a, err := adm.GetAccountByUsername(ctx, *acct)
		if err != nil {
			return err
		}
		id = a.ID
	}
	exp := time.Now().Add(24 * time.Hour)
	if *expires != "" {
		if exp, err = time.Parse("2006-01-02", *expires); err != nil {
			if exp, err = time.Parse(time.RFC3339, *expires); err != nil {
				return fmt.Errorf("--expires: %w", err)
			}
		}
	}
	var rs []string
	if *roles != "" {
		rs = strings.Split(*roles, ",")
	}
	t, secret, err := adm.CreateToken(ctx, id, rs, exp, *comment)
	if err != nil {
		return err
	}
	return emit(map[string]any{"token_id": t.ID, "expires_at": t.ExpiresAt, "roles": t.Roles, "token_secret": secret}, func() {
		fmt.Printf("token id:     %s (expires %s)\n", t.ID, t.ExpiresAt.Format(time.RFC3339))
		fmt.Printf("token secret: %s\n", secret)
	})
}

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"dovetail/app"
	"dovetail/service/actor"
	"dovetail/service/admin"
	"dovetail/service/export"
	"dovetail/store/identity"
)

func adminUsage() {
	fmt.Fprintln(os.Stderr, `usage: dovetail admin -c <file> <subcommand> [flags]

subcommands:
  bootstrap   --username root@example.org [--password-stdin] [--token-lifetime 24h] [--comment text]
              create the first admin account and an API token (prints the token once)
  export      dump all identity data (domains, mailboxes, accounts, aliases, tokens) as JSON to stdout
  import      <file.json>  load an identity dump (existing objects are skipped)
  domain      create <name> | list | enable <name> | disable <name> | delete <name>
  mailbox     create | list | inspect <uuid> | recalc <uuid> | delete <uuid>
  account     create --username u@d --mailbox <uuid> [--roles read,write,send] [--password-stdin]
              | list [--domain example.org] [--prefix x] | get <username|uuid> | delete <uuid>
  alias       create <address> --to <uuid|address>[,...] | delete <address>
  token       create --account <uuid> --expires 2027-01-01 --comment text [--roles read]
  hold        <mailbox-uuid> --reason text [--case ref] [--no-delivery] [--allow-send] [--sealed]
  release     <mailbox-uuid> [--note text]
  export-mailbox <mailbox-uuid> --out file.tar[.gz]   Maildir++ archive with manifest and audit log
  import-mailbox <mailbox-uuid> --dir /path/to/Maildir | --archive file.tar[.gz]

The CLI talks to RADOS directly with the same config file and acts as the
system actor; every action is audited in the admin log.`)
}

func runAdmin(args []string) error {
	fs := newFlagSet("admin")
	cfgPath := configFlag(fs)
	if err := fs.Parse(args); err != nil {
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
	adm := a.Services.Admin
	sub, subargs := rest[0], rest[1:]
	switch sub {
	case "bootstrap":
		return cmdBootstrap(ctx, adm, subargs)
	case "export":
		exp, err := adm.ExportIdentity(ctx)
		if err != nil {
			return err
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(exp)
	case "import":
		if len(subargs) != 1 {
			return fmt.Errorf("import needs a file")
		}
		data, err := os.ReadFile(subargs[0])
		if err != nil {
			return err
		}
		summary, err := adm.ImportIdentity(ctx, data)
		if err != nil {
			return err
		}
		fmt.Println(summary)
		return nil
	case "domain":
		return cmdDomain(ctx, adm, subargs)
	case "mailbox":
		return cmdMailbox(ctx, a, subargs)
	case "account":
		return cmdAccount(ctx, adm, subargs)
	case "alias":
		return cmdAlias(ctx, adm, subargs)
	case "token":
		return cmdToken(ctx, adm, subargs)
	case "hold":
		return cmdHold(ctx, adm, subargs)
	case "release":
		return cmdRelease(ctx, adm, subargs)
	case "export-mailbox":
		return cmdExportMailbox(ctx, a, subargs)
	case "import-mailbox":
		return cmdImportMailbox(ctx, a, subargs)
	}
	adminUsage()
	return fmt.Errorf("unknown admin subcommand %q", sub)
}

func readPasswordStdin() (string, error) {
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
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
	fmt.Printf("admin account: %s (%s)\n", res.Account.Username, res.Account.ID)
	fmt.Printf("token id:      %s (expires %s)\n", res.Token.ID, res.Token.ExpiresAt.Format(time.RFC3339))
	fmt.Printf("token secret:  %s\n", res.Secret)
	fmt.Println("The secret is shown once. Use it as: Authorization: Bearer <secret>")
	return nil
}

func cmdDomain(ctx context.Context, adm *admin.Service, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("domain: create|list|enable|disable|delete")
	}
	switch args[0] {
	case "create":
		if len(args) != 2 {
			return fmt.Errorf("domain create <name>")
		}
		d, err := adm.CreateDomain(ctx, identity.Domain{Name: args[1], Enabled: true})
		if err != nil {
			return err
		}
		return printJSON(d)
	case "list":
		names, _, err := adm.ListDomains(ctx, "", 0)
		if err != nil {
			return err
		}
		for _, n := range names {
			fmt.Println(n)
		}
		return nil
	case "enable", "disable":
		if len(args) != 2 {
			return fmt.Errorf("domain %s <name>", args[0])
		}
		en := args[0] == "enable"
		d, err := adm.UpdateDomain(ctx, args[1], func(d *identity.Domain) error { d.Enabled = en; return nil })
		if err != nil {
			return err
		}
		return printJSON(d)
	case "delete":
		if len(args) != 2 {
			return fmt.Errorf("domain delete <name>")
		}
		return adm.DeleteDomain(ctx, args[1])
	}
	return fmt.Errorf("unknown domain subcommand %q", args[0])
}

func cmdMailbox(ctx context.Context, a *app.App, args []string) error {
	adm := a.Services.Admin
	if len(args) == 0 {
		return fmt.Errorf("mailbox: create|inspect|recalc|delete")
	}
	parse := func() (uuid.UUID, error) {
		if len(args) != 2 {
			return uuid.Nil, fmt.Errorf("mailbox %s <uuid>", args[0])
		}
		return uuid.Parse(args[1])
	}
	switch args[0] {
	case "create":
		m, err := adm.CreateMailbox(ctx, identity.Mailbox{Enabled: true})
		if err != nil {
			return err
		}
		return printJSON(m)
	case "list":
		fmt.Printf("%-36s  %-8s  %-5s  %s\n", "MAILBOX", "ENABLED", "HOLD", "ACCOUNTS")
		return adm.ListMailboxes(ctx, func(id uuid.UUID) error {
			m, err := adm.GetMailbox(ctx, id)
			if err != nil {
				return nil
			}
			accts, _, _ := adm.ListMailboxAccounts(ctx, id)
			fmt.Printf("%-36s  %-8v  %-5v  %s\n", id, m.Enabled, m.Hold != nil, strings.Join(accts, ","))
			return nil
		})
	case "inspect":
		id, err := parse()
		if err != nil {
			return err
		}
		info, err := adm.InspectMailbox(ctx, id, a.DefaultQuota())
		if err != nil {
			return err
		}
		return printJSON(info)
	case "recalc":
		id, err := parse()
		if err != nil {
			return err
		}
		u, err := adm.RecalculateQuota(ctx, id)
		if err != nil {
			return err
		}
		return printJSON(u)
	case "delete":
		id, err := parse()
		if err != nil {
			return err
		}
		return adm.DeleteMailbox(ctx, id)
	}
	return fmt.Errorf("unknown mailbox subcommand %q", args[0])
}

func cmdAccount(ctx context.Context, adm *admin.Service, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("account: create|list|get|delete")
	}
	switch args[0] {
	case "list":
		fs := newFlagSet("account list")
		domain := fs.String("domain", "", "only this domain (default: all domains)")
		prefix := fs.String("prefix", "", "username prefix")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		domains := []string{*domain}
		if *domain == "" {
			var err error
			if domains, _, err = adm.ListDomains(ctx, "", 0); err != nil {
				return err
			}
		}
		fmt.Printf("%-40s  %-36s  %-8s  %s\n", "USERNAME", "MAILBOX", "ENABLED", "ROLES")
		for _, d := range domains {
			after := ""
			for {
				names, more, err := adm.ListAccounts(ctx, d, *prefix, after, 500)
				if err != nil {
					return err
				}
				for _, n := range names {
					after = n
					a, err := adm.GetAccountByUsername(ctx, n)
					if err != nil {
						continue
					}
					mb := "-"
					if a.MailboxID != uuid.Nil {
						mb = a.MailboxID.String()
					}
					fmt.Printf("%-40s  %-36s  %-8v  %s\n", a.Username, mb, a.Enabled, strings.Join(a.Roles, ","))
				}
				if !more {
					break
				}
			}
		}
		return nil
	case "create":
		fs := newFlagSet("account create")
		username := fs.String("username", "", "username (e-mail address)")
		mbs := fs.String("mailbox", "", "mailbox uuid")
		roles := fs.String("roles", "read,write,send", "comma-separated roles")
		pwStdin := fs.Bool("password-stdin", false, "read the primary password from stdin")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		req := admin.CreateAccountRequest{Username: *username, Roles: strings.Split(*roles, ",")}
		if *mbs != "" {
			id, err := uuid.Parse(*mbs)
			if err != nil {
				return err
			}
			req.MailboxID = id
		}
		if *pwStdin {
			pw, err := readPasswordStdin()
			if err != nil {
				return err
			}
			req.Password = pw
		}
		acct, err := adm.CreateAccount(ctx, req)
		if err != nil {
			return err
		}
		return printJSON(redact(acct))
	case "get":
		if len(args) != 2 {
			return fmt.Errorf("account get <username|uuid>")
		}
		var acct identity.Account
		var err error
		if id, perr := uuid.Parse(args[1]); perr == nil {
			acct, err = adm.GetAccount(ctx, id)
		} else {
			acct, err = adm.GetAccountByUsername(ctx, args[1])
		}
		if err != nil {
			return err
		}
		return printJSON(redact(acct))
	case "delete":
		if len(args) != 2 {
			return fmt.Errorf("account delete <uuid>")
		}
		id, err := uuid.Parse(args[1])
		if err != nil {
			return err
		}
		return adm.DeleteAccount(ctx, id)
	}
	return fmt.Errorf("unknown account subcommand %q", args[0])
}

// redact strips secrets from an account for display.
func redact(a identity.Account) map[string]any {
	pws := make([]map[string]any, 0, len(a.Passwords))
	for _, p := range a.Passwords {
		pws = append(pws, map[string]any{"id": p.ID, "kind": p.Kind, "comment": p.Comment, "created": p.Created})
	}
	return map[string]any{
		"id": a.ID, "username": a.Username, "domain": a.Domain, "mailbox_id": a.MailboxID, "roles": a.Roles,
		"enabled": a.Enabled, "passwords": pws, "totp_enabled": a.TOTP != nil && a.TOTP.Confirmed, "created": a.Created, "updated": a.Updated,
	}
}

func cmdAlias(ctx context.Context, adm *admin.Service, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("alias: create|delete")
	}
	switch args[0] {
	case "create":
		fs := newFlagSet("alias create")
		to := fs.String("to", "", "comma-separated destinations: mailbox uuids or addresses")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() != 1 || *to == "" {
			return fmt.Errorf("alias create <address> --to <uuid|address>[,...]")
		}
		al := identity.Alias{Address: fs.Arg(0), Enabled: true}
		for _, d := range strings.Split(*to, ",") {
			d = strings.TrimSpace(d)
			if id, err := uuid.Parse(d); err == nil {
				al.Destinations = append(al.Destinations, identity.Destination{MailboxID: id})
			} else {
				al.Destinations = append(al.Destinations, identity.Destination{Address: d})
			}
		}
		out, err := adm.CreateAlias(ctx, al)
		if err != nil {
			return err
		}
		return printJSON(out)
	case "delete":
		if len(args) != 2 {
			return fmt.Errorf("alias delete <address>")
		}
		return adm.DeleteAlias(ctx, args[1])
	}
	return fmt.Errorf("unknown alias subcommand %q", args[0])
}

func cmdToken(ctx context.Context, adm *admin.Service, args []string) error {
	if len(args) == 0 || args[0] != "create" {
		return fmt.Errorf("token: create --account <uuid> --expires YYYY-MM-DD --comment text [--roles r1,r2]")
	}
	fs := newFlagSet("token create")
	acct := fs.String("account", "", "account uuid")
	expires := fs.String("expires", "", "expiry date (YYYY-MM-DD) or RFC3339")
	comment := fs.String("comment", "", "purpose of the token (required)")
	roles := fs.String("roles", "", "comma-separated roles (default: the account's)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	id, err := uuid.Parse(*acct)
	if err != nil {
		return fmt.Errorf("--account: %w", err)
	}
	exp, err := time.Parse("2006-01-02", *expires)
	if err != nil {
		if exp, err = time.Parse(time.RFC3339, *expires); err != nil {
			return fmt.Errorf("--expires: %w", err)
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
	fmt.Printf("token id:     %s (expires %s)\n", t.ID, t.ExpiresAt.Format(time.RFC3339))
	fmt.Printf("token secret: %s\n", secret)
	return nil
}

func cmdHold(ctx context.Context, adm *admin.Service, args []string) error {
	fs := newFlagSet("hold")
	reason := fs.String("reason", "", "reason for the hold (required)")
	caseRef := fs.String("case", "", "case reference")
	noDelivery := fs.Bool("no-delivery", false, "refuse new deliveries while on hold")
	allowSend := fs.Bool("allow-send", false, "allow sending while on hold")
	sealed := fs.Bool("sealed", false, "deny all user access")
	if len(args) < 1 {
		return fmt.Errorf("hold <mailbox-uuid> --reason text")
	}
	id, err := uuid.Parse(args[0])
	if err != nil {
		return err
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	accept := !*noDelivery
	m, err := adm.Hold(ctx, id, admin.HoldRequest{Reason: *reason, CaseRef: *caseRef, AcceptDelivery: &accept, AllowSend: *allowSend, Sealed: *sealed})
	if err != nil {
		return err
	}
	return printJSON(m)
}

func cmdRelease(ctx context.Context, adm *admin.Service, args []string) error {
	fs := newFlagSet("release")
	note := fs.String("note", "", "note for the audit log")
	if len(args) < 1 {
		return fmt.Errorf("release <mailbox-uuid>")
	}
	id, err := uuid.Parse(args[0])
	if err != nil {
		return err
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	m, err := adm.Release(ctx, id, *note)
	if err != nil {
		return err
	}
	return printJSON(m)
}

func cmdExportMailbox(ctx context.Context, a *app.App, args []string) error {
	fs := newFlagSet("export-mailbox")
	out := fs.String("out", "", "output file (.tar or .tar.gz; gzip when the name ends in .gz)")
	if len(args) < 1 {
		return fmt.Errorf("export-mailbox <mailbox-uuid> --out file.tar.gz")
	}
	id, err := uuid.Parse(args[0])
	if err != nil {
		return err
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("--out is required")
	}
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	sum, err := a.Services.Export.ExportMailbox(ctx, id, f, export.Options{Gzip: strings.HasSuffix(*out, ".gz")})
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	fmt.Printf("exported %d messages (%d bytes) in %d folders, %d audit records, %d errors -> %s\n", sum.Messages, sum.Bytes, sum.Folders, sum.Audit, len(sum.Errors), *out)
	for _, e := range sum.Errors {
		fmt.Println("  error:", e)
	}
	return nil
}

func cmdImportMailbox(ctx context.Context, a *app.App, args []string) error {
	fs := newFlagSet("import-mailbox")
	dir := fs.String("dir", "", "Maildir directory (Dovecot layout) to import")
	archive := fs.String("archive", "", "tar or tar.gz archive to import")
	if len(args) < 1 {
		return fmt.Errorf("import-mailbox <mailbox-uuid> --dir path | --archive file")
	}
	id, err := uuid.Parse(args[0])
	if err != nil {
		return err
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	var sum export.Summary
	switch {
	case *dir != "":
		sum, err = a.Services.Export.ImportMaildir(ctx, id, *dir)
	case *archive != "":
		f, ferr := os.Open(*archive)
		if ferr != nil {
			return ferr
		}
		defer f.Close()
		sum, err = a.Services.Export.ImportArchive(ctx, id, f)
	default:
		return fmt.Errorf("one of --dir or --archive is required")
	}
	if err != nil {
		return err
	}
	fmt.Printf("imported %d messages (%d bytes) into %d folders, %d errors\n", sum.Messages, sum.Bytes, sum.Folders, len(sum.Errors))
	for _, e := range sum.Errors {
		fmt.Println("  error:", e)
	}
	return nil
}

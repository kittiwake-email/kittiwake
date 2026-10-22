package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"kittiwake/client/admin"
)

// ---- domain ----

func (a *cli) domain(args []string) error {
	const u = "domain create <name> [--disabled] | list | get <name> | enable <name> | disable <name> | delete <name>"
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprintln(os.Stderr, "usage: kittiwake-admin "+u)
		return nil
	}
	switch args[0] {
	case "create":
		pos, rest, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		fs := newFlagSet("domain create")
		disabled := fs.Bool("disabled", false, "create disabled")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		req := admin.DomainRequest{Name: pos[0]}
		if *disabled {
			req.Enabled = boolPtr(false)
		}
		d, err := a.c.CreateDomain(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(d, nil)
	case "list":
		names, err := a.c.ListDomainsAll(a.ctx)
		if err != nil {
			return err
		}
		if names == nil {
			names = []string{}
		}
		return emit(names, func() {
			for _, n := range names {
				fmt.Println(n)
			}
		})
	case "get", "enable", "disable", "delete":
		pos, _, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		switch args[0] {
		case "get":
			d, err := a.c.GetDomain(a.ctx, pos[0])
			if err != nil {
				return err
			}
			return emit(d, nil)
		case "delete":
			if err := a.c.DeleteDomain(a.ctx, pos[0]); err != nil {
				return err
			}
			return ok("deleted " + pos[0])
		default:
			d, err := a.c.UpdateDomain(a.ctx, pos[0], admin.DomainRequest{Enabled: boolPtr(args[0] == "enable")})
			if err != nil {
				return err
			}
			return emit(d, nil)
		}
	}
	return fmt.Errorf("unknown domain subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

// ---- mailbox ----

func quotaFlags(fs *flag.FlagSet) (*int64, *int64, *int64, *int64) {
	return fs.Int64("soft-bytes", -1, "soft size limit in bytes (0 = default)"),
		fs.Int64("hard-bytes", -1, "hard size limit in bytes"),
		fs.Int64("soft-messages", -1, "soft message count limit"),
		fs.Int64("hard-messages", -1, "hard message count limit")
}

func quotaFrom(sb, hb, sm, hm int64) *admin.QuotaLimits {
	if sb < 0 && hb < 0 && sm < 0 && hm < 0 {
		return nil
	}
	q := &admin.QuotaLimits{}
	if sb >= 0 {
		q.SoftBytes = sb
	}
	if hb >= 0 {
		q.HardBytes = hb
	}
	if sm >= 0 {
		q.SoftMessages = sm
	}
	if hm >= 0 {
		q.HardMessages = hm
	}
	return q
}

func (a *cli) mailbox(args []string) error {
	const u = `mailbox create [--soft-bytes N --hard-bytes N --soft-messages N --hard-messages N]
       mailbox list [--orphaned] | inspect <uuid> | delete <uuid> | recalc <uuid> | holds
       mailbox set <uuid> [quota flags]                            (quota flags replace the whole quota)
       mailbox suspend <uuid> --reason text    soft: LMTP 450, the MTA queues; users locked out
       mailbox disable <uuid> --reason text    hard: LMTP 550, rejected at SMTP time; users locked out
       mailbox activate <uuid>
       mailbox hold <uuid> --reason text [--case ref] [--no-delivery] [--allow-send] [--sealed]
       mailbox release <uuid> [--note text]
       mailbox export <uuid> [--no-gzip] | exports [--mailbox uuid] | download <name> [--out file]
       mailbox import <uuid> --archive file.tar[.gz] | --dir /path/to/Maildir`
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprintln(os.Stderr, "usage: kittiwake-admin "+u)
		return nil
	}
	switch args[0] {
	case "create":
		fs := newFlagSet("mailbox create")
		sb, hb, sm, hm := quotaFlags(fs)
		clusterID := fs.Int64("cluster", 0, "pin the Ceph cluster by registry id (default: random among clusters open for placement)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		req := admin.MailboxRequest{Quota: quotaFrom(*sb, *hb, *sm, *hm), ClusterID: *clusterID}
		m, err := a.c.CreateMailbox(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(m, nil)
	case "list":
		fs := newFlagSet("mailbox list")
		orphaned := fs.Bool("orphaned", false, "only mailboxes without an account (after account/domain removal; attach a new account to keep the data)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		var items []admin.MailboxListItem
		var err error
		if *orphaned {
			items, err = a.c.ListOrphanedMailboxes(a.ctx)
		} else {
			items, err = a.c.ListMailboxes(a.ctx)
		}
		if err != nil {
			return err
		}
		if items == nil {
			items = []admin.MailboxListItem{}
		}
		return emit(items, func() {
			var rows [][]string
			for _, m := range items {
				st := m.State
				if m.StateReason != "" {
					st += " (" + m.StateReason + ")"
				}
				rows = append(rows, []string{m.ID, strconv.FormatInt(m.ClusterID, 10), st, fmt.Sprint(m.OnHold), strings.Join(m.Accounts, ",")})
			}
			table([]string{"MAILBOX", "CLUSTER", "STATE", "HOLD", "ACCOUNTS"}, rows)
		})
	case "holds":
		items, err := a.c.ListHolds(a.ctx)
		if err != nil {
			return err
		}
		if items == nil {
			items = []admin.Mailbox{}
		}
		return emit(items, func() {
			var rows [][]string
			for _, m := range items {
				h := m.Hold
				rows = append(rows, []string{m.ID, h.Reason, h.CaseRef, h.SetBy, fmtTime(h.SetAt), fmt.Sprint(h.Sealed)})
			}
			table([]string{"MAILBOX", "REASON", "CASE", "SET BY", "SET AT", "SEALED"}, rows)
		})
	case "exports":
		fs := newFlagSet("mailbox exports")
		mb := fs.String("mailbox", "", "only this mailbox")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		items, err := a.c.ListExports(a.ctx, *mb)
		if err != nil {
			return err
		}
		if items == nil {
			items = []admin.ExportItem{}
		}
		return emit(items, func() {
			var rows [][]string
			for _, e := range items {
				msgs := "-"
				if e.Result != nil {
					msgs = strconv.Itoa(e.Result.Messages)
				}
				rows = append(rows, []string{e.Name, strconv.FormatInt(e.Bytes, 10), msgs, fmtTime(e.Modified)})
			}
			table([]string{"NAME", "BYTES", "MESSAGES", "MODIFIED"}, rows)
		})
	case "download":
		pos, rest, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		fs := newFlagSet("mailbox download")
		out := fs.String("out", "", "output file (default: the export's name in the current directory)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		dst := *out
		if dst == "" {
			dst = pos[0]
		}
		rc, err := a.c.DownloadExport(a.ctx, pos[0])
		if err != nil {
			return err
		}
		defer rc.Close()
		f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return err
		}
		n, err := io.Copy(f, rc)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		return emit(map[string]any{"file": dst, "bytes": n}, func() { fmt.Printf("saved %s (%d bytes)\n", dst, n) })
	}
	// Everything else takes a mailbox uuid first.
	pos, rest, err := splitArgs(args[1:], 1, u)
	if err != nil {
		return err
	}
	id := pos[0]
	switch args[0] {
	case "inspect":
		info, err := a.c.InspectMailbox(a.ctx, id)
		if err != nil {
			return err
		}
		return emit(info, nil)
	case "set":
		fs := newFlagSet("mailbox set")
		sb, hb, sm, hm := quotaFlags(fs)
		if err := fs.Parse(rest); err != nil {
			return err
		}
		req := admin.MailboxRequest{Quota: quotaFrom(*sb, *hb, *sm, *hm)}
		if req.Quota == nil {
			return errors.New("nothing to change")
		}
		m, err := a.c.UpdateMailbox(a.ctx, id, req)
		if err != nil {
			return err
		}
		return emit(m, nil)
	case "suspend", "disable":
		fs := newFlagSet("mailbox " + args[0])
		reason := fs.String("reason", "", "why (required; shown to operators, logged in the audit trail)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *reason == "" {
			return errors.New("--reason is required")
		}
		state := map[string]string{"suspend": "suspended", "disable": "disabled"}[args[0]]
		m, err := a.c.SetMailboxState(a.ctx, id, state, *reason)
		if err != nil {
			return err
		}
		return emit(m, nil)
	case "activate":
		m, err := a.c.SetMailboxState(a.ctx, id, "active", "")
		if err != nil {
			return err
		}
		return emit(m, nil)
	case "delete":
		if err := a.c.DeleteMailbox(a.ctx, id); err != nil {
			return err
		}
		return ok("deleted " + id + " (data removal scheduled)")
	case "recalc":
		usage, err := a.c.RecalculateMailbox(a.ctx, id)
		if err != nil {
			return err
		}
		return emit(usage, nil)
	case "hold":
		fs := newFlagSet("mailbox hold")
		reason := fs.String("reason", "", "reason (required)")
		caseRef := fs.String("case", "", "case reference")
		noDelivery := fs.Bool("no-delivery", false, "refuse new deliveries")
		allowSend := fs.Bool("allow-send", false, "allow sending")
		sealed := fs.Bool("sealed", false, "deny all user access")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *reason == "" {
			return errors.New("--reason is required")
		}
		m, err := a.c.HoldMailbox(a.ctx, id, admin.HoldRequest{Reason: *reason, CaseRef: *caseRef, AcceptDelivery: boolPtr(!*noDelivery), AllowSend: *allowSend, Sealed: *sealed})
		if err != nil {
			return err
		}
		return emit(m, nil)
	case "release":
		fs := newFlagSet("mailbox release")
		note := fs.String("note", "", "note for the audit log")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		m, err := a.c.ReleaseMailbox(a.ctx, id, *note)
		if err != nil {
			return err
		}
		return emit(m, nil)
	case "export":
		fs := newFlagSet("mailbox export")
		noGzip := fs.Bool("no-gzip", false, "plain tar instead of tar.gz")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		s, err := a.c.ScheduleExport(a.ctx, id, !*noGzip)
		if err != nil {
			return err
		}
		return emit(s, func() {
			fmt.Printf("export scheduled, task %s; poll: kittiwake-admin mailbox exports --mailbox %s\n", s.TaskID, id)
		})
	case "import":
		fs := newFlagSet("mailbox import")
		archive := fs.String("archive", "", "tar or tar.gz archive")
		dir := fs.String("dir", "", "Maildir directory (packed and uploaded)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		var sum admin.ImportSummary
		switch {
		case *archive != "":
			f, err := os.Open(*archive)
			if err != nil {
				return err
			}
			defer f.Close()
			sum, err = a.c.ImportMailbox(a.ctx, id, f, isGzipName(*archive))
			if err != nil {
				return err
			}
		case *dir != "":
			pr, pw := io.Pipe()
			go func() { pw.CloseWithError(packMaildir(*dir, pw)) }()
			var err error
			sum, err = a.c.ImportMailbox(a.ctx, id, pr, true)
			_ = pr.Close()
			if err != nil {
				return err
			}
		default:
			return errors.New("one of --archive or --dir is required")
		}
		return emit(sum, func() {
			fmt.Printf("imported %d messages (%d bytes) into %d folders, %d errors\n", sum.Messages, sum.Bytes, sum.Folders, len(sum.Errors))
			for _, e := range sum.Errors {
				fmt.Println("  error:", e)
			}
		})
	}
	return fmt.Errorf("unknown mailbox subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

// ---- account ----

func (a *cli) account(args []string) error {
	const u = `account create --username u@d --mailbox <uuid> [--roles read,write,send] [--password-stdin] [--disabled]
       account list [--domain d] [--prefix p] | get <username|uuid> | delete <id>
       account set <id> [--roles r,..] [--mailbox uuid] [--enable|--disable]
       account password <id> --comment text [--kind primary|application] [--expires YYYY-MM-DD|RFC3339] --password-stdin
       account password <id> list
       account password <id> expire <password-id> --expires YYYY-MM-DD|RFC3339|never [--comment text]
       account password <id> remove <password-id>
                                              (an expiring primary is added as a temporary password; the permanent one keeps working)
       account totp-disable <id> | sessions <id> | revoke-session <id> <session-id>
       account token create <id> --expires YYYY-MM-DD|RFC3339 --comment text [--roles r,..]
       account tokens <id> | token delete <id> <token-id>`
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprintln(os.Stderr, "usage: kittiwake-admin "+u)
		return nil
	}
	switch args[0] {
	case "create":
		fs := newFlagSet("account create")
		username := fs.String("username", "", "username (e-mail address)")
		name := fs.String("name", "", "display name (From name of the account's identities)")
		mailbox := fs.String("mailbox", "", "mailbox uuid")
		roles := fs.String("roles", "read,write,send", "comma-separated roles")
		services := fs.String("services", "", "comma-separated services the account may use: imap,pop3,jmap (default: all)")
		pwStdin := fs.Bool("password-stdin", false, "read the primary password from stdin")
		disabled := fs.Bool("disabled", false, "create disabled")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *username == "" {
			return errors.New("--username is required")
		}
		req := admin.AccountRequest{Username: *username, MailboxID: *mailbox, Roles: splitList(*roles)}
		if *name != "" {
			req.Name = name
		}
		if *services != "" {
			req.Services = splitList(*services)
		}
		if *pwStdin {
			pw, err := readPasswordStdin()
			if err != nil {
				return err
			}
			req.Password = pw
		}
		if *disabled {
			req.Enabled = boolPtr(false)
		}
		acct, err := a.c.CreateAccount(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(acct, nil)
	case "list":
		fs := newFlagSet("account list")
		domain := fs.String("domain", "", "only this domain (default: all)")
		prefix := fs.String("prefix", "", "username prefix")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		domains := []string{*domain}
		if *domain == "" {
			var err error
			if domains, err = a.c.ListDomainsAll(a.ctx); err != nil {
				return err
			}
		}
		accounts := []admin.Account{}
		for _, d := range domains {
			names, err := a.c.ListAccountsAll(a.ctx, d, *prefix)
			if err != nil {
				return err
			}
			for _, n := range names {
				acct, err := a.c.GetAccount(a.ctx, n)
				if err != nil {
					if admin.IsNotFound(err) {
						continue
					}
					return err
				}
				accounts = append(accounts, acct)
			}
		}
		return emit(accounts, func() {
			var rows [][]string
			for _, x := range accounts {
				mb := x.MailboxID
				if mb == "" || mb == "00000000-0000-0000-0000-000000000000" {
					mb = "-"
				}
				rows = append(rows, []string{x.Username, mb, fmt.Sprint(x.Enabled), strings.Join(x.Roles, ","), fmt.Sprint(x.TOTPEnabled)})
			}
			table([]string{"USERNAME", "MAILBOX", "ENABLED", "ROLES", "TOTP"}, rows)
		})
	case "token":
		return a.accountToken(args[1:], u)
	}
	pos, rest, err := splitArgs(args[1:], 1, u)
	if err != nil {
		return err
	}
	id := pos[0]
	switch args[0] {
	case "get":
		acct, err := a.c.GetAccount(a.ctx, id)
		if err != nil {
			return err
		}
		return emit(acct, nil)
	case "set":
		fs := newFlagSet("account set")
		roles := fs.String("roles", "", "comma-separated roles")
		services := fs.String("services", "", "comma-separated services the account may use (imap,pop3,jmap); \"none\" disables every login")
		mailbox := fs.String("mailbox", "", "move to this mailbox uuid")
		name := fs.String("name", "", "display name (\"\" is not settable here; use --clear-name)")
		clearName := fs.Bool("clear-name", false, "remove the display name")
		en, dis := enableFlags(fs)
		if err := fs.Parse(rest); err != nil {
			return err
		}
		enabled, err := enabledFrom(*en, *dis)
		if err != nil {
			return err
		}
		req := admin.AccountRequest{Roles: splitList(*roles), MailboxID: *mailbox, Enabled: enabled}
		if *clearName {
			empty := ""
			req.Name = &empty
		} else if *name != "" {
			req.Name = name
		}
		switch *services {
		case "":
		case "none":
			req.Services = []string{}
		default:
			req.Services = splitList(*services)
		}
		if req.Roles == nil && req.MailboxID == "" && req.Enabled == nil && req.Name == nil && req.Services == nil {
			return errors.New("nothing to change")
		}
		acct, err := a.c.UpdateAccount(a.ctx, id, req)
		if err != nil {
			return err
		}
		return emit(acct, nil)
	case "delete":
		if err := a.c.DeleteAccount(a.ctx, id); err != nil {
			return err
		}
		return ok("deleted " + id)
	case "password":
		if len(rest) > 0 {
			switch rest[0] {
			case "list":
				list, err := a.c.ListPasswords(a.ctx, id)
				if err != nil {
					return err
				}
				if list == nil {
					list = []admin.Password{}
				}
				return emit(list, func() {
					var rows [][]string
					for _, p := range list {
						exp := "never"
						if p.ExpiresAt != nil {
							exp = fmtTime(*p.ExpiresAt)
						}
						rows = append(rows, []string{p.ID, p.Kind, p.Comment, exp, fmtTime(p.Created)})
					}
					table([]string{"ID", "KIND", "COMMENT", "EXPIRES", "CREATED"}, rows)
				})
			case "remove":
				if len(rest) != 2 {
					return errors.New("usage: kittiwake-admin account password <account> remove <password-id>")
				}
				if err := a.c.DeletePassword(a.ctx, id, rest[1]); err != nil {
					return err
				}
				return ok("removed password " + rest[1])
			case "expire":
				fs := newFlagSet("account password expire")
				expires := fs.String("expires", "", "YYYY-MM-DD, RFC3339 or \"never\" (required)")
				comment := fs.String("comment", "", "new comment (optional)")
				if len(rest) < 2 {
					return errors.New("usage: kittiwake-admin account password <account> expire <password-id> --expires DATE|never [--comment text]")
				}
				if err := fs.Parse(rest[2:]); err != nil {
					return err
				}
				if *expires == "" {
					return errors.New("--expires is required (a date, or \"never\")")
				}
				req := admin.PasswordUpdate{Comment: *comment}
				if *expires != "never" {
					t, err := parseDate(*expires)
					if err != nil {
						return fmt.Errorf("--expires: %w", err)
					}
					req.ExpiresAt = &t
				}
				p, err := a.c.UpdatePassword(a.ctx, id, rest[1], req)
				if err != nil {
					return err
				}
				return emit(p, nil)
			}
		}
		fs := newFlagSet("account password")
		kind := fs.String("kind", "primary", "primary or application")
		comment := fs.String("comment", "", "what the password is for (required)")
		expires := fs.String("expires", "", "expiry: YYYY-MM-DD or RFC3339 (default: never)")
		pwStdin := fs.Bool("password-stdin", false, "read the password from stdin (required)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if !*pwStdin {
			return errors.New("--password-stdin is required (passwords are never taken from arguments)")
		}
		if *comment == "" {
			return errors.New("--comment is required")
		}
		var exp *time.Time
		if *expires != "" {
			t, err := parseDate(*expires)
			if err != nil {
				return fmt.Errorf("--expires: %w", err)
			}
			exp = &t
		}
		pw, err := readPasswordStdin()
		if err != nil {
			return err
		}
		ps, err := a.c.SetPassword(a.ctx, id, admin.PasswordRequest{Password: pw, Kind: *kind, Comment: *comment, ExpiresAt: exp})
		if err != nil {
			return err
		}
		return emit(ps, nil)
	case "totp-disable":
		if err := a.c.DisableTOTP(a.ctx, id); err != nil {
			return err
		}
		return ok("totp disabled for " + id)
	case "sessions":
		list, err := a.c.ListSessions(a.ctx, id)
		if err != nil {
			return err
		}
		if list == nil {
			list = []admin.Session{}
		}
		return emit(list, func() {
			var rows [][]string
			for _, s := range list {
				rows = append(rows, []string{s.ID, s.Client, strings.Join(s.Roles, ","), fmtTime(s.Created), fmtTime(s.LastUsed), s.LastIP, fmtTime(s.ExpiresAt)})
			}
			table([]string{"SESSION", "CLIENT", "ROLES", "CREATED", "LAST USED", "LAST IP", "EXPIRES"}, rows)
		})
	case "revoke-session":
		if len(rest) != 1 {
			return errors.New("usage: account revoke-session <id> <session-id>")
		}
		if err := a.c.RevokeSession(a.ctx, id, rest[0]); err != nil {
			return err
		}
		return ok("revoked session " + rest[0])
	case "tokens":
		list, err := a.c.ListTokens(a.ctx, id)
		if err != nil {
			return err
		}
		if list == nil {
			list = []admin.Token{}
		}
		return emit(list, func() {
			var rows [][]string
			for _, t := range list {
				rows = append(rows, []string{t.ID, t.Comment, strings.Join(t.Roles, ","), fmtTime(t.ExpiresAt), fmtTime(t.LastUsed)})
			}
			table([]string{"TOKEN", "COMMENT", "ROLES", "EXPIRES", "LAST USED"}, rows)
		})
	}
	return fmt.Errorf("unknown account subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

func (a *cli) accountToken(args []string, u string) error {
	if len(args) == 0 {
		return errors.New("usage: account token create <id> ... | token delete <id> <token-id>")
	}
	switch args[0] {
	case "create":
		pos, rest, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		fs := newFlagSet("account token create")
		expires := fs.String("expires", "", "expiry: YYYY-MM-DD or RFC3339 (required)")
		comment := fs.String("comment", "", "purpose of the token (required)")
		roles := fs.String("roles", "", "comma-separated roles (default: the account's)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *expires == "" || *comment == "" {
			return errors.New("--expires and --comment are required")
		}
		exp, err := parseDate(*expires)
		if err != nil {
			return fmt.Errorf("--expires: %w", err)
		}
		tk, err := a.c.CreateToken(a.ctx, pos[0], admin.TokenRequest{Roles: splitList(*roles), ExpiresAt: exp, Comment: *comment})
		if err != nil {
			return err
		}
		return emit(tk, func() {
			fmt.Printf("token id:     %s (expires %s)\n", tk.Token.ID, tk.Token.ExpiresAt.Format(time.RFC3339))
			fmt.Printf("token secret: %s\n", tk.Secret)
			fmt.Println("The secret is shown once.")
		})
	case "delete":
		if len(args) != 3 {
			return errors.New("usage: account token delete <id> <token-id>")
		}
		if err := a.c.DeleteToken(a.ctx, args[1], args[2]); err != nil {
			return err
		}
		return ok("deleted token " + args[2])
	}
	return fmt.Errorf("unknown token subcommand %q", args[0])
}

// ---- alias ----

func parseDestinations(s string) []admin.Destination {
	var out []admin.Destination
	for _, d := range splitList(s) {
		if strings.Contains(d, "@") {
			out = append(out, admin.Destination{Address: d})
		} else {
			out = append(out, admin.Destination{MailboxID: d})
		}
	}
	return out
}

func (a *cli) alias(args []string) error {
	const u = `alias create <address> --to <uuid|address>[,...] [--disabled]
       alias list --domain d | get <address> | delete <address>
       alias set <address> [--to ...] [--enable|--disable]`
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprintln(os.Stderr, "usage: kittiwake-admin "+u)
		return nil
	}
	if args[0] == "list" {
		fs := newFlagSet("alias list")
		domain := fs.String("domain", "", "domain (required)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *domain == "" {
			return errors.New("--domain is required")
		}
		names, err := a.c.ListAliasesAll(a.ctx, *domain)
		if err != nil {
			return err
		}
		if names == nil {
			names = []string{}
		}
		return emit(names, func() {
			for _, n := range names {
				fmt.Println(n)
			}
		})
	}
	pos, rest, err := splitArgs(args[1:], 1, u)
	if err != nil {
		return err
	}
	addr := pos[0]
	switch args[0] {
	case "create":
		fs := newFlagSet("alias create")
		to := fs.String("to", "", "destinations: mailbox uuids or addresses, comma-separated (required)")
		disabled := fs.Bool("disabled", false, "create disabled")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if *to == "" {
			return errors.New("--to is required")
		}
		req := admin.AliasRequest{Address: addr, Destinations: parseDestinations(*to)}
		if *disabled {
			req.Enabled = boolPtr(false)
		}
		al, err := a.c.CreateAlias(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(al, nil)
	case "get":
		al, err := a.c.GetAlias(a.ctx, addr)
		if err != nil {
			return err
		}
		return emit(al, nil)
	case "set":
		fs := newFlagSet("alias set")
		to := fs.String("to", "", "replace destinations")
		en, dis := enableFlags(fs)
		if err := fs.Parse(rest); err != nil {
			return err
		}
		enabled, err := enabledFrom(*en, *dis)
		if err != nil {
			return err
		}
		req := admin.AliasRequest{Enabled: enabled, Destinations: parseDestinations(*to)}
		if req.Enabled == nil && req.Destinations == nil {
			return errors.New("nothing to change")
		}
		al, err := a.c.UpdateAlias(a.ctx, addr, req)
		if err != nil {
			return err
		}
		return emit(al, nil)
	case "delete":
		if err := a.c.DeleteAlias(a.ctx, addr); err != nil {
			return err
		}
		return ok("deleted " + addr)
	}
	return fmt.Errorf("unknown alias subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

// ---- audit ----

func (a *cli) audit(args []string) error {
	fs := newFlagSet("audit")
	mailbox := fs.String("mailbox", "", "mailbox uuid (default: the admin log)")
	account := fs.String("account", "", "username")
	action := fs.String("action", "", "action, e.g. login-failed")
	ip := fs.String("ip", "", "client IP")
	from := fs.String("from", "", "first day YYYY-MM-DD (default today)")
	to := fs.String("to", "", "last day YYYY-MM-DD (default: --from)")
	limit := fs.Int("limit", 100, "records per page")
	after := fs.String("after", "", "cursor from a previous page")
	all := fs.Bool("all", false, "follow cursors and return everything")
	if len(args) > 0 && args[0] == "help" {
		fs.Usage()
		return nil
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	q := admin.AuditQuery{Mailbox: *mailbox, Account: *account, Action: *action, ClientIP: *ip, Limit: *limit, After: *after}
	var err error
	if *from != "" {
		if q.From, err = parseDate(*from); err != nil {
			return fmt.Errorf("--from: %w", err)
		}
	}
	if *to != "" {
		if q.To, err = parseDate(*to); err != nil {
			return fmt.Errorf("--to: %w", err)
		}
	}
	var items []admin.AuditRecord
	next := ""
	if *all {
		items, err = a.c.AuditAll(a.ctx, q)
	} else {
		var p admin.AuditPage
		p, err = a.c.Audit(a.ctx, q)
		items, next = p.Items, p.Next
	}
	if err != nil {
		return err
	}
	if items == nil {
		items = []admin.AuditRecord{}
	}
	return emit(map[string]any{"items": items, "next": next}, func() {
		var rows [][]string
		for _, r := range items {
			actor := r.Account
			if r.Admin != "" {
				actor = r.Admin + " for " + r.Account
			}
			rows = append(rows, []string{r.Time.Format(time.RFC3339), r.Protocol, actor, r.ClientIP, r.Action, r.Target, r.Result, r.Detail})
		}
		table([]string{"TIME", "PROTO", "ACTOR", "CLIENT IP", "ACTION", "TARGET", "RESULT", "DETAIL"}, rows)
		if next != "" {
			fmt.Printf("more: --after %s\n", next)
		}
	})
}

// ---- queue / keys / identity / openapi ----

func (a *cli) queue(args []string) error {
	const u = "queue stats <type> | dead <type> | retry <type> <task-id>"
	if len(args) < 2 || args[0] == "help" {
		return errors.New("usage: kittiwake-admin " + u)
	}
	typ := args[1]
	switch args[0] {
	case "stats":
		st, err := a.c.QueueStats(a.ctx, typ)
		if err != nil {
			return err
		}
		return emit(st, nil)
	case "dead":
		list, err := a.c.QueueDead(a.ctx, typ)
		if err != nil {
			return err
		}
		if list == nil {
			list = []admin.Task{}
		}
		return emit(list, func() {
			var rows [][]string
			for _, t := range list {
				rows = append(rows, []string{t.ID, strconv.Itoa(t.Attempts), fmtTime(t.Created), t.LastError})
			}
			table([]string{"TASK", "ATTEMPTS", "CREATED", "LAST ERROR"}, rows)
		})
	case "retry":
		if len(args) != 3 {
			return errors.New("usage: kittiwake-admin " + u)
		}
		if err := a.c.QueueRetry(a.ctx, typ, args[2]); err != nil {
			return err
		}
		return ok("requeued " + args[2])
	}
	return fmt.Errorf("unknown queue subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

// token manages admin tokens (admins are tokens, not accounts).
func (a *cli) token(args []string) error {
	const u = `token create --comment text --expires YYYY-MM-DD|RFC3339 [--read-only]
       token list
       token delete <token-id>`
	if len(args) == 0 || args[0] == "help" {
		fmt.Println("usage: kittiwake-admin " + u)
		return nil
	}
	switch args[0] {
	case "create":
		fs := newFlagSet("token create")
		expires := fs.String("expires", "", "expiry: YYYY-MM-DD or RFC3339 (required)")
		comment := fs.String("comment", "", "purpose of the token (required)")
		readOnly := fs.Bool("read-only", false, "mint a read-only token (role admin-ro: GET-only)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *expires == "" || *comment == "" {
			return errors.New("--expires and --comment are required")
		}
		exp, err := parseDate(*expires)
		if err != nil {
			return fmt.Errorf("--expires: %w", err)
		}
		tk, err := a.c.CreateAdminToken(a.ctx, admin.AdminTokenRequest{ReadOnly: *readOnly, ExpiresAt: exp, Comment: *comment})
		if err != nil {
			return err
		}
		return emit(tk, func() {
			fmt.Printf("token id:     %s (%s, expires %s)\n", tk.Token.ID, strings.Join(tk.Token.Roles, ","), tk.Token.ExpiresAt.Format(time.RFC3339))
			fmt.Printf("token secret: %s\n", tk.Secret)
			fmt.Println("The secret is shown once.")
		})
	case "list":
		list, err := a.c.ListAdminTokens(a.ctx)
		if err != nil {
			return err
		}
		return emit(list, func() {
			var rows [][]string
			for _, t := range list {
				rows = append(rows, []string{t.ID, t.Comment, strings.Join(t.Roles, ","), fmtTime(t.ExpiresAt), fmtTime(t.Created), fmtTime(t.LastUsed)})
			}
			table([]string{"ID", "COMMENT", "ROLE", "EXPIRES", "CREATED", "LAST USED"}, rows)
		})
	case "delete":
		if len(args) != 2 {
			return errors.New("usage: kittiwake-admin token delete <token-id>")
		}
		if err := a.c.DeleteAdminToken(a.ctx, args[1]); err != nil {
			return err
		}
		return ok("revoked " + args[1])
	}
	return fmt.Errorf("unknown token subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

func (a *cli) cluster(args []string) error {
	const u = `cluster add --name text --mon-host "[addr]:6789,..." --user cephx-user --key-stdin|--keyring file --pools messages,attachments,index [--no-placement]
       cluster list | get <id>
       cluster set <id> [--name text] [--mon-host ...] [--user ...] [--key-stdin|--keyring file] [--pools m,a,i]
       cluster enable <id> | disable <id>      placement of new mailboxes (existing ones keep working)
       cluster remove <id>                     only when no mailbox refers to it`
	if len(args) == 0 || args[0] == "help" {
		fmt.Println("usage: kittiwake-admin " + u)
		return nil
	}
	clusterFlags := func(fs *flag.FlagSet) (name, mon, user, pools *string, keyStdin *bool, keyring *string) {
		name = fs.String("name", "", "cluster name (unique)")
		mon = fs.String("mon-host", "", "mon_host as in ceph.conf")
		user = fs.String("user", "", "CephX user without \"client.\"")
		pools = fs.String("pools", "", "messages,attachments,index pool names")
		keyStdin = fs.Bool("key-stdin", false, "read the CephX key from stdin")
		keyring = fs.String("keyring", "", "read the CephX key from this keyring file")
		return
	}
	readKey := func(stdin bool, keyring string) (string, error) {
		switch {
		case stdin && keyring != "":
			return "", errors.New("use either --key-stdin or --keyring")
		case stdin:
			return readPasswordStdin()
		case keyring != "":
			return keyFromKeyring(keyring)
		}
		return "", nil
	}
	buildReq := func(name, mon, user, pools string, key string) admin.ClusterRequest {
		req := admin.ClusterRequest{Name: name, MonHost: mon, User: user, Key: key}
		if pools != "" {
			p := splitList(pools)
			if len(p) == 3 {
				req.Pools = &admin.Pools{Messages: p[0], Attachments: p[1], Index: p[2]}
			}
		}
		return req
	}
	printCluster := func(c admin.Cluster) {
		fmt.Printf("id:        %d\nname:      %s\nfsid:      %s\nmon_host:  %s\nuser:      %s\npools:     %s, %s, %s\nplacement: %v\n",
			c.ID, c.Name, c.FSID, c.MonHost, c.User, c.Pools.Messages, c.Pools.Attachments, c.Pools.Index, c.Placement)
	}
	switch args[0] {
	case "add":
		fs := newFlagSet("cluster add")
		name, mon, user, pools, keyStdin, keyring := clusterFlags(fs)
		noPlacement := fs.Bool("no-placement", false, "register without accepting new mailboxes")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *name == "" || *mon == "" || *user == "" || *pools == "" || (!*keyStdin && *keyring == "") {
			return errors.New("--name, --mon-host, --user, --pools and a key (--key-stdin or --keyring) are required")
		}
		key, err := readKey(*keyStdin, *keyring)
		if err != nil {
			return err
		}
		req := buildReq(*name, *mon, *user, *pools, key)
		if req.Pools == nil {
			return errors.New("--pools needs exactly three names: messages,attachments,index")
		}
		if *noPlacement {
			req.Placement = boolPtr(false)
		}
		c, err := a.c.AddCluster(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(c, func() { printCluster(c) })
	case "list":
		list, err := a.c.ListClusters(a.ctx)
		if err != nil {
			return err
		}
		if list == nil {
			list = []admin.Cluster{}
		}
		return emit(list, func() {
			var rows [][]string
			for _, c := range list {
				rows = append(rows, []string{strconv.FormatInt(c.ID, 10), c.Name, c.FSID, c.MonHost, fmt.Sprint(c.Placement)})
			}
			table([]string{"ID", "NAME", "FSID", "MON_HOST", "PLACEMENT"}, rows)
		})
	case "get", "set", "enable", "disable", "remove":
		if len(args) < 2 {
			return fmt.Errorf("usage: kittiwake-admin cluster %s <id>", args[0])
		}
		id, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("cluster id %q is not a number", args[1])
		}
		switch args[0] {
		case "get":
			c, err := a.c.GetCluster(a.ctx, id)
			if err != nil {
				return err
			}
			return emit(c, func() { printCluster(c) })
		case "set":
			fs := newFlagSet("cluster set")
			name, mon, user, pools, keyStdin, keyring := clusterFlags(fs)
			if err := fs.Parse(args[2:]); err != nil {
				return err
			}
			key, err := readKey(*keyStdin, *keyring)
			if err != nil {
				return err
			}
			req := buildReq(*name, *mon, *user, *pools, key)
			if *pools != "" && req.Pools == nil {
				return errors.New("--pools needs exactly three names: messages,attachments,index")
			}
			if req.Name == "" && req.MonHost == "" && req.User == "" && req.Key == "" && req.Pools == nil {
				return errors.New("nothing to change")
			}
			c, err := a.c.UpdateCluster(a.ctx, id, req)
			if err != nil {
				return err
			}
			return emit(c, func() { printCluster(c) })
		case "enable", "disable":
			c, err := a.c.UpdateCluster(a.ctx, id, admin.ClusterRequest{Placement: boolPtr(args[0] == "enable")})
			if err != nil {
				return err
			}
			return emit(c, func() { fmt.Printf("cluster %d (%s): placement %v\n", c.ID, c.Name, c.Placement) })
		case "remove":
			if err := a.c.DeleteCluster(a.ctx, id); err != nil {
				return err
			}
			return ok(fmt.Sprintf("removed cluster %d", id))
		}
	}
	return fmt.Errorf("unknown cluster subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

// keyFromKeyring extracts the first "key = ..." of a Ceph keyring file.
func keyFromKeyring(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok && strings.TrimSpace(k) == "key" {
			return strings.TrimSpace(v), nil
		}
	}
	return "", fmt.Errorf("%s: no key found", path)
}

func (a *cli) keys(args []string) error {
	if len(args) != 1 || args[0] != "rotate" {
		return errors.New("usage: kittiwake-admin keys rotate")
	}
	if err := a.c.RotateKeys(a.ctx); err != nil {
		return err
	}
	return ok("signing keys rotated")
}

func (a *cli) identity(args []string) error {
	const u = "identity export [--out file] | import <file>"
	if len(args) == 0 || args[0] == "help" {
		return errors.New("usage: kittiwake-admin " + u)
	}
	switch args[0] {
	case "export":
		fs := newFlagSet("identity export")
		out := fs.String("out", "", "write to this file instead of stdout")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		dump, err := a.c.ExportIdentity(a.ctx)
		if err != nil {
			return err
		}
		if *out == "" {
			_, err = os.Stdout.Write(dump)
			return err
		}
		if err := os.WriteFile(*out, dump, 0o600); err != nil {
			return err
		}
		return ok("wrote " + *out)
	case "import":
		if len(args) != 2 {
			return errors.New("usage: kittiwake-admin " + u)
		}
		data, err := os.ReadFile(args[1])
		if err != nil {
			return err
		}
		summary, err := a.c.ImportIdentity(a.ctx, data)
		if err != nil {
			return err
		}
		return emit(map[string]string{"summary": summary}, func() { fmt.Println(summary) })
	}
	return fmt.Errorf("unknown identity subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

func (a *cli) openapi(args []string) error {
	fs := newFlagSet("openapi")
	out := fs.String("out", "", "write to this file instead of stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := a.c.OpenAPI(a.ctx)
	if err != nil {
		return err
	}
	if *out == "" {
		_, err = os.Stdout.Write(spec)
		return err
	}
	if err := os.WriteFile(*out, spec, 0o644); err != nil {
		return err
	}
	return ok("wrote " + *out)
}

func (a *cli) node(args []string) error {
	const u = `node list [--mode lmtp|imap|pop3|jmap|admin|postfix|worker|search]`
	if len(args) == 0 || args[0] == "help" {
		fmt.Println("usage: kittiwake-admin " + u)
		return nil
	}
	if args[0] != "list" {
		return fmt.Errorf("unknown node subcommand %q\nusage: kittiwake-admin %s", args[0], u)
	}
	fs := newFlagSet("node list")
	mode := fs.String("mode", "", "only this mode")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	list, err := a.c.ListNodes(a.ctx, *mode)
	if err != nil {
		return err
	}
	if list == nil {
		list = []admin.Node{}
	}
	return emit(list, func() {
		var rows [][]string
		for _, n := range list {
			state := "alive"
			if !n.Alive {
				state = "dead"
			}
			rows = append(rows, []string{n.Mode, n.Address, n.Host, n.Version, state, fmtTime(n.Started), fmtTime(n.LastSeen)})
		}
		table([]string{"MODE", "ADDRESS", "HOST", "VERSION", "STATE", "STARTED", "LAST SEEN"}, rows)
	})
}

package main

import (
	"crypto/ed25519"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	cryptoauth "kittiwake/auth"
	"kittiwake/client/admin"
)

// ---- namespace ----

func (a *cli) namespace(args []string) error {
	const u = "namespace create <name> [--disabled] [--ingress host [--ingress-tls strict|insecure|off]] | list | get <name> | set <name> [--setting key=value...] [--ingress host [--ingress-tls policy] | --no-ingress] | enable <name> | disable <name> | delete <name>"
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprintln(os.Stderr, "usage: kittiwake-admin "+u)
		return nil
	}
	printNamespace := func(ns admin.Namespace) {
		fmt.Printf("name:     %s\nenabled:  %v\ncreated:  %s\n", ns.Name, ns.Enabled, fmtTime(ns.Created))
		if ns.Ingress != nil {
			tls := ns.Ingress.TLS
			if tls == "" {
				tls = "(proxy default)"
			}
			fmt.Printf("ingress:  %s (tls %s)\n", ns.Ingress.Host, tls)
		} else {
			fmt.Printf("ingress:  none (proxies use the node registry)\n")
		}
		for k, v := range ns.Settings {
			fmt.Printf("setting:  %s=%s\n", k, v)
		}
	}
	switch args[0] {
	case "create":
		pos, rest, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		fs := newFlagSet("namespace create")
		disabled := fs.Bool("disabled", false, "create disabled")
		ingress := fs.String("ingress", "", "ingress hostname proxies connect to for this namespace (default: none, proxies use the node registry)")
		ingressTLS := fs.String("ingress-tls", "", "TLS policy for the ingress hop: strict, insecure or off (default: the proxy's configured default)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		req := admin.NamespaceRequest{Name: pos[0]}
		if *disabled {
			req.Enabled = boolPtr(false)
		}
		if *ingress != "" {
			req.Ingress = &admin.Ingress{Host: *ingress, TLS: *ingressTLS}
		} else if *ingressTLS != "" {
			return errors.New("--ingress-tls needs --ingress")
		}
		ns, err := a.c.CreateNamespace(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(ns, func() { printNamespace(ns) })
	case "list":
		names, err := a.c.ListNamespacesAll(a.ctx)
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
			ns, err := a.c.GetNamespace(a.ctx, pos[0])
			if err != nil {
				return err
			}
			return emit(ns, func() { printNamespace(ns) })
		case "enable", "disable":
			ns, err := a.c.UpdateNamespace(a.ctx, pos[0], admin.NamespaceRequest{Enabled: boolPtr(args[0] == "enable")})
			if err != nil {
				return err
			}
			return emit(ns, func() { printNamespace(ns) })
		default:
			if err := a.c.DeleteNamespace(a.ctx, pos[0]); err != nil {
				return err
			}
			return emit(map[string]string{"deleted": pos[0]}, func() { fmt.Println("deleted", pos[0]) })
		}
	case "set":
		pos, rest, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		fs := newFlagSet("namespace set")
		var settings stringList
		fs.Var(&settings, "setting", "key=value (repeatable; replaces all settings)")
		ingress := fs.String("ingress", "", "ingress hostname proxies connect to for this namespace")
		ingressTLS := fs.String("ingress-tls", "", "TLS policy for the ingress hop: strict, insecure or off (default: the proxy's configured default)")
		noIngress := fs.Bool("no-ingress", false, "remove the ingress (proxies use the node registry)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if len(settings) == 0 && *ingress == "" && !*noIngress {
			return errors.New("nothing to set: --setting key=value, --ingress host or --no-ingress")
		}
		if *noIngress && *ingress != "" {
			return errors.New("--ingress and --no-ingress exclude each other")
		}
		if *ingressTLS != "" && *ingress == "" {
			return errors.New("--ingress-tls needs --ingress")
		}
		req := admin.NamespaceRequest{}
		if len(settings) > 0 {
			req.Settings = map[string]string{}
		}
		for _, kv := range settings {
			k, v, ok := strings.Cut(kv, "=")
			if !ok || k == "" {
				return fmt.Errorf("--setting %q: want key=value", kv)
			}
			req.Settings[k] = v
		}
		if *ingress != "" {
			req.Ingress = &admin.Ingress{Host: *ingress, TLS: *ingressTLS}
		}
		if *noIngress {
			req.Ingress = &admin.Ingress{}
		}
		ns, err := a.c.UpdateNamespace(a.ctx, pos[0], req)
		if err != nil {
			return err
		}
		return emit(ns, func() { printNamespace(ns) })
	}
	return fmt.Errorf("unknown namespace subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

// stringList is a repeatable string flag.
type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

// ---- domain ----

func (a *cli) domain(args []string) error {
	const u = "domain create <name> [--namespace ns] [--disabled] | list [--namespace ns] | get <name> | enable <name> | disable <name> | delete <name>"
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
		namespace := fs.String("namespace", "", "namespace the domain belongs to (default: the token's own, else default)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		req := admin.DomainRequest{Name: pos[0], Namespace: *namespace}
		if *disabled {
			req.Enabled = boolPtr(false)
		}
		d, err := a.c.CreateDomain(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(d, nil)
	case "list":
		fs := newFlagSet("domain list")
		namespace := fs.String("namespace", "", "only this namespace")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		names, err := a.c.ListDomainsAll(a.ctx, *namespace)
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
       mailbox purge <uuid>                    remove what the user already deleted (retention set) now instead of after the retention period
       mailbox set <uuid> [quota flags]                            (quota flags replace the whole quota)
       mailbox suspend <uuid> --reason text    soft: LMTP 450, the MTA queues; users locked out
       mailbox disable <uuid> --reason text    hard: LMTP 550, rejected at SMTP time; users locked out
       mailbox activate <uuid>
       mailbox hold <uuid> --reason text [--case ref] [--no-delivery] [--allow-send] [--sealed]
       mailbox release <uuid> [--note text]
       mailbox set <uuid> [--backup-interval 24h] [--backup-retention 2160h] [--backup-disabled|--backup-enabled]
                                               (backup settings; "" clears a field so it inherits again)`
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprintln(os.Stderr, "usage: kittiwake-admin "+u)
		return nil
	}
	switch args[0] {
	case "create":
		fs := newFlagSet("mailbox create")
		sb, hb, sm, hm := quotaFlags(fs)
		storageID := fs.Int64("storage", 0, "pin the Ceph storage by registry id (default: weighted by fresh capacity headroom among the namespace's eligible storage entries)")
		namespace := fs.String("namespace", "", "namespace the mailbox belongs to (default: the token's own, else default)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		req := admin.MailboxRequest{Quota: quotaFrom(*sb, *hb, *sm, *hm), StorageID: *storageID, Namespace: *namespace}
		m, err := a.c.CreateMailbox(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(m, nil)
	case "list":
		fs := newFlagSet("mailbox list")
		orphaned := fs.Bool("orphaned", false, "only mailboxes without an account (after account/domain removal; attach a new account to keep the data)")
		namespace := fs.String("namespace", "", "only this namespace")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		var items []admin.MailboxListItem
		var err error
		if *orphaned {
			items, err = a.c.ListOrphanedMailboxes(a.ctx, *namespace)
		} else {
			items, err = a.c.ListMailboxes(a.ctx, *namespace)
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
				rows = append(rows, []string{m.ID, m.Namespace, strconv.FormatInt(m.StorageID, 10), st, fmt.Sprint(m.OnHold), strings.Join(m.Accounts, ",")})
			}
			table([]string{"MAILBOX", "NAMESPACE", "STORAGE", "STATE", "HOLD", "ACCOUNTS"}, rows)
		})
	case "holds":
		fs := newFlagSet("mailbox holds")
		namespace := fs.String("namespace", "", "only this namespace")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		items, err := a.c.ListHolds(a.ctx, *namespace)
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
		bi := fs.String("backup-interval", "unset", "backup interval, e.g. 24h (\"\" inherits); lower values multiply the load")
		br := fs.String("backup-retention", "unset", "backup retention, e.g. 2160h (\"\" inherits)")
		bd := fs.Bool("backup-disabled", false, "switch backups off for this mailbox")
		be := fs.Bool("backup-enabled", false, "switch backups back on")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		req := admin.MailboxRequest{Quota: quotaFrom(*sb, *hb, *sm, *hm)}
		if *bi != "unset" || *br != "unset" || *bd || *be {
			b := &admin.BackupSettingsRequest{}
			if *bi != "unset" {
				b.Interval = bi
			}
			if *br != "unset" {
				b.Retention = br
			}
			if *bd || *be {
				v := *bd
				b.Disabled = &v
			}
			req.Backup = b
		}
		if req.Quota == nil && req.Backup == nil {
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
	case "purge":
		res, err := a.c.PurgeMailbox(a.ctx, id)
		if err != nil {
			return err
		}
		return emit(res, func() {
			fmt.Printf("purge scheduled for %s: %d retained messages (task %s)\n", id, res.Retained, res.TaskID)
		})
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
	}
	return fmt.Errorf("unknown mailbox subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

// ---- account ----

func (a *cli) account(args []string) error {
	const u = `account create --username u@d --mailbox <uuid> [--roles read,write,send] [--password-stdin] [--disabled]
       account list [--domain d] [--prefix p] | get <username|uuid> | delete <id>
       account set <id> [--roles r,..] [--mailbox uuid] [--enable|--disable]
       account password <id> set --password-stdin [--kind primary|application] [--comment text] [--expires DATE] [--plain]
                                   stdin starting with { or $ is stored as an existing hash ({SHA512-CRYPT}$6$..., $2y$..., {PLAIN}text)
       account password <id> list | expire <password-id> --expires DATE|never [--comment text] | remove <password-id>
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
			if domains, err = a.c.ListDomainsAll(a.ctx, ""); err != nil {
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
		const pu = `account password <account> set --password-stdin [--kind primary|application] [--comment text] [--expires DATE] [--plain]
       account password <account> list
       account password <account> expire <password-id> --expires DATE|never [--comment text]
       account password <account> remove <password-id>`
		if len(rest) == 0 || rest[0] == "help" {
			fmt.Println("usage: kittiwake-admin " + pu)
			return nil
		}
		switch rest[0] {
		case "set":
			fs := newFlagSet("account password set")
			kind := fs.String("kind", "primary", "primary or application")
			comment := fs.String("comment", "", "what the password is for (default: \"set by admin\", or \"migrated hash\" for a hash)")
			expires := fs.String("expires", "", "expiry: YYYY-MM-DD or RFC3339 (default: never)")
			pwStdin := fs.Bool("password-stdin", false, "read the password from stdin (required); a value starting with { or $ is taken as an existing hash ({SHA512-CRYPT}$6$..., $2y$..., {PLAIN}text)")
			plain := fs.Bool("plain", false, "treat the stdin value as a plain password even if it starts with { or $")
			if err := fs.Parse(rest[1:]); err != nil {
				return err
			}
			if !*pwStdin {
				return errors.New("--password-stdin is required (passwords are never taken from arguments)")
			}
			var exp *time.Time
			if *expires != "" {
				t, err := parseDate(*expires)
				if err != nil {
					return fmt.Errorf("--expires: %w", err)
				}
				exp = &t
			}
			secret, err := readPasswordStdin()
			if err != nil {
				return err
			}
			req := admin.PasswordRequest{Kind: *kind, Comment: *comment, ExpiresAt: exp}
			if !*plain && (strings.HasPrefix(secret, "{") || strings.HasPrefix(secret, "$")) {
				req.Hash = secret
			} else {
				req.Password = secret
				if req.Comment == "" {
					req.Comment = "set by admin"
				}
			}
			ps, err := a.c.SetPassword(a.ctx, id, req)
			if err != nil {
				return err
			}
			return emit(ps, nil)
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
					rows = append(rows, []string{p.ID, p.Kind, p.Scheme, p.Comment, exp, fmtTime(p.Created)})
				}
				table([]string{"ID", "KIND", "SCHEME", "COMMENT", "EXPIRES", "CREATED"}, rows)
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
		return fmt.Errorf("unknown password subcommand %q\nusage: kittiwake-admin %s", rest[0], pu)
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
		namespace := fs.String("namespace", "", "bind the token to one namespace (a tenant admin); default: the whole platform")
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
		tk, err := a.c.CreateAdminToken(a.ctx, admin.AdminTokenRequest{ReadOnly: *readOnly, Namespace: *namespace, ExpiresAt: exp, Comment: *comment})
		if err != nil {
			return err
		}
		return emit(tk, func() {
			scope := "platform"
			if tk.Token.Namespace != "" {
				scope = "namespace " + tk.Token.Namespace
			}
			fmt.Printf("token id:     %s (%s, %s, expires %s)\n", tk.Token.ID, strings.Join(tk.Token.Roles, ","), scope, tk.Token.ExpiresAt.Format(time.RFC3339))
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

func (a *cli) storage(args []string) error {
	const u = `storage add --name text --mon-host "[addr]:6789,..." --user cephx-user --key-stdin|--keyring file --pools messages,attachments,index [--no-placement]
       storage list | get <id>
       storage set <id> [--name text] [--mon-host ...] [--user ...] [--key-stdin|--keyring file] [--pools m,a,i]
       storage enable <id> | disable <id>      placement of new mailboxes (existing ones keep working)
       storage remove <id>                     only when no mailbox refers to it`
	if len(args) == 0 || args[0] == "help" {
		fmt.Println("usage: kittiwake-admin " + u)
		return nil
	}
	storageFlags := func(fs *flag.FlagSet) (name, mon, user, pools *string, keyStdin *bool, keyring *string) {
		name = fs.String("name", "", "storage name (unique)")
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
	buildReq := func(name, mon, user, pools string, key string) admin.StorageRequest {
		req := admin.StorageRequest{Name: name, MonHost: mon, User: user, Key: key}
		if pools != "" {
			p := splitList(pools)
			if len(p) == 3 {
				req.Pools = &admin.Pools{Messages: p[0], Attachments: p[1], Index: p[2]}
			}
		}
		return req
	}
	printStorage := func(c admin.Storage) {
		fmt.Printf("id:        %d\nname:      %s\nnamespace: %s\nfsid:      %s\nmon_host:  %s\nuser:      %s\npools:     %s, %s, %s\nplacement: %v\n",
			c.ID, c.Name, c.Namespace, c.FSID, c.MonHost, c.User, c.Pools.Messages, c.Pools.Attachments, c.Pools.Index, c.Placement)
	}
	switch args[0] {
	case "add":
		fs := newFlagSet("storage add")
		name, mon, user, pools, keyStdin, keyring := storageFlags(fs)
		noPlacement := fs.Bool("no-placement", false, "register without accepting new mailboxes")
		namespace := fs.String("namespace", "", "namespace that owns the storage (default: default)")
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
		req.Namespace = *namespace
		if req.Pools == nil {
			return errors.New("--pools needs exactly three names: messages,attachments,index")
		}
		if *noPlacement {
			req.Placement = boolPtr(false)
		}
		c, err := a.c.AddStorage(a.ctx, req)
		if err != nil {
			return err
		}
		return emit(c, func() { printStorage(c) })
	case "list":
		list, err := a.c.ListStorage(a.ctx)
		if err != nil {
			return err
		}
		if list == nil {
			list = []admin.Storage{}
		}
		return emit(list, func() {
			var rows [][]string
			for _, c := range list {
				rows = append(rows, []string{strconv.FormatInt(c.ID, 10), c.Name, c.Namespace, c.FSID, c.MonHost, fmt.Sprint(c.Placement)})
			}
			table([]string{"ID", "NAME", "NAMESPACE", "FSID", "MON_HOST", "PLACEMENT"}, rows)
		})
	case "get", "set", "enable", "disable", "remove":
		if len(args) < 2 {
			return fmt.Errorf("usage: kittiwake-admin storage %s <id>", args[0])
		}
		id, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return fmt.Errorf("storage id %q is not a number", args[1])
		}
		switch args[0] {
		case "get":
			c, err := a.c.GetStorage(a.ctx, id)
			if err != nil {
				return err
			}
			return emit(c, func() { printStorage(c) })
		case "set":
			fs := newFlagSet("storage set")
			name, mon, user, pools, keyStdin, keyring := storageFlags(fs)
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
			c, err := a.c.UpdateStorage(a.ctx, id, req)
			if err != nil {
				return err
			}
			return emit(c, func() { printStorage(c) })
		case "enable", "disable":
			c, err := a.c.UpdateStorage(a.ctx, id, admin.StorageRequest{Placement: boolPtr(args[0] == "enable")})
			if err != nil {
				return err
			}
			return emit(c, func() { fmt.Printf("storage %d (%s): placement %v\n", c.ID, c.Name, c.Placement) })
		case "remove":
			if err := a.c.DeleteStorage(a.ctx, id); err != nil {
				return err
			}
			return ok(fmt.Sprintf("removed storage %d", id))
		}
	}
	return fmt.Errorf("unknown storage subcommand %q\nusage: kittiwake-admin %s", args[0], u)
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
			nss := "all"
			if len(n.Namespaces) > 0 {
				nss = strings.Join(n.Namespaces, ",")
			}
			rows = append(rows, []string{n.Mode, n.Address, n.Host, n.Version, nss, state, fmtTime(n.Started), fmtTime(n.LastSeen)})
		}
		table([]string{"MODE", "ADDRESS", "HOST", "VERSION", "NAMESPACES", "STATE", "STARTED", "LAST SEEN"}, rows)
	})
}

// ---- backup ----

func (a *cli) backup(args []string) error {
	const u = `backup status                                   platform-wide summary and policy
       backup show <mailbox>                           schedule state, settings and runs of a mailbox
       backup run <mailbox> [--full]                   back the mailbox up now (full: rewrite every folder)
       backup restore <mailbox> <run>                  restore the run into a NEW mailbox (job)
       backup restore-message <mailbox> <run> <message> [--into mailbox]
       backup export <mailbox> <run>                   Maildir++ tar.gz on the backup target (job; download link when S3)
       backup verify <mailbox>                         check the mailbox's backups on the target (job)
       backup forget <mailbox>                         delete every backup of the mailbox (job)
       backup jobs [--mailbox uuid] [--kind k] | job <id>`
	if len(args) == 0 || args[0] == "help" {
		fmt.Fprintln(os.Stderr, "usage: kittiwake-admin "+u)
		return nil
	}
	jobOut := func(j admin.BackupJob, err error) error {
		if err != nil {
			return err
		}
		return emit(j, func() {
			fmt.Printf("%s job %s %s; poll: kittiwake-admin backup job %s\n", j.Kind, j.ID, j.State, j.ID)
		})
	}
	switch args[0] {
	case "status":
		st, err := a.c.BackupStatus(a.ctx)
		if err != nil {
			return err
		}
		return emit(st, func() {
			s := st.Summary
			fmt.Printf("mailboxes %d  scheduled %d  disabled %d  overdue %d  failed %d  verify-failed %d\n", s.Mailboxes, s.Scheduled, s.Disabled, s.Overdue, s.Failed, s.VerifyFailed)
			fmt.Printf("runs %d  bytes %d  blobs %d  jobs pending %d  configured load %.1f runs/hour\n", s.Runs, s.Bytes, s.Blobs, s.Jobs, s.Rate)
			if !s.OldestDue.IsZero() {
				fmt.Printf("oldest overdue: %s\n", fmtTime(s.OldestDue))
			}
			fmt.Printf("policy: interval %s (min %s, max %s), retention %s, maintain %s, export expiry %s\n",
				st.Policy["default_interval"], st.Policy["min_interval"], st.Policy["max_interval"], st.Policy["default_retention"], st.Policy["maintain_interval"], st.Policy["export_expiry"])
		})
	case "show":
		pos, _, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		mb, err := a.c.MailboxBackups(a.ctx, pos[0])
		if err != nil {
			return err
		}
		return emit(mb, func() {
			st := mb.State
			fmt.Printf("policy: interval %s retention %s enabled %v\n", mb.Policy.Interval, mb.Policy.Retention, mb.Policy.Enabled)
			fmt.Printf("state: scheduled %v due %s status %s last run %s at %s modseq %d\n", st.Scheduled, fmtTime(st.Due), st.Status, st.LastRun, fmtTime(st.LastAt), st.LastModSeq)
			if st.Error != "" {
				fmt.Printf("error: %s\n", st.Error)
			}
			if !st.Verified.IsZero() {
				fmt.Printf("verified: %s %s\n", fmtTime(st.Verified), st.VerifyError)
			}
			var rows [][]string
			for _, r := range mb.Runs {
				rows = append(rows, []string{r.ID, fmtTime(r.Finished), strconv.Itoa(r.Messages), strconv.Itoa(r.NewMessages), strconv.FormatInt(r.Bytes, 10), strconv.Itoa(len(r.Packs)), fmt.Sprint(r.Full), fmt.Sprint(r.Fuzzy), fmtTime(r.Expires)})
			}
			table([]string{"RUN", "FINISHED", "MESSAGES", "NEW", "BYTES", "PACKS", "FULL", "FUZZY", "EXPIRES"}, rows)
		})
	case "run":
		pos, rest, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		fs := newFlagSet("backup run")
		full := fs.Bool("full", false, "rewrite every folder entry instead of reusing unchanged ones")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		s, err := a.c.RunBackup(a.ctx, pos[0], *full)
		if err != nil {
			return err
		}
		return emit(s, func() {
			fmt.Printf("backup scheduled, task %s; poll: kittiwake-admin backup show %s\n", s.TaskID, pos[0])
		})
	case "restore":
		pos, _, err := splitArgs(args[1:], 2, u)
		if err != nil {
			return err
		}
		return jobOut(a.c.RestoreBackup(a.ctx, pos[0], pos[1]))
	case "restore-message":
		pos, rest, err := splitArgs(args[1:], 3, u)
		if err != nil {
			return err
		}
		fs := newFlagSet("backup restore-message")
		into := fs.String("into", "", "mailbox to restore into (default: the source mailbox)")
		if err := fs.Parse(rest); err != nil {
			return err
		}
		return jobOut(a.c.RestoreBackupMessage(a.ctx, pos[0], pos[1], pos[2], *into))
	case "export":
		pos, _, err := splitArgs(args[1:], 2, u)
		if err != nil {
			return err
		}
		return jobOut(a.c.ExportBackup(a.ctx, pos[0], pos[1]))
	case "verify":
		pos, _, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		return jobOut(a.c.VerifyBackups(a.ctx, pos[0]))
	case "forget":
		pos, _, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		return jobOut(a.c.ForgetBackups(a.ctx, pos[0]))
	case "jobs":
		fs := newFlagSet("backup jobs")
		mb := fs.String("mailbox", "", "only this mailbox")
		kind := fs.String("kind", "", "restore | restore-message | export | verify | forget")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		jobs, err := a.c.ListBackupJobs(a.ctx, *mb, *kind)
		if err != nil {
			return err
		}
		if jobs == nil {
			jobs = []admin.BackupJob{}
		}
		return emit(jobs, func() {
			var rows [][]string
			for _, j := range jobs {
				rows = append(rows, []string{j.ID, j.Kind, j.Mailbox, j.RunID, j.State, fmtTime(j.Created), firstNonEmpty(j.Target, j.ObjectKey), j.Error})
			}
			table([]string{"ID", "KIND", "MAILBOX", "RUN", "STATE", "CREATED", "RESULT", "ERROR"}, rows)
		})
	case "job":
		pos, _, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		j, err := a.c.GetBackupJob(a.ctx, pos[0])
		if err != nil {
			return err
		}
		return emit(j, func() {
			fmt.Printf("%s %s %s mailbox %s run %s requested by %s at %s\n", j.Kind, j.ID, j.State, j.Mailbox, j.RunID, j.Requested, fmtTime(j.Created))
			if j.Target != "" {
				fmt.Printf("mailbox: %s (%d messages)\n", j.Target, j.Messages)
			}
			if j.ObjectKey != "" {
				fmt.Printf("archive: %s (%d bytes, %d messages, expires %s)\n", j.ObjectKey, j.Bytes, j.Messages, fmtTime(j.Expires))
			}
			if j.URL != "" {
				fmt.Printf("download: %s\n", j.URL)
			}
			if j.Error != "" {
				fmt.Printf("error: %s\n", j.Error)
			}
			if len(j.Detail) > 0 {
				fmt.Printf("detail: %s\n", string(j.Detail))
			}
		})
	}
	return fmt.Errorf("unknown backup subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

// proxyKey manages the keys of proxy sites that verify passwords
// themselves (README "Proxies"). create generates the key pair here
// unless --public-key hands in one generated elsewhere; the private key
// is printed once, for the proxy's proxy.handoff_key.
func (a *cli) proxyKey(args []string) error {
	const u = `proxy-key create <name> [--public-key base64]   register a site; without --public-key a key pair is generated and the private key printed once
       proxy-key list
       proxy-key delete <id>
       proxy-key generate                              print a key pair without registering it (register the public key with create --public-key)`
	if len(args) == 0 || args[0] == "help" {
		fmt.Println("usage: kittiwake-admin " + u)
		return nil
	}
	switch args[0] {
	case "generate":
		priv, err := cryptoauth.NewHandoffKey()
		if err != nil {
			return err
		}
		pub := priv.Public().(ed25519.PublicKey)
		out := map[string]string{"id": cryptoauth.HandoffKeyID(pub), "public_key": cryptoauth.EncodeHandoffPublic(pub), "private_key": cryptoauth.EncodeHandoffPrivate(priv)}
		return emit(out, func() {
			fmt.Printf("id:          %s\npublic key:  %s\nprivate key: %s\n", out["id"], out["public_key"], out["private_key"])
			fmt.Println("Put the private key in the proxy's proxy.handoff_key and register the public key with proxy-key create --public-key.")
		})
	case "create":
		fs := newFlagSet("proxy-key create")
		pubText := fs.String("public-key", "", "base64 Ed25519 public key generated on the proxy (default: generate a pair here)")
		pos, rest, err := splitArgs(args[1:], 1, u)
		if err != nil {
			return err
		}
		if err := fs.Parse(rest); err != nil {
			return err
		}
		var priv ed25519.PrivateKey
		if *pubText == "" {
			if priv, err = cryptoauth.NewHandoffKey(); err != nil {
				return err
			}
			*pubText = cryptoauth.EncodeHandoffPublic(priv.Public().(ed25519.PublicKey))
		}
		k, err := a.c.CreateProxyKey(a.ctx, admin.ProxyKeyRequest{Name: pos[0], PublicKey: *pubText})
		if err != nil {
			return err
		}
		type created struct {
			admin.ProxyKey
			PrivateKey string `json:"private_key,omitempty"`
		}
		out := created{ProxyKey: k}
		if priv != nil {
			out.PrivateKey = cryptoauth.EncodeHandoffPrivate(priv)
		}
		return emit(out, func() {
			fmt.Printf("id:          %s\nname:        %s\npublic key:  %s\n", k.ID, k.Name, k.PublicKey)
			if out.PrivateKey != "" {
				fmt.Printf("private key: %s\n", out.PrivateKey)
				fmt.Println("The private key is shown once; put it in the proxy's proxy.handoff_key.")
			}
		})
	case "list":
		list, err := a.c.ListProxyKeys(a.ctx)
		if err != nil {
			return err
		}
		return emit(list, func() {
			var rows [][]string
			for _, k := range list {
				rows = append(rows, []string{k.ID, k.Name, k.PublicKey, fmtTime(k.Created)})
			}
			table([]string{"ID", "NAME", "PUBLIC KEY", "CREATED"}, rows)
		})
	case "delete":
		if len(args) != 2 {
			return errors.New("usage: kittiwake-admin proxy-key delete <id>")
		}
		if err := a.c.DeleteProxyKey(a.ctx, args[1]); err != nil {
			return err
		}
		return ok("removed proxy key " + args[1])
	}
	return fmt.Errorf("unknown proxy-key subcommand %q\nusage: kittiwake-admin %s", args[0], u)
}

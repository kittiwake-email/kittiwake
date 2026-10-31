// Command kittiwake-admin administers a Kittiwake platform through its admin
// API. This is where operators work; the local `kittiwake admin` command
// only bootstraps the first admin token.
//
//	kittiwake-admin [-u URL] [-t TOKEN] [-o plain|json] <command> [subcommand] [flags] [args]
//
// Connection settings in order of precedence: flags, environment
// (KITTIWAKE_ADMIN_URL, KITTIWAKE_ADMIN_TOKEN), config file
// ~/.config/kittiwake/admin.yaml (url:, token:).
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"kittiwake/client/admin"
)

// Version is set at build time.
var Version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		var ae *admin.APIError
		if errors.As(err, &ae) {
			fmt.Fprintf(os.Stderr, "error: %d %s\n", ae.Status, strings.ReplaceAll(firstNonEmpty(ae.Detail, ae.Title), "\n", ": "))
		} else {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
		os.Exit(1)
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: kittiwake-admin [flags] <command> [subcommand] [args]

flags:
  -u, --url URL        admin API base URL (env KITTIWAKE_ADMIN_URL; default http://[::1]:8081)
  -t, --token TOKEN    admin API token (env KITTIWAKE_ADMIN_TOKEN)
  -o, --output FORMAT  plain (default) or json
  -v, --version        print the version
  config file: ~/.config/kittiwake/admin.yaml with "url:" and "token:" (env KITTIWAKE_ADMIN_CONFIG overrides the path)

commands:
  status                              version and time of the admin API
  namespace create|list|get|set|enable|disable|delete   tenant namespaces (platform admins)
  domain    create|list|get|enable|disable|delete
  mailbox   create|list|inspect|set|suspend|disable|activate|delete|recalc|purge|hold|release|holds
  backup    status|show|run|restore|restore-message|export|verify|forget|jobs|job
  account   create|list|get|set|delete|password (set|list|expire|remove)|totp-disable|sessions|revoke-session|token|tokens
  alias     create|list|get|set|delete
  audit     query the audit log
  queue     stats|dead|retry
  token     create|list|delete     admin tokens (full or --read-only; --namespace binds a tenant admin)
  proxy-key create|list|delete|generate   keys of proxy sites that verify passwords themselves
  storage   add|list|get|set|enable|disable|remove   Ceph storage registry (placement of new mailboxes)
  node      list [--mode m]       running daemon nodes (registered with heartbeats)
  keys      rotate
  identity  export|import
  openapi   fetch the OpenAPI spec
  version
Run "kittiwake-admin <command> help" for the subcommands and their flags.`)
}

type cli struct {
	c   *admin.Client
	ctx context.Context
}

func run(args []string) error {
	fs := flag.NewFlagSet("kittiwake-admin", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var url, token string
	fs.StringVar(&url, "url", "", "")
	fs.StringVar(&url, "u", "", "")
	fs.StringVar(&token, "token", "", "")
	fs.StringVar(&token, "t", "", "")
	of := outputFlag(fs)
	ver := fs.Bool("version", false, "")
	fs.BoolVar(ver, "v", false, "")
	if err := fs.Parse(args); err != nil {
		usage()
		return err
	}
	if err := setOutput(*of); err != nil {
		return err
	}
	rest := fs.Args()
	if *ver || (len(rest) > 0 && rest[0] == "version") {
		return emit(map[string]string{"version": Version}, func() { fmt.Println("kittiwake-admin", Version) })
	}
	if len(rest) == 0 || rest[0] == "help" || rest[0] == "-h" || rest[0] == "--help" {
		usage()
		return nil
	}
	s, err := resolve(url, token, os.Getenv, configPath())
	if err != nil {
		return err
	}
	offline := rest[0] == "openapi" || (rest[0] == "proxy-key" && len(rest) > 1 && rest[1] == "generate")
	if s.Token == "" && !offline {
		return errors.New("no admin token: use -t, KITTIWAKE_ADMIN_TOKEN or ~/.config/kittiwake/admin.yaml")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	app := &cli{c: admin.New(s.URL, s.Token, admin.WithUserAgent("kittiwake-admin/"+Version)), ctx: ctx}
	cmd, sub := rest[0], rest[1:]
	switch cmd {
	case "status":
		st, err := app.c.Status(ctx)
		if err != nil {
			return err
		}
		return emit(st, func() {
			fmt.Printf("version: %s\ntime:    %s\n", st.Version, st.Time.Format(time.RFC3339))
			if st.Schema != nil {
				fmt.Printf("schema:  %d (applied by %s on %s)\n", st.Schema.Version, st.Schema.AppliedBy, st.Schema.AppliedAt.Format(time.RFC3339))
			}
		})
	case "namespace":
		return app.namespace(sub)
	case "domain":
		return app.domain(sub)
	case "mailbox":
		return app.mailbox(sub)
	case "account":
		return app.account(sub)
	case "alias":
		return app.alias(sub)
	case "audit":
		return app.audit(sub)
	case "queue":
		return app.queue(sub)
	case "token":
		return app.token(sub)
	case "proxy-key":
		return app.proxyKey(sub)
	case "storage":
		return app.storage(sub)
	case "node":
		return app.node(sub)
	case "keys":
		return app.keys(sub)
	case "backup":
		return app.backup(sub)
	case "identity":
		return app.identity(sub)
	case "openapi":
		return app.openapi(sub)
	}
	usage()
	return fmt.Errorf("unknown command %q", cmd)
}

// ---- helpers ----

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	return fs
}

// splitArgs takes n positional arguments before the flags (e.g. "hold
// <uuid> --reason x") and returns them with the remaining args.
func splitArgs(args []string, n int, usage string) ([]string, []string, error) {
	if len(args) < n {
		return nil, nil, errors.New("usage: " + usage)
	}
	for _, a := range args[:n] {
		if strings.HasPrefix(a, "-") {
			return nil, nil, errors.New("usage: " + usage)
		}
	}
	return args[:n], args[n:], nil
}

func readPasswordStdin() (string, error) {
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		return "", errors.New("empty password on stdin")
	}
	return line, nil
}

func boolPtr(b bool) *bool { return &b }

func enableFlags(fs *flag.FlagSet) (enable, disable *bool) {
	enable = fs.Bool("enable", false, "enable")
	disable = fs.Bool("disable", false, "disable")
	return
}

func enabledFrom(enable, disable bool) (*bool, error) {
	switch {
	case enable && disable:
		return nil, errors.New("--enable and --disable are exclusive")
	case enable:
		return boolPtr(true), nil
	case disable:
		return boolPtr(false), nil
	}
	return nil, nil
}

func splitList(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	var out []string
	for _, x := range strings.Split(s, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

func parseDate(s string) (time.Time, error) {
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func ok(what string) error {
	return emit(map[string]any{"ok": true, "result": what}, func() { fmt.Println(what) })
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}

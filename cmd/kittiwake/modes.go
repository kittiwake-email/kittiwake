package main

import (
	"context"
	"fmt"
	"os"

	"kittiwake/app"
	"kittiwake/proto/imap"
	"kittiwake/proto/pop3"
	"kittiwake/proto/submission"
)

// protocolModes registers the IMAP, POP3 and submission servers with the app.
func protocolModes() []app.ModeFunc {
	return []app.ModeFunc{imapMode, pop3Mode, submissionMode}
}

func submissionMode(ctx context.Context, a *app.App, start func(string, func() error)) error {
	if !a.Cfg.HasMode("submission") {
		return nil
	}
	host, _ := os.Hostname()
	srv := submission.New(submission.Config{
		Hostname: host, MaxMessageSize: a.Cfg.Mail.MaxMessageSize,
		SaveToSent: a.Cfg.Submission.SaveToSent, RequireTLSProxy: a.Cfg.Submission.RequireTLSProxy,
	}, a.Services.Auth, a.Services.Submit, a.Log)
	ln, err := app.Listen(a.Cfg.Listen.Submission)
	if err != nil {
		return fmt.Errorf("submission listen: %w", err)
	}
	a.AddCloser(srv.Shutdown)
	a.MarkHandled("submission")
	start("submission", func() error { return srv.Serve(ln) })
	return nil
}

func pop3Mode(ctx context.Context, a *app.App, start func(string, func() error)) error {
	if !a.Cfg.HasMode("pop3") {
		return nil
	}
	host, _ := os.Hostname()
	srv := pop3.New(pop3.Config{Hostname: host}, a.Services.Auth, a.Services.Mailbox, a.Log)
	ln, err := app.Listen(a.Cfg.Listen.POP3)
	if err != nil {
		return fmt.Errorf("pop3 listen: %w", err)
	}
	a.AddCloser(srv.Shutdown)
	a.MarkHandled("pop3")
	start("pop3", func() error { return srv.Serve(ln) })
	return nil
}

func imapMode(ctx context.Context, a *app.App, start func(string, func() error)) error {
	if !a.Cfg.HasMode("imap") {
		return nil
	}
	host, _ := os.Hostname()
	srv := imap.New(imap.Config{
		Hostname: host, MaxLiteralSize: a.Cfg.Mail.MaxMessageSize,
		CompatPrefix: a.Cfg.IMAP.CompatNamespace.Prefix, CompatDelim: a.Cfg.IMAP.CompatNamespace.Delimiter,
	}, a.Services.Auth, a.Services.Mailbox, a.Services.Delivery, a.Log)
	ln, err := app.Listen(a.Cfg.Listen.IMAP)
	if err != nil {
		return fmt.Errorf("imap listen: %w", err)
	}
	a.AddCloser(srv.Shutdown)
	a.MarkHandled("imap")
	start("imap", func() error { return srv.Serve(ln) })
	return nil
}

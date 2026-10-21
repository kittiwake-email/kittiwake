# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

`README.md` is the authoritative design document, agreed with the owner; read it before
designing anything. Milestone 1 is implemented (see Roadmap in README): identity store,
admin service/CLI, LMTP, Postfix socketmap, sessions/TOTP, JMAP, REST. Not yet: IMAP, POP3,
workers (purge/GC/reconcile), EmailSubmission, search backend, DAV.

## Commands

```
make build            # bin/dovetail (cgo; needs librados-dev, installed on the dev box, Ceph 20.x)
make test             # go test ./...   (all unit tests run on the in-memory RADOS; no Ceph needed)
make test-race
make lint             # go vet + gofmt check
go test ./store/index -run TestFolders          # one test
go test ./mail -run TestRoundTrip -v
make run-dev          # whole platform in one process on the memory backend with a seeded account
bin/dovetail check-config --config examples/dovetail.yaml
bin/dovetail admin --config <cfg> bootstrap --username root@example.org --password-stdin
```

Never run `go mod tidy` blindly: go-ceph pulls cgo-only packages; `go get <mod>@<ver>` for
new deps. `go build ./...` must stay green with cgo (librados backend compiles even without a
cluster).

## Layout

```
rados/        RADOS interface (Get/Write tx/omap/xattr/lock/watch), librados.go + memory.go
config/       YAML config with defaults + validation (config.Default())
mail/         MIME parser → Document JSON, attachment extraction, raw skeleton + Reconstruct
auth/         crypto primitives: password schemes, TOTP, Ed25519 JWT, refresh tokens
store/        identity, index, message, attach, audit, session, queue (object layouts live here)
service/      actor (ctx principal), auditor, auth, delivery, mailbox, admin (business logic +
              authorisation + audit; protocol adapters call ONLY these)
proto/        proxyproto, httpx (auth middleware), lmtp, socketmap, jmap, rest
app/          wiring config→client→stores→services→listeners; metrics/ Prometheus registry
cmd/dovetail  serve | check-config | admin ... | version
examples/     dovetail.yaml (full, documented), dev.yaml (memory backend)
contrib/postfix  main.cf snippet
api/openapi.yaml REST spec (route↔spec consistency is tested)
```

Conventions: every store/service test builds a fixture on `rados.NewMemoryClient()`; services
take an `actor` from the context and refuse without one; errors are package sentinels
(`ErrNotFound`, `ErrForbidden`, ...) compared with `errors.Is`; the service layer maps store
errors, protocol adapters map service errors to wire codes (`delivery.Status`, httpx).

## What this is

Dovetail: a massively scalable mail platform intended to replace Dovecot (the name is the
joint that fits onto Postfix, and the successor of Dovecot). Go module path: `dovetail`.
Binary name: `dovetail`. Not an MTA: Postfix stays in front (LMTP in, SMTP relay out).

## Architectural decisions

These are settled. Do not design around them.

- **Library first, daemon second.** Layers: `rados` interface (librados + in-memory mock) →
  stores (identity, index, messages, attachments, changelog, audit, sessions, queue) →
  services as Go interfaces (`Auth`, `Delivery`, `Mailbox`, `Submit`, `Admin`, `Audit`,
  `Notify`) → protocol adapters (LMTP, IMAP, POP3, JMAP, REST, socketmap, CLI) →
  `cmd/dovetail`. A protocol adapter never touches a store or RADOS directly. Authorisation
  (roles, enabled flags, hold) and audit happen in the services, once, via a context that
  carries actor, client IP and protocol. Each layer is tested against the mock of the layer
  below. The service interfaces are the project's public Go API.

- **One Go binary (`dovetail`), multiple modes:** `lmtp`, `imap`, `pop3`, `api`, `postfix`,
  `worker` (+ `dav` later), plus an `admin` subcommand that talks to RADOS directly
  (bootstrap, export/import, reconcile). Modes combine in one process or split across many.
- **Everything in Ceph RADOS. No SQL, no Redis.** Stateless daemons via librados (cgo; target
  latest stable Ceph). Four pools: `messages`, `attachments`, `index`, `meta`. Pure RADOS,
  not S3/RGW. The RGW metadata approach is the model: key lookups are direct object reads,
  uniqueness via exclusive create, listings via omap, reconciliation repairs listings.
- **Identity store (meta pool, RADOS namespaces):** `domains/<name>`, `mailboxes/<uuid>`,
  `accounts/<uuid>`, `names/<username>` → account uuid, `aliases/<address>` (catch-all is
  `@<domain>`), `tokens/<sha256>`. Parent objects' omap hold listings. Multi-object writes are
  primary-first and idempotent; updates are conditional on a version xattr. Behind a Go
  interface; RADOS is the only implementation.
- **Admin API is the only way identity data is created/changed** (plus the `admin` CLI).
  Self-service endpoints let accounts manage their own passwords, app passwords, TOTP,
  tokens and sessions. All of it audited.
- **Postfix integration:** `postfix` mode serves a Sendmail-style socketmap (`domains`,
  `mailboxes`, `aliases` maps) for `virtual_mailbox_domains/maps` and `virtual_alias_maps`.
  Postfix config and helpers live in `contrib/postfix/`.
- **Message format:** parsed JSON (schema version, headers array, verbatim original headers,
  HTML and plain-text body, full MIME tree, attachment references) plus a **raw skeleton**:
  the original bytes with each attachment's encoded body replaced by a reference.
  Reconstruction must be byte-exact; verify at delivery and fall back to full raw if not.
  Every non-primary-body MIME part is an attachment; parts under a threshold stay inline.
- **Attachments** are cluster-wide deduplicated by content hash with reference counting
  (omap keys per referencing message on the hash-named object); only a worker deletes orphans.
- **Delivery order:** attachments, message JSON, raw, index entry + change log (commit
  point), refs. LMTP acks only after the index write.
- **Mailbox index lives in RADOS omap:** one root object per mailbox (typed collections,
  UIDVALIDITY/UIDNEXT per folder, mailbox-wide `modseq`, quota usage; atomic transactions)
  and one index object per folder (omap key = zero-padded UID, value = compact record with
  object name, flags+keywords, size, date, modseq, threading headers, list-view headers).
- **Change tracking:** every mutation bumps `modseq` and appends to the bounded change log
  `<mailbox>/changes`. Needed by JMAP `/changes` and later IMAP CONDSTORE/QRESYNC.
- **Workers** consume a RADOS-based task queue (queue object per task type, omap entries,
  RADOS lock lease, run-after, retry with backoff, dead-letter set). One worker holds a
  RADOS lock as scheduler for periodic tasks. Reconciliation reports/repairs, never deletes
  mailboxes on its own.
- **Full-text search** is a future, pluggable backend behind an interface; publish
  index/remove events on every mutation from day one.
- **Protocols:** LMTP inbound (one recipient per transaction; aliases may fan out), **JMAP is
  the mailbox API** (RFC 8620/8621, plus 8887 WebSocket, 9425 Quotas, 9404 Blob; later 9610
  Contacts and Calendars). No custom mailbox REST API. A small custom REST surface (OpenAPI
  spec in repo) covers only OAuth/session/self-service endpoints and the admin API. IMAP4rev1
  + IDLE, UIDPLUS, MOVE; POP3 INBOX only. The JMAP server layer is written in this repo.
- **Identity:** mailbox = UUID that owns typed collections. Accounts link to a mailbox
  (many-to-one), have roles (`read`, `write`, `send`, `admin`), passwords marked `primary`
  or `application` (Dovecot-style scheme prefixes; argon2id for new ones), optional TOTP
  secret, and API tokens (stored as SHA-256, prefixed `dvt_`, expiry and comment mandatory,
  may only narrow the account's roles). Admin accounts have `admin` and no mailbox.
- **Sessions (OAuth 2.0 shaped, RFC 9700):** `POST /v1/oauth/token` with password gives a
  15-minute Ed25519-signed JWT access token (verified locally, no lookup) plus an opaque
  refresh token stored hashed in RADOS (`sessions/<account-id>`), rotated on every use with
  reuse detection. `POST /v1/oauth/revoke` (RFC 7009). Signing keys in RADOS (`keys/jwt`),
  rotated. Keep the endpoint OAuth-standard so PKCE/OIDC can be added later.
- **TOTP (RFC 6238):** with a secret present, primary passwords need a code (`totp_code` at
  the token endpoint, error `totp_required` when missing) and are rejected on IMAP/POP3;
  application passwords never need a code. Replay state in RADOS `totp/<account-id>`.
  IMAP/POP3 also accept OAUTHBEARER/XOAUTH2 with a session access token.
- **Sending:** `From` must be one of the account's identities (own address + aliases to its
  mailbox). Sent copy goes to Sent. Relay failure = API error, nothing stored.
- **Domains, aliases, catch-all** are first-class. LMTP resolution: exact address → alias
  (bounded depth, may fan out) → catch-all → reject. Same answer served to Postfix.
- **Quotas:** soft/hard limits on size and count stored on the mailbox object; usage in the
  index root; hard limit rejects at LMTP (5.2.2 default, configurable temp).
- **Retention:** deleted mail goes to Trash and is purged by a worker after a configurable
  period. IMAP EXPUNGE / POP3 DELE are lazy: the message leaves the index but is kept in a
  retention set for the same period. Nothing is destroyed immediately.
- **Spam routing:** a configurable header/value from the MTA files the message into Junk at
  LMTP time. No Sieve-style user rules.
- **Legal hold:** a mailbox on hold (record on the mailbox object: reason, case ref, who,
  when, options) refuses every user mutation on every protocol, suspends all purges and
  deletion, keeps accepting delivery by default, refuses sending by default, optionally
  `sealed` (no user access). Enforced in the `Mailbox` service. Release is audited and
  purges resume with a grace period.
- **Maildir export/import:** byte-exact messages in Dovecot Maildir++ layout (flags in
  filename, `dovecot-keywords`, `dovecot-uidlist`, mtime = internal date) plus
  `manifest.json` with SHA-256 per file and the audit log, as a tar. CLI writes to a path;
  admin API runs it as a worker task into the export directory. Import is the Dovecot
  migration path.
- **Auditing:** every mailbox action and every identity change (who, real client IP,
  protocol, action, target, result) goes to the structured log and to per-mailbox per-day
  audit objects (`<mailbox>/audit/<date>`, plus `audit/admin/<date>`), queryable via the
  admin API. Audit is not optional; plumb an audit context through every handler.
- **Real client IP:** HAProxy sends PROXY protocol on every connection, so all listeners
  accept PROXY protocol v1/v2, optionally required. `X-Forwarded-For` is only an HTTP
  fallback. Never log the proxy's address as the client.
- **Brute-force protection** is HAProxy's job; just log failed logins with the client address.
- **IMAP IDLE / JMAP push** use RADOS watch/notify on the mailbox root object.
- **CalDAV/CardDAV and JMAP Contacts/Calendars come later.** Object prefixes
  `<mailbox>/cal/` and `<mailbox>/card/` are reserved; do not design anything mail-specific
  into the mailbox root or quota/audit code.
- **No TLS in the daemon** (HAProxy terminates it). **IPv6-first.** Single YAML config file.
- **Observability:** structured JSON logging and Prometheus metrics in every component;
  `/metrics`, `/healthz`, `/readyz` on a management listener.

## Roadmap

Milestone 1: config loading, identity store + `admin bootstrap`, admin API CRUD for
domains/mailboxes/accounts/aliases/tokens, Postfix socketmap, LMTP delivery into RADOS
(parse, attachments, index, change log), sessions/TOTP, JMAP read side (session resource,
Mailbox/get, Email/get, Email/query, Email/changes, blob download). Then JMAP write side and
push, self-service endpoints, IMAP/POP3, workers, remaining admin API, search backend,
integration tests, DAV and JMAP contacts/calendars.

## Reference implementations

Dovecot (mature mail handling), WildDuck (https://docs.wildduck.email/, full API design),
Postfix (LMTP behaviour, socketmap), Ceph RGW (metadata in RADOS without a database),
Stalwart and Cyrus (JMAP servers).

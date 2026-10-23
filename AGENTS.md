# AGENTS.md

Guidance for coding agents (Claude Code, Codex and others) working in this repository.

## Project state

`README.md` is the authoritative design document, agreed with the owner; read it before
designing anything. See its "Status" section for what is implemented (nearly everything in
the mail design: LMTP, JMAP incl. submission, IMAP, POP3, REST/admin, sessions/TOTP, workers,
legal hold, Maildir export/import) and what is not (CONDSTORE, WebSocket, search backend,
reconciliation worker, DAV, Ceph integration tests).

## Releases

Rolling releases `<yy>.<n>` (26.0, 26.1, …, first of 2027 is 27.1). `app/version.go` holds
`Release`; the Makefile embeds the commit (`26.0+<commit>[-dirty]`) via `app.Commit`, and
`Migrate` records the product version in `schema_version.applied_by`. To release: bump
`Release`, commit, `make build`. Never hard-code the version anywhere else.

## Commits

Plain commit messages: subject line plus an explanatory body when useful. Never add
`Claude-Session` or other session/attribution trailers. Never mention the coding assistant,
model, or agent in commit subjects, bodies, or co-author trailers.

## Commands

```
make build            # bin/kittiwake (cgo; needs librados-dev, installed on the dev box, Ceph 20.x)
make test             # go test ./...   (all unit tests run on the in-memory RADOS; no Ceph needed)
make test-race
make test-sql         # store conformance suites on a real MariaDB (KITTIWAKE_TEST_DSN; see Makefile for the dev container)
make lint             # go vet + gofmt check
go test ./store/index -run TestFolders          # one test
go test ./mail -run TestRoundTrip -v
make run-dev          # whole platform in one process on the memory backend with a seeded account
make integration      # RADOS conformance suite on a real cluster (lab box, see ../kittiwake-labs)
make e2e              # daemon on librados + LMTP/JMAP/IMAP/POP3/admin/export (lab box)
make deploy           # build here, push binary to the lab box, restart its systemd instances
                      # (kittiwake@<mode>, one process per mode; see contrib/systemd and ../kittiwake-labs)
bin/kittiwake check-config -c examples/kittiwake.yaml
bin/kittiwake migrate -c <cfg>                          # create/upgrade the MariaDB schema (store/mariadb/migrations)
bin/kittiwake admin -c <cfg> bootstrap                 # first admin token; `admin token --read-only` for more
bin/kittiwake-admin -u http://[::1]:8081 -t kwt_... mailbox list       # everything else: the API client
bin/kittiwake-admin cluster add|list|enable|disable|remove            # Ceph cluster registry (mailbox placement)
bin/kittiwake-admin ... -o json ...                                   # -o plain|json everywhere
```

Lab: the sibling repository `kittiwake-labs` (`/home/wido/repos/kittiwake/kittiwake-labs`,
Ansible) describes and provisions the Ceph test cluster and the `kittiwake` box (IPv6-only, no
GitHub access; rsync the tree to `/root/kittiwake`). Real-cluster findings so far: go-ceph
panics on zero-length WriteFull (handled in rados/librados.go) and assert_version fails with
ERANGE or EOVERFLOW depending on direction (both map to ErrVersionMismatch).

Never run `go mod tidy` blindly: go-ceph pulls cgo-only packages; `go get <mod>@<ver>` for
new deps. `go build ./...` must stay green with cgo (librados backend compiles even without a
cluster).

## Layout

```
rados/        RADOS interface (Get/Write tx/omap/xattr/lock/watch), librados.go + memory.go
worker/       background tasks + scheduler (purge, GC, removal, audit purge, orphan sweep, export)
config/       YAML config with defaults + validation (config.Default())
mail/         MIME parser → Document JSON, attachment extraction, raw skeleton + Reconstruct
auth/         crypto primitives: password schemes, TOTP, Ed25519 JWT, refresh tokens
store/        identity, index, message, attach, audit, session, queue (object layouts live here)
service/      actor (ctx principal), auditor, auth, delivery, mailbox, submit, admin, export
              (business logic + authorisation + audit; protocol adapters call ONLY these)
proto/        proxyproto, httpx (auth middleware), lmtp, socketmap, jmap, rest, imap, pop3
app/          wiring config→client→stores→services→listeners; metrics/ Prometheus registry
cmd/kittiwake  serve | check-config | admin (bootstrap, token only) | version
cmd/kittiwake-admin  operator CLI over client/admin (typed Go client for the admin API)
client/admin  pure HTTP client for the admin API; importable by third parties
examples/     kittiwake.yaml (full, documented), dev.yaml (memory backend)
contrib/postfix  main.cf snippet
api/openapi.yaml REST spec, embedded (api.OpenAPI) and served at /openapi.yaml on the admin
              listener; route↔spec consistency is tested — add new routes to both
```

Conventions: every store/service test builds a fixture on `rados.NewMemoryClient()`; services
take an `actor` from the context and refuse without one; errors are package sentinels
(`ErrNotFound`, `ErrForbidden`, ...) compared with `errors.Is`; the service layer maps store
errors, protocol adapters map service errors to wire codes (`delivery.Status`, httpx).
Tests that query the audit store must pin `auditor.Now`/`audit.Store.Now` to the fixture
clock (audit objects are per UTC day). Smoke-test the binary with
`make run-dev` plus Python's smtplib.LMTP/imaplib/poplib and curl against 127.0.0.1 ports
2424/1143/1110/8080 (jmap) and 8081 (admin) (see examples/dev.yaml); kill it by PID, not by `pkill -f`.

## What this is

Kittiwake: a massively scalable mail platform intended to replace Dovecot (the name is the
joint that fits onto Postfix, and the successor of Dovecot). Go module path: `kittiwake`.
Binary name: `kittiwake`. Not an MTA: Postfix stays in front (LMTP in, SMTP relay out).

## Architectural decisions

These are settled. Do not design around them.

- **Library first, daemon second.** Layers: `rados` interface (librados + in-memory mock) →
  stores (identity, index, messages, attachments, changelog, audit, sessions, queue) →
  services as Go interfaces (`Auth`, `Delivery`, `Mailbox`, `Submit`, `Admin`, `Audit`,
  `Notify`) → protocol adapters (LMTP, IMAP, POP3, JMAP, REST, socketmap, CLI) →
  `cmd/kittiwake`. A protocol adapter never touches a store or RADOS directly. Authorisation
  (roles, enabled flags, hold) and audit happen in the services, once, via a context that
  carries actor, client IP and protocol. Each layer is tested against the mock of the layer
  below. The service interfaces are the project's public Go API.

- **One Go binary (`kittiwake`), multiple modes:** `lmtp`, `imap`, `pop3`, `jmap` (JMAP +
  OAuth + /v1/me), `admin` (/v1/admin only, own listener), `socketmap`, `worker` (+ `dav` later), plus an
  `admin` subcommand that talks to the metadata database directly (bootstrap, export/import). Modes combine in one process or split across many.
- **Mail data in Ceph RADOS, metadata in MariaDB, no Redis.** Stateless daemons via librados
  (cgo; target latest stable Ceph). Per cluster three pools: `messages`, `attachments`,
  `index`. Pure RADOS, not S3/RGW. Metadata (domains, mailboxes, accounts, aliases, tokens,
  signing keys, sessions, worker queue, storage registry, admin audit) is in MariaDB
  (`store/mariadb`, embedded migrations, `kittiwake migrate`); the daemon refuses an unknown
  schema version. MariaDB commits every DDL statement on its own, so migration statements are
  idempotent and a failed migration is re-run. `database.read_dsn` sends SELECT-only
  operations to a replica (`DB.r()`), writes and transactions to the primary (`DB.exec`, `tx`);
  after a write a process reads from the primary for `read_stickiness`.
- **Identity store is an interface (`identity.Backend`)** with two implementations: MariaDB
  (production) and RADOS (`identity.Rados`, memory backend for development and unit tests;
  RGW-style objects: exclusive create for uniqueness, omap listings, version-conditional
  updates). `session.Store` and `queue.Store` are interfaces the same way. Both run the same
  conformance suites (`store/*/<pkg>test`; `make test-sql` against a real MariaDB).
- **Multiple Ceph clusters, all equal, called storage.** A storage is one Ceph cluster as a
  registry row (`kittiwake-admin storage add`, table `storage`), not config; the fsid read on
  connect verifies the record. Each mailbox has a `storage_id`; every data store resolves
  pools through `storage.Resolver` (Fixed for the memory backend, Registry otherwise).
  Placement is random among connected storage entries open for placement; an unreachable
  storage defers (LMTP 451, IMAP `[UNAVAILABLE]`, JMAP `serverUnavailable`). Sweeps walk
  every storage; a removal task carries the storage id.
- **Every multi-object write undoes itself; sweeps are insurance.** A write that touches
  several RADOS objects has one commit point (the folder record) and compensates everything it
  wrote when it fails before it (`index.unappend`, `delivery.undoStore`). Sweeps (attachment
  GC with dangling-reference checks and version-conditional deletes, weekly orphan sweep incl.
  lone `.raw` objects, hourly upload sweep, audit purge) only catch crashes; never make a new
  write path depend on a sweep, and never add a sweep that walks a pool more often than the
  orphan sweep. Tests inject failures with a wrapping `rados.IOContext` (see
  `service/delivery/undo_test.go`).
- **Admin API is the only way identity data is created/changed.** `kittiwake admin` does only
  bootstrap/token; operators use `kittiwake-admin` (client/admin). Do not add admin features
  to cmd/kittiwake; add them to the API + spec + client.
  Self-service endpoints let accounts manage their own passwords, app passwords, TOTP,
  tokens and sessions. All of it audited.
- **Postfix integration:** `socketmap` mode serves a Sendmail-style socketmap (`domains`,
  `mailboxes`, `aliases` maps) for `virtual_mailbox_domains/maps` and `virtual_alias_maps`.
  Postfix config and helpers live in `contrib/postfix/`.
- **Scalability is the governing principle** (1M mailboxes, 150k-message mailboxes): count
  objects and omap keys per message, shard every per-mailbox map, never read a whole folder
  for a page. **Storage is cheap, I/O is expensive:** count RADOS round trips per operation;
  merge writes into one transaction (change log rides in the root tx via `Root.Log`), run
  independent writes concurrently (`index.parallel`), batch low-value writes (audit
  `StartBuffering`), cache hot reads (`index.GetRootCached`, delivery `lookupCache`,
  `mailbox.RootCached`), and let adapters apply their own writes locally (IMAP
  `noteAppended`). Measure on the lab box before/after storage changes.
- **Message format:** ONE object per message: the parsed JSON (schema version, headers
  array, verbatim original header block incl. Received/Message-ID/DKIM, UTF-8 bodies, full
  MIME tree with small parts' content inline and attachment hashes), xattrs mailbox/created/
  `loc`. **No raw copy**: `mail.Render` rebuilds a deterministic RFC 5322 message (its length
  is Document.Size = what IMAP/JMAP report). Only `multipart/signed` keeps the original
  bytes (`.raw`, config `mail.keep_raw`). Every non-primary-body MIME part is an attachment;
  parts under a threshold stay inline as Part.Data.
- **Attachments** are deduplicated per Ceph cluster by content hash with reference counting
  (omap keys per referencing message on the hash-named object); only a worker deletes orphans.
- **Delivery order:** attachments, message JSON, raw, index entry + change log (commit
  point), refs. LMTP acks only after the index write.
- **Mailbox index lives in RADOS omap:** root object = small JSON only (folders, counters,
  modseq, usage; NO omap). Folder records in shards of 65536 UIDs (`<mb>/<fid>`, `<mb>/<fid>.<n>`),
  keys = inverted UID so ascending iteration is newest-first (`ListFolderDesc` is the cheap
  path; `ListFolder` ascending reads whole shards). Compact Record with short JSON tags.
  Message id → location via the message object's `loc` xattr (`message.Store.Location`).
  Thread/Message-ID maps sharded over `<mb>/t.<xx>` (256). Keep omaps bounded.
- **Change tracking:** every mutation bumps `modseq` and appends to the bounded change log
  `<mailbox>/changes`. Needed by JMAP `/changes` and later IMAP CONDSTORE/QRESYNC.
- **Workers** consume the task queue in MariaDB (`SELECT ... FOR UPDATE SKIP LOCKED`,
  lease, run-after, retry with backoff, dead-letter state; the memory backend keeps the same
  queue as omap entries). One worker holds a lease row as scheduler for periodic tasks. Reconciliation reports/repairs, never deletes
  mailboxes on its own.
- **Full-text search** is a future, pluggable backend behind an interface; publish
  index/remove events on every mutation from day one.
- **Protocols:** LMTP inbound (one recipient per transaction; aliases may fan out), **JMAP is
  the mailbox API** (RFC 8620/8621, plus 8887 WebSocket, 9425 Quotas, 9404 Blob; later 9610
  Contacts and Calendars). No custom mailbox REST API. A small custom REST surface (OpenAPI
  spec in repo) covers only OAuth/session/self-service endpoints and the admin API. IMAP4rev1
  + IDLE, UIDPLUS, MOVE; POP3 INBOX only. The JMAP server layer is written in this repo.
- **Identity:** mailbox = UUID that owns typed collections. Accounts link to a mailbox
  (many-to-one), have roles (`read`, `write`, `send`), passwords marked `primary` or
  `application`, each with a required comment and optional `expires_at` (temporary
  passwords; expired ones never match) (Dovecot-style scheme prefixes; argon2id for new
  ones), optional TOTP
  secret, and API tokens (stored as SHA-256, prefixed `kwt_`, expiry and comment mandatory,
  may only narrow the account's roles). **Admins are tokens, not accounts:** admin tokens
  (`tokens/<hash>` with zero AccountID, listed in `tokens/_admin`) carry `admin` or
  `admin-ro`; accounts can never have admin roles. Every GET on the admin API accepts
  `admin-ro`; services use `authorizeRead`/`IsAdminReader` for reads and `IsAdmin` for writes.
- **Sessions (OAuth 2.0 shaped, RFC 9700):** `POST /v1/oauth/token` with password gives a
  15-minute Ed25519-signed JWT access token (verified locally, no lookup) plus an opaque
  refresh token stored hashed in the `sessions` table, rotated on every use with reuse
  detection. `POST /v1/oauth/revoke` (RFC 7009). Signing keys in `signing_keys`, rotated. Keep the endpoint OAuth-standard so PKCE/OIDC can be added later.
- **TOTP (RFC 6238):** with a secret present, primary passwords need a code (`totp_code` at
  the token endpoint, error `totp_required` when missing) and are rejected on IMAP/POP3;
  application passwords never need a code. Replay state in `totp_steps`. Accounts list the
  services they may use (`imap`, `pop3`, `jmap`); the check happens at login per protocol.
  IMAP/POP3 also accept OAUTHBEARER/XOAUTH2 with a session access token.
- **Sending:** `From` must be one of the account's identities (own address + aliases to its
  mailbox). Sent copy goes to Sent. Relay failure = API error, nothing stored.
- **Domains, aliases, catch-all** are first-class. LMTP resolution: exact address → alias
  (bounded depth, may fan out) → catch-all → reject. Same answer served to Postfix.
- **Lifecycle:** accounts belong to a domain and point at a mailbox; deleting an account
  (or a domain once empty of accounts/aliases) orphans the mailbox with its data, by design;
  attach a new account to rename/move. `GET /v1/admin/mailboxes?orphaned=true` lists them.
- **Mailbox state:** `active` | `suspended` (LMTP 450 → MTA queues; users locked out) |
  `disabled` (LMTP 550; socketmap says NOTFOUND so SMTP rejects; users locked out), with a
  required reason. There is no mailbox `enabled` flag; `admin.SetMailboxState` is the only
  way to change it. Account `enabled` only blocks that credential.
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
  `/metrics`, `/healthz`, `/readyz` on a management listener. Log/audit lines about a message
  carry `message_id` (the Message-ID header) next to Kittiwake's `msg` uuid.

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

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

Greenfield. `README.md` is the authoritative design document and has been agreed with the
owner; read it before designing anything. There is no Go module yet. When one exists, replace
the "Commands" section below with the real invocations.

## Commands

None exist yet. The daemon is written in Go, so expect `go build ./...`, `go test ./...`,
`go test ./path/to/pkg -run TestName` once a `go.mod` is created. Tests use mocks for Ceph and
SQL; real integration tests come later.

## What this is

Dovetail: a massively scalable mail platform intended to replace Dovecot (the name is the
joint that fits onto Postfix, and the successor of Dovecot). Go module path: `dovetail`.
Binary name: `dovetail`. It is **not** an MTA: Postfix
stays in front, receives and filters mail from the internet, hands it to this daemon over LMTP,
and takes outbound mail back over SMTP from a configurable relay host.

## Architectural decisions

These are settled. Do not design around them.

- **One Go binary (`dovetail`), multiple modes:** `lmtp`, `imap`, `pop3`, `api`, `worker`. Modes can be
  combined in one process or split across many. Each is independently scalable.
- **Stateless daemons.** Everything the platform writes goes to Ceph RADOS via librados (cgo
  is accepted; target the latest stable Ceph). SQL (MySQL/MariaDB) is **read-only** and holds
  accounts, mailboxes, roles, API tokens. Redis is not part of the design unless a concrete
  need appears.
- **Storage pools:** messages, attachments and index are separate RADOS pools. Pure RADOS, not
  S3/RGW.
- **Message format:** parsed JSON (headers array, verbatim original headers, HTML and
  plain-text body, attachment references) plus a **raw skeleton**: the original bytes with each
  attachment's encoded body replaced by a reference. Reconstruction must be byte-exact; verify
  at delivery and fall back to storing the full raw message when it is not.
- **Attachments** are cluster-wide deduplicated by content hash with reference counting; only
  a worker deletes orphans.
- **Mailbox index lives in RADOS omap:** one root object per mailbox (folders, UIDVALIDITY,
  UIDNEXT, quota totals, advanced with atomic transactions) and one index object per folder
  (omap key = zero-padded UID, value = compact record with object name, flags, size, date,
  list-view headers).
- **Workers** consume a RADOS-based task queue (one queue object per task type, omap entries,
  lease via RADOS lock): trash purge, attachment GC, quota recalculation, mailbox removal,
  search indexing.
- **Full-text search** is a future, pluggable backend. Keep search behind an interface, keep
  plain text in the parsed JSON, and publish index/remove events on every delivery, flag
  change, move and deletion from day one.
- **Identity:** mailbox = UUID that owns folders and messages. Accounts link to a mailbox
  (many-to-one), have roles, zero or more passwords (Dovecot-style scheme prefixes: bcrypt,
  argon2, SHA512-CRYPT, ...) and API tokens. Every API token needs an expiration date and a
  comment. Admin accounts are accounts with the `admin` role and no mailbox.
- **SQL lookups are admin-configurable queries** with variables (`%{username}`, `%{domain}`,
  `%{address}`), Dovecot-style; a default schema must work out of the box. Expose as many
  lookups as possible as queries.
- **Protocols:** LMTP inbound (one recipient per transaction), REST (primary; OpenAPI spec
  kept in the repo; separate mailbox API and admin API), IMAP4rev1 + IDLE, UIDPLUS, MOVE,
  POP3, JMAP later.
- **Quotas:** soft and hard limits on size and message count; hard limit rejects at LMTP.
- **Retention:** deleted mail goes to Trash and is purged by a worker after a configurable
  period. IMAP EXPUNGE / POP3 DELE are lazy: the message leaves the index but is kept in a
  retention set for the same period. Nothing is destroyed immediately.
- **Spam routing:** a configurable header/value from the MTA files the message into Junk at
  LMTP time. No Sieve-style user rules.
- **Brute-force protection** is HAProxy's job; just log failed logins with the client address.
- **Attachment refcounts** are omap keys per referencing message on the hash-named attachment
  object. **Delivery order:** attachments, message JSON, raw, index entry (commit point), refs.
- **IMAP IDLE / push** uses RADOS watch/notify on the mailbox root object.
- **No TLS in the daemon** (HAProxy terminates it). **IPv6-first.** Single YAML config file.
- **Observability:** structured JSON logging and Prometheus metrics in every component.

## Roadmap

Milestone 1: config loading, SQL auth/lookup layer, LMTP delivery into RADOS (parse,
attachments, index), REST mailbox API read paths. Then IMAP/POP3, sending, workers, admin API,
search backend, integration tests, JMAP.

## Reference implementations

Dovecot (mature mail handling), WildDuck (https://docs.wildduck.email/, full REST API design),
Postfix (LMTP behaviour).

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

This is a greenfield project: as of the initial commit the repository contains only `README.md`,
which is the design document. There is no Go module, build system, or test suite yet. When those
are added, update the "Commands" section below with the real invocations (module path, how to run
a single package's tests, any integration-test prerequisites such as a Ceph or MariaDB instance).

## Commands

None exist yet. The daemon is to be written in Go, so expect the standard toolchain
(`go build ./...`, `go test ./...`, `go test ./path/to/pkg -run TestName`) once a `go.mod` is created.
Replace this paragraph with the actual commands as soon as they are real.

## What this is

A new, massively scalable mail platform intended to replace Dovecot. It is **not** an MTA: Postfix
(or another MTA) stays in front, receives and filters mail from the internet, and hands messages
to this daemon over LMTP. The README is the authoritative spec; read it before designing anything.

## Architectural constraints (from the README)

These are hard requirements that shape every module. Do not design around them.

- **Stateless daemons.** All persistent state lives in Ceph RADOS (messages, attachments) and SQL
  (mailboxes, accounts). Daemons scale out by adding more instances; inter-daemon coordination is
  expected to go through something like Redis Sentinel.
- **Go, modular, heavily tested.** Each module should carry its own extensive tests.
- **Protocols:** LMTP inbound; IMAP and POP3 for legacy clients; a full REST API over every
  mailbox is the primary interface (intended to back a webmail and mobile app); JMAP is optional.
- **No TLS in the daemon.** A proxy such as HAProxy terminates TLS. Do not add TLS handling.
- **IPv6-first.** Assume an IPv6-only environment; IPv4 is nice-to-have.
- **Container-friendly but not container-mandatory.**

### Storage model (Ceph RADOS, not S3/RGW)

- Messages and attachments are stored as **pure RADOS objects** in **separate pools**.
- A raw message is parsed into a JSON document containing: headers as a JSON array, the body as
  both HTML and plain text, and references to attachment objects. The **original unparsed headers**
  (including Received: lines) must also be kept in that same JSON for traceability.
- Attachments are extracted into their own objects and **deduplicated by content hash**, so
  multiple messages can reference the same attachment object.

### Identity model (SQL, read-only)

- A **mailbox** is identified by a UUID and is the thing that owns messages; on its own it is
  not addressable.
- **Accounts** link to a mailbox. Many accounts may point at one mailbox (multiple credentials
  for the same mail). An account has a username (usually the e-mail address) and may have
  multiple passwords (application passwords).
- Mailboxes, accounts and metadata live in **MySQL/MariaDB**. The platform **only reads** from
  SQL, never writes. HA of the database is the DBA's concern, not ours.
- Like Dovecot, admins supply their own SQL queries in the config so they can use their own
  schema; a default schema must work out of the box. Queries support variables (username, domain,
  etc.). Rows inserted directly by an admin must be picked up by the platform.
- LMTP delivery looks up the recipient address in SQL to find the account/mailbox, then stores
  the message in that mailbox.

### Configuration

A single YAML file holding: Ceph connection info (monitors, credentials, pool names), SQL
connection info plus the admin-supplied queries, and Redis info if used.

## Reference implementations

When unsure how to handle a mail-protocol edge case, look at Dovecot (mature mail handling),
WildDuck (https://docs.wildduck.email/, full REST API design), and Postfix (LMTP behaviour).

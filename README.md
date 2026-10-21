# Dovetail

Dovetail is a new, massively scalable and modern mail platform built to replace Dovecot. The
name is the joint that fits two pieces of wood together: Dovetail fits onto Postfix, and it is
the successor of Dovecot.

The purpose of this project is to build a new, massively scalable and modern mail platform to
mainly replace Dovecot. It is **not** an MTA and does not replace Postfix: Postfix receives and
filters mail from the internet and hands it to this platform over LMTP. Outbound mail is handed
back to a configurable SMTP host.

License: none for now, internal use only.

# Features

## Stateless and scalable

The daemons that run this platform are stateless, allowing them to scale out. All persistent
state lives in Ceph RADOS (messages, attachments, mailbox indexes, task queues) and in SQL
(accounts, mailboxes, metadata; read-only). Everything the platform itself writes goes to
RADOS. More servers with more daemons means the platform scales.

Redis (or similar) is only introduced if a concrete need appears; it is not part of the base
design.

## One binary, multiple modes

The platform ships as a single Go binary, `dovetail`, that can run in one of several modes. Each mode is a
separately scalable component:

- `lmtp` – inbound delivery from the MTA
- `imap` – IMAP server for legacy clients
- `pop3` – POP3 server for legacy clients
- `api` – REST API (mailbox API and admin API)
- `worker` – background task execution (trash purge, attachment garbage collection, quota
  recalculation, mailbox removal, …)

Modes can be combined in one process for small setups and split across many processes for large
ones.

## Language

The daemon is written in Go. cgo is accepted (needed for librados). Target the latest stable
Ceph release.

## Modular and tested

The code is modular and each module contains extensive tests. For now tests use mocks for Ceph
and SQL; real integration tests against live services come later.

## Observability

Structured logging and metrics everywhere. Every component exposes Prometheus metrics and logs
structured (JSON) output.

## Containers

The daemon can run inside containers, but this is not mandatory.

## Protocols

Inbound e-mail is delivered using LMTP: an MTA like Postfix receives and filters the message from
the internet before delivering it to this daemon. One recipient per LMTP transaction for now.

Supported protocols:

- LMTP (inbound)
- REST API (primary interface)
- IMAP: IMAP4rev1 plus IDLE, UIDPLUS and MOVE to start; more extensions later as needed
- POP3
- JMAP as an option (later)

IMAP and POP3 must work but are not the primary target; they serve legacy clients. Each mailbox
is fully accessible through the REST API, which is the basis for a modern webmail and a mobile
app.

## Sending mail

Sending is in scope. The REST API accepts outbound messages and submits them over SMTP to a
configurable relay host (typically Postfix). The platform does not queue or retry outbound mail
itself beyond the submission; that is the MTA's job.

## TLS

TLS is not handled by this daemon; a proxy like HAProxy does this.

## IPv6-first

Assume an IPv6-only environment. IPv4 is nice to have, not mandatory.

# Storage: Ceph RADOS

All e-mail messages are stored in Ceph RADOS objects: not S3/RGW, pure RADOS via librados.
Messages, attachments and indexes live in different RADOS pools.

## Message parsing

A raw e-mail message is parsed into a JSON document which is stored as a RADOS object:

- Headers as a JSON array (parsed)
- The original, unparsed headers (including `Received:` lines) kept verbatim for traceability
- Body as HTML and plain text
- References to attachment objects

## Raw message

The original raw message is also kept so that IMAP clients can fetch the exact bytes and so that
the message can be reproduced for forensics. Because attachments make up the bulk of mail
volume, storing both the raw message and the extracted attachments would roughly double storage.

**Raw skeleton.** The raw message is stored with each attachment's encoded
body cut out and replaced by a reference to the attachment object. All MIME framing, boundaries
and the original `Content-Transfer-Encoding` stay in place, so the exact bytes can be rebuilt by
re-encoding the attachment and splicing it back in. At delivery time the platform rebuilds the
message immediately and compares it with the original; if the bytes match, the skeleton is
stored, otherwise the full raw message is stored for that one message. A config switch can force
full raw storage.

## Attachments

Attachments are extracted and stored as separate RADOS objects in their own pool. They are
deduplicated **cluster-wide** by content hash, so the same attachment is referenced from many
messages across many mailboxes. This requires reference counting: an attachment object is only
removed by a background worker once no message references it.

## Mailbox index

RADOS has no listing or query capability, so each mailbox needs an index: folders, message
UIDs, flags (seen, answered, deleted, …), sizes and dates. SQL is read-only and would not scale
for this, so the index lives in RADOS as well, using omap (the sorted key-value map every RADOS
object carries, with atomic compare-and-set and prefix listing).

**Design:**

- One **mailbox root object** per mailbox UUID in the index pool. It holds the folder list,
  per-folder UIDVALIDITY and UIDNEXT counters and quota usage totals. Counters are advanced
  with atomic RADOS write transactions so concurrent LMTP and API processes never hand out the
  same UID.
- One **folder index object** per folder. Omap key is the zero-padded UID; value is a compact
  record with the message object name, flags, size, internal date and a few list-view headers
  (subject, from, date) so listing a folder needs no message fetches.
- The index pool is small and hot and can be placed on fast storage with its own replication
  settings.

Limitations accepted for now: no cross-mailbox queries, and search is limited to header
matching over the index until the full-text search component exists.

## Full-text search

Full-text search will be required in the future, so the design plans for it from the start:

- Search is behind a Go interface with a pluggable backend. The first backend is the simple
  header matcher over the folder index; a real engine (for example OpenSearch, Meilisearch or an
  embedded Bleve index) is added later as an optional, separately scalable component.
- The parsed message JSON already contains the plain-text body and the attachment list, so an
  indexer never has to re-parse raw mail.
- Every delivery, flag change, move and deletion publishes an event through the worker task
  queue (`index-message`, `remove-message`), so a search backend can be populated and kept in
  sync by workers. Until a backend is configured these events are no-ops.
- Re-indexing a whole mailbox is a worker task, so a search backend can be introduced or rebuilt
  for existing mail.

## Folders

Mailboxes have folders (INBOX, Sent, Trash, …, plus user-created, nested).

## Retention

Deleted messages go to Trash and are purged after a configurable trash period by a background
worker.

## Workers and background tasks

The `worker` mode executes background tasks:

- Purge Trash messages older than the retention period
- Decrement attachment reference counts and delete orphaned attachment objects
- Recalculate quota usage for a mailbox
- Remove all RADOS data of a mailbox that no longer exists in SQL

**Queue design:** a RADOS queue object per task type in the index pool, with each task
as an omap entry and a lease via a RADOS lock, so multiple workers can run without duplicating
work. If this proves clumsy, Redis becomes the concrete need.

## Quotas

Each mailbox has quotas:

- Soft and hard limit on total size
- Soft and hard limit on message count

Exceeding a soft limit produces warnings (logged, exposed via API); exceeding a hard limit
rejects delivery at LMTP time.

# Mailboxes, Accounts and Domains

The system knows **mailboxes**. A mailbox has a UUID and owns messages and folders. A mailbox by
itself is not addressable.

**Accounts** are created and linked to a mailbox. Multiple accounts can point to the same
mailbox, allowing multiple credentials to access one mailbox. An account consists of a username
(usually the e-mail address) and zero or more passwords (allowing application passwords).

## Roles

Accounts have roles that restrict what they may do (for example full access, read-only,
send-only).

## API tokens

An account may have no password at all and only API tokens, so applications can connect without
a password. Each API token carries its own roles, which may be narrower than the account's.
API tokens are stored in SQL alongside the accounts and are looked up with admin-configurable
queries like everything else. Every API token **must** have an expiration date and a comment
describing its purpose; tokens without either are invalid and rejected. Expired tokens are
rejected.

## Admin accounts

Admin accounts are regular accounts in SQL that carry the `admin` role. They are not linked to
a mailbox. An admin account authenticates to the admin API with one of its API tokens (same
rules: expiration date and comment required), so an admin can hold multiple tokens for
different tools.

## Password hashing

Passwords are stored hashed with a scheme prefix, Dovecot-style, so multiple schemes coexist:
bcrypt, argon2, SHA512-CRYPT and the other common Dovecot schemes.

## Auth database in SQL

Mailboxes, accounts, roles, tokens and their metadata are stored in MySQL/MariaDB. The platform
does not handle database HA; that is the DBA's job (for example MariaDB Galera). The platform
**only reads** from SQL, never writes.

## SQL queries

The admin can provide their own SQL queries so they can use their own schema, similar to
Dovecot. As many lookups as possible are expressed as admin-configurable queries, with
variables such as `%{username}`, `%{domain}`, `%{address}`. A default schema works out of the
box. Rows inserted directly by an admin are picked up by the platform.

## LMTP delivery

When a message is delivered (by Postfix) the platform queries the database to resolve the
recipient address to a mailbox, checks the quota, stores the message in RADOS, updates the
mailbox index and acknowledges delivery.

# REST API

The REST API is described by an OpenAPI specification that is maintained in this repository.
There are two distinct APIs:

- **Mailbox API** – authenticated as an account (password or API token), scoped to that
  account's mailbox and roles. Folders, messages, flags, search, attachments, sending.
- **Admin API** – authenticated with an API token belonging to an account with the `admin`
  role, for operating the platform. Because SQL is read-only, the admin API does not create
  accounts or mailboxes. Scope:
  1. Mailbox inspection: storage usage, message and folder counts, quota status, index health
  2. Mailbox maintenance: verify or rebuild the index, force quota recalculation, purge Trash now
  3. Mailbox removal: delete all RADOS data for a mailbox removed from SQL
  4. Worker and queue visibility: pending and failed tasks, retry
  5. Support access: read a mailbox on behalf of a user, audited
  6. Platform status: Ceph and SQL health, version, running instances

# Daemon

## Configuration

The daemon takes a single YAML file as configuration:

- Mode(s) to run and listen addresses
- Ceph cluster connection information
  - Monitors
  - Credentials
  - RADOS pools (messages, attachments, index)
- SQL information
  - Host
  - Credentials
  - SQL queries, with variables such as username, domain, address
- Outbound SMTP relay host
- Trash retention period
- Logging and metrics settings
- Redis information (only if it ever becomes needed)

# Roadmap

## Milestone 1 (minimum viable)

- Configuration loading
- SQL auth and lookup layer with admin-configurable queries
- LMTP delivery into RADOS (parsing, attachment extraction, index update)
- REST mailbox API: read folders and messages

## Later

- IMAP, POP3
- Sending via SMTP relay
- Workers: trash purge, attachment GC, quota recalculation
- Admin API
- Full-text search backend
- Real integration tests against Ceph and MariaDB
- JMAP

# Inspiration

When building this, look at:

- Dovecot – exists for years, has a lot of proper e-mail handling code
- WildDuck – https://docs.wildduck.email/ – has a full API
- Postfix – mainly the LMTP part

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
- `dav` – CalDAV/CardDAV (future)

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
structured (JSON) output. Each process serves `/metrics`, `/healthz` (process up) and `/readyz`
(Ceph and SQL reachable) on a dedicated management listener.

## Containers

The daemon can run inside containers, but this is not mandatory.

## Protocols

Inbound e-mail is delivered using LMTP: an MTA like Postfix receives and filters the message from
the internet before delivering it to this daemon. One recipient per LMTP transaction for now
(one recipient address may still fan out to several mailboxes through aliases).

Supported protocols:

- LMTP (inbound)
- REST API (primary interface)
- IMAP: IMAP4rev1 plus IDLE, UIDPLUS and MOVE to start; more extensions later as needed
- POP3: serves INBOX only; UIDL is derived from the folder UID and UIDVALIDITY; DELE is lazy
  (see Retention)
- JMAP as an option (later)

IMAP and POP3 must work but are not the primary target; they serve legacy clients. Each mailbox
is fully accessible through the REST API, which is the basis for a modern webmail and a mobile
app.

## Sending mail

Sending is in scope. The REST API accepts outbound messages and submits them over SMTP to a
configurable relay host (typically Postfix), optionally with SASL credentials. The platform does
not queue or retry outbound mail itself beyond the submission; that is the MTA's job. If the
relay rejects or is unreachable the API returns an error and nothing is stored. A copy of every
sent message is stored in the Sent folder, going through the same parse-and-store path as
inbound mail. The mailbox API also supports appending an arbitrary message to a folder (drafts,
imports, IMAP APPEND).

The `From` address (and `Sender`, if present) must be one of the addresses the account is
allowed to use, as returned by a configurable SQL query (normally the account's own address and
its aliases). Submissions with another `From` are rejected. Dovetail adds its own `Received:`
header naming the authenticated account so the MTA logs can be tied to the audit log.

## Message size limit

A configurable maximum message size is enforced at LMTP and at the API append and send
endpoints. Postfix is expected to enforce its own limit first.

## TLS

TLS is not handled by this daemon; a proxy like HAProxy does this.

## Client IP passthrough

Because HAProxy sits in front, the daemon would only ever see the proxy's address. HAProxy
forwards every connection with the PROXY protocol, so all listeners (IMAP, POP3, LMTP and the
HTTP API) accept PROXY protocol v1 and v2 and take the real client address from it. PROXY
protocol can be required per listener so a connection without the header is rejected. As an
optional fallback the HTTP listeners can trust `X-Forwarded-For` from configured proxy
addresses. The real client IP is used in logs, audit records and failed-login records.

## Auditing

Every action on a mailbox is audited so that it can be inspected afterwards: who did what,
from where, and when. An audit record contains:

- Timestamp
- Mailbox UUID
- Account username and, when a token was used, the token id and its comment
- Protocol (`imap`, `pop3`, `api`, `lmtp`, `admin`)
- Client IP address (real address, see "Client IP passthrough")
- Action (login, login-failed, logout, list, fetch, flag-change, move, copy, delete, expunge,
  append, send, folder-create/rename/delete, …) with its target (folder, message UID(s))
- Result (ok, denied, error)

Audit records are written to two places:

1. The structured log stream, so they can be shipped to a central log system.
2. RADOS, in a per-mailbox audit object per day (`<mailbox-uuid>/audit/<YYYY-MM-DD>` in the
   index pool, records as omap entries keyed by timestamp), so the admin API can query the
   history of a mailbox without a log system. Audit retention in RADOS is configurable and a
   worker purges expired audit objects.

Admin actions (support access, maintenance) are audited the same way, with the admin account
as the actor and the target mailbox as the subject. Read-heavy actions (fetch, list) are
audited by default but can be reduced to session-level (login/logout) per protocol in the
config to limit volume.

## Brute-force protection

Dovetail does not rate-limit or lock out failed logins; stateless daemons have no shared
counters. This is left to HAProxy (and tools like fail2ban on the structured logs). Every
failed authentication is logged with the client address so those tools have what they need.

## IPv6-first

Assume an IPv6-only environment. IPv4 is nice to have, not mandatory.

# Storage: Ceph RADOS

All e-mail messages are stored in Ceph RADOS objects: not S3/RGW, pure RADOS via librados.
Messages, attachments and indexes live in different RADOS pools.

## Message parsing

A raw e-mail message is parsed into a JSON document which is stored as a RADOS object:

- A schema version field, so the format can evolve
- Headers as a JSON array (parsed, decoded to UTF-8)
- The original, unparsed headers (including `Received:` lines) kept verbatim for traceability
- Body as HTML and plain text (both derived if only one exists), converted to UTF-8
- The full MIME structure (part tree with content types, dispositions, Content-IDs, sizes)
- References to attachment objects

**What is an attachment:** every MIME part that is not the primary text/plain or text/html body
is treated as an attachment (including inline images referenced by Content-ID and any
message/rfc822 part). Parts smaller than a configurable threshold (default 4 KiB) stay inline in
the skeleton rather than becoming tiny RADOS objects.

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

**Reference counting design:** the attachment object is named by its content hash. Its omap
holds one key per referencing message (`ref/<mailbox>/<message>`), so adding and removing a
reference are idempotent omap operations and concurrent deliveries of the same attachment never
race on a counter. A worker deletes an attachment only when its reference set is empty and the
object has not been touched for a grace period. Creation uses RADOS exclusive-create so two
simultaneous first uploads of the same attachment resolve cleanly.

## Delivery ordering and crash safety

Delivery writes in this order: attachments, message JSON, raw skeleton, then the folder index
entry (the commit point), then the attachment references. A crash before the index write leaves
orphan objects but never a dangling index entry. Each message object carries its mailbox UUID and
creation time in xattrs so an orphan-sweep worker task can find and remove objects that never
reached an index. LMTP only acknowledges after the index write succeeds.

## Object naming

- Message JSON: `<mailbox-uuid>/<message-uuid>` in the messages pool
- Raw skeleton or full raw: `<mailbox-uuid>/<message-uuid>.raw` in the messages pool
- Attachment: `<sha256-hex>` in the attachments pool
- Mailbox root: `<mailbox-uuid>` in the index pool
- Folder index: `<mailbox-uuid>/<folder-id>` in the index pool
- Task queues: `queue/<task-type>` in the index pool
- Retention set (expunged and trashed messages waiting for purge): `<mailbox-uuid>/retention`
  in the index pool, omap keyed by purge-at timestamp
- Audit: `<mailbox-uuid>/audit/<YYYY-MM-DD>` in the index pool

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
  record with the message object name, flags, size, internal date (UTC) and a few list-view
  headers (subject, from, date) so listing a folder needs no message fetches.
- Flags are the IMAP system flags plus arbitrary keywords; the folder's keyword list lives in
  the folder index object's xattrs. IMAP SUBSCRIBE state is per mailbox (shared by all accounts
  on it), as are flags.
- Rebuilding a folder index (admin maintenance) assigns a new UIDVALIDITY so clients resync.
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

Mailboxes have folders. The default set, created when a mailbox is first used, is INBOX,
Drafts, Sent, Junk, Trash and Archive, each tagged with its IMAP SPECIAL-USE attribute (RFC
6154). Users can create nested folders. Folders are identified internally by an id, not by
name, so renaming does not move data.

## Mailbox provisioning

Mailboxes are created in SQL by the admin. The RADOS side (root object, default folders) is
created lazily the first time a mailbox receives mail or an account logs in. No explicit
provisioning step is needed.

## Change notification (IMAP IDLE, push)

Daemons are stateless and share nothing, so a process serving an IMAP IDLE session must learn
about deliveries done by another process. RADOS watch/notify is used for this: an IMAP or API
process watches the mailbox root object, and any process that changes a folder index sends a
notify on that root object. This gives IDLE and future webmail push without Redis.

## Threading

The folder index record stores `Message-ID`, `In-Reply-To` and `References` so the REST API can
build conversation views and IMAP can offer THREAD later without fetching messages.

## Retention

Deleted messages go to Trash and are purged after a configurable trash period by a background
worker.

IMAP EXPUNGE (and POP3 DELE) never destroys data immediately. An expunged message is removed
from its folder index and parked in the retention set of the mailbox, where it stays for the
same retention period before the worker purges it (lazy expunge, as Dovecot calls it). Deleting
from Trash via the API behaves the same way.

## Spam routing

If the MTA (Postfix with rspamd, SpamAssassin, …) marks a message as spam, LMTP delivery files
it into the Junk folder instead of INBOX. The header name and the value that means "spam" are
configurable (for example `X-Spam-Flag: YES` or `X-Spam: Yes`). User-defined filter rules
(Sieve-style) are not in scope for now.

## Workers and background tasks

The `worker` mode executes background tasks:

- Purge Trash messages older than the retention period
- Decrement attachment reference counts and delete orphaned attachment objects
- Recalculate quota usage for a mailbox
- Remove all RADOS data of a mailbox that no longer exists in SQL
- Purge audit objects older than the audit retention period

Periodic tasks (orphan sweep, audit purge, retention purge scan, SQL-vs-RADOS mailbox
reconciliation) are enqueued by a scheduler role that the workers elect among themselves with
a RADOS lock on a `scheduler` object, so exactly one worker schedules and any worker executes.

Reconciliation only **reports** mailboxes that exist in RADOS but not in SQL (visible in the
admin API and logs); it never deletes data by itself. Removal is an explicit admin API action.

**Queue design:** a RADOS queue object per task type in the index pool, with each task
as an omap entry and a lease via a RADOS lock, so multiple workers can run without duplicating
work. Tasks carry a run-after time; failed tasks are retried with backoff and after a
configurable number of attempts land in a dead-letter set visible in the admin API. If this
proves clumsy, Redis becomes the concrete need.

## Quotas

Each mailbox has quotas:

- Soft and hard limit on total size
- Soft and hard limit on message count

Exceeding a soft limit produces warnings (logged, exposed via API); exceeding a hard limit
rejects delivery at LMTP time with a permanent error (5.2.2) by default, configurable to a
temporary error so the MTA keeps retrying.

Limits are per mailbox and come from SQL (columns on the mailbox row, read through a
configurable query); a config-wide default applies when the row has no limit. Usage counters
(bytes and message count) live in the mailbox root object in RADOS and are updated atomically
with every delivery and deletion; a worker task can recalculate them from the folder indexes.

# Mailboxes, Accounts and Domains

The system knows **mailboxes**. A mailbox has a UUID and owns messages and folders (and later
calendars and address books). A mailbox by itself is not addressable.

## Domains, aliases and catch-all

Domains are first-class in the default schema: a domain row (name, enabled) that accounts and
aliases belong to. Delivery to an address in an unknown or disabled domain is rejected.

**Aliases** map a source address to a destination: either a mailbox or another address (which
is resolved again, with a bounded depth to stop loops). An alias may have several destinations,
in which case LMTP delivers to each mailbox. Aliases are also what makes an address usable as a
`From` when sending.

A **catch-all** is a per-domain fallback: an alias whose source is the whole domain (`@example.org`),
used when no account or alias matches the exact address.

Resolution order at LMTP time: exact account address → alias → catch-all → reject. Every step is
an admin-configurable SQL query, so a different schema can express the same thing.

**Accounts** are created and linked to a mailbox. Multiple accounts can point to the same
mailbox, allowing multiple credentials to access one mailbox. An account consists of a username
(usually the e-mail address) and zero or more passwords (allowing application passwords).

## Roles

Accounts and API tokens carry a set of roles. The role set is:

- `read` – list folders, read messages and attachments, search
- `write` – change flags, move/copy/delete messages, manage folders, append messages
- `send` – submit outbound mail
- `admin` – admin API (never combined with a mailbox)

A normal full-access account has `read`, `write` and `send`. A token's roles may only narrow
the account's roles; a token claiming a role the account lacks is rejected.

Roles are returned by a configurable SQL query as a list of strings, so the admin may store them
as a column, a join table or anything else.

## Credentials per protocol

- IMAP and POP3 authenticate with username and a password (any of the account's passwords,
  including application passwords).
- The REST API authenticates with either HTTP basic auth (username and password) or a bearer
  API token.
- API tokens are stored **hashed** in SQL (SHA-256 of the token); Dovetail hashes the presented
  token and looks it up by hash through the configured query. Tokens are random, prefixed
  `dvt_` so they are recognisable in logs and secret scanners.
- Accounts and mailboxes have an `enabled` flag. A disabled account cannot log in. Delivery to
  a disabled mailbox is rejected with a permanent LMTP error by default (configurable to a
  temporary error).
- Because SQL is read-only there is no self-service password or token management through
  Dovetail; that lives in whatever manages the SQL database.

## Future: CalDAV and CardDAV

Calendars and contacts will be needed later, served over CalDAV and CardDAV. The design leaves
room for them now:

- A mailbox is the storage container for everything a user owns, not just mail. Its root
  object lists **collections** with a type (`mail-folder` now; `calendar` and `addressbook`
  later), so calendars and address books hang off the same mailbox UUID with the same quota,
  audit and retention machinery.
- Object name prefixes `<mailbox-uuid>/cal/` and `<mailbox-uuid>/card/` are reserved.
- The role set can grow with `calendar` and `contacts` roles.
- A future `dav` mode serves CalDAV/CardDAV from the same binary.

Nothing of this is built in the first milestones, but nothing in the mail design may block it.

## Out of scope

Shared folders between mailboxes and IMAP ACLs. Encryption at rest and compression are handled
by the Ceph pools (BlueStore compression and encryption), not by Dovetail.

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
All paths are versioned (`/v1/...`), list endpoints are paginated with cursors, and errors use
a single JSON problem format. There are two distinct APIs:

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
  7. Audit inspection: query the audit history of a mailbox or an account by time range,
     action, client IP

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
- Audit retention period and per-protocol audit detail level
- Trusted proxy addresses (PROXY protocol, X-Forwarded-For)
- Spam header name and value for Junk routing
- Default quota limits, over-quota and disabled-mailbox LMTP reply policy
- Inline attachment size threshold
- Maximum message size
- Management listener (metrics, health)
- Logging and metrics settings
- Redis information (only if it ever becomes needed)

Secrets (Ceph key, SQL password) can be given inline, as a path to a file, or via `${ENV_VAR}`
substitution.

## Key dependencies

- `github.com/ceph/go-ceph` for librados (cgo)
- `github.com/go-sql-driver/mysql` for MariaDB/MySQL
- `github.com/emersion/go-smtp` (LMTP server and SMTP client), `go-imap` v2, `go-message`
  (MIME parsing)
- `net/http` with the standard mux for the REST API; `log/slog` for logging;
  `prometheus/client_golang` for metrics

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

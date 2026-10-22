# Dovetail

Dovetail is a new, massively scalable and modern mail platform built to replace Dovecot. The
name is the joint that fits two pieces of wood together: Dovetail fits onto Postfix, and it is
the successor of Dovecot.

It is **not** an MTA and does not replace Postfix: Postfix receives and filters mail from the
internet and hands it to Dovetail over LMTP. Outbound mail is handed back to a configurable
SMTP relay host.

License: none for now, internal use only.

# Features

## Scalability is the governing principle

Dovetail is designed for a million mailboxes, each of which may hold 150k messages and
10+ GB spanning decades. Every design decision is weighed against that: the number of RADOS
objects per message, the number of omap keys per object (Ceph warns above 200k and one
object means one OSD serialising all of it), round trips per delivery, and whether any
request has to read a whole folder. Concretely:

- One RADOS object per message (plus deduplicated attachments); no raw copy of the message
- Per-mailbox maps are sharded; folder indexes are sharded by UID range; no map grows with
  the number of messages on a single object
- Folder listings are newest-first by construction, so the first page of any folder is one
  omap read, and nothing loads a whole folder into memory for a page
- Each component (LMTP, IMAP, POP3, JMAP, admin, worker) runs as its own process and scales
  independently; all state is in RADOS

**Storage is cheap, I/O is expensive.** Every RADOS round trip is counted. The write path
for one delivered message is three sequential round trips on the critical path: the root
transaction (UID, modseq, counters and the change-log entry in one write), the message
object written concurrently with the thread-map entries, and the folder index entry. Reads
that would repeat per message (domain, account, alias, mailbox, root for the quota check)
are cached per process for a couple of seconds. Audit records are batched into one omap
write per object per second. Notifications and search events are off the critical path, and
search events are not written at all until a search backend consumes them. Protocol
adapters apply what they just wrote themselves instead of reading it back.

Measure on the lab cluster (see `TESTENV.md`) before and after storage changes. Reference
numbers on the lab cluster (3 small OSDs): one LMTP delivery or IMAP APPEND costs about 15 ms
sequentially, an mbsync push of 3000 messages (60 MB) takes under a minute.

## Stateless and scalable, everything in Ceph RADOS

The daemons that run this platform are stateless, allowing them to scale out. **All persistent
state lives in Ceph RADOS**: messages, attachments, mailbox indexes, task queues, and also the
identity data (domains, mailboxes, accounts, aliases, tokens). There is no SQL database. Ceph
is the only stateful dependency, so backups, replication and HA follow Ceph.

This is the same approach the Ceph RADOS Gateway (RGW) takes for its users, buckets and
metadata: direct object reads for lookups by key, exclusive create for uniqueness, omap for
listings, and a reconciliation process that repairs listings after a crash.

Redis (or similar) is only introduced if a concrete need appears; it is not part of the base
design.

## One binary, multiple modes

The platform ships as a single Go binary, `dovetail`, that can run in one of several modes.
Each mode is a separately scalable component:

- `lmtp` – inbound delivery from the MTA
- `imap` – IMAP server for legacy clients
- `pop3` – POP3 server for legacy clients
- `jmap` – JMAP (mailbox API) with the OAuth token and self-service endpoints clients need
  next to it; user-facing
- `admin` – the admin REST API; operator-facing, on its own listener
- `postfix` – socketmap lookup service for Postfix (domains, recipients, aliases)
- `worker` – background task execution (trash purge, attachment garbage collection, quota
  recalculation, mailbox removal, reconciliation, …)
- `dav` – CalDAV/CardDAV (future)

Modes can be combined in one process for small setups and split across many processes for
large ones. The binary also has an `admin` subcommand for bootstrapping and offline
maintenance that talks to RADOS directly (like `radosgw-admin`).

## Language

The daemon is written in Go. cgo is accepted (needed for librados). Target the latest stable
Ceph release.

## Internal architecture: a Go core with protocols on top

Dovetail is built as a **library first, daemon second**. All functionality lives in Go
packages with clean interfaces, and every protocol is a thin adapter that translates wire
format into calls on those packages. Nothing that matters is implemented inside a protocol
handler, so new protocols and tools (a CLI, a migration tool, a future JMAP extension, DAV,
a test harness) are built by calling the same Go APIs.

Layers, from the bottom up:

1. **`rados` layer** – a small interface over librados (get/put/omap/xattr/lock/watch/notify,
   atomic write transactions) with two implementations: real librados and an in-memory mock
   used by all unit tests.
2. **Stores** – typed persistence packages on top of the rados layer, one per concern:
   identity (domains, mailboxes, accounts, aliases, tokens), mailbox index, message store,
   attachment store, change log, audit, sessions, task queue. These know object layouts; nothing
   above them does.
3. **Services** – the business logic, exposed as Go interfaces: `Auth` (passwords, tokens,
   sessions, TOTP), `Delivery` (parse, dedup, store, index, quota, spam routing), `Mailbox`
   (folders, messages, flags, move/copy/expunge, search, changes, retention, hold), `Submit`
   (outbound mail), `Admin` (identity CRUD, maintenance, export/import), `Audit`, `Notify`
   (watch/notify fan-out). Every call takes a context carrying the actor, client IP and
   protocol so auditing and authorisation are done once, here, not per protocol.
4. **Protocol adapters** – LMTP, IMAP, POP3, JMAP, the REST endpoints (OAuth, self-service,
   admin), Postfix socketmap, and the `admin` CLI. Each is a separate package that depends only
   on the service interfaces.
5. **`cmd/dovetail`** – wires config, stores, services and the selected modes together.

Rules that follow from this:

- A protocol adapter never touches a store or RADOS directly.
- Authorisation (roles, hold state, enabled flags) and audit live in the services, so every
  protocol gets them for free and cannot bypass them.
- The service interfaces are the public Go API of the project and are versioned with care;
  external programs may import them.
- Each layer is unit-tested against the mock of the layer below it.

## Modular and tested

The code is modular and each module contains extensive tests. For now tests use an in-memory
mock of RADOS; real integration tests against a live Ceph cluster come later.

## Observability

Structured logging and metrics everywhere. Every component exposes Prometheus metrics and logs
structured (JSON) output. Every log line and audit record about a message carries its
`Message-ID` header (delivery, submission, purge, raw fetch), next to Dovetail's own message
uuid, so a message can be followed from the MTA logs through the platform. Each process serves `/metrics`, `/healthz` (process up) and `/readyz`
(Ceph reachable) on a dedicated management listener.

## Containers

The daemon can run inside containers, but this is not mandatory.

## Protocols

Inbound e-mail is delivered using LMTP: an MTA like Postfix receives and filters the message
from the internet before delivering it to this daemon. One recipient per LMTP transaction for
now (one recipient address may still fan out to several mailboxes through aliases).

Supported protocols:

- LMTP (inbound)
- **JMAP** (primary interface): the standard JSON mail API (RFC 8620 core, RFC 8621 mail)
- IMAP: IMAP4rev1 plus IDLE, UIDPLUS and MOVE to start; more extensions later as needed
- POP3: serves INBOX only; UIDL is derived from the folder UID and UIDVALIDITY; DELE is lazy
  (see Retention)
- Postfix socketmap (lookups for the MTA)

IMAP and POP3 must work but are not the primary target; they serve legacy clients. Each
mailbox is fully accessible through JMAP, which is the basis for a modern webmail and a mobile
app. Dovetail does not invent its own mailbox API: JMAP already covers mailboxes, messages,
threads, search, flags, blobs, sending, quotas and push, and its newer extensions cover
contacts and calendars.

## Postfix integration

Postfix must know which domains and recipients Dovetail accepts so it can reject unknown
recipients at SMTP time instead of bouncing after LMTP. Since there is no SQL database for
Postfix to query, Dovetail's `postfix` mode serves a **Sendmail-style socketmap server**, which
Postfix supports natively (`socketmap:inet:host:port:name`, read-only lookups). Maps offered:

- `domains` – for `virtual_mailbox_domains`: is this domain known and enabled
- `mailboxes` – for `virtual_mailbox_maps`: does this address resolve to a mailbox
- `aliases` – for `virtual_alias_maps`: alias expansion (optional; Dovetail can also expand
  aliases itself at LMTP time)

The socketmap protocol is unauthenticated, so the listener is meant for a private network
only. Lookups are plain object reads in RADOS and are cached briefly in the process.

Example Postfix configuration (`main.cf` snippets for `virtual_mailbox_domains`,
`virtual_mailbox_maps`, `virtual_alias_maps` and the LMTP transport) lives in this repository
under `contrib/postfix/`. No glue code is needed: Postfix speaks socketmap natively.

## Sending mail

Sending is in scope. JMAP `EmailSubmission` accepts outbound messages and submits them over
SMTP to a configurable relay host (typically Postfix), optionally with SASL credentials. The
platform does not queue or retry outbound mail itself beyond the submission; that is the
MTA's job. If the relay rejects or is unreachable the API returns an error and nothing is
stored. A copy of every sent message is stored in the Sent folder, going through the same
parse-and-store path as inbound mail. Arbitrary messages can be added to a folder with JMAP
`Email/import` or `Email/set` (drafts), and IMAP APPEND.

The `From` address (and `Sender`, if present) must be one of the account's identities: its
own address and the aliases that point at its mailbox. Submissions with another `From` are
rejected. Dovetail adds its own `Received:` header naming the authenticated account so the MTA
logs can be tied to the audit log.

## Message size limit

A configurable maximum message size is enforced at LMTP, JMAP blob upload, `Email/import` and
`EmailSubmission`. Postfix is expected to enforce its own limit first.

## TLS

TLS is not handled by this daemon; a proxy like HAProxy does this.

## Client IP passthrough

Because HAProxy sits in front, the daemon would only ever see the proxy's address. HAProxy
forwards every connection with the PROXY protocol, so all listeners (IMAP, POP3, LMTP and the
HTTP API) accept PROXY protocol v1 and v2 and take the real client address from it. PROXY
protocol can be required per listener so a connection without the header is rejected.
"Optional" mode (header parsed if present) delays the banner of server-speaks-first protocols
(IMAP, POP3, LMTP) by a short wait for every client that sends no header, so it is only for
mixed setups; use "required" behind HAProxy and "off" for direct clients. As an
optional fallback the HTTP listeners can trust `X-Forwarded-For` from configured proxy
addresses. The real client IP is used in logs, audit records and failed-login records.

## Auditing

Every action on a mailbox is audited so that it can be inspected afterwards: who did what,
from where, and when. An audit record contains:

- Timestamp
- Mailbox UUID
- Account username and, when a token was used, the token id and its comment
- Protocol (`imap`, `pop3`, `jmap`, `lmtp`, `admin`)
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

Admin actions (support access, maintenance, and every create/update/delete of identity data)
are audited the same way, with the admin account as the actor and the target mailbox, account
or domain as the subject. Identity changes are additionally logged in a platform-wide audit
object (`audit/admin/<YYYY-MM-DD>`), since they are not tied to one mailbox. Read-heavy
actions (fetch, list) are audited by default but can be reduced to session-level
(login/logout) per protocol in the config to limit volume.

## Brute-force protection

Dovetail does not rate-limit or lock out failed logins; stateless daemons have no shared
counters. This is left to HAProxy (and tools like fail2ban on the structured logs). Every
failed authentication is logged with the client address so those tools have what they need.

## IPv6-first

Assume an IPv6-only environment. IPv4 is nice to have, not mandatory.

# Storage: Ceph RADOS

Everything is stored in Ceph RADOS objects: not S3/RGW, pure RADOS via librados. Four pools:

- **messages** – parsed message JSON and raw skeletons
- **attachments** – deduplicated attachment objects
- **index** – mailbox roots, folder indexes, change logs, audit, sessions, task queues
- **meta** – identity data (domains, mailboxes, accounts, aliases, tokens), split into RADOS
  namespaces like RGW's metadata pool

## Message document

A raw e-mail message is parsed into a JSON document which is the one RADOS object per
message (`<mailbox-uuid>/<message-uuid>` in the messages pool):

- A schema version field, so the format can evolve
- Headers as a JSON array (parsed, decoded to UTF-8)
- The **original, unparsed header block kept verbatim**, including every `Received:` line,
  `Message-ID`, DKIM signatures and all other trace headers, so forensics can follow where a
  message came from
- Body as HTML and plain text (both derived if only one exists), converted to UTF-8
- The full MIME structure (part tree with content types, dispositions, Content-IDs, sizes),
  with the decoded content of small non-body parts inline and references to attachment
  objects for the rest
- The xattrs `mailbox`, `created` and `loc` (the folder/UID the index placed it at)

**What is an attachment:** every MIME part that is not the primary text/plain or text/html
body (including inline images referenced by Content-ID and any message/rfc822 part) is an
attachment. Parts smaller than a configurable threshold (default 4 KiB) stay inline in the
document rather than becoming tiny RADOS objects.

## No raw copy: messages are rendered

The original bytes of a message are **not stored**. IMAP `BODY[]`, POP3 `RETR`, JMAP blob
downloads and Maildir exports serve a message **rendered from the document**: the verbatim
original headers, followed by the MIME tree re-encoded deterministically (UTF-8 bodies,
base64 attachments, the original boundaries). The rendering is stable, and its length is
the size the index records and the protocols report. Forensic needs are met by the verbatim
header block (`Received:` trace, `Message-ID`, DKIM) plus the Message-ID in every log and
audit line about the message. What is lost is byte-exactness of the body: client-side DKIM
verification will fail (the MTA verified on arrival), which is accepted.

The exception is **signed mail**: `multipart/signed` (PGP/MIME, S/MIME) only verifies on the
exact bytes, so for those the original is kept next to the document
(`<mailbox-uuid>/<message-uuid>.raw`). `mail.keep_raw` selects `never`, `signed` (default)
or `always`.

## Attachments

Attachments are extracted and stored as separate RADOS objects in their own pool. They are
deduplicated **cluster-wide** by content hash, so the same attachment is referenced from many
messages across many mailboxes. This requires reference counting: an attachment object is only
removed by a background worker once no message references it.

**Reference counting design:** the attachment object is named by its content hash. Its omap
holds one key per referencing message (`ref/<mailbox>/<message>`), so adding and removing a
reference are idempotent omap operations and concurrent deliveries of the same attachment
never race on a counter. A worker deletes an attachment only when its reference set is empty
and the object has not been touched for a grace period. Creation uses RADOS exclusive-create
so two simultaneous first uploads of the same attachment resolve cleanly.

## Crash safety

Each message object carries its mailbox UUID, creation time and location in xattrs so an
orphan-sweep worker task can find and remove objects that never reached an index. LMTP only
acknowledges after the index write succeeds. See "Mailbox index" for the write order.

## Object naming

Mail data:

- Message document: `<mailbox-uuid>/<message-uuid>` in the messages pool (xattr `loc` =
  `<folder-id>/<uid>`); kept original bytes, only for signed mail:
  `<mailbox-uuid>/<message-uuid>.raw`
- Attachment: `<sha256-hex>` in the attachments pool
- Mailbox root: `<mailbox-uuid>` in the index pool (folders, counters, modseq; **no omap**)
- Folder index shards: `<mailbox-uuid>/<folder-id>` for UIDs below 65536, then
  `<mailbox-uuid>/<folder-id>.<n>` per further block of 65536 UIDs
- Thread and Message-ID maps: `<mailbox-uuid>/t.<xx>`, 256 shards by hash of the key
- Change log: `<mailbox-uuid>/changes` in the index pool, omap keyed by modseq (bounded)
- Retention set (expunged and trashed messages waiting for purge): `<mailbox-uuid>/retention`
  in the index pool, omap keyed by purge-at timestamp
- Audit: `<mailbox-uuid>/audit/<YYYY-MM-DD>` and `audit/admin/<YYYY-MM-DD>` in the index pool
- Sessions: `sessions/<account-id>` in the index pool; signing keys: `keys/jwt`; TOTP replay
  state: `totp/<account-id>`
- Task queues: `queue/<task-type>` in the index pool

Per message this is **one object plus its deduplicated attachments**, one omap entry in its
folder shard and one in its thread shard. Nothing else grows with the message count.

## Mailbox index

RADOS has no listing or query capability, so each mailbox needs an index: folders, message
UIDs, flags (seen, answered, deleted, …), sizes and dates. The index lives in RADOS using omap
(the sorted key-value map every RADOS object carries, with atomic compare-and-set and prefix
listing), laid out so that no object's omap grows beyond a bounded size:

- One **mailbox root object** per mailbox UUID. Its data is a small JSON with the collection
  list (folders now, calendars and address books later), per-folder UIDVALIDITY and UIDNEXT
  counters, the mailbox-wide `modseq` counter and quota usage totals, advanced with atomic
  RADOS write transactions so concurrent processes never hand out the same UID or modseq. It
  carries no per-message omap entries.
- **Folder index shards.** Each folder's records live in omap objects of at most 65536 UIDs.
  Keys are the inverted UID, so plain ascending omap iteration yields the newest message
  first: the first page of a folder is a single read of a single object, however large the
  folder. Values are compact (message uuid, thread id, flags, size, dates, modseq, subject,
  from, has-attachments; ~150 bytes). A full-folder walk (IMAP SELECT, expunge, recalc)
  reads shards of at most 65536 entries each.
- **Message lookup by id** (JMAP ids are message uuids) reads the `loc` xattr of the message
  object; moves update it. No per-message map on the root.
- **Thread and Message-ID maps** are sharded over 256 objects per mailbox by key hash:
  `m/<message-id>` → thread id (for threading new deliveries) and `t/<thread-id>/<uuid>` →
  location (JMAP `Thread/get`).
- Flags are the IMAP system flags plus arbitrary keywords; the folder's keyword list lives in
  the root. IMAP SUBSCRIBE state is per mailbox (shared by all accounts on it), as are flags.
- Rebuilding a folder index (admin maintenance) assigns a new UIDVALIDITY so clients resync.
- The index pool is small and hot and can be placed on fast storage with its own replication
  settings.

**Change tracking.** JMAP's `/changes` methods (and IMAP CONDSTORE/QRESYNC later) need every
mailbox to have a monotonically increasing modification sequence and a change log. The
mailbox root object keeps the `modseq` counter, each folder index record stores the modseq of
its last change, and a bounded change log (`<mailbox-uuid>/changes`, omap keyed by modseq)
records created/updated/destroyed ids. Clients with a state older than the log's horizon get
`cannotCalculateChanges` and resync.

**Delivery writes**, in order: attachments (idempotent, deduplicated), the root transaction
that allocates UID and modseq and updates counters, the message object at its final
location, the folder index entry (the commit point), the thread map entries, the change log.
A crash before the index entry leaves an orphan object for the sweep but never a dangling
entry.

Limitations accepted for now: no cross-mailbox queries, and search is limited to header
matching over the index until the full-text search component exists.

## Full-text search

Full-text search will be required in the future, so the design plans for it from the start:

- Search is behind a Go interface with a pluggable backend, exposed through JMAP
  `Email/query` filters and `SearchSnippet`. The first backend is the simple header matcher
  over the folder index; a real engine (for example OpenSearch, Meilisearch or an embedded
  Bleve index) is added later as an optional, separately scalable component.
- The parsed message JSON already contains the plain-text body and the attachment list, so an
  indexer never has to re-parse raw mail.
- Every delivery, flag change, move and deletion publishes an event through the worker task
  queue (`index-message`, `remove-message`), so a search backend can be populated and kept in
  sync by workers. Until a backend is configured these events are no-ops.
- Re-indexing a whole mailbox is a worker task, so a search backend can be introduced or
  rebuilt for existing mail.

## Folders

Mailboxes have folders. The default set, created when a mailbox is first used, is INBOX,
Drafts, Sent, Junk, Trash and Archive, each tagged with its IMAP SPECIAL-USE attribute (RFC
6154). Users can create nested folders. Folders are identified internally by an id, not by
name, so renaming does not move data.

## Mailbox provisioning

Mailboxes are created through the admin API (which writes the mailbox object in the meta
pool). The index side (root object, default folders) is created lazily the first time a
mailbox receives mail or an account logs in.

## Change notification (IMAP IDLE, JMAP push)

Daemons are stateless and share nothing, so a process serving an IMAP IDLE session or a JMAP
push connection must learn about deliveries done by another process. RADOS watch/notify is
used for this: an IMAP or API process watches the mailbox root object, and any process that
changes a folder index sends a notify on that root object. This gives IDLE and JMAP
`StateChange` push without Redis.

## Threading

The folder index record stores `Message-ID`, `In-Reply-To` and `References` so JMAP `Thread`
and conversation views work, and IMAP can offer THREAD later, without fetching messages.

## Retention

Deleted messages go to Trash and are purged after a configurable trash period by a background
worker.

IMAP EXPUNGE (and POP3 DELE) never destroys data immediately. An expunged message is removed
from its folder index and parked in the retention set of the mailbox, where it stays for the
same retention period before the worker purges it (lazy expunge, as Dovecot calls it).
Destroying a message from Trash via JMAP behaves the same way.

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
- Remove all data of a deleted mailbox
- Maildir export and import of a mailbox
- Purge audit objects older than the audit retention period
- Orphan sweep (message objects that never reached an index)
- Identity reconciliation (repair domain/mailbox/account listings)
- Search indexing events (no-op until a backend is configured)

Periodic tasks are enqueued by a scheduler role that the workers elect among themselves with
a RADOS lock on a `scheduler` object, so exactly one worker schedules and any worker executes.

**Queue design:** a RADOS queue object per task type in the index pool, with each task as an
omap entry and a lease via a RADOS lock, so multiple workers can run without duplicating
work. Tasks carry a run-after time; failed tasks are retried with backoff and after a
configurable number of attempts land in a dead-letter set visible in the admin API. If this
proves clumsy, Redis becomes the concrete need.

## Legal hold (read-only lockdown)

A mailbox can be placed on **hold** through the admin API (or the `admin` CLI), for example
when law enforcement requires preservation. A hold is recorded on the mailbox object with the
reason, a case reference, who set it and when, and is audited. While a mailbox is on hold:

- Every mutating operation by the user is refused on every protocol: flag changes, moves,
  copies, deletes, expunge, folder create/rename/delete, imports and drafts. JMAP returns
  `forbidden` state errors, IMAP reports the folders as read-only (`[READ-ONLY]`), POP3 DELE
  is refused.
- **Nothing is purged.** Trash retention, lazy-expunge retention and audit retention are
  suspended for the mailbox, and the mailbox cannot be deleted. Attachment GC is unaffected
  because references stay in place.
- Inbound delivery continues by default so evidence keeps being collected; it can be switched
  off per hold. Sending from the mailbox is refused by default and can be allowed per hold.
- User login and reading remain possible by default (the user may not be aware of the hold);
  a hold can also deny all user access (`sealed`).
- Removing a hold is an admin action and is audited. Purges resume from that point with the
  normal retention periods counted from the original deletion times, so a long hold does not
  cause an immediate mass purge: after release, items past their retention are purged with a
  configurable grace period.

Hold state is checked in the `Mailbox` service, so every protocol and future protocol
enforces it automatically.

## Mailbox export and import (Maildir)

A complete mailbox can be exported, for legal requests, migration and backup:

- **Format: Maildir++** in the Dovecot layout, so the export is directly usable by Dovecot
  and by standard tools: `cur/new/tmp` per folder, subfolders as `.Folder.Sub` directories,
  `subscriptions`, IMAP flags in the filename info (`:2,FRST`), keywords via
  `dovecot-keywords`, internal date as the file mtime, UIDs preserved via `dovecot-uidlist`.
- Each message is the byte-exact original, rebuilt from the raw skeleton and attachments.
- Alongside the Maildir, a `manifest.json` with mailbox id, folders, per-message UID/flags/
  object names and a SHA-256 per file, the mailbox's audit log for the requested period, and
  the hold record if any, so a chain of custody can be documented.
- The output is a tar archive (optionally gzip). `dovetail admin export-mailbox <uuid>` writes
  it to a path; the admin API creates an export task that a worker executes, writing to a
  configured export directory, with progress and the result downloadable through the admin
  API. Exports are audited.
- mbox can be added later as a second format; Maildir is the primary one.

The reverse, **import** of a Maildir into a mailbox (`dovetail admin import-mailbox` and an
admin API task), is planned as the migration path from Dovecot: messages go through the normal
parse-and-store path, flags, keywords, folders and (where possible) UIDs are preserved.

## Quotas

Each mailbox has quotas:

- Soft and hard limit on total size
- Soft and hard limit on message count

Exceeding a soft limit produces warnings (logged, exposed via JMAP Quotas); exceeding a hard
limit rejects delivery at LMTP time with a permanent error (5.2.2) by default, configurable to
a temporary error so the MTA keeps retrying.

Limits are stored on the mailbox object in the meta pool and set through the admin API; a
config-wide default applies when the mailbox has none. Usage counters (bytes and message
count) live in the mailbox root object in the index pool and are updated atomically with every
delivery and deletion; a worker task can recalculate them from the folder indexes.

# Domains, Mailboxes, Accounts

All identity data is created, changed and removed through the **admin API** (or the `admin`
CLI) and stored in the meta pool. There is no external database.

## Domains, aliases and catch-all

Domains are first-class: a domain has a name, an enabled flag and settings. Delivery to an
address in an unknown or disabled domain is rejected, and Postfix sees the same answer through
the socketmap.

**Aliases** map a source address to one or more destinations: a mailbox or another address
(which is resolved again, with a bounded depth to stop loops). With several destinations LMTP
delivers to each mailbox. Aliases that point at a mailbox are also the identities its accounts
may send as.

A **catch-all** is a per-domain fallback: the alias `@<domain>`, used when no account or alias
matches the exact address.

Resolution order at LMTP time (and in the Postfix socketmap): exact account address → alias →
catch-all → reject.

## Mailboxes

A **mailbox** has a UUID and owns folders and messages (and later calendars and address
books). A mailbox by itself is not addressable. It carries its quota limits and a **state**:

- `active` – delivery and user access work.
- `suspended` – soft. LMTP answers `450 4.2.1`, so Postfix keeps the mail queued and
  retries for its configured queue lifetime; users are locked out; sending is refused. For
  payment holds, migrations and maintenance: nothing is lost.
- `disabled` – hard. LMTP answers `550 5.2.1`, and the Postfix socketmap reports the
  recipient as unknown so mail is rejected during the SMTP dialogue (no backscatter
  bounces); users are locked out.

Suspending or disabling requires a reason, which is stored with the state and audited.
Accounts have their own `enabled` flag that only blocks that credential; it does not affect
delivery to the mailbox.

## Accounts

**Accounts** are linked to a mailbox. Multiple accounts can point to the same mailbox,
allowing multiple credentials to access one mailbox. An account consists of a username
(usually the e-mail address), zero or more passwords and optionally a TOTP secret. Every
password is marked primary or application, carries a **comment** (what it is for) and an
optional **expiry** (null = never): a helpdesk can hand out a temporary password that stops
working by itself, next to the permanent one. Expired passwords are dropped the next time a
password is set.

**Lifecycle, by design.** An account belongs to its domain (a domain cannot be removed while
it still has accounts or aliases) and points at a mailbox; the mailbox owns the data and
knows nothing about names. Removing an account, or a whole domain, leaves its mailboxes
**orphaned** with all their mail intact; attaching a new account to an orphaned mailbox is how
an address is renamed or moved to another domain without touching the data. Orphaned
mailboxes are listed with `dovetail-admin mailbox list --orphaned` (`GET
/v1/admin/mailboxes?orphaned=true`); a mailbox is only ever deleted explicitly. Accounts have an enabled flag; a disabled account cannot log in,
while the mailbox state (see above) decides what happens to inbound mail.

## Roles

Accounts and API tokens carry a set of roles. The role set is:

- `read` – list folders, read messages and attachments, search (JMAP get/query/changes)
- `write` – change flags, move/copy/delete messages, manage folders, import messages (JMAP
  set)
- `send` – submit outbound mail (JMAP `EmailSubmission`)
A normal full-access account has `read`, `write` and `send`. Accounts cannot carry admin
roles; see "Admin tokens". A token's roles may only narrow
the account's roles; a token claiming a role the account lacks is rejected.

## Self-service

Because Dovetail owns the identity data, an authenticated account can manage its own
credentials through the REST endpoints: change passwords, create and revoke application
passwords, enrol and disable TOTP (with recovery codes), create and revoke its own API tokens,
and list and revoke its sessions. Every such change is audited.

## API tokens

An account may have no password at all and only API tokens, so applications can connect
without a password. Each API token carries its own roles, which may be narrower than the
account's. Every API token **must** have an expiration date and a comment describing its
purpose; tokens without either cannot be created. Expired tokens are rejected. Tokens are
random, prefixed `dvt_` so they are recognisable in logs and secret scanners, and stored only
as a SHA-256 hash (`tokens/<hash>`); the plaintext is shown once at creation.

## Admin tokens

Admins are not accounts and are never mixed with mail users. An admin is an **API token**
with a comment, an expiration date and one role:

- `admin` – full administration
- `admin-ro` – read-only: every `GET` on the admin API (listings, inspection, audit, queue
  status), for monitoring and reporting jobs that must never change anything

Admin tokens live in the meta pool next to account tokens but belong to no account and no
domain; they are managed through the admin API (`/v1/admin/tokens`) or `dovetail-admin token`.
The first one is minted with `dovetail admin bootstrap`, which talks to RADOS directly and
refuses once admin tokens exist; `dovetail admin token` mints further ones (including
read-only) for recovery. The admin API accepts bearer tokens only; there are no admin
passwords or sessions.

## Password hashing

Passwords are stored hashed with a scheme prefix, Dovecot-style, so multiple schemes coexist
and imported Dovecot hashes keep working: bcrypt, argon2, SHA512-CRYPT and the other common
Dovecot schemes. New passwords are hashed with argon2id by default.

## API authentication: sessions and tokens

Interactive clients (webmail, mobile app) never send the password on every request. They log
in once and receive a session, following OAuth 2.0 (RFC 6749, bearer tokens RFC 6750) and the
OAuth 2.0 Security Best Current Practice (RFC 9700, January 2025):

- **Login** at the OAuth token endpoint (`POST /v1/oauth/token`) with username, password, an
  optional narrower scope (subset of the account's roles) and a client/device name
  ("Wido's phone"). The response is an **access token** and a **refresh token**.
- **Access token:** a short-lived (default 15 minutes) signed JWT (RFC 7519, EdDSA/Ed25519)
  carrying account id, mailbox id, roles and session id. Every daemon verifies it locally
  with the public key, so no lookup is needed per request and the daemons stay stateless.
- **Refresh token:** an opaque random string, long-lived (default 30 days, sliding), stored
  hashed in RADOS (`sessions/<account-id>` in the index pool, one omap entry per session). It
  is **rotated on every use** with reuse detection (RFC 9700 §4.14): presenting an already
  rotated refresh token revokes the whole session.
- **Revocation:** `POST /v1/oauth/revoke` (RFC 7009) ends a session. Users can list and
  revoke their own sessions; admins via the admin API. Revoking a session stops refresh
  immediately; an outstanding access token lives at most its 15 minutes. Disabling or
  deleting an account invalidates its sessions on next refresh.
- **Signing keys** are generated by Dovetail, stored in RADOS (`keys/jwt` in the index pool)
  and rotated on a schedule; daemons cache the current and previous public key. Rotating keys
  is also the "log everyone out" switch.
- **Session metadata** (client name, creation time, last use, last client IP) is kept with
  the session and shown in the session list, and every login, refresh and revoke is audited.
- The token endpoint and the API are shaped as a standard OAuth 2.0 authorization server so
  that an **authorization code + PKCE** flow with a hosted login page (RFC 7636, RFC 8252 for
  native apps) and login via an external OpenID Connect provider can be added later without
  changing clients that already use bearer tokens.

**Long-lived API tokens** (above) remain for applications and integrations that cannot do an
interactive login. They are presented the same way, as a bearer token.

**HTTP basic auth** with username and password is also accepted on JMAP for scripts and curl,
but clients are expected to use sessions. Basic auth is refused for passwords that require
TOTP.

## Two-factor authentication (TOTP)

Accounts can require a time-based one-time password (TOTP, RFC 6238) with their password:

- The TOTP secret (and its parameters: digits, period, algorithm) is stored on the account
  object. Whether TOTP is enabled is simply whether a secret exists. Enrolment (secret
  generation, QR provisioning URI, confirmation with a first code) and recovery codes are
  handled by the self-service endpoints; admins can disable TOTP for an account.
- Each password has a kind: **primary** or **application**. When an account has a TOTP
  secret, a primary password is only accepted together with a valid code; application
  passwords never require a code (that is what they are for).
- At the token endpoint the client sends the code in a `totp_code` field. If the password is
  correct but the code is missing, the response is the error `totp_required` so the client
  can prompt for it; a wrong code is a failed login and is audited as such.
- IMAP and POP3 cannot carry a code. On those protocols a primary password that requires TOTP
  is rejected; clients must use an application password or OAUTHBEARER with a session token.
- Codes are verified with a configurable clock skew window (default one period either way).
  Replay of the same code within its window is rejected; the last used time step is recorded
  per account in RADOS (`totp/<account-id>` in the index pool).

## Credentials per protocol

- IMAP and POP3 authenticate with username and a password (any of the account's passwords,
  including application passwords) via `LOGIN`/`PLAIN`, **or** with an access token via the
  SASL `OAUTHBEARER` mechanism (RFC 7628, plus Google-style `XOAUTH2` for client
  compatibility). This lets a mobile app that already holds a session use IMAP without ever
  storing the password.
- JMAP and the REST endpoints authenticate with a bearer token (session access token or
  long-lived API token) or HTTP basic auth.

## LMTP delivery

When a message is delivered (by Postfix) the platform resolves the recipient address
(account → alias → catch-all), checks that domain, mailbox and quota allow delivery, stores
the message in RADOS, updates the mailbox index and change log, and acknowledges delivery.

## Future: CalDAV and CardDAV

Calendars and contacts will be needed later, served over CalDAV and CardDAV and over JMAP
Contacts (RFC 9610) and JMAP Calendars. The design leaves room for them now:

- A mailbox is the storage container for everything a user owns, not just mail. Its root
  object lists **collections** with a type (`mail-folder` now; `calendar` and `addressbook`
  later), so calendars and address books hang off the same mailbox UUID with the same quota,
  audit and retention machinery.
- Object name prefixes `<mailbox-uuid>/cal/` and `<mailbox-uuid>/card/` are reserved.
- The role set can grow with `calendar` and `contacts` roles.
- A future `dav` mode serves CalDAV/CardDAV from the same binary; JMAP Contacts and JMAP
  Calendars are added to the `api` mode at the same time.

Nothing of this is built in the first milestones, but nothing in the mail design may block it.

## Out of scope

Shared folders between mailboxes and IMAP ACLs. Encryption at rest and compression are handled
by the Ceph pools (BlueStore compression and encryption), not by Dovetail.

# APIs

## Mailbox API: JMAP

The mailbox API **is JMAP**. Rather than designing a custom REST API, Dovetail implements the
IETF standard that was built for exactly this purpose (webmail and mobile clients) and is
served by Fastmail, Cyrus, Stalwart and others:

- RFC 8620 JMAP core: session resource at `/.well-known/jmap`, batched method calls, state
  strings and `/changes` for efficient sync, blob upload/download, push via EventSource
- RFC 8621 JMAP for Mail: `Mailbox`, `Thread`, `Email` (get/query/changes/set/import/parse),
  `SearchSnippet`, `Identity`, `EmailSubmission` (sending), `VacationResponse` (later)
- RFC 8887 JMAP over WebSocket (push and lower latency for mobile apps)
- RFC 9425 JMAP Quotas
- RFC 9404 JMAP Blob extensions
- Later: RFC 9610 JMAP Contacts and the JMAP Calendars specification, alongside CardDAV and
  CalDAV, so one API serves mail, contacts and calendars

How JMAP maps onto the design:

- JMAP `Mailbox` = folder; `Email` = message in the index plus the parsed JSON; `Thread` =
  the threading headers already kept in the index; `blobId` = attachment hash or message
  part; `Identity` = the account's address and the aliases pointing at its mailbox;
  `EmailSubmission` = the SMTP relay; `Quota` = the mailbox quota; keywords (`$seen`,
  `$flagged`, …) = IMAP flags.
- An `Email` lives in exactly one `Mailbox` (`maxMailboxesPerEmail` = 1). A copy (IMAP COPY)
  is a new `Email` with its own id; attachments are shared through dedup either way.
- JMAP `StateChange` push is driven by the RADOS watch/notify on the mailbox root object.
- JMAP authenticates with bearer tokens, which is exactly the session and API-token model
  above.

Because there is no mature JMAP server library in Go, the JMAP layer is written in this
repository as its own module, with the JMAP method handlers as the only consumer of the
mailbox storage interface besides IMAP and POP3.

## Account REST endpoints

A small custom REST surface, described by an OpenAPI specification in this repository, covers
what JMAP does not: the OAuth token and revoke endpoints, session listing and revocation, and
the self-service credential management (passwords, application passwords, TOTP, own API
tokens). Paths are versioned (`/v1/...`) and errors use a single JSON problem format.

## Admin API

A custom REST API (also in the OpenAPI specification), authenticated with an API token
belonging to an account with the `admin` role, for operating the platform:

1. **Domains:** create, update (enable/disable, settings, catch-all), delete, list
2. **Mailboxes:** create, update (quota limits, enable/disable), delete (schedules removal of
   all data), list, inspect (storage usage, message and folder counts, quota status, index
   health)
3. **Accounts:** create, update (roles, enable/disable, link to mailbox), delete, list by
   domain or mailbox, prefix search; set or reset passwords; disable TOTP; create and revoke
   API tokens on behalf of an account
4. **Aliases:** create, update, delete, list by domain
5. **Mailbox maintenance:** verify or rebuild the index, force quota recalculation, purge
   Trash now, re-index for search
5a. **Legal hold:** place a mailbox on hold (reason, case reference, options), release it,
   list mailboxes on hold
5b. **Export/import:** start a Maildir export or import task, track progress, download the
   result
6. **Worker and queue visibility:** pending and failed tasks, retry, dead-letter set
7. **Support access:** read a mailbox on behalf of a user, audited
8. **Platform status:** Ceph health, version, running instances, reconciliation findings
9. **Audit inspection:** query the audit history of a mailbox, an account or the admin log by
   time range, action, client IP
10. **Session management:** list and revoke sessions of an account; rotate signing keys

# Daemon

## Configuration

The daemon takes a single YAML file as configuration:

- Mode(s) to run and listen addresses (one listener per mode; JMAP and the admin API are
  separate listeners and separate code paths)
- Ceph cluster connection information
  - Monitors
  - Credentials
  - RADOS pools (messages, attachments, index, meta)
- Outbound SMTP relay host (and optional SASL credentials)
- Trash retention period
- Audit retention period and per-protocol audit detail level
- Trusted proxy addresses (PROXY protocol, X-Forwarded-For)
- Session settings: access token lifetime, refresh token lifetime, key rotation interval
- Spam header name and value for Junk routing
- Default quota limits, over-quota LMTP reply policy (suspended mailboxes always answer 4xx,
  disabled ones 5xx)
- Inline attachment size threshold
- Maximum message size
- Export directory for worker-run exports; post-hold purge grace period
- Management listener (metrics, health)
- Logging and metrics settings
- Redis information (only if it ever becomes needed)

Secrets (Ceph key, SMTP relay password) can be given inline, as a path to a file, or via
`${ENV_VAR}` substitution.

## Development backend

`ceph.backend: memory` runs the whole platform against the in-memory RADOS implementation
used by the tests: one process, no Ceph, nothing persisted. `dovetail serve --seed-account
user@example.org:password` creates a domain, mailbox, account and an admin on that backend at
startup, so a developer can speak LMTP, JMAP and the admin API to a fresh process immediately
(`make run-dev`).

## `dovetail admin` and `dovetail-admin`

`dovetail admin` talks to RADOS directly with the daemon's config file and does only what
cannot be done through the API because no token exists yet: `bootstrap` (first admin token)
and `token` (mint a full or read-only admin token). Everything else is done over
the admin API so the daemon's code base stays small.

`dovetail-admin` is the operator's tool: a Go client for the admin API (also importable as the
package `client/admin`) that covers every endpoint: domains, mailboxes, accounts, aliases,
tokens, sessions, legal hold, audit queries, queues, key rotation, identity export/import and
Maildir export/import (it packs a local Maildir directory and uploads it). It takes the admin
URL and token from flags, environment (`DOVETAIL_ADMIN_URL`, `DOVETAIL_ADMIN_TOKEN`) or
`~/.config/dovetail/admin.yaml`, and prints tables or JSON (`-o json`).

The admin listener serves its own OpenAPI document at `/openapi.yaml` (unauthenticated) so
other clients can be generated from the exact contract the running version implements.

## Key dependencies

- `github.com/ceph/go-ceph` for librados (cgo)
- `github.com/emersion/go-smtp` (LMTP server and SMTP client), `go-imap` v2, `go-message`
  (MIME parsing)
- `net/http` with the standard mux for JMAP and REST; `log/slog` for logging;
  `prometheus/client_golang` for metrics

# Status

## Implemented

- Configuration loading, single YAML file, `memory` backend for development
- RADOS abstraction with librados (cgo) and in-memory implementations
- Identity store in RADOS (domains, mailboxes, accounts, aliases, catch-all, tokens);
  `dovetail admin bootstrap`, identity export/import
- Admin service, REST admin API (domains, mailboxes, accounts, aliases, tokens, sessions,
  hold/release, audit queries, queue inspection, key rotation, export/import) with an
  OpenAPI spec that is tested against the registered routes
- Postfix socketmap service (`domains`, `mailboxes`, `aliases` maps)
- MIME parser: JSON document, attachment extraction with dedup, byte-exact raw skeleton
- LMTP delivery: resolution (account → alias → catch-all), domain/mailbox/hold/quota
  admission, spam routing to Junk, change log, notifications, audit
- Sessions: OAuth token endpoint (password and refresh grants), Ed25519 JWT access tokens,
  rotating refresh tokens with reuse detection, revocation, TOTP with recovery codes,
  self-service password/app-password/TOTP/session endpoints, signing-key rotation
- JMAP: core, `Mailbox/*`, `Email/*` (get/query/changes/set/import), `Thread/get`,
  `Quota/get`, `Identity/get`, `EmailSubmission/set` with onSuccess handling, blobs
  (upload/download, attachments, raw, parts), EventSource push
- IMAP4rev1 with IDLE, UIDPLUS, MOVE, SPECIAL-USE, NAMESPACE, UNSELECT, SASL PLAIN /
  OAUTHBEARER / XOAUTH2; POP3 with UIDL, TOP, SASL
- Outbound submission via SMTP relay with identity enforcement and Sent copies
- Legal hold (read-only / sealed, delivery and sending switches, purge suspension)
- Maildir++ export (tar, manifest, audit log) and import (directory or archive), as CLI,
  worker task and admin API
- Workers: trash/expunge retention purge, attachment GC, mailbox removal, quota
  recalculation, audit purge, orphan sweep, export; lock-elected scheduler
- Structured logging, Prometheus metrics, health endpoints, PROXY protocol on all listeners
- Unit tests for every package on the in-memory RADOS; no Ceph needed to run them

## Not yet

- IMAP CONDSTORE/QRESYNC (modseq is tracked; capability not advertised yet), JMAP over
  WebSocket, `Email/queryChanges`
- Identity reconciliation worker (listings are repaired only by the explicit tools)
- Full-text search backend (events are published; no consumer yet)
- Self-service API token management (tokens are admin-managed)
- Real integration tests against a Ceph cluster
- CalDAV/CardDAV and JMAP Contacts/Calendars

# Inspiration

When building this, look at:

- Dovecot – exists for years, has a lot of proper e-mail handling code
- WildDuck – https://docs.wildduck.email/ – has a full API
- Postfix – mainly the LMTP part
- Ceph RADOS Gateway – how to keep users, buckets and metadata in RADOS without a database
- Stalwart, Cyrus – JMAP server implementations

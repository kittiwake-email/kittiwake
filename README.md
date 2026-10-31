# Kittiwake

Kittiwake is a new, massively scalable and modern mail platform built to replace Dovecot. The
kittiwake is a small sea-going gull that nests on cliffs in colonies of hundreds of thousands:
a bird, like the dove it succeeds, that lives at sea (Ceph) and at scale. Kittiwake fits onto
Postfix and is the successor of Dovecot. Home: https://kittiwake.email

It is **not** an MTA and does not replace Postfix: Postfix receives and filters mail from the
internet and hands it to Kittiwake over LMTP. Outbound mail is handed back to a configurable
SMTP relay host.

**Status: proof of concept.** Kittiwake works and runs on a small lab, but it has not been
tested at large scale. People are welcome and encouraged to try it at real sizes and on real
workloads. If you do, please reach out to Wido den Hollander, by
[opening an issue](https://github.com/kittiwake-email/kittiwake/issues) or by mail to
wido@widodh.nl, with what you found.

License: GNU Affero General Public License v3.0 (see `LICENSE`).
Copyright 2026 Wido den Hollander. Anyone may use, study, change and redistribute Kittiwake
under the AGPL; anyone who runs a modified version for others over a network must offer them
its source. Other terms, including a commercial license, are available from the copyright holder.
See `ACKNOWLEDGEMENTS.md` for the people, tools and AI models behind Kittiwake.

# Features

## Scalability is the governing principle

Kittiwake is designed for a million mailboxes, each of which may hold 150k messages and
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
for a delivery includes private message/attachment writes, concurrent target-version reads,
a root compare-and-set that prepares a recoverable mutation, concurrent index target writes,
and a final root transaction that publishes counters, modseq and changes together. A cached
root can avoid the initial read; a conflict retries the plan against current state. Changes
confined to the root use one atomic write. This adds durable recovery work compared with the
old non-transactional path; current measurements are in the lab notes. Identity lookups are
cached for a few seconds, audit writes are batched, and notifications run off the critical path.

Measure on the lab cluster (see the `kittiwake-labs` repository) before and after storage
changes. Reference
numbers on the lab cluster (3 small OSDs): one LMTP delivery or IMAP APPEND costs about 15 ms
sequentially, an mbsync push of 3000 messages (60 MB) takes under a minute.

## Stateless daemons, mail data in Ceph, metadata in MariaDB

The daemons that run this platform are stateless, allowing them to scale out. Persistent
state is split by its nature:

- **Mail data lives in Ceph RADOS**: messages, attachments, mailbox indexes, change logs and
  audit logs, per mailbox, on the Ceph cluster the mailbox was placed on. This is the large,
  write-heavy part, and the part that is sharded over many clusters (see "Multiple Ceph
  clusters").
- **Metadata lives in MariaDB**: domains, mailboxes (state, quota limits, hold, placement),
  accounts, aliases, API and admin tokens, JWT signing keys, sessions, the worker queue and
  the registry of Ceph clusters. This is small, global and read-heavy; it needs uniqueness,
  transactions and listings, which a relational database gives for free. The schema is
  versioned: numbered migrations are embedded in the binary and applied by `kittiwake
  migrate`; a daemon refuses to start on a schema it does not know.

**Reads scale with replicas.** The database config takes a primary DSN for every write
and transaction and an optional `read_dsn`, typically a MariaDB replica on the same host as
the daemons, for SELECT-only operations: lookups, listings, token lookups, queue statistics,
audit reads. Every daemon host can run its own replica, so read capacity grows with the
number of hosts while a single primary takes writes. Ordinary replica reads may be stale
for the replication lag plus the cache TTL. Destructive workers and uncached mailbox-state
reads always use the primary. After a successful identity write, the process uses the
primary for `read_stickiness` (default 2 s); this is a best-effort read-after-write aid,
not a replication-lag bound. Background queue, heartbeat, audit and token-touch writes
do not extend identity-read stickiness.
Without `read_dsn` all queries use the primary.

The identity store is an interface with two backends. MariaDB is the production backend.
The RADOS backend (the RGW-style design: one object per record, exclusive create for
uniqueness, omap for listings) remains for development and tests on the memory backend, and
both run the same conformance suites. Nothing else is stateful: no Redis, no memcached.

## Node registry

Every process registers each mode it runs in a `nodes` table in the database: mode, the
address it is reached on (the `node.advertise` host, by default the machine's first global
address with IPv6 preferred, joined with the mode's listener port), hostname, version, the
namespaces it serves (empty: all), start time; it refreshes the row every `node.heartbeat`
(10 s) and removes it on orderly shutdown.
A node whose heartbeat is three intervals old counts as dead and is purged after a day.
This is the service discovery of the platform: daemons that need another internal service
(JMAP, IMAP and MCP looking for `search` nodes) read the live list from the database, cache
it, and pick a node by consistent hashing of the mailbox id so a mailbox keeps hitting the
node that holds its hot index; proxies pick the nodes of a namespace from it the same way
(by login name) and are themselves not registered, since they never write. For operators, `kittiwake-admin node list` and
`GET /v1/admin/nodes` show what runs where in which version. Override `node.advertise` when
the detected address is not the one other nodes can reach (dual-stack hosts, NAT, a name
that resolves differently per site).

## Multiple Ceph clusters

One Ceph cluster is a bottleneck long before a million mailboxes: the object count and the
omap load of 150k-message mailboxes have to be spread. Kittiwake therefore places **each
mailbox on one of several Ceph clusters**, all equal, and can grow by adding clusters, also
in other datacenters within a few milliseconds of each other.

- **Storage, not config.** A *storage* is one Ceph cluster registered as a row in the
  metadata database: name, `mon_host`, CephX user and key, the three data pools, and a
  placement flag. `kittiwake-admin storage add` hands the details to the admin daemon,
  which connects once, reads the cluster's **fsid** and verifies pool capabilities before
  storing it. A unique temporary object checks writes, xattrs and version assertions;
  index and attachment pools must also support OMAP. Connections repeat this probe
  before making storage available. Opening a pool alone is not sufficient verification.
  The daemon config carries no storage at all (only `ceph.config_file` and `ceph.options`
  as librados defaults for every connection).
- **Fsid is the identity.** Mailboxes reference their storage by its integer registry id
  (`storage_id`); the fsid guards the entry: a daemon that connects and finds another fsid
  than recorded refuses that storage, so a mistyped `mon_host` can never serve the wrong
  data.
- **Every daemon connects to every storage** it finds in the registry, re-reads the
  registry every 30 seconds (and at once when it meets a storage id it does not know), so a
  storage added or closed for placement is picked up without a restart. The management
  listener's `/storage` shows each storage's connection state.
- **Placement** of a new mailbox is weighted by measured raw cluster headroom above
  `ceph.placement_reserve` (15% by default), among enabled connected storage entries.
  Capacity refreshes with registry reloads; failed or more than two-minute-old readings
  exclude new automatic placement. `kittiwake-admin mailbox create --storage <id>`
  explicitly pins one. Existing mailboxes keep their storage. This is a raw-capacity
  signal, not an IOPS measurement or a guarantee about the fullest OSD, pool quota,
  CRUSH rule or erasure-coding overhead. Capacity and placement eligibility are exposed
  in storage status. Disable placement to drain a storage; deletion also requires no
  mailboxes, removal pins or pending attachment collection.
- **Where things live.** Every data store resolves a mailbox to its storage's pools
  through a cached identity read (the same cached record the state check already loads).
  Index, messages, attachments and the per-mailbox audit log sit on the mailbox's storage.
  Attachment deduplication is per storage. Garbage collection and sweeps walk every
  connected storage; a removal task for a deleted mailbox carries the storage id, since the
  record it would look up is gone. Platform-wide admin audit records live in the database.
- **Unavailable storage.** A mailbox whose storage is not connected from this process is
  served as temporarily unavailable: LMTP answers 4xx so the MTA queues, users get a clear
  error, the log names the storage.
- **Latency.** Serving a mailbox in another datacenter costs one round trip per sequential
  RADOS write; the write path is three. Reads on the hot path are cached or session-local.
- **Not yet:** moving a mailbox between storage entries (a worker task: suspend soft, copy,
  flip, delete), per-domain default storage.

## Namespaces

A **namespace** is the unit of tenant isolation, above domains. It exists for two reasons:
a single Ceph cluster cannot grow forever, and some tenants may not, for legal reasons,
share an environment with others. A namespace owns:

- **Storage.** Every storage entry belongs to exactly one namespace. A namespace with its
  own pools and CephX key on a shared cluster is already better spread than one big pool
  set; a namespace on its own cluster is fully separate. Several storage entries may share
  a cluster (the same fsid), never a pool. New mailboxes are placed among the storage
  entries of their namespace only.
- **Domains, and through them mailboxes and accounts.** A domain is assigned to a namespace
  when it is created (`default` unless told otherwise) and never moves. A mailbox is created
  in a namespace and placed on its storage; an account may only be attached to a mailbox in
  its domain's namespace, so an orphaned mailbox is re-attached only within its namespace
  and an alias delivers only to mailboxes of its domain's namespace. Mail data never crosses
  a namespace.
- **Settings.** The same keys as domain settings (`backup.interval`, `backup.retention`),
  consulted after the domain and before the configured defaults.

The `default` namespace always exists and cannot be removed; a namespace can be removed
when it holds no domains, mailboxes or storage. Disabling a namespace disables every domain
in it: delivery is rejected and logins fail. Names are DNS labels and immutable.

**Daemons serve every namespace unless told otherwise.** The configuration key `namespaces`
lists the namespaces a process serves; empty means all. A restricted process connects only
to the storage of its namespaces (other tenants' credentials never load into it), registers
the list in the node registry (`kittiwake-admin node list` shows it), dequeues only tasks of
its namespaces, and refuses the rest clearly: IMAP, POP3 and JMAP logins into a mailbox it
does not serve fail with "mailbox is not served by this server", LMTP answers 4xx so the MTA
retries elsewhere. This is how IMAP, POP3, JMAP, workers and backup nodes scale out per
namespace. Postfix's socketmap needs only the database and answers for every namespace.
Proxies in front route a client to the servers of its namespace, so that the whole platform
looks like one cluster from outside; see "Proxies".

**Schedulers are per namespace.** Workers and backup nodes elect a leader per namespace
among the nodes that serve it; periodic tasks (purges, garbage collection, sweeps, backup
scheduling and expiry) are enqueued per namespace and walk only that namespace's storage,
so a slow cluster in one namespace never delays another. Every task row carries its
namespace: from its mailbox, or named explicitly for namespace-wide work.

**Admin tokens have a scope.** A token bound to no namespace is the platform admin. A token
bound to one namespace is a **tenant admin**: it sees and changes only that namespace's
domains, mailboxes, accounts, aliases, backups, restores and audit trail, every create lands
in its namespace whatever the request says, listings are filtered to it, and a record of
another namespace reads as not found. Platform-level operations stay with the platform
admin: namespaces, storage registration (a tenant admin reads its own storage entries
without keys), admin tokens, nodes, queues, key rotation and identity export. The admin API
and `kittiwake-admin` take `--namespace` on creates and listings; tenant tokens are minted
with `kittiwake-admin token create --namespace <name>`.

**Backups per namespace.** `backup.namespaces.<name>` in the backup node's configuration
gives a namespace its own target and key ring (a tenant's bucket under the tenant's keys);
namespaces without one use the default target. Runs, blobs and jobs record their
namespace, so expiry works per namespace and a removed mailbox's backups still find their
target. Blobs are deduplicated within a namespace, never across.

## Proxies

Namespaces let every tenant's servers sit in their own datacenter; the **`proxy` mode** is
what lets clients ignore that. A proxy process fronts IMAP, POP3, SMTP submission and JMAP:
it reads the login name, finds the namespace of its domain and hands the connection to a
server of that namespace. Proxies run everywhere (anycast, GeoDNS), the mailboxes stay where
they are, and from outside there is one hostname.

- **Routing by domain.** Every domain belongs to exactly one namespace, so the domain of the
  login name is all the proxy needs; it never reads accounts. On HTTP a session token names
  its user in the signed claims, an API token is one indexed read of the tokens table, a
  refresh token carries its account id, and the account's domain comes from the account and
  domain rows alone.
- **Two ways to verify** (`proxy.auth`). With **`local`** (the default) the proxy checks the
  password itself against the account's hashes, so a wrong password is refused at the edge
  and never costs a backend a connection, a TLS handshake to an ingress or an argon2id
  verification; the refusal is logged and counted at the proxy (`kittiwake_auth_total`,
  `kittiwake_proxy_sessions_total{outcome="refused"}` and the audit line in its log, since
  it writes nothing). A verified client is handed to the backend with a **handoff token**:
  30 seconds, signed with the proxy site's Ed25519 key, bound to the account, the password
  that matched, the client address and the protocol, presented as `AUTHENTICATE XOAUTH2`,
  `AUTH XOAUTH2` or `Bearer`. The backend knows the public keys of the proxy sites
  (`kittiwake-admin proxy-key create <site>` prints the private key once for the proxy's
  `proxy.handoff_key`; `generate` makes a pair offline for `create --public-key`; `delete`
  revokes a site), checks the token with no hashing, re-checks the account's state and
  writes the `login` audit record with `proxy=<site>` and the real client address. A hash
  due for an upgrade (an imported Dovecot scheme) takes the pass-through path once, so the
  backend rewrites it. Session creation (the token endpoint) and bearer logins always pass
  through. With **`passthrough`** the proxy verifies nothing: it replays the client's LOGIN,
  AUTHENTICATE, USER/PASS or AUTH against the backend (LITERAL+ and SASL-IR, so the replay
  needs no round trips) and relays the answer, continuation requests included
  (OAUTHBEARER error flows); the backend checks the password, counts the failure and writes
  the audit record. Either way the client address travels in PROXY protocol v2 (with the
  TLS flag, so submission still offers AUTH only to TLS clients), a wrong password through
  the proxy reads exactly as a wrong password at the backend, an unknown domain or a
  disabled namespace is refused the same way, a namespace without a live server gets
  `NO [UNAVAILABLE]`, `-ERR [SYS/TEMP]`, `454` or `503`, and after a successful login the
  proxy splices the two connections byte for byte. Successful logins cost the same in both
  modes (one backend connection and one exchange); local mode is about where failures stop
  and where the hashing CPU runs.
- **Two ways to a namespace.** Without an ingress, the proxy picks a live node that serves
  the namespace from the node registry (by mode: `imap`, `pop3`, `jmap`, `submission`),
  sticky by rendezvous hash of the login name so a user keeps hitting the node that holds its
  hot index, and skips a node that refuses connections for `proxy.down_for`. A namespace
  whose servers are not reachable directly carries an **ingress**: one hostname (an HAProxy
  in that datacenter terminating TLS, accepting the PROXY header from the proxies only) and a
  TLS policy, `strict` (certificate verified against the system roots and the hostname),
  `insecure` (TLS, any certificate) or `off` (plain TCP); `proxy.ingress_tls` is the default
  for namespaces that set none, and `proxy.ingress_ports` the port per protocol on every
  ingress. `kittiwake-admin namespace set <name> --ingress <host> [--ingress-tls strict]`
  sets it, `--no-ingress` removes it. HTTP goes to the ingress or node the same way, with the
  client address in `X-Forwarded-For` (a pooled HTTP connection serves many clients, so PROXY
  protocol cannot carry it); the backends trust the proxy's address for it
  (`proxy.trusted_addresses`) and their JMAP listener accepts connections without a PROXY
  header (`proxy_protocol: optional`, free for a client-speaks-first protocol).
- **Read-only, and little of it.** A proxy holds no mail state, never touches Ceph and never
  writes to the database: it does not register in the node registry (anycast or DNS finds
  it). With `passthrough` it needs `SELECT` on `namespaces`, `domains`, `nodes`, `tokens`,
  `schema_version` and the `id`, `uuid`, `username` and `domain_id` columns of `accounts`,
  nothing else, so a compromised proxy site yields no password hashes. With `local` it also
  reads `accounts` in full, `account_passwords` and the `id` and `uuid` columns of
  `mailboxes` (the account row joins its mailbox): the hashes are then readable at the
  proxy, which is the price of refusing bad logins there; argon2id makes them slow to
  attack offline, and a site key leaked with them is revoked with one `proxy-key delete`.
  Either way give the proxy its own database user with exactly those grants
  (`database.proxy_dsn`, so a proxy sharing a box with backends never holds the read-write
  credentials), and it never sees Ceph keys or backup keys. It runs alone in its process,
  with its own listeners (`listen.proxy`) so it can share a box with backends, and exposes
  `kittiwake_proxy_sessions_total{protocol,outcome}` on its management port.
- **LMTP is not proxied.** Postfix routes per recipient already: the socketmap offers a
  `transport` map that answers `lmtp:[<ingress>]:<port>` for recipients of a namespace with
  an ingress and `NOTFOUND` for the rest, so `transport_maps` sends a tenant's mail to its
  datacenter and everything else to `virtual_transport`. Workers and backup nodes serve
  their namespaces where they run and need no proxy.

## One binary, multiple modes

The platform ships as a single Go binary, `kittiwake`, that can run in one of several modes.
Each mode is a separately scalable component:

- `lmtp` – inbound delivery from the MTA
- `imap` – IMAP server for legacy clients
- `pop3` – POP3 server for legacy clients
- `jmap` – JMAP (mailbox API) with the OAuth token and self-service endpoints clients need
  next to it; user-facing
- `submission` – SMTP message submission (RFC 6409) for mail clients; HAProxy terminates
  TLS on port 465
- `admin` – the admin REST API; operator-facing, on its own listener
- `postfix` – socketmap lookup service for Postfix (domains, recipients, aliases)
- `worker` – background task execution (trash purge, attachment garbage collection, quota
  recalculation, mailbox removal, reconciliation, …)
- `backup` – mailbox backups to the backup target, restores, exports, verification (the only
  mode holding the target credentials and the backup keys)
- `proxy` – IMAP, POP3, submission and JMAP front end that routes every login to the servers
  of its namespace; no mail state, read-only database access (see "Proxies")
- `dav` – CalDAV/CardDAV (future)

Modes can be combined in one process for small setups and split across many processes for
large ones. The binary also has an `admin` subcommand for bootstrapping and offline
maintenance that talks to RADOS directly (like `radosgw-admin`).

## Language

The daemon is written in Go. cgo is accepted (needed for librados). Target the latest stable
Ceph release.

## Internal architecture: a Go core with protocols on top

Kittiwake is built as a **library first, daemon second**. All functionality lives in Go
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
5. **`cmd/kittiwake`** – wires config, stores, services and the selected modes together.

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
`Message-ID` header (delivery, submission, purge, raw fetch), next to Kittiwake's own message
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
- SMTP submission (outbound, RFC 6409) on implicit TLS (RFC 8314, port 465 at HAProxy)
- Postfix socketmap (lookups for the MTA)

IMAP and POP3 must work but are not the primary target; they serve legacy clients. Each
mailbox is fully accessible through JMAP, which is the basis for a modern webmail and a mobile
app. Kittiwake does not invent its own mailbox API: JMAP already covers mailboxes, messages,
threads, search, flags, blobs, sending, quotas and push, and its newer extensions cover
contacts and calendars.

## Postfix integration

Postfix must know which domains and recipients Kittiwake accepts so it can reject unknown
recipients at SMTP time instead of bouncing after LMTP. Since there is no SQL database for
Postfix to query, Kittiwake's `socketmap` mode serves a **Sendmail-style socketmap server**, which
Postfix supports natively (`socketmap:inet:host:port:name`, read-only lookups). Maps offered:

- `domains` – for `virtual_mailbox_domains`: is this domain known and enabled
- `mailboxes` – for `virtual_mailbox_maps`: does this address resolve to a mailbox
- `aliases` – for `virtual_alias_maps`: alias expansion (optional; Kittiwake can also expand
  aliases itself at LMTP time)

The socketmap protocol is unauthenticated, so the listener is meant for a private network
only. Lookups are plain object reads in RADOS and are cached briefly in the process.

Example Postfix configuration (`main.cf` snippets for `virtual_mailbox_domains`,
`virtual_mailbox_maps`, `virtual_alias_maps` and the LMTP transport) lives in this repository
under `contrib/postfix/`. No glue code is needed: Postfix speaks socketmap natively.

## Sending mail

Sending is in scope. JMAP `EmailSubmission` accepts outbound messages and submits them over
SMTP to a configurable relay host (typically Postfix), optionally with SASL credentials. The
hop to the relay uses STARTTLS: `smtp.tls` is `opportunistic` by default (encrypt when the
relay offers STARTTLS, without certificate verification, like Postfix "may"), `required`
insists on STARTTLS and a certificate that verifies against the system roots and the relay
name (`tls_server_name` when the certificate carries another name,
`tls_insecure_skip_verify` for lab relays), `off` is plain text for a relay on the same
host. With `required`, SASL credentials never travel in plain text. The
platform does not queue or retry outbound mail itself beyond the submission; that is the
MTA's job. If the relay rejects or is unreachable the API returns an error and nothing is
stored. A copy of every message sent through JMAP is stored in the Sent folder, going
through the same parse-and-store path as inbound mail. Arbitrary messages can be added to a folder with JMAP
`Email/import` or `Email/set` (drafts), and IMAP APPEND.

The `From` address (and `Sender`, if present) must be one of the account's identities: its
own address and the enabled aliases that reach it, either with the mailbox as a destination
or with an address destination that is the account's address or another such alias (chains
up to `mail.max_alias_depth`). Catch-alls and disabled aliases never count. Another account
attached to the same mailbox is not an identity of this one, nor is an alias that only
reaches the mailbox through that account's address, so on a shared mailbox each account
sends as itself or as the shared aliases, never as a colleague. The reverse lookup of
address destinations is an indexed query (MariaDB) or a listing on the destination domain
(RADOS backend); destinations outside a hosted domain are not followed. Submissions with another `From` are
rejected (`EmailSubmission/set` fails the submission, the audit log records the attempt).
JMAP `Identity/get` lists these identities, all with the account's display name. Kittiwake adds its own `Received:` header naming the authenticated account so the MTA
logs can be tied to the audit log.

### SMTP submission

Mail clients send through the `submission` mode: an SMTP server (RFC 6409) that hands every
message to the same submit service as JMAP, so identity checks, hold, size limit, audit and
relay behave identically. HAProxy terminates **implicit TLS on port 465** (RFC 8314) and
forwards with PROXY v2; the daemon speaks no TLS. STARTTLS on 587 is not offered (HAProxy
cannot terminate it); it may come later. With `submission.require_tls_proxy` (default on)
AUTH is only offered when the PROXY header carries the SSL TLV (HAProxy
`send-proxy-v2-ssl`), so a password never crosses a plain-text client connection.

- **Authentication:** SASL `PLAIN`, `LOGIN` (Outlook and devices), `OAUTHBEARER` and
  `XOAUTH2`, as on IMAP: a TOTP account's primary password is refused, application passwords
  and session tokens work. There is no per-account service switch: the `send` role is what
  permits submission (and JMAP sending); an account without it cannot log in here.
- **Envelope:** `MAIL FROM` must be one of the account's identities and is checked before the
  client uploads the message; the null sender `<>` is refused. Recipients are not looked up;
  the relay decides. Header `From`/`Sender` are checked at `DATA` like JMAP.
- **Synchronous relay:** the client gets `250` only after the relay accepted the message. A
  relay rejection is passed back with the relay's own code; an unreachable relay is `451`.
- **Headers:** a missing `Message-ID` or `Date` is added (RFC 6409 §8); the `Received:` line
  says `with ESMTPSA` (RFC 3848).
- **No Sent copy by default** (`submission.save_to_sent: false`): SMTP clients store their
  own over IMAP, and a second copy would duplicate it. With it off, submission does no RADOS
  I/O at all apart from the buffered audit write.

### Rate limiting and the relay

Kittiwake does not rate-limit sending (stateless daemons have no shared counters); the relay
does, and how is decided per environment. Kittiwake gives it what it needs to act per
account, for JMAP and submission alike:

- every outbound message carries `X-Kittiwake-Account: <account uuid>` (a copy supplied by
  the client is removed first), for header checks, milters (rspamd) and log correlation;
- with `smtp.xclient: true`, every relay session starts with Postfix
  [XCLIENT](https://www.postfix.org/XCLIENT_README.html) `LOGIN=<account uuid>
  ADDR=<client address>`: Postfix then sees the account as `sasl_username` (for policy
  services such as postfwd or policyd, which run before the message body exists and so cannot
  see headers) and logs the real client address instead of Kittiwake's. The relay must list
  the Kittiwake hosts in `smtpd_authorized_xclient_hosts` and permit relaying for
  authenticated senders; XCLIENT replaces `smtp.username`/`password`. See
  `contrib/postfix/`.

## Message size limit

A configurable maximum message size is enforced at LMTP, JMAP blob upload, `Email/import` and
`EmailSubmission`. Postfix is expected to enforce its own limit first.

## TLS

TLS is not handled by this daemon; a proxy like HAProxy does this. For SMTP submission that
means implicit TLS (port 465) only; see "SMTP submission".

## Client IP passthrough

Because HAProxy sits in front, the daemon would only ever see the proxy's address. HAProxy
forwards every connection with the PROXY protocol, so all listeners (IMAP, POP3, LMTP,
submission and the HTTP API) accept PROXY protocol v1 and v2 and take the real client address from it. PROXY
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

Kittiwake does not rate-limit or lock out failed logins; stateless daemons have no shared
counters. This is left to HAProxy (and tools like fail2ban on the structured logs). Every
failed authentication is logged with the client address so those tools have what they need.

## IPv6-first

Assume an IPv6-only environment. IPv4 is nice to have, not mandatory.

# Storage: Ceph RADOS

Mail data uses Ceph RADOS directly through librados, with three pools per storage:

- **messages** – parsed message JSON and raw skeletons
- **attachments** – deduplicated attachment objects
- **index** – mailbox roots, folder indexes, change logs and mailbox audit

Production identity registries, sessions, task queues and admin audit live in MariaDB.
The development memory backend also models these registries as RADOS objects; its
`meta` pool is not an additional production data pool.

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
downloads and backup exports serve a message **rendered from the document**: the verbatim
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

Attachments use mailbox-scoped deduplication: one immutable payload generation per
content hash within a mailbox. Identical content in different mailboxes has separate
copies. This deliberately spends storage to avoid a popular attachment concentrating
reference updates on one OSD across unrelated mailboxes.

**Reference format 3:** a scalar counter lives at `a/<hash>` in the mailbox's capped
index maps. Append increments it in the same durable root journal that publishes the
message; physical purge decrements it in the journal that removes its retention entry.
Retries recover the journal or see an already removed retention entry. There are no
per-message attachment receipts, ownership objects, or payload reverse-reference keys.
A message/retention record carries only its forward hash and generation references.
The counter covers both live and retained messages, preserving download authorization.

Upload first enqueues a SQL GC candidate, reserves a zero-count generation in the index,
and writes the immutable payload. Collection retires zero-count generations under the
mailbox journal before deleting data. A mailbox-wide random upload epoch fences delayed
reservations, including candidates observed before reservation. A delayed collector can
only delete its old generation. Counter keys are removed at retirement; no permanent
attachment tombstones are needed. The GC registry is keyset-paged and uses enqueue tokens
so completion cannot erase a concurrent enqueue. Weekly payload sweeps repair crash gaps.
The in-memory development backend alone uses sharded OMAP candidate queues.

This layout requires resetting the disposable lab: old attachment formats are not read
or migrated. Stop every old writer and worker before deploying root format 5. Mixed
versions and downgrade after new writes are unsupported.

New message documents also store wire MIME headers, encoded sizes and line counts. IMAP
ENVELOPE, BODYSTRUCTURE and top-level header FETCH use these without reading attachment
payloads. Legacy documents fall back to reconstruction; body-part FETCH still reconstructs
full MIME. A RawStored marker avoids probing missing raw objects for ordinary new messages.

## Crash safety

Each message object carries its mailbox UUID, creation time and location in xattrs so an
orphan-sweep worker task can find and remove objects that never reached an index. LMTP only
acknowledges after the index write succeeds. See "Mailbox index" for the write order.

## Object naming

Mail data:

- Message document: `<mailbox-uuid>/<message-uuid>` in the messages pool (xattr `loc` =
  `<folder-id>/<uid>`); kept original bytes, only for signed mail:
  `<mailbox-uuid>/<message-uuid>.raw`
- Attachment: `v3/<mailbox-uuid>/<sha256-hex>/<generation-uuid>` in the attachments
  pool. Scalar counters share the bounded index maps; SQL `attachment_gc` stores
  transient collection candidates, with no RADOS payload OMAP.
- Mailbox root: `<mailbox-uuid>` in the index pool (folders, counters, modseq, pending mutation; bounded change-log omap)
- Folder index shards: `<mailbox-uuid>/<folder-id>` for UIDs below 65536, then
  `<mailbox-uuid>/<folder-id>.<n>` per further block of 65536 UIDs
- Thread, Message-ID and location maps: `<mailbox-uuid>/t.<xx>` entry buckets,
  with hash-prefix overflow children. Every node is capped at 4,096 keys and
  4 MiB of key/value data, including a single large conversation. Deleted keys
  free reusable space; empty node objects retain stale-writer fences. Message-ID
  mappings count live copies and disappear when the last live copy is removed.
  Replies can resolve only Message-IDs still represented by live mail.
- Change log: `c/<modseq>` omap keys on the mailbox root (bounded)
- Retention set: `<mailbox-uuid>/retention.<16-hex-range>` in the index pool,
  at most 65,536 deletion entries per object, keyed by purge-at timestamp. Root
  allocation and a low watermark track active ranges. Location entries point directly to retained messages and are
  removed after physical purge; empty shard objects retain their version fences.
- Audit: daily headers at `<mailbox-uuid>/audit/<YYYY-MM-DD>` and capped
  segments at `<day>/<16-hex-segment>` in the index pool. Each segment holds at
  most 4,096 keys and 4 MiB of keys plus values. A scalar `_catalog` tracks the
  oldest unprocessed day; expiry resumes across long holds. Retired segments
  retain empty version fences until mailbox removal. Cursors use day/segment/key.
  Pre-segment lab audit data must be reset before deployment.

Sessions, signing keys, TOTP replay state and task queues are tables in MariaDB (the memory
backend keeps them in the index pool: `sessions/<account-id>`, `keys/jwt`,
`totp/<account-id>`, `queue/<task-type>`).

Per message this is **one object plus its deduplicated attachments**, one omap entry in its
folder shard, thread membership, and an authoritative location entry in a hashed thread shard.
Location tombstones survive expunge to prevent stale message xattrs from reviving mail.

## Mailbox index

RADOS has no listing or query capability, so each mailbox needs an index: folders, message
UIDs, flags (seen, answered, deleted, …), sizes and dates. The index lives in RADOS using omap
(the sorted key-value map every RADOS object carries, with atomic compare-and-set and prefix
listing), laid out so that no object's omap grows beyond a bounded size:

- One **mailbox root object** per mailbox UUID. Its data is a small JSON with the collection
  list (folders now, calendars and address books later), per-folder UIDVALIDITY and UIDNEXT
  counters, the mailbox-wide `modseq` counter and quota usage totals, advanced with atomic
  RADOS write transactions so concurrent processes never hand out the same UID or modseq. It
  carries a bounded change-log omap and, while a mutation is pending, its recovery journal.
- **Folder index shards.** Each folder's records live in omap objects of at most 65536 UIDs.
  Keys are the inverted UID, so plain ascending omap iteration yields the newest message
  first: the first page of a folder is a single read of a single object, however large the
  folder. Values are compact (message uuid, thread id, flags, size, dates, modseq, subject,
  from, has-attachments; ~150 bytes). A full-folder walk (IMAP SELECT, expunge, recalc)
  uses descending cursor pages and sorts UIDs where ascending order is required. The legacy
  ascending ListFolder API still reads whole shards; do not use it for paginated scans.
- **Message lookup by id** uses `loc/<uuid>` entries in the hashed thread-map objects.
  Moves update them within the recoverable mutation; removals write a tombstone. The message
  object's `loc` xattr is only a compatibility hint for legacy messages.
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
its last change, and a bounded change log (root omap keys `c/<modseq>`)
records created/updated/destroyed ids. Clients with a state older than the log's horizon get
`cannotCalculateChanges` and resync.

**Delivery commit and recovery.** The message and attachment data are written before the
root CAS accepts a mutation journal. Before acceptance, failures undo private data. Once
accepted, the journal is a durable commit decision: failures return a pending error and
must preserve message data. Any subsequent reader or writer can complete version-fenced
folder, thread, location and retention writes. Only then does a root CAS publish counters,
modseq and changes atomically. Delayed helpers cannot overwrite newer mutations. Empty
index objects are retained as tombstones where deletion could permit stale exclusive creates.

**Root format 5 rollout.** Stop all old writers and workers before starting this version;
older binaries do not understand the capped maps or journaled attachment counters.
This lab upgrade requires a data reset; old roots are rejected instead of being
lazily upgraded. Do not mix writers or downgrade after format-5 writes.

Reads are not cross-object snapshots: a record read concurrent with a mutation may see newer
record data before its root state is published. Synchronization clients must advance only
to the published root/change-log state and process subsequent changes. Reading the root
helps complete pending mutations; recovery also wakes watchers. Notifications are best effort,
with protocol polling as fallback.

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

## IMAP namespaces and migrating from Dovecot

Kittiwake has one personal namespace: empty prefix, `/` as hierarchy delimiter, INBOX
special and case-insensitive, `INBOX/x` a child of INBOX, UTF-8 names (UTF8=ACCEPT; older
clients send modified UTF-7). No shared or public namespaces.

Two things make a migration from Dovecot or Courier smooth:

- **Compatibility namespace.** `imap.compat_namespace` (prefix `INBOX.`, delimiter `.` is
  the classic Courier-era layout) advertises a second personal namespace that maps onto the
  same folders: `INBOX.Projects.2026` is `Projects/2026`, `INBOX.` alone is INBOX. A client
  still configured with "IMAP path prefix: INBOX" keeps working after the switch, with no
  reconfiguration on migration day. Like Dovecot's alias namespace it is hidden: LIST shows
  the compat names only for patterns under the prefix. Children of INBOX (`INBOX/x`) are not
  shown in the compat view, since they would collide with top-level folders there; a literal
  compat delimiter in a folder name is escaped as `\2e` in that view.
- **A delimiter inside a folder name.** A folder called `A/B Ltd` is one folder: stored
  escaped the way Dovecot's listescape plugin does it (`A\2fB Ltd`, `\5c` for a backslash),
  passed through unchanged on IMAP, shown decoded on JMAP. A Dovecot installation with
  listescape hands such names over verbatim through mbsync.

mbsync reads NAMESPACE on both sides, strips the old prefix and translates the delimiter, so
the common Dovecot layouts (`INBOX.` prefix with `.`, no prefix with `.`, no prefix with `/`,
`mail/` prefix) land as the right hierarchy without options beyond `Patterns *`. What mbsync
does not carry: SPECIAL-USE (a localised `Verzonden items` becomes a plain folder next to
the role folder `Sent`; roles can be assigned afterwards), custom keywords and subscriptions.
For keyword-preserving bulk moves the Maildir++ importer reads a Dovecot maildir directly.

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

An admin can cut that short for one mailbox: `kittiwake-admin mailbox purge <uuid>` (`POST
/v1/admin/mailboxes/{id}/purge`) schedules a worker task that removes the whole retention set
now, with the objects behind it. It only ever touches what the account holder already
deleted; live messages stay. A mailbox on hold refuses it. Deleting a mailbox removes
everything at once, retention included.

JMAP uploads (blobs for `Email/import` and attachments for `Email/set`) are not kept in the
process that received them: they go to an `uploads` namespace in the mailbox's messages pool,
so the next request may land on any JMAP node behind the load balancer. An upload can be
referenced for one hour; the worker sweeps expired ones hourly (the namespace only ever holds
young blobs, so the sweep never walks the pool), and removing a mailbox removes its uploads.

## Spam routing

If the MTA (Postfix with rspamd, SpamAssassin, …) marks a message as spam, LMTP delivery files
it into the Junk folder instead of INBOX. The header name and the value that means "spam" are
configurable (for example `X-Spam-Flag: YES` or `X-Spam: Yes`). User-defined filter rules
(Sieve-style) are not in scope for now.

## Workers and background tasks

The SQL scheduler coalesces periodic tasks by task type and mailbox: a pending or
leased job prevents another copy from accumulating at the next tick. Dead-letter jobs
release that key so a later interval can try again. Explicit requests and event tasks
remain distinct. Production retention work uses an indexed `mailboxes.purge_after`
registry: expunge/destroy registers a deadline before accepting the Ceph mutation,
and completed purges schedule the next retained deadline. A signal arriving while
a purge is leased remains queued for a successor. Weekly discovery repairs gaps
across the Ceph/SQL boundary using bounded keyset batches and a durable SQL cursor;
it does not rescan every mailbox at every hourly tick. The development backend
retains its simple periodic scheduler.

The `worker` mode executes background tasks:

- Purge Trash messages older than the retention period
- Decrement attachment reference counts and delete orphaned attachment objects
- Recalculate quota usage for a mailbox
- Remove all data of a deleted mailbox
- Purge audit objects older than the audit retention period
- Orphan sweep (message objects that never reached an index)
- Identity reconciliation (repair domain/mailbox/account listings)
- Search indexing events (no-op until a backend is configured)

**Recovery and cleanup.** Multi-object index mutations use the durable root journal described
above. Private message data is undone only before journal acceptance; pending mutations
are completed, not rolled back. Mailbox removal deletes message, index, upload and audit data.
Sweeps provide crash cleanup rather than committing normal writes:

- **Attachment GC** (daily) pages an indexed SQL candidate registry per storage in
  production. Fresh enqueue tokens prevent completion from dropping a concurrent
  removal; candidates are published before and after counter changes. Storage cannot
  be unregistered while collection candidates remain. The development backend keeps
  its simple hash-sharded OMAP queue. Daily GC does not scan every live attachment. It checks age and existence of references and uses
  version-fenced deletion. Weekly reconciliation walks attachments to remove dangling
  references and discover crash leftovers. Metadata reads do not enumerate references.
- **Orphan sweep** (weekly by default; one stat per message object, so it is the only
  sweep that scales with the total message count) removes message documents without an
  index or retention entry and `.raw` objects without a document, once they are older than
  the orphan age, and asks for a quota recalculation of every mailbox it touched, since an
  orphan is the trace of a torn write that may have left the counters off.
- **Upload sweep** (hourly) removes expired JMAP uploads from their own namespace.
- **Audit purge** (daily) removes daily audit objects past the retention; removed mailboxes
  lose theirs with the mailbox, so nothing depends on the mailbox list for cleanup.

The orphan sweep is the one full pool walk left; with the write paths self-cleaning it is
insurance rather than a mechanism, and its interval is what to raise first on a large
installation.

Periodic tasks are enqueued by a scheduler role that the workers elect among themselves with
a lease row in the database, so exactly one worker schedules and any worker executes.

**Queue design:** a `tasks` table in MariaDB; workers claim due tasks with `SELECT ... FOR
UPDATE SKIP LOCKED`, so multiple workers never take the same task, and hold them under a
two-minute lease that a running task renews every thirty seconds. A worker that crashes
stops renewing, and another worker takes the task over within one lease; a worker that is
stopped (restart, deploy) hands its running task back at once, due immediately and without
counting a failure. Every long task is written to be resumable: a mailbox removal lists what
is still there on every pass, so the next worker continues where the last one stopped. A
worker whose lease was taken over stops and leaves the task alone. Tasks carry a run-after
time; failed tasks are retried with backoff and after a configurable number of attempts land
in a dead-letter state visible in the admin API. The memory backend keeps the same queue as
omap entries with a RADOS lock.

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

## Backups

Mail data is backed up by Kittiwake itself, per mailbox, to an S3-compatible object store
(any provider) or a directory. Nothing at the Ceph layer does this job: replication and
erasure coding protect against hardware, not against a deleted mailbox, a writer bug, a
dropped pool, a lost cluster or a compromised admin token. The design follows the same two
rules as everything else: storage is cheap, requests are expensive; and nothing runs in the
delivery or client I/O path.

**The `backup` mode.** A daemon mode of its own, next to `worker`, and the only process that
holds the backup target's credentials and the encryption keys. It drains the backup task
queues (`backup`, `backup-job`, `backup-maintain`, `backup-expire`) from the MariaDB task
queue with the same lease, retry and dead-letter semantics as the worker, and the backup
nodes elect a scheduler per namespace among themselves (lease rows separate from the
worker scheduler's). Every other mode only files tasks and reads the catalog. Scale by
adding backup nodes, per namespace when a tenant needs its own (see "Namespaces" for
per-namespace targets and keys).

**What a backup is.** A logical copy, not a copy of RADOS objects, so it survives index
format changes: message documents, kept original bytes (`.raw`) and attachment payloads
verbatim, and the index as the folder list plus the message records of every folder. One run
of a changed mailbox writes:

- one or more **packs** (`mb/<mailbox>/<run>.<n>.pack`, capped at `pack_size`): the new
  message documents, their kept originals, small attachments, the records of the folders that
  changed and the audit-log days since the previous run, each entry sealed on its own so a
  single message is one ranged GET;
- **blobs** (`blob/<xx>/<name>`) for attachments from `blob_threshold` up, stored once per
  content across all mailboxes and all storage clusters, named by an HMAC of the content hash
  so a provider cannot test for a known file;
- one **manifest** (`mb/<mailbox>/<run>.manifest`): the folders with references to their
  record entries, retention entries, audit days, the hold record, accounts, quota, the root
  modseq and every pack the run references. Folders whose modseq did not move keep pointing at
  the entries of earlier runs, and messages the previous run holds are never copied again.

**Incremental by construction.** The run reads the mailbox root once: an unchanged modseq
since the last run is a skip (one RADOS read, no writes). Changed folders are listed and
compared with the previous run's entries; a changed folder of 150k messages costs one listing
and one records entry, not a copy of its messages. The root is read again at the end; if the
modseq moved meanwhile the run is retried once and otherwise marked *fuzzy* (restore relies
on the index's own recovery). Mailboxes on hold are backed up like any other, their manifests
record the hold, and their runs never expire.

**Encryption.** Every object is sealed before it leaves the process with AES-256-GCM under a
per-object key derived (HKDF-SHA256, random salt) from one of the configured pre-shared keys,
so a public cloud sees only ciphertext and keyed names. Keys have ids: the current key seals,
every listed key opens, so rotation is adding a key and moving `current_key`. **Losing the
keys means losing the backups; escrow them outside the platform.**

**Catalog.** MariaDB keeps what the scheduler and the admin API need without listing the
bucket: per mailbox the effective interval, next due time, last run and modseq, status and
verification result; the runs with their pack lists; the blobs the target holds and which
mailboxes reference them; restore and export jobs. Every fact in it is also in the manifests
on the target, so a restore needs only the bucket and the keys.

**Schedule: a continuous drip, never one big run.** Each mailbox has its own interval and next
due time; the scheduler enqueues due mailboxes in due order, bounded per tick, so the queue
holds the next moments of work and nothing else. New mailboxes get their first run spread
uniformly over one interval; after a run the next due time is the interval plus a little
jitter. A million mailboxes at the default of 24 hours is about twelve runs per second, most
of them a single root read. The interval is configurable per mailbox and per domain
(`backup.interval` and `backup.retention` in the domain settings apply to mailboxes with
accounts in that domain; a mailbox's own setting wins; `off` disables), bounded by
`min_interval` (default 30 minutes) and `max_interval` (30 days). **Lower intervals multiply
the load linearly**: a mailbox at 30 minutes costs 48 times the root reads and, when it is
active, 48 times the writes of one at 24 hours. The platform summary reports the configured
aggregate load (runs per hour) so the effect of settings is visible before it arrives.

**Restore** is side by side and never in place: a run is restored into a *new* mailbox
through the normal append path (new message ids and UIDs; flags, dates, folders, keywords and
subscriptions kept; attachments re-registered, never copied with their counters), and the
operator re-points the accounts when satisfied. A single message can be restored into a live
mailbox, into the folder it was in. Expunged messages still in the retention set are backed
up too and restored into Trash on request.

**Export** replaces the former Maildir export: an export job renders a run as a
Dovecot-compatible Maildir++ tar.gz (flags in file names, `dovecot-keywords`,
`dovecot-uidlist`, a `manifest.json` with a SHA-256 per file, the audit log and the hold
record for a chain of custody) from the backup alone, never from the mail stores, and writes
it **unencrypted** under `export/` on the target with an expiry. On S3 the job carries a
presigned download link; the exports prefix must expire (`export_expiry`) and should be the
only place mail ever sits in the clear. Importing a Maildir is gone with it: migrating from
Dovecot is an IMAP-to-IMAP job (imapsync, mbsync).

**Verification** runs per mailbox on its own interval (`maintain_interval`) and on demand:
every manifest and pack the catalog knows must be on the target with its recorded size (one
listing, no downloads); a sample of the latest run's entries (all of them on demand) must
decrypt to the recorded hashes; objects under the mailbox prefix the catalog does not know
are reported and, past a grace period, removed. A failure is recorded on the mailbox, counted
in the summary, and forces the next run to be a full one so whatever still exists in RADOS is
captured again.

**Expiry and GC.** Runs leave retention `retention` after they finished (never while the
mailbox is on hold); packs no remaining run references are deleted; when a mailbox's last run
goes, everything under its prefix goes and its blob references are released; blobs no mailbox
references are deleted after the grace period; export archives expire. With S3 Object Lock
(`lock: true`, compliance mode, the bucket must support it) nothing can be deleted before
its retention ends, a compromised admin token included; targets without it simply ignore the
lock.

**MariaDB is not covered.** The administrator backs up the metadata database, binary logs
included, so it can be restored to a point in time. Read replicas are recommended, as they are
also a copy of the database when the primary fails, but a replica replays a dropped table as
faithfully as anything else and does not protect against accidental removal. Only a backup
with binlogs does. A database restored to an older point leaves mailbox roots in RADOS the
database no longer knows; the reconciliation worker reports them.

Operators use `kittiwake-admin backup` (status, show, run, restore, restore-message, export,
verify, forget, jobs) and `kittiwake-admin mailbox set --backup-interval/--backup-retention/
--backup-disabled`; the admin API has the same surface under `/v1/admin/backups` and
`/v1/admin/mailboxes/{id}/backups`.

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
(usually the e-mail address), a display name, the **services** it may log in to, zero or more
passwords and optionally a TOTP secret.

**Services.** Every account lists which of `imap`, `pop3` and `jmap` it may use (the set grows
with CalDAV and CardDAV); a new account gets all of them unless told otherwise. A phone-only
user, or a security-minded one, keeps just `jmap`. The check happens at login on every
protocol: IMAP and POP3 answer `AUTHENTICATIONFAILED` with a clear reason, the OAuth token
endpoint answers `invalid_grant`, and API tokens of the account stop working on the JMAP
listener. `jmap` covers the JMAP listener including its REST self-service endpoints, so an
account that removes `jmap` itself (`PATCH /v1/me`) locks itself out of this API until an
administrator (`kittiwake-admin account set --services`) restores it. Access tokens already
issued keep working until they expire (minutes); refreshing one re-checks. The display name is the `From:` name of every identity of the account; the account
sets it itself (`PATCH /v1/me`, or JMAP `Identity/set` on any of its identities) and an
admin can set it too. Every
password is marked primary or application, carries a **comment** (what it is for) and an
optional **expiry** (null = never): a helpdesk can hand out a temporary password that stops
working by itself, next to the permanent one. Expired passwords are dropped the next time a
password is set.

**Migrated passwords.** An admin can set the primary password from an existing hash as
another platform stores it (`kittiwake-admin account password <account> set --password-stdin` with a value that starts with `{` or `$`,
or the `hash` field of the admin password endpoint): Dovecot-prefixed forms such as
`{SHA512-CRYPT}$6$...`, `{BLF-CRYPT}$2y$...`, `{SSHA512}...`, `{ARGON2ID}...`, or a bare
`$6$`, `$5$`, `$2y$` or `$argon2id$` hash whose scheme is inferred. The hash is checked for
shape, never verified, stored verbatim, audited as a migrated hash, and shown with its
scheme in the admin password list (the account holder's own `/v1/me` view carries no hash
details); the first successful login upgrades it to argon2id, so a
migrated user base converges on the current scheme without password resets. Schemes we
cannot verify (MD5-CRYPT) are refused.

**Lifecycle, by design.** An account belongs to its domain (a domain cannot be removed while
it still has accounts or aliases) and points at a mailbox; the mailbox owns the data and
knows nothing about names. Removing an account, or a whole domain, leaves its mailboxes
**orphaned** with all their mail intact; attaching a new account to an orphaned mailbox is how
an address is renamed or moved to another domain without touching the data. Orphaned
mailboxes are listed with `kittiwake-admin mailbox list --orphaned` (`GET
/v1/admin/mailboxes?orphaned=true`); a mailbox is only ever deleted explicitly. Accounts have an enabled flag; a disabled account cannot log in,
while the mailbox state (see above) decides what happens to inbound mail.

## Roles

Accounts and API tokens carry a set of roles. The role set is:

- `read` – list folders, read messages and attachments, search (JMAP get/query/changes)
- `write` – change flags, move/copy/delete messages, manage folders, import messages (JMAP
  set)
- `send` – submit outbound mail (JMAP `EmailSubmission` and SMTP submission)
A normal full-access account has `read`, `write` and `send`. Accounts cannot carry admin
roles; see "Admin tokens". A token's roles may only narrow
the account's roles; a token claiming a role the account lacks is rejected.

## Self-service

Because Kittiwake owns the identity data, an authenticated account can manage its own
credentials through the REST endpoints: change passwords, create and revoke application
passwords, enrol and disable TOTP (with recovery codes), create and revoke its own API tokens,
and list and revoke its sessions. Every such change is audited.

## API tokens

An account may have no password at all and only API tokens, so applications can connect
without a password. Each API token carries its own roles, which may be narrower than the
account's. Every API token **must** have an expiration date and a comment describing its
purpose; tokens without either cannot be created. Expired tokens are rejected. Tokens are
random, prefixed `kwt_` so they are recognisable in logs and secret scanners, and stored only
as a SHA-256 hash (`tokens/<hash>`); the plaintext is shown once at creation.

## Admin tokens

Admins are not accounts and are never mixed with mail users. An admin is an **API token**
with a comment, an expiration date and one role:

- `admin` – full administration
- `admin-ro` – read-only: every `GET` on the admin API (listings, inspection, audit, queue
  status), for monitoring and reporting jobs that must never change anything

Admin tokens live in the meta pool next to account tokens but belong to no account and no
domain; they are managed through the admin API (`/v1/admin/tokens`) or `kittiwake-admin token`.
A token may be bound to one namespace, which makes it a tenant admin (see "Namespaces").
The first one is minted with `kittiwake admin bootstrap`, which talks to RADOS directly and
refuses once admin tokens exist; `kittiwake admin token` mints further ones (including
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
- **Signing keys** are generated by Kittiwake, stored in RADOS (`keys/jwt` in the index pool)
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
- SMTP submission authenticates like IMAP (`PLAIN`, `LOGIN`, `OAUTHBEARER`, `XOAUTH2`) and
  additionally requires the `send` role.

Session access tokens are JWTs and cost no I/O to verify. An API or admin token costs one or
two RADOS reads. A password costs two reads plus an argon2id verification, which at the
default parameters is tens of milliseconds of CPU (measured 54 ms per basic-auth request on
the four-core lab box against 2.5 ms for a session token), so clients that send basic auth
on every request would otherwise burn a core per ~20 requests per second. Successful
verifications are therefore cached per process for `cache.credentials` (default 5 s): the
cache key is a hash of the credential, only positive results are cached, a cache hit skips
the per-request `login` audit record, and a password change, password removal or TOTP change
invalidates the account's entries in the process that made it. Wrong passwords are never
cached and always cost the hash: brute-force throttling stays at the proxy.

## Caches

Every daemon keeps three small per-process read caches, configured under `cache:` as a
TTL each (default 5 s, `0s` disables one). They exist to collapse the repeated reads of one
client's command stream or one MTA's recipient lookups, not to hold data, so keep them
short. A change made through the same process invalidates its own entry and is visible at
once; a change made by another process (the admin daemon suspending a mailbox, a password
rotated through JMAP on another node) is visible within the TTL. Nothing is cached across
processes or nodes: no Redis, no memcached.

- `cache.credentials`: verified passwords and API tokens (above).
- `cache.identity`: domains, mailboxes (state, hold, quota limits), accounts and aliases.
  These are read by the mailbox service on **every** IMAP, POP3 and JMAP command (state and
  hold check), by LMTP for every recipient (domain, account or alias, mailbox) and by the
  Postfix socketmap for every `domains`, `mailboxes` and `aliases` query. Not-found results
  are cached too, so an MTA probing unknown recipients costs one read per TTL. The cache
  lives in the identity store itself, so every write through the store drops the entry.
- `cache.root`: the mailbox root (folder list, counters, modseq, quota usage) where a copy
  up to the TTL old is acceptable: the LMTP quota check, IMAP folder-name resolution (a
  miss re-reads), and JMAP state strings in method responses. A process's own root writes
  refresh the copy, so the `newState` after a JMAP set is exact; push notifications from
  other processes always re-read.

Admin reads and all optimistic-concurrency updates bypass the caches.

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
by the Ceph pools (BlueStore compression and encryption), not by Kittiwake.

# APIs

## Mailbox API: JMAP

The mailbox API **is JMAP**. Rather than designing a custom REST API, Kittiwake implements the
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
5b. **Backups:** per-mailbox backup settings, state and runs; run now; restore (into a new
   mailbox, or one message into a live one), export as Maildir, verify and forget as jobs
   executed by the backup nodes; platform-wide backup summary
6. **Worker and queue visibility:** pending and failed tasks, retry, dead-letter set
7. **Support access:** read a mailbox on behalf of a user, audited
8. **Platform status:** Ceph health, version, running instances, reconciliation findings
9. **Audit inspection:** query the audit history of a mailbox, an account or the admin log by
   time range, action, client IP
10. **Session management:** list and revoke sessions of an account; rotate signing keys

# Daemon

## Configuration

The daemon takes a single YAML file as configuration:

- Listen addresses, one per mode, each with the management (metrics, health) address of
  the process that runs that mode alone; `listen.management` for a process running several
  modes. JMAP and the admin API are separate listeners and separate code paths. Optionally
  the modes to run in one process, otherwise each systemd instance passes its mode with
  `--modes` and takes everything else, including its management port, from the YAML
- `namespaces`: the namespaces this process serves (empty: all); every mode honours it
- Metadata database DSN (MariaDB), optional read replica DSN and read stickiness
- Ceph: `config_file` and `options` applied to every storage connection (the Ceph
  clusters themselves are registered in the database as storage); pool names only for the memory backend
- Outbound SMTP relay host, STARTTLS mode (off, opportunistic, required), optional SASL
  credentials or Postfix XCLIENT
- SMTP submission: Sent copy (default off), AUTH only behind a TLS proxy (default on)
- Trash retention period
- Audit retention period and per-protocol audit detail level
- Trusted proxy addresses (PROXY protocol, X-Forwarded-For); the proxy mode's listeners,
  `proxy.auth` (local verification with a site handoff key, or pass-through),
  default ingress TLS policy, ingress ports and timeouts
- Session settings: access token lifetime, refresh token lifetime, key rotation interval
- Cache TTLs: verified credentials, identity objects, mailbox root (see "Caches")
- IMAP compatibility namespace (prefix and delimiter of an old layout)
- Spam header name and value for Junk routing
- Default quota limits, over-quota LMTP reply policy (suspended mailboxes always answer 4xx,
  disabled ones 5xx)
- Inline attachment size threshold
- Maximum message size
- Backups: target (S3 or directory), keys, per-namespace targets and keys, default interval
  and its bounds, retention, verification interval, export expiry, Object Lock, pack size,
  blob threshold, spool directory, concurrency; post-hold purge grace period
- Management listener (metrics, health)
- Logging and metrics settings
- Redis information (only if it ever becomes needed)

Secrets (database DSN, Ceph key, SMTP relay password) can be given inline, as a path to a
file, or via `${ENV_VAR}` substitution.

## Development backend

`ceph.backend: memory` runs the whole platform against the in-memory RADOS implementation
used by the tests: one process, no Ceph, nothing persisted. `kittiwake serve --seed-account
user@example.org:password` creates a domain, mailbox, account and an admin on that backend at
startup, so a developer can speak LMTP, JMAP and the admin API to a fresh process immediately
(`make run-dev`).

## `kittiwake admin` and `kittiwake-admin`

`kittiwake admin` talks to RADOS directly with the daemon's config file and does only what
cannot be done through the API because no token exists yet: `bootstrap` (first admin token)
and `token` (mint a full or read-only admin token). Everything else is done over
the admin API so the daemon's code base stays small.

`kittiwake-admin` is the operator's tool: a Go client for the admin API (also importable as the
package `client/admin`) that covers every endpoint: domains, mailboxes, accounts, aliases,
tokens, sessions, legal hold, audit queries, queues, key rotation, identity export/import and
backups (settings, runs, restore, export, verify). It takes the admin
URL and token from flags, environment (`KITTIWAKE_ADMIN_URL`, `KITTIWAKE_ADMIN_TOKEN`) or
`~/.config/kittiwake/admin.yaml`, and prints tables or JSON (`-o json`).

The admin listener serves its own OpenAPI document at `/openapi.yaml` (unauthenticated) so
other clients can be generated from the exact contract the running version implements.

## Versioning and releases

Kittiwake has rolling releases numbered `<two-digit year>.<release within the year>`: 26.0
is the first release of 2026; the second number counts up with every release of that year
and the first release of 2027 is 27.1. The release number lives in one place,
`app/version.go` (`Release`); cutting a release is bumping it and committing.

Every build embeds the commit: the version a binary reports is `<release>+<short commit>`,
e.g. `26.0+2b4ab79`, with `-dirty` appended when the tree had uncommitted changes
(`make build` sets it; `make version` prints it). That string appears in `kittiwake
version`, `kittiwake-admin version`, the startup log line, `/v1/admin/status`, the node
registry (`kittiwake-admin node list` shows which build runs where) and the User-Agent of the
admin CLI.

The database records it too: `schema_version` keeps, per schema version, when it was applied
and by which product version (`applied_by`), so `kittiwake-admin status` can say "schema 2,
applied by 26.0+2b4ab79 on …" and an operator sees at once whether a box runs a build older
than the schema it talks to (a daemon refuses to start on a schema version it does not know).

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
- Metadata in MariaDB (domains, mailboxes, accounts with services, aliases, catch-all,
  tokens, sessions, signing keys, queue, storage registry, admin audit) with embedded
  migrations and `kittiwake migrate`; RADOS backend for development; shared conformance
  suites; `kittiwake admin bootstrap`, identity export/import
- Multiple Ceph clusters: registry with fsid verification, per-mailbox placement, periodic
  reload, per-storage stores and sweeps, `kittiwake-admin storage` and `/v1/admin/storage`
- Namespaces: tenant isolation owning storage, domains and mailboxes; per-process served
  set; per-namespace schedulers, task partitioning and backup targets; tenant admin tokens;
  `kittiwake-admin namespace` and `/v1/admin/namespaces`
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
- SMTP submission mode (RFC 6409, implicit TLS at HAProxy) with PLAIN/LOGIN/OAUTHBEARER/
  XOAUTH2; account header and optional Postfix XCLIENT for per-account rate limiting at the
  relay
- Legal hold (read-only / sealed, delivery and sending switches, purge suspension)
- Backups: `backup` mode with S3 and filesystem targets, pre-shared-key encryption, packs
  and deduplicated blobs, incremental runs, per-domain and per-mailbox schedules, side-by-side
  restore, single-message restore, Maildir++ export from a backup, verification, expiry and
  GC, catalog in MariaDB, admin API and CLI
- Workers: trash/expunge retention purge, attachment GC, mailbox removal, quota
  recalculation, audit purge, orphan sweep; lock-elected scheduler
- Proxy mode: IMAP, POP3, submission and JMAP routed by login domain to the namespace's
  nodes or ingress, credentials passed through, read-only database use
- Structured logging, Prometheus metrics, health endpoints, PROXY protocol on all listeners
- Unit tests for every package on the in-memory RADOS; no Ceph needed to run them

## Not yet

- IMAP CONDSTORE/QRESYNC (modseq is tracked; capability not advertised yet), JMAP over
  WebSocket, `Email/queryChanges`
- Moving a mailbox between Ceph clusters; per-domain default cluster
- Backups: blob reclamation is coarse (a blob is released when a mailbox's last run expires,
  not when its last referencing run does); the lab has no S3 cost and throughput numbers yet
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

### IMAP and query working sets

Selected IMAP sessions retain UID, message identity, flags, modseq, size and dates;
headers and threading input are not copied into each session. Header/body searches
stream index pages and release reconstructed messages after each match. Ordinary
FLAGS/date/size operations still use session metadata without added index reads.
Folder offset queries discard skipped pages instead of retaining offset+limit
records. Sorted/filtered queries stream 1,000-record pages and keep only sort keys
and locators. Positive limited queries retain at most position+limit candidates in
a heap; thread collapse first retains the best locator per matching thread. Only
output records are fetched again. Negative offsets and unlimited requests retain
compact locators for all matches. Filtering still scans matching folders; deep
offsets, large limits and thread collapse can consume proportional memory and I/O.

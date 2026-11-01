# Kittiwake

Kittiwake is a massively scalable, modern mail platform built to replace Dovecot. The
kittiwake is a small sea-going gull that nests on cliffs in colonies of hundreds of
thousands: a bird, like the dove it succeeds, that lives at sea (Ceph) and at scale.
Home: https://kittiwake.email

It is **not** an MTA and does not replace Postfix: Postfix receives and filters mail from
the internet and hands it to Kittiwake over LMTP, and outbound mail goes back to an SMTP
relay. Kittiwake serves IMAP, POP3, JMAP and SMTP submission to clients, an admin REST API
to operators and a socketmap to Postfix. Mail data lives in Ceph RADOS, metadata in MariaDB,
and the daemons are stateless: one binary, `kittiwake`, runs any of the modes (`lmtp`,
`imap`, `pop3`, `jmap`, `submission`, `admin`, `socketmap`, `worker`, `backup`, `proxy`),
under systemd or in containers, and every mode scales by running more of it.

## Scalability is the governing principle

Kittiwake is designed for a million mailboxes, each of which may hold 150k messages and
10+ GB spanning decades. Every design decision is weighed against that:

- One RADOS object per message, attachments deduplicated, no raw copy of the message.
- Every per-mailbox map is sharded; nothing grows with the number of messages on a single
  object, and no request reads a whole folder for one page.
- **Storage is cheap, I/O is expensive:** round trips are counted, independent writes run
  concurrently, hot reads are cached, low-value writes are batched.
- Daemons hold no state, so capacity is a matter of running more processes; the metadata
  database is read from replicas, mail data is spread over as many Ceph clusters as needed,
  and tenants are isolated in namespaces with their own storage.

The full design, feature by feature down to the object layouts, is in
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## IPv6-first

Kittiwake is built for IPv6 and tested only on IPv6: every listener, every address in the
documentation, the lab. IPv4 is not tested and nothing is promised for it. IPv4 clients are
served by terminating IPv4 on HAProxy at the edge, which terminates TLS anyway, and
forwarding to Kittiwake over IPv6 with the PROXY protocol, so the real client address is
still what the logs and audit records show.

## Status

**Proof of concept.** Kittiwake works and runs on a small lab, but it has not been tested at
large scale. People are welcome and encouraged to try it at real sizes and on real
workloads. If you do, please reach out to Wido den Hollander, by
[opening an issue](https://github.com/kittiwake-email/kittiwake/issues) or by mail to
wido@widodh.nl, with what you found. `ARCHITECTURE.md` "Status" lists what is implemented
and what is not.

## Getting started

```
make build                     # bin/kittiwake and bin/kittiwake-admin (cgo: librados-dev)
make test                      # every unit test on the in-memory RADOS, no Ceph needed
make run-dev                   # the whole platform in one process on the memory backend
make docker                    # the container image; contrib/docker has a compose example
bin/kittiwake check-config -c examples/kittiwake.yaml
```

- `examples/kittiwake.yaml`: the documented configuration; `kittiwake help env` lists the
  `KITTIWAKE_*` environment overrides containers use.
- `contrib/systemd`, `contrib/postfix`, `contrib/docker`: units, Postfix snippet, image config
  and compose file.
- `AGENTS.md`: the developer guide (commands, layout, conventions, lab findings).
- `api/openapi.yaml`: the admin and account REST API, also served by the running daemon.
- The sibling repository `kittiwake-labs` provisions, deploys and benchmarks the test lab.

## License

GNU Affero General Public License v3.0 (see `LICENSE`). Copyright 2026 Wido den Hollander.
Anyone may use, study, change and redistribute Kittiwake under the AGPL; anyone who runs a
modified version for others over a network must offer them its source. Other terms,
including a commercial license, are available from the copyright holder. See
`ACKNOWLEDGEMENTS.md` for the people, tools and AI models behind Kittiwake.

# Test environment (temporary)

Lab setup used for integration testing against a real Ceph cluster. This file is
temporary and will be removed; nothing in it is secret.

| Role              | Address            | Notes                                              |
|-------------------|--------------------|----------------------------------------------------|
| Ceph node 1       | `2a14:9b80::2001`  |                                                    |
| Ceph node 2       | `2a14:9b80::2002`  |                                                    |
| Ceph node 3       | `2a14:9b80::2003`  |                                                    |
| Dovetail test box | `2a14:9b80::3143`  | hostname `dovetail`; `/root/ceph.keyring` holds admin credentials |

Ceph client packages on the Dovetail box come from the official Ceph DEB repository
(download.ceph.com), not from the distribution.

## What is installed on the Dovetail box

- `ceph-common` and `librados-dev` 21.1.2 from `https://download.ceph.com/debian-umbrella/ noble main`
  (key in `/etc/apt/keyrings/ceph.gpg`, source in `/etc/apt/sources.list.d/ceph.list`)
- Go 1.27.3 in `/usr/local/go`, symlinked into `/usr/local/bin`
- `/etc/ceph/ceph.conf` (mons on the three Ceph nodes) and `/etc/ceph/ceph.client.dovetail.keyring`
- Pools `dovetail.messages`, `dovetail.attachments`, `dovetail.index` (and the now unused
  `dovetail.meta`), application `dovetail`; CephX user `client.dovetail` with rwx on them
- MariaDB 10.11 (`mariadb-server` from Ubuntu), databases `dovetail` (the daemons) and
  `dovetail_test` (conformance suites), user `dovetail`/`dovetail` on the local socket
- The repository synced to `/root/dovetail` (the box has no IPv4, so GitHub is unreachable;
  sync with rsync from a workstation)

## Running the tests

From a workstation:

```
rsync -az --delete --exclude .git --exclude bin ./ root@[2a14:9b80::3143]:/root/dovetail/
ssh root@2a14:9b80::3143 'cd /root/dovetail && make test'          # unit tests (memory backend)
ssh root@2a14:9b80::3143 'cd /root/dovetail && make test-sql DOVETAIL_TEST_DSN="dovetail:dovetail@unix(/run/mysqld/mysqld.sock)/dovetail_test"'
ssh root@2a14:9b80::3143 'cd /root/dovetail && make integration'   # RADOS conformance on the cluster
ssh root@2a14:9b80::3143 'cd /root/dovetail && make e2e'           # daemon on librados, LMTP/JMAP/IMAP/POP3/export
```

`make integration` uses `/etc/ceph/ceph.conf`, user `dovetail` and pool `dovetail.index`
(override with `DOVETAIL_TEST_CEPH_CONF`, `DOVETAIL_TEST_CEPH_USER`, `DOVETAIL_TEST_CEPH_POOL`);
it works in its own RADOS namespaces and cleans up. `make e2e` runs `examples/lab.yaml` and
leaves a throwaway `e2e-<timestamp>.example` domain in MariaDB. After a schema change run
`dovetail migrate -c /etc/dovetail/dovetail.yaml` on the box before restarting the instances
(a daemon refuses to start on an old schema).

## systemd instances on the box

The lab box runs every mode as its own systemd instance from `/etc/dovetail/dovetail.yaml`
(`dovetail@lmtp`, `dovetail@imap`, `dovetail@pop3`, `dovetail@jmap`, `dovetail@admin`,
`dovetail@postfix`, `dovetail@worker`; grouped in `dovetail.target`), as user `dovetail`,
binary `/usr/local/bin/dovetail`. Management ports are 9091..9097 in that order
(`/etc/dovetail/instances/<mode>.env`).

```
make deploy                 # build locally, copy to /usr/local/bin, restart dovetail.target
make deploy-units           # (re)install unit files, env files, example config
ssh root@2a14:9b80::3143 'systemctl status dovetail.target; journalctl -u "dovetail@*" -f'
ssh root@2a14:9b80::3143 'cd /root/dovetail && E2E_ATTACH=1 BIN=/usr/local/bin/dovetail scripts/e2e.sh /etc/dovetail/dovetail.yaml'
```

## Administration on the box

`/usr/local/bin/dovetail-admin` talks to the admin listener; its token is in
`/root/.config/dovetail/admin.yaml` (minted with `dovetail admin -c /etc/dovetail/dovetail.yaml
bootstrap`, or `... token --comment x [--read-only]` later). The OpenAPI document of the running version is at
`http://[::1]:8081/openapi.yaml`.

```
dovetail-admin status
dovetail-admin mailbox list
dovetail-admin -o json account list --domain lab.example
dovetail-admin audit --mailbox <uuid> --all
```

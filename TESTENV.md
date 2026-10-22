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
- Pools `dovetail.messages`, `dovetail.attachments`, `dovetail.index`, `dovetail.meta`
  (application `dovetail`); CephX user `client.dovetail` with rwx on exactly those pools
- The repository synced to `/root/dovetail` (the box has no IPv4, so GitHub is unreachable;
  sync with rsync from a workstation)

## Running the tests

From a workstation:

```
rsync -az --delete --exclude .git --exclude bin ./ root@[2a14:9b80::3143]:/root/dovetail/
ssh root@2a14:9b80::3143 'cd /root/dovetail && make test'          # unit tests (memory backend)
ssh root@2a14:9b80::3143 'cd /root/dovetail && make integration'   # RADOS conformance on the cluster
ssh root@2a14:9b80::3143 'cd /root/dovetail && make e2e'           # daemon on librados, LMTP/JMAP/IMAP/POP3/export
```

`make integration` uses `/etc/ceph/ceph.conf`, user `dovetail` and pool `dovetail.index`
(override with `DOVETAIL_TEST_CEPH_CONF`, `DOVETAIL_TEST_CEPH_USER`, `DOVETAIL_TEST_CEPH_POOL`);
it works in its own RADOS namespaces and cleans up. `make e2e` runs `examples/lab.yaml` and
leaves a throwaway `e2e-<timestamp>.example` domain in the meta pool.

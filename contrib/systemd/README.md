# systemd units

`kittiwake@.service` is a template: the instance name is the mode it runs
(`lmtp`, `imap`, `pop3`, `jmap`, `admin`, `postfix`, `worker`). All instances read
`/etc/kittiwake/kittiwake.yaml`; `instances/<mode>.env` gives each one its own
management port (metrics and health) so they can share a host.

Install:

```
install -m 644 contrib/systemd/kittiwake@.service contrib/systemd/kittiwake.target /etc/systemd/system/
install -d /etc/kittiwake/instances && install -m 644 contrib/systemd/instances/*.env /etc/kittiwake/instances/
useradd --system --home /var/lib/kittiwake --shell /usr/sbin/nologin kittiwake
install -d -o kittiwake -g kittiwake /var/lib/kittiwake /var/lib/kittiwake/exports
chgrp kittiwake /etc/ceph/ceph.client.kittiwake.keyring && chmod 640 /etc/ceph/ceph.client.kittiwake.keyring
systemctl daemon-reload
systemctl enable --now kittiwake.target
```

`make deploy` builds the binary locally and copies it to `/usr/local/bin/kittiwake` on the
host `DEPLOY_ADDR` (default: the lab box), then restarts `kittiwake.target`. `make deploy-units` installs the
units, env files and (only if absent) the lab config.

Day-to-day administration goes over the admin listener (`[::1]:8081` in the lab) with
`kittiwake-admin`; `kittiwake admin bootstrap` is only for the first token.

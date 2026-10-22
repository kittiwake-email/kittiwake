# systemd units

`dovetail@.service` is a template: the instance name is the mode it runs
(`lmtp`, `imap`, `pop3`, `jmap`, `admin`, `postfix`, `worker`). All instances read
`/etc/dovetail/dovetail.yaml`; `instances/<mode>.env` gives each one its own
management port (metrics and health) so they can share a host.

Install:

```
install -m 644 contrib/systemd/dovetail@.service contrib/systemd/dovetail.target /etc/systemd/system/
install -d /etc/dovetail/instances && install -m 644 contrib/systemd/instances/*.env /etc/dovetail/instances/
useradd --system --home /var/lib/dovetail --shell /usr/sbin/nologin dovetail
install -d -o dovetail -g dovetail /var/lib/dovetail /var/lib/dovetail/exports
chgrp dovetail /etc/ceph/ceph.client.dovetail.keyring && chmod 640 /etc/ceph/ceph.client.dovetail.keyring
systemctl daemon-reload
systemctl enable --now dovetail.target
```

`make deploy` builds the binary locally and copies it to `/usr/local/bin/dovetail` on the
host `DEPLOY_ADDR` (default: the lab box), then restarts `dovetail.target`. `make deploy-units` installs the
units, env files and (only if absent) the lab config.

Day-to-day administration goes over the admin listener (`[::1]:8081` in the lab) with
`dovetail-admin`; `dovetail admin bootstrap` is only for the first token.

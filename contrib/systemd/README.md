# systemd units

`kittiwake@.service` is a template: one instance per mode (`lmtp`, `imap`, `pop3`, `jmap`,
`admin`, `socketmap`, `worker`). Every instance reads `/etc/kittiwake/kittiwake.yaml`; the
instance name is the mode it runs, and that mode's management (metrics, health) address
comes from `listen.<mode>.management` in the same file, so the YAML is the only source of
truth and a host runs exactly the instances that were enabled on it. `kittiwake.target`
groups the enabled instances.

```
install -m 644 contrib/systemd/kittiwake@.service contrib/systemd/kittiwake.target /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now kittiwake@imap kittiwake@jmap      # the modes this host runs
systemctl restart kittiwake.target                        # restart all of them
```

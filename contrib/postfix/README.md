# Postfix integration

1. Run Dovetail with `postfix` in `modes` and a `listen.postfix` address only
   reachable by the Postfix hosts (the socketmap protocol is unauthenticated).
2. Add the lines from `main.cf.snippet` to `/etc/postfix/main.cf`.
3. `postfix reload`.

Check lookups by hand:

```
postmap -q example.org socketmap:inet:[::1]:2525:domains
postmap -q wido@example.org socketmap:inet:[::1]:2525:mailboxes
```

The `mailboxes` map answers with the mailbox UUID(s) a recipient resolves to;
Postfix only cares that the key exists. Disabled domains, disabled aliases and
unknown users answer NOTFOUND.

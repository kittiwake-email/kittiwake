#!/usr/bin/env bash
# End-to-end test of a built dovetail binary against a real backend:
# bootstrap an admin, create domain/mailbox/account through the admin API,
# deliver over LMTP, read through JMAP, IMAP and POP3, export the mailbox.
#
#   scripts/e2e.sh [config]      (default examples/lab.yaml)
#   E2E_ATTACH=1 scripts/e2e.sh /etc/dovetail/dovetail.yaml
#       does not start a daemon but tests the already running services
#       (e.g. the systemd instances), which also proves that separate
#       processes see each other's writes through RADOS.
#
# Requires: bin/dovetail (make build), python3, curl. Leaves the created
# test domain in place (e2e-<timestamp>.example) so runs do not collide;
# delete it with the admin API when done.
set -euo pipefail
CFG=${1:-examples/lab.yaml}
BIN=${BIN:-bin/dovetail}
STAMP=$(date +%s)
DOMAIN="e2e-$STAMP.example"
USER="user@$DOMAIN"
PASS="e2e-password-$STAMP"
LOG=$(mktemp -t dovetail-e2e.XXXXXX.log)
LMTP=$(python3 -c "import yaml,sys;print(yaml.safe_load(open('$CFG'))['listen']['lmtp']['address'])" 2>/dev/null || echo "[::1]:2424")
JMAP=$(python3 -c "import yaml;print(yaml.safe_load(open('$CFG'))['listen']['jmap']['address'])" 2>/dev/null || echo "[::1]:8080")
ADMIN=$(python3 -c "import yaml;print(yaml.safe_load(open('$CFG'))['listen']['admin']['address'])" 2>/dev/null || echo "[::1]:8081")
IMAP=$(python3 -c "import yaml;print(yaml.safe_load(open('$CFG'))['listen']['imap']['address'])" 2>/dev/null || echo "[::1]:1143")
POP3=$(python3 -c "import yaml;print(yaml.safe_load(open('$CFG'))['listen']['pop3']['address'])" 2>/dev/null || echo "[::1]:1110")

echo "== config check"; $BIN check-config --config "$CFG"
echo "== mint an admin token (admins are tokens, not accounts)"
BOOT=$($BIN admin --config "$CFG" token --comment "e2e-$STAMP" --expires "$(date -u -d '+1 hour' +%Y-%m-%dT%H:%M:%SZ)" 2>&1) || { echo "$BOOT"; exit 1; }
TOKEN=$(echo "$BOOT" | awk '/token secret/ {print $3}')
[ -n "$TOKEN" ] || { echo "no token in: $BOOT"; exit 1; }

if [ -z "${E2E_ATTACH:-}" ]; then
  echo "== start daemon"
  $BIN serve --config "$CFG" >"$LOG" 2>&1 &
  PID=$!
  trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null; echo "daemon log: $LOG"' EXIT
else
  echo "== attaching to running services"
fi
for i in $(seq 1 50); do curl -s -o /dev/null "http://$ADMIN/" && break; sleep 0.2; done
curl -sf "http://${JMAP%:*}:9090/readyz" >/dev/null 2>&1 || true

api() { curl -sS -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" "$@"; }
echo "== create domain, mailbox and account via admin API"
api -X POST "http://$ADMIN/v1/admin/domains" -d "{\"name\":\"$DOMAIN\",\"enabled\":true}" >/dev/null
MB=$(api -X POST "http://$ADMIN/v1/admin/mailboxes" -d '{}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
api -X POST "http://$ADMIN/v1/admin/accounts" -d "{\"username\":\"$USER\",\"mailbox_id\":\"$MB\",\"roles\":[\"read\",\"write\",\"send\"],\"password\":\"$PASS\"}" >/dev/null
api -X POST "http://$ADMIN/v1/admin/aliases" -d "{\"address\":\"info@$DOMAIN\",\"enabled\":true,\"destinations\":[{\"mailbox_id\":\"$MB\"}]}" >/dev/null
echo "mailbox $MB, account $USER"

export LMTP JMAP ADMIN IMAP POP3 USER PASS DOMAIN MB TOKEN
python3 - <<'PY'
import os, smtplib, imaplib, poplib, json, base64, urllib.request, urllib.parse, urllib.error
def hp(s):
    h, p = s.rsplit(':', 1); return h.strip('[]'), int(p)
LMTP, JMAP, ADMIN, IMAP, POP3 = (hp(os.environ[k]) for k in ("LMTP", "JMAP", "ADMIN", "IMAP", "POP3"))
USER, PASS, DOMAIN, MB, TOKEN = (os.environ[k] for k in ("USER", "PASS", "DOMAIN", "MB", "TOKEN"))
jmapbase = "http://[%s]:%d" % JMAP; adminbase = "http://[%s]:%d" % ADMIN
def req(method, url, body=None, token=None, raw=False, form=False):
    h = {}; data = None
    if body is not None:
        if form: data = urllib.parse.urlencode(body).encode(); h["Content-Type"] = "application/x-www-form-urlencoded"
        else: data = json.dumps(body).encode(); h["Content-Type"] = "application/json"
    if token: h["Authorization"] = "Bearer " + token
    r = urllib.request.Request(url, data=data, method=method, headers=h)
    with urllib.request.urlopen(r) as resp:
        b = resp.read(); return resp.status, (b if raw else json.loads(b or b"null"))
pdf = b"%PDF-1.4 " + os.urandom(20000)
msg = (b"From: Alice <alice@example.com>\r\nTo: " + USER.encode() + b"\r\nSubject: e2e\r\nMessage-ID: <e2e@example.com>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=B\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\nhello ceph\r\n--B\r\nContent-Type: application/pdf; name=a.pdf\r\nContent-Transfer-Encoding: base64\r\nContent-Disposition: attachment; filename=a.pdf\r\n\r\n" + base64.encodebytes(pdf).replace(b"\n", b"\r\n") + b"--B--\r\n")
c = smtplib.LMTP(*LMTP); c.ehlo("e2e")
c.sendmail("alice@example.com", [USER], msg)
c.sendmail("alice@example.com", ["info@" + DOMAIN], b"From: a@b\r\nSubject: via alias\r\n\r\nx\r\n")
try:
    c.sendmail("alice@example.com", ["nobody@" + DOMAIN], msg); raise SystemExit("unknown recipient accepted")
except smtplib.SMTPRecipientsRefused as e:
    assert e.recipients["nobody@" + DOMAIN][0] == 550
c.quit(); print("LMTP: delivered 2, refused unknown")
st, tok = req("POST", jmapbase + "/v1/oauth/token", {"grant_type": "password", "username": USER, "password": PASS, "client_name": "e2e"}, form=True)
access = tok["access_token"]
st, sess = req("GET", jmapbase + "/.well-known/jmap", token=access)
acct = sess["primaryAccounts"]["urn:ietf:params:jmap:mail"]; assert acct == MB, (acct, MB)
def jmap(calls):
    return req("POST", jmapbase + "/jmap/api", {"using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"], "methodCalls": calls}, token=access)[1]["methodResponses"]
r = jmap([["Mailbox/get", {"accountId": acct, "ids": None}, "0"]])
inbox = [m for m in r[0][1]["list"] if m["role"] == "inbox"][0]
assert inbox["totalEmails"] == 2, inbox
r = jmap([["Email/query", {"accountId": acct, "filter": {"inMailbox": inbox["id"], "subject": "e2e"}}, "q"],
          ["Email/get", {"accountId": acct, "#ids": {"resultOf": "q", "name": "Email/query", "path": "/ids"}, "properties": ["subject", "attachments", "blobId"]}, "g"]])
em = r[1][1]["list"][0]; att = em["attachments"][0]
st, data = req("GET", jmapbase + "/jmap/download/%s/%s/a.pdf" % (acct, att["blobId"]), token=access, raw=True)
assert data == pdf, "attachment bytes differ"
st, raw = req("GET", jmapbase + "/jmap/download/%s/%s/raw.eml" % (acct, em["blobId"]), token=access, raw=True)
assert b"Message-ID: <e2e@example.com>" in raw and b"hello ceph" in raw and base64.encodebytes(pdf).replace(b"\n", b"\r\n") in raw, "rendered message lacks headers/body/attachment"
print("JMAP: session, query, get, attachment exact, message rendered with trace headers")
m = imaplib.IMAP4(*IMAP); m.login(USER, PASS); assert m.select("INBOX")[1][0] == b"2"
typ, d = m.uid("fetch", "1", "(BODY.PEEK[])"); assert d[0][1] == raw, "IMAP BODY[] differs from the JMAP rendering"
m.uid("store", "1", "+FLAGS", "(\\Seen)"); m.logout(); print("IMAP: select, fetch (same rendering as JMAP), store")
p = poplib.POP3(*POP3); p.user(USER); p.pass_(PASS); assert p.stat()[0] == 2; p.quit(); print("POP3: stat")
st, exp = req("POST", adminbase + "/v1/admin/mailboxes/%s/export" % MB, {"gzip": True}, token=TOKEN)
print("admin: export scheduled task", exp["task_id"][:8])
st, info = req("GET", adminbase + "/v1/admin/mailboxes/%s" % MB, token=TOKEN)
assert info["usage"]["messages"] == 2, info["usage"]
import time; time.sleep(2.5)  # audit records are batched and flushed every second
st, aud = req("GET", adminbase + "/v1/admin/audit?mailbox=%s&limit=200" % MB, token=TOKEN)
acts = sorted({a["action"] for a in aud["items"]}); print("audit actions:", acts)
assert "deliver" in acts and "login" in acts
PY
echo "== waiting for export task"
for i in $(seq 1 60); do N=$(api "http://$ADMIN/v1/admin/exports?mailbox=$MB" | python3 -c 'import sys,json;print(len(json.load(sys.stdin)["items"]))'); [ "$N" -ge 1 ] && break; sleep 1; done
api "http://$ADMIN/v1/admin/exports?mailbox=$MB" | python3 -c 'import sys,json;i=json.load(sys.stdin)["items"][0];print("export:",i["name"],i["bytes"],"bytes, messages",i["result"]["messages"])'
echo "== E2E OK (domain $DOMAIN left in place)"

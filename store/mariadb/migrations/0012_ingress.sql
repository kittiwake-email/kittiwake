-- Namespace ingress: the hostname proxies connect to for a namespace
-- whose servers they cannot reach directly (an HAProxy in the namespace's
-- datacenter terminating TLS), and the TLS policy for that hop ('' = the
-- proxy's configured default, else strict, insecure or off). Empty host:
-- proxies pick the namespace's nodes from the node registry.
ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS ingress_host VARCHAR(253) NOT NULL DEFAULT '' AFTER settings;
ALTER TABLE namespaces ADD COLUMN IF NOT EXISTS ingress_tls VARCHAR(16) NOT NULL DEFAULT '' AFTER ingress_host;
